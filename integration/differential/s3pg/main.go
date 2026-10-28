// Copyright 2026 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command s3pg is a differential test of the storage/s3pg driver (S3 + PostgreSQL) against
// the upstream storage/aws driver (S3 + MySQL) it is derived from.
//
// Both drivers are driven with the same deterministic workload, and the resulting buckets
// (every object's key, bytes, Content-Type and Cache-Control), signed checkpoints and
// coordination state are compared. See README.md.
package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	f_log "github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/migrate"
	awsstorage "github.com/transparency-dev/tessera/storage/aws"
	awsantispam "github.com/transparency-dev/tessera/storage/aws/antispam"
	"github.com/transparency-dev/tessera/storage/s3pg"
	s3pgantispam "github.com/transparency-dev/tessera/storage/s3pg/antispam"
	"golang.org/x/mod/sumdb/note"
	"golang.org/x/sync/errgroup"
)

const (
	nameA = "aws"  // upstream storage/aws: S3 + MySQL
	nameB = "s3pg" // storage/s3pg: S3 + PostgreSQL

	signerKey   = "PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
	verifierKey = "example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
	origin      = "example.com/log/testdata"
)

var (
	s3Endpoint    = flag.String("s3_endpoint", "http://127.0.0.1:9000", "S3-compatible endpoint (path-style, credentials test/test)")
	mysqlRoot     = flag.String("mysql_dsn", "root:root@tcp(127.0.0.1:3306)/", "MySQL DSN without database name; a database per log is created")
	pgDSN         = flag.String("pg_dsn", "postgres://postgres:postgres@127.0.0.1:5432/tessera_test?sslmode=disable", "PostgreSQL URL DSN; a schema per log is created")
	scale         = flag.String("scale", "full", "Workload: full (~131.6k entries, crosses 2*256*256) or small (~1.6k entries)")
	bucketPrefix  = flag.String("bucket_prefix", "", "Config.BucketPrefix for both drivers")
	scenarios     = flag.String("scenarios", "log,gc,antispam,migrate,retry", "Comma-separated scenarios to run (gc, antispam and migrate need log)")
	pgSchemaMode  = flag.String("pg_schema_mode", "search_path", "How s3pg stores select their schema: search_path (in the DSN) or qualified (Config.Schema / AntispamOpts.Schema; DSN search_path = decoy)")
	antispamBatch = flag.String("antispam_batches", "64,1000", "Comma-separated AntispamOpts.MaxBatchSize values; one follower pair per value")
	gcWait        = flag.Duration("gc_stall", 20*time.Second, "How long a driver's GC progress may stall before it is considered stuck")
	keep          = flag.Bool("keep", false, "Do not delete buckets, databases and schemas at the end")
	rerunTimeout  = flag.Duration("rerun_timeout", 6*time.Minute, "Timeout for the migration re-run")
)

var (
	signer     note.Signer
	verifier   note.Verifier
	failures   atomic.Int32
	knownDiffs atomic.Int32
)

func main() {
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	var err error
	if signer, err = note.NewSigner(signerKey); err != nil {
		fatalf("NewSigner: %v", err)
	}
	if verifier, err = note.NewVerifier(verifierKey); err != nil {
		fatalf("NewVerifier: %v", err)
	}
	if *pgSchemaMode != "search_path" && *pgSchemaMode != "qualified" {
		fatalf("--pg_schema_mode must be search_path or qualified")
	}
	slog.SetDefault(slog.New(&errorRecorder{Handler: slog.Default().Handler()}))
	ctx := context.Background()
	r, err := newResources(ctx, *s3Endpoint, *mysqlRoot, *pgDSN, *pgSchemaMode == "qualified")
	if err != nil {
		fatalf("resources: %v", err)
	}
	if !*keep {
		defer r.cleanup(context.Background())
	}
	report("run %s, scale=%s, bucket_prefix=%q, pg_schema_mode=%s", r.runID, *scale, *bucketPrefix, *pgSchemaMode)
	if err := run(ctx, r); err != nil {
		failures.Add(1)
		report("ERROR: %v", err)
	}
	if err := r.checkDecoy(ctx); err != nil {
		failures.Add(1)
		report("ERROR: %v", err)
	}
	if n := failures.Load(); n > 0 {
		report("RESULT: %d difference(s)/failure(s)", n)
		if !*keep {
			r.cleanup(context.Background())
		}
		os.Exit(1)
	}
	report("RESULT: no unexplained differences (%d known, explained difference(s))", knownDiffs.Load())
}

func run(ctx context.Context, r *resources) error {
	want := map[string]bool{}
	for _, s := range strings.Split(*scenarios, ",") {
		want[strings.TrimSpace(s)] = true
	}
	if want["log"] {
		if err := runLog(ctx, r, want); err != nil {
			return err
		}
	}
	if want["retry"] {
		return scenarioRetry(ctx, r)
	}
	return nil
}

// runLog runs the log scenario, and the scenarios which use its log (gc, antispam, migrate).
func runLog(ctx context.Context, r *resources, want map[string]bool) error {
	p := fullPlan()
	if *scale == "small" {
		p = smallPlan()
	}

	// Scenario 1: the log itself.
	a, b, err := newLogPair(ctx, r, "log")
	if err != nil {
		return err
	}
	for pi, ph := range p.phases {
		start := time.Now()
		eg := errgroup.Group{}
		for _, s := range []*side{a, b} {
			eg.Go(func() error { return runPhase(ctx, s, p, pi, 0) })
		}
		if err := eg.Wait(); err != nil {
			return fmt.Errorf("phase %s: %v", ph.name, err)
		}
		size := p.starts[pi] + uint64(ph.batch*ph.steps)
		report("phase %-12s batch=%-5d steps=%-3d size=%-6d (%v; slowest sequencing of a step: %s %v, %s %v)", ph.name, ph.batch, ph.steps, size,
			time.Since(start).Round(time.Millisecond), nameA, a.maxSeq.Round(time.Millisecond), nameB, b.maxSeq.Round(time.Millisecond))
		if err := compareLogs(ctx, a, b, size); err != nil {
			return err
		}
	}
	pre, err := a.br.snapshot(ctx)
	if err != nil {
		return err
	}

	// The remaining scenarios run against live (idle) Appenders, which provide the LogReaders.
	gcInterval := time.Duration(0)
	if want["gc"] {
		gcInterval = 250 * time.Millisecond
	}
	lrA, stopA, err := startIdle(ctx, a, gcInterval)
	if err != nil {
		return err
	}
	defer stopA()
	lrB, stopB, err := startIdle(ctx, b, gcInterval)
	if err != nil {
		return err
	}
	defer stopB()

	if want["gc"] {
		if err := scenarioGC(ctx, a, b, pre, p.size); err != nil {
			return err
		}
	}
	if err := compareReaders(ctx, lrA, lrB, p.size, want["gc"]); err != nil {
		return err
	}
	if want["antispam"] {
		for _, mb := range strings.Split(*antispamBatch, ",") {
			var n uint
			if _, err := fmt.Sscan(mb, &n); err != nil {
				return err
			}
			if err := scenarioAntispam(ctx, r, p, lrA, n); err != nil {
				return err
			}
		}
	}
	if want["migrate"] {
		if err := scenarioMigrate(ctx, r, a, lrA); err != nil {
			return err
		}
	}
	return nil
}

func fatalf(f string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+f+"\n", args...)
	os.Exit(2)
}

func report(f string, args ...any) {
	fmt.Printf("[%s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(f, args...))
}

func check(ok bool, f string, args ...any) {
	if ok {
		report("  ok   "+f, args...)
		return
	}
	failures.Add(1)
	report("  DIFF "+f, args...)
}

// side is one of the two drivers under comparison, together with its bucket and coordination DB.
type side struct {
	name      string
	bucket    string
	insp      *sql.DB // handle for inspecting the coordination tables
	br        *bucketReader
	newDriver func(ctx context.Context) (tessera.Driver, func(), error)
	maxSeq    time.Duration // longest time to sequence one step's entries in the last runPhase
}

// newLogPair creates an upstream and an s3pg log, each with a fresh bucket and database, both
// using --bucket_prefix.
func newLogPair(ctx context.Context, r *resources, tag string) (*side, *side, error) {
	return newLogPairPrefix(ctx, r, tag, *bucketPrefix, *bucketPrefix)
}

// newLogPairPrefix is newLogPair with explicit bucket prefixes for the upstream and s3pg logs.
func newLogPairPrefix(ctx context.Context, r *resources, tag, prefixA, prefixB string) (*side, *side, error) {
	bucketA, err := r.bucket(ctx, tag+"-"+nameA)
	if err != nil {
		return nil, nil, err
	}
	dsnA, inspA, err := r.mysqlDB(ctx, tag)
	if err != nil {
		return nil, nil, err
	}
	bucketB, err := r.bucket(ctx, tag+"-"+nameB)
	if err != nil {
		return nil, nil, err
	}
	dsnB, schemaB, inspB, err := r.pgSchema(ctx, tag)
	if err != nil {
		return nil, nil, err
	}
	return newSide(r, nameA, bucketA, prefixA, dsnA, "", inspA), newSide(r, nameB, bucketB, prefixB, dsnB, schemaB, inspB), nil
}

// newSide returns a side for the named driver, with the given bucket (and prefix) and coordination
// database. pgSchema is the s3pg Config.Schema ("" to use the DSN's search_path).
func newSide(r *resources, name, bucket, prefix, dsn, pgSchema string, insp *sql.DB) *side {
	s := &side{name: name, bucket: bucket, insp: insp, br: newBucketReader(r.s3c, bucket, prefix)}
	switch name {
	case nameA:
		s.newDriver = func(ctx context.Context) (tessera.Driver, func(), error) {
			d, err := awsstorage.New(ctx, awsstorage.Config{
				SDKConfig: r.sdkCfg, S3Options: r.s3Opts, Bucket: bucket, BucketPrefix: prefix,
				DSN: dsn, MaxIdleConns: 2,
			})
			return d, func() {}, err
		}
	case nameB:
		s.newDriver = func(ctx context.Context) (tessera.Driver, func(), error) {
			d, err := s3pg.New(ctx, s3pg.Config{
				SDKConfig: r.sdkCfg, S3Options: r.s3Opts, Bucket: bucket, BucketPrefix: prefix,
				DSN: dsn, Schema: pgSchema, MaxIdleConns: 2,
			})
			if err != nil {
				return nil, nil, err
			}
			return d, func() { _ = d.(*s3pg.Storage).Close() }, nil
		}
	}
	return s
}

// compareLogs compares the buckets, checkpoints and coordination state of two logs which are
// expected to have published a checkpoint of the given size.
func compareLogs(ctx context.Context, a, b *side, size uint64) error {
	sa, err := a.br.snapshot(ctx)
	if err != nil {
		return err
	}
	sb, err := b.br.snapshot(ctx)
	if err != nil {
		return err
	}
	d := diffSnapshots(sa, sb)
	check(d.identical(), "objects: %v", d)

	cpA, cpB := sa[layout.CheckpointPath].data, sb[layout.CheckpointPath].data
	textEq, sigEq, desc := compareCheckpoints(cpA, cpB)
	check(textEq && sigEq, "checkpoint: text identical=%v, signature identical=%v; text=%s", textEq, sigEq, desc)
	var roots [2][]byte
	for i, cp := range [][]byte{cpA, cpB} {
		c, _, _, err := f_log.ParseCheckpoint(cp, origin, verifier)
		if err != nil {
			check(false, "%s checkpoint does not verify: %v", []string{nameA, nameB}[i], err)
			continue
		}
		roots[i] = c.Hash
		check(c.Size == size, "%s checkpoint verifies, size %d (want %d)", []string{nameA, nameB}[i], c.Size, size)
	}

	ca, errA := readCoord(ctx, a.insp)
	cb, errB := readCoord(ctx, b.insp)
	if err := errors.Join(errA, errB); err != nil {
		return err
	}
	check(reflect.DeepEqual(ca, cb), "coordination state: %s=[%v] %s=[%v]", nameA, ca, nameB, cb)
	check(ca.IntSize == int64(size) && ca.Next == int64(size) && ca.SeqRows == 0 && slices.Equal(ca.IntRoot, roots[0]),
		"coordination state matches checkpoint (IntCoord.seq = SeqCoord.next = %d, root, empty Seq)", size)
	return nil
}

// startIdle starts an Appender which adds nothing, for its LogReader and background jobs (GC).
func startIdle(ctx context.Context, s *side, gcInterval time.Duration) (tessera.LogReader, func(), error) {
	actx, cancel := context.WithCancel(ctx)
	drv, closeFn, err := s.newDriver(actx)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	_, shutdown, lr, err := tessera.NewAppender(actx, drv, appendOptions(1, gcInterval))
	if err != nil {
		cancel()
		closeFn()
		return nil, nil, err
	}
	return lr, func() {
		_ = shutdown(ctx)
		cancel()
		time.Sleep(100 * time.Millisecond)
		closeFn()
	}, nil
}

// scenarioGC waits for garbage collection to finish (or stall) on both logs, then compares the
// remaining objects with each other and with a model of upstream's GC semantics.
func scenarioGC(ctx context.Context, a, b *side, pre map[string]object, size uint64) error {
	report("scenario gc: GC interval 250ms, published size %d", size)
	target := int64(size / layout.EntryBundleWidth * layout.EntryBundleWidth)
	start := time.Now()
	var ca, cb coordState
	lastChange := [2]time.Time{start, start}
	last := [2]int64{-1, -1}
	for {
		var errA, errB error
		ca, errA = readCoord(ctx, a.insp)
		cb, errB = readCoord(ctx, b.insp)
		if err := errors.Join(errA, errB); err != nil {
			return err
		}
		for i, v := range []int64{ca.GCFrom, cb.GCFrom} {
			if v != last[i] {
				last[i], lastChange[i] = v, time.Now()
			}
		}
		doneA := ca.GCFrom == target || time.Since(lastChange[0]) > *gcWait
		doneB := cb.GCFrom == target || time.Since(lastChange[1]) > *gcWait
		if doneA && doneB {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Let any in-flight GC run finish.
	time.Sleep(time.Second)
	report("  GCCoord.fromSize: %s=%d %s=%d (target %d) after %v", nameA, ca.GCFrom, nameB, cb.GCFrom, target, time.Since(start).Round(time.Second))
	sa, err := a.br.snapshot(ctx)
	if err != nil {
		return err
	}
	sb, err := b.br.snapshot(ctx)
	if err != nil {
		return err
	}
	full := gcModel(pre, uint64(target))
	db := diffSnapshots(full, sb)
	check(db.identical() && cb.GCFrom == target, "%s: GC complete (fromSize %d), remaining objects == model of complete GC (%d of %d pre-GC objects removed): %v",
		nameB, cb.GCFrom, len(pre)-len(full), len(pre), db)
	// A failed upstream GC run does not advance GCCoord.fromSize, so the next run retries from the
	// same place and deletes the partial objects of the same (up to) 101 bundles again.
	reach := min(uint64(ca.GCFrom)+101*layout.EntryBundleWidth, uint64(target))
	da := diffSnapshots(gcModel(pre, reach), sa)
	check(da.identical(), "%s: GCCoord.fromSize %d, remaining objects == model of GC reaching bundle %d: %v", nameA, ca.GCFrom, reach/layout.EntryBundleWidth, da)

	d := diffSnapshots(sa, sb)
	if d.identical() && ca.GCFrom == cb.GCFrom {
		check(true, "post-GC objects and GCCoord identical: %v", d)
		return nil
	}
	if ca.GCFrom < target && cb.GCFrom == target && da.identical() && db.identical() {
		known("post-GC state differs: GCCoord.fromSize %s=%d %s=%d; objects: %v. Upstream GC run fails (see stderr: DeleteObjects MalformedXML for "+
			"a prefix without objects) and never advances past fromSize=%d; s3pg skips empty prefixes", nameA, ca.GCFrom, nameB, cb.GCFrom, d, ca.GCFrom)
		return nil
	}
	check(false, "post-GC objects: %v; GCCoord.fromSize %s=%d %s=%d", d, nameA, ca.GCFrom, nameB, cb.GCFrom)
	return nil
}

// compareReaders compares what the two drivers' LogReaders return for the same requests.
func compareReaders(ctx context.Context, lrA, lrB tessera.LogReader, size uint64, gcDone bool) error {
	report("scenario logreader: LogReader results for the same requests")
	type req struct {
		desc string
		f    func(tessera.LogReader) ([]byte, error)
	}
	nFull := size / layout.EntryBundleWidth
	last := uint8(size % layout.EntryBundleWidth)
	reqs := []req{
		{"ReadCheckpoint", func(lr tessera.LogReader) ([]byte, error) { return lr.ReadCheckpoint(ctx) }},
		{"IntegratedSize", func(lr tessera.LogReader) ([]byte, error) {
			n, err := lr.IntegratedSize(ctx)
			return fmt.Append(nil, n), err
		}},
		{"NextIndex", func(lr tessera.LogReader) ([]byte, error) {
			n, err := lr.NextIndex(ctx)
			return fmt.Append(nil, n), err
		}},
	}
	for _, i := range []uint64{0, 1, nFull / 2, nFull - 1} {
		reqs = append(reqs,
			req{fmt.Sprintf("ReadEntryBundle(%d, 0)", i), func(lr tessera.LogReader) ([]byte, error) { return lr.ReadEntryBundle(ctx, i, 0) }},
			req{fmt.Sprintf("ReadTile(0, %d, 0)", i), func(lr tessera.LogReader) ([]byte, error) { return lr.ReadTile(ctx, 0, i, 0) }})
	}
	reqs = append(reqs,
		req{fmt.Sprintf("ReadEntryBundle(%d, %d)", nFull, last), func(lr tessera.LogReader) ([]byte, error) { return lr.ReadEntryBundle(ctx, nFull, last) }},
		req{fmt.Sprintf("ReadTile(0, %d, %d)", nFull, last), func(lr tessera.LogReader) ([]byte, error) { return lr.ReadTile(ctx, 0, nFull, last) }},
		req{fmt.Sprintf("ReadEntryBundle(%d, 0) (does not exist)", nFull+1), func(lr tessera.LogReader) ([]byte, error) { return lr.ReadEntryBundle(ctx, nFull+1, 0) }},
	)
	ok := 0
	for _, r := range reqs {
		dA, errA := r.f(lrA)
		dB, errB := r.f(lrB)
		if (errA == nil) == (errB == nil) && string(dA) == string(dB) {
			ok++
			continue
		}
		check(false, "%s: %s=(%d bytes, %v) %s=(%d bytes, %v)", r.desc, nameA, len(dA), errA, nameB, len(dB), errB)
	}
	check(ok == len(reqs), "%d/%d LogReader requests returned identical results (incl. errors)", ok, len(reqs))

	// A partial resource whose full version exists. After GC the partial object is gone.
	full, err := lrB.ReadEntryBundle(ctx, 0, 0)
	if err != nil {
		return err
	}
	dA, errA := lrA.ReadEntryBundle(ctx, 0, 1)
	dB, errB := lrB.ReadEntryBundle(ctx, 0, 1)
	switch {
	case (errA == nil) == (errB == nil) && string(dA) == string(dB):
		check(true, "ReadEntryBundle(0, 1) (partial; GC done=%v): identical (%d bytes, %v)", gcDone, len(dA), errA)
	case gcDone && errA != nil && errB == nil && string(dB) == string(full) && !errors.Is(errA, os.ErrNotExist):
		known("ReadEntryBundle(0, 1) after the partial was garbage collected: %s returns an error (%v), %s falls back to the full bundle "+
			"(%d bytes): upstream does not map S3 NoSuchKey to os.ErrNotExist, so fetcher.PartialOrFullResource cannot fall back", nameA, errA, nameB, len(dB))
	default:
		check(false, "ReadEntryBundle(0, 1): %s=(%d bytes, %v) %s=(%d bytes, %v)", nameA, len(dA), errA, nameB, len(dB), errB)
	}
	return nil
}

// known reports a difference which is explained by an intentional deviation of s3pg from upstream.
func known(f string, args ...any) {
	knownDiffs.Add(1)
	report("  KNOWN "+f, args...)
}

// idHasher is a copy of tessera's default antispam identity hasher for tlog-tiles bundles.
func idHasher(bundle []byte) ([][]byte, error) {
	eb := &api.EntryBundle{}
	if err := eb.UnmarshalText(bundle); err != nil {
		return nil, fmt.Errorf("unmarshal: %v", err)
	}
	r := make([][]byte, 0, len(eb.Entries))
	for _, e := range eb.Entries {
		h := sha256.Sum256(e)
		r = append(r, h[:])
	}
	return r, nil
}

type antispamRow struct {
	h   []byte
	idx uint64
}

func dumpAntispam(ctx context.Context, db *sql.DB) ([]antispamRow, uint64, error) {
	rows, err := db.QueryContext(ctx, "SELECT h, idx FROM AntispamIDSeq ORDER BY idx")
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rows.Close() }()
	var res []antispamRow
	for rows.Next() {
		var r antispamRow
		if err := rows.Scan(&r.h, &r.idx); err != nil {
			return nil, 0, err
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var next uint64
	err = db.QueryRowContext(ctx, "SELECT nextIdx FROM AntispamFollowCoord WHERE id = 0").Scan(&next)
	return res, next, err
}

// scenarioAntispam runs the upstream and s3pg antispam followers over the same log, then compares
// their complete hash -> index tables, and lookups through their Decorators.
func scenarioAntispam(ctx context.Context, r *resources, p *plan, lr tessera.LogReader, maxBatch uint) error {
	report("scenario antispam: both followers follow the %s log (size %d), MaxBatchSize=%d", nameA, p.size, maxBatch)
	tag := fmt.Sprintf("as%d", maxBatch)
	dsnA, inspA, err := r.mysqlDB(ctx, tag)
	if err != nil {
		return err
	}
	dsnB, schemaB, inspB, err := r.pgSchema(ctx, tag)
	if err != nil {
		return err
	}
	asA, err := awsantispam.NewAntispam(ctx, dsnA, awsantispam.AntispamOpts{MaxBatchSize: maxBatch, MaxIdleConns: 2})
	if err != nil {
		return err
	}
	asB, err := s3pgantispam.NewAntispam(ctx, dsnB, s3pgantispam.AntispamOpts{MaxBatchSize: maxBatch, MaxIdleConns: 2, Schema: schemaB})
	if err != nil {
		return err
	}
	defer func() { _ = asB.Close() }()
	fA, fB := asA.Follower(idHasher), asB.Follower(idHasher)
	fctx, cancel := context.WithCancel(ctx)
	start := time.Now()
	go fA.Follow(fctx, lr)
	go fB.Follow(fctx, lr)
	for {
		nA, errA := fA.EntriesProcessed(ctx)
		nB, errB := fB.EntriesProcessed(ctx)
		if errA == nil && errB == nil && nA >= p.size && nB >= p.size {
			break
		}
		if time.Since(start) > 15*time.Minute {
			cancel()
			return fmt.Errorf("followers did not catch up: %d %d %v %v", nA, nB, errA, errB)
		}
		time.Sleep(200 * time.Millisecond)
	}
	cancel()
	time.Sleep(200 * time.Millisecond)
	report("  followers caught up in %v", time.Since(start).Round(time.Millisecond))

	rowsA, nextA, errA := dumpAntispam(ctx, inspA)
	rowsB, nextB, errB := dumpAntispam(ctx, inspB)
	if err := errors.Join(errA, errB); err != nil {
		return err
	}
	check(nextA == p.size && nextB == p.size, "AntispamFollowCoord.nextIdx %s=%d %s=%d (log size %d)", nameA, nextA, nameB, nextB, p.size)
	same := len(rowsA) == len(rowsB)
	for i := 0; same && i < len(rowsA); i++ {
		same = rowsA[i].idx == rowsB[i].idx && string(rowsA[i].h) == string(rowsB[i].h)
	}
	check(same, "AntispamIDSeq tables identical: %s has %d rows, %s has %d rows", nameA, len(rowsA), nameB, len(rowsB))
	first := p.firstIndex()
	modelOK := len(rowsB) == len(first)
	for _, row := range rowsB {
		if len(row.h) != 32 || first[[32]byte(row.h)] != row.idx {
			modelOK = false
			break
		}
	}
	check(modelOK, "AntispamIDSeq == model (first index of each of the %d distinct identities among %d entries)", len(first), p.size)

	// Lookups via the Decorators: every entry whose content is a duplicate, plus a random sample
	// of the rest, plus contents which were never added.
	errNotFound := errors.New("not found")
	notFound := func(context.Context, *tessera.Entry) tessera.IndexFuture {
		return func() (tessera.Index, error) { return tessera.Index{}, errNotFound }
	}
	addA, addB := asA.Decorator()(notFound), asB.Decorator()(notFound)
	rng := rand.New(rand.NewPCG(1, 2))
	var probes [][]byte
	dups, dupProbes := 0, 0
	for i := uint64(0); i < p.size; i++ {
		c := p.contentAt(i)
		if first[sha256.Sum256(c)] != i {
			dups++
			if dups%7 == 0 || i < 5000 { // all early duplicates, and a sample of the rest
				probes = append(probes, c)
				dupProbes++
			}
		} else if rng.IntN(100) == 0 {
			probes = append(probes, c)
		}
	}
	for i := range 50 {
		probes = append(probes, fmt.Appendf(nil, "absent-%d", i))
	}
	mismatch := 0
	for _, c := range probes {
		want, wantFound := first[sha256.Sum256(c)]
		var got [2]string
		for j, add := range []tessera.AddFn{addA, addB} {
			idx, err := add(ctx, tessera.NewEntry(c))()
			switch {
			case errors.Is(err, errNotFound):
				got[j] = "absent"
			case err != nil:
				got[j] = "error: " + err.Error()
			default:
				got[j] = fmt.Sprintf("%d dup=%v", idx.Index, idx.IsDup)
			}
		}
		exp := "absent"
		if wantFound {
			exp = fmt.Sprintf("%d dup=true", want)
		}
		if got[0] != exp || got[1] != exp {
			mismatch++
			if mismatch < 5 {
				report("  lookup %x: %s=%s %s=%s want %s", sha256.Sum256(c), nameA, got[0], nameB, got[1], exp)
			}
		}
	}
	check(mismatch == 0, "Decorator lookups: %d probes (%d of the %d duplicate entries, 50 never added), %d mismatches vs model", len(probes), dupProbes, dups, mismatch)
	return nil
}

// scenarioMigrate migrates the upstream log into a fresh upstream target and a fresh s3pg target,
// and compares the targets.
func scenarioMigrate(ctx context.Context, r *resources, srcSide *side, srcA tessera.LogReader) error {
	cpRaw, err := srcA.ReadCheckpoint(ctx)
	if err != nil {
		return err
	}
	cp, _, _, err := f_log.ParseCheckpoint(cpRaw, origin, verifier)
	if err != nil {
		return err
	}
	report("scenario migrate: source = %s log, size %d", nameA, cp.Size)
	a, b, err := newLogPair(ctx, r, "mig")
	if err != nil {
		return err
	}
	start := time.Now()
	errs := migrateBoth(ctx, cp, srcA, a, b)
	if err := errors.Join(errs[0], errs[1]); err != nil {
		return err
	}
	report("  both migrations completed (root matches source) in %v", time.Since(start).Round(time.Millisecond))
	sa, err := a.br.snapshot(ctx)
	if err != nil {
		return err
	}
	sb, err := b.br.snapshot(ctx)
	if err != nil {
		return err
	}
	d := diffSnapshots(sa, sb)
	check(d.identical(), "migration targets: %v", d)
	ca, errA := readCoord(ctx, a.insp)
	cb, errB := readCoord(ctx, b.insp)
	if err := errors.Join(errA, errB); err != nil {
		return err
	}
	check(reflect.DeepEqual(ca, cb), "migration coordination state: %s=[%v] %s=[%v]", nameA, ca, nameB, cb)
	check(ca.IntSize == int64(cp.Size) && slices.Equal(ca.IntRoot, cp.Hash), "migration IntCoord == source checkpoint (%d, %x)", cp.Size, cp.Hash)

	// Cross-check against the source: every migrated object also present in the source is identical.
	src, err := srcSide.br.snapshot(ctx)
	if err != nil {
		return err
	}
	inSrc, same := 0, 0
	var notInSrc []string
	for k, o := range sa {
		so, ok := src[k]
		if !ok {
			notInSrc = append(notInSrc, k)
			continue
		}
		inSrc++
		if string(so.data) == string(o.data) {
			same++
		}
	}
	slices.Sort(notInSrc)
	check(inSrc == same, "migration target vs source: %d objects also in source, %d identical; %d not in source: %v",
		inSrc, same, len(notInSrc), trunc(notInSrc))
	if err := migrateLogReader(ctx, cp.Size, a, b); err != nil {
		return err
	}

	// Re-run the migration into the same (complete) buckets with fresh databases: every bundle and
	// tile write now hits an existing object and must be accepted as an idempotent re-write.
	report("scenario migrate-rerun: migrate again into the same buckets, with fresh databases")
	dsnA, inspA, err := r.mysqlDB(ctx, "mig2")
	if err != nil {
		return err
	}
	dsnB, schemaB, inspB, err := r.pgSchema(ctx, "mig2")
	if err != nil {
		return err
	}
	a2, b2 := newSide(r, nameA, a.bucket, *bucketPrefix, dsnA, "", inspA), newSide(r, nameB, b.bucket, *bucketPrefix, dsnB, schemaB, inspB)
	start = time.Now()
	rctx, cancel := context.WithTimeout(ctx, *rerunTimeout)
	defer cancel()
	errs = migrateBoth(rctx, cp, srcA, a2, b2)
	report("  re-run results after %v: %s: %v; %s: %v", time.Since(start).Round(time.Millisecond), nameA, errs[0], nameB, errs[1])
	sa2, err := a.br.snapshot(ctx)
	if err != nil {
		return err
	}
	sb2, err := b.br.snapshot(ctx)
	if err != nil {
		return err
	}
	da, db := diffSnapshots(sa, sa2), diffSnapshots(sb, sb2)
	ca2, errA := readCoord(ctx, inspA)
	cb2, errB := readCoord(ctx, inspB)
	if err := errors.Join(errA, errB); err != nil {
		return err
	}
	check(errs[1] == nil && db.identical() && reflect.DeepEqual(cb2, cb), "%s re-run succeeded, bucket unchanged (%v), coordination state == first run", nameB, db)
	switch {
	case errs[0] == nil && da.identical() && reflect.DeepEqual(ca2, ca):
		check(true, "%s re-run succeeded, bucket unchanged (%v), coordination state == first run", nameA, da)
	case errs[0] != nil && *bucketPrefix != "" && errs[1] == nil && da.identical():
		known("%s re-run failed with BucketPrefix=%q (%v; IntCoord=%d), %s succeeded: upstream setObjectIfNoneMatch re-applies the bucket prefix when "+
			"reading back an existing object after PreconditionFailed, so idempotent re-writes fail; fixed in s3pg", nameA, *bucketPrefix, errs[0], ca2.IntSize, nameB)
	default:
		check(false, "%s re-run: err=%v, bucket unchanged=%v, coordination state=[%v]", nameA, errs[0], da.identical(), ca2)
	}
	return nil
}

// migrateBoth migrates the source log into both sides concurrently.
func migrateBoth(ctx context.Context, cp *f_log.Checkpoint, src tessera.LogReader, sides ...*side) []error {
	errs := make([]error, len(sides))
	eg := errgroup.Group{}
	for i, s := range sides {
		eg.Go(func() error {
			drv, closeFn, err := s.newDriver(ctx)
			if err != nil {
				errs[i] = err
				return nil
			}
			defer closeFn()
			mt, err := tessera.NewMigrationTarget(ctx, drv, tessera.NewMigrationOptions())
			if err != nil {
				errs[i] = err
				return nil
			}
			if err := mt.Migrate(ctx, 8, cp.Size, cp.Hash, src.ReadEntryBundle); err != nil {
				errs[i] = fmt.Errorf("%s: %v", s.name, err)
			}
			return nil
		})
	}
	_ = eg.Wait()
	return errs
}

// migrateLogReader checks the MigrationWriter's LogReader, whose IntegratedSize is called by the
// antispam follower which MigrationOptions.WithAntispam installs. size is the migrated tree size.
func migrateLogReader(ctx context.Context, size uint64, sides ...*side) error {
	type mwLifecycle interface {
		MigrationWriter(context.Context, *tessera.MigrationOptions) (migrate.MigrationWriter, tessera.LogReader, error)
	}
	type result struct {
		desc     string
		panicked bool
		size     uint64
		err      error
	}
	var res []result
	for _, s := range sides {
		drv, closeFn, err := s.newDriver(ctx)
		if err != nil {
			return err
		}
		_, lr, err := drv.(mwLifecycle).MigrationWriter(ctx, tessera.NewMigrationOptions())
		if err != nil {
			closeFn()
			return err
		}
		func() {
			defer func() {
				if p := recover(); p != nil {
					res = append(res, result{desc: fmt.Sprintf("%s: panic: %v", s.name, p), panicked: true})
				}
			}()
			sz, err := lr.IntegratedSize(ctx)
			res = append(res, result{desc: fmt.Sprintf("%s: %d, %v", s.name, sz, err), size: sz, err: err})
		}()
		closeFn()
	}
	desc := res[0].desc + "; " + res[1].desc
	switch a, b := res[0], res[1]; {
	case a.panicked == b.panicked && a.size == b.size && (a.err == nil) == (b.err == nil):
		check(true, "same behaviour of MigrationWriter's LogReader.IntegratedSize (called by MigrationOptions.WithAntispam followers): %s", desc)
	case a.panicked && !b.panicked && b.err == nil && b.size == size:
		known("MigrationWriter's LogReader.IntegratedSize (called by MigrationOptions.WithAntispam followers): %s. Upstream leaves the "+
			"LogReader's integratedSize/nextIndex funcs nil; s3pg wires them to the sequencer and returns the migrated size %d", desc, size)
	default:
		check(false, "MigrationWriter's LogReader.IntegratedSize (migrated size %d): %s", size, desc)
	}
	return nil
}
