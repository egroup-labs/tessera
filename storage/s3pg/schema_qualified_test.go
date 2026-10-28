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

package s3pg

// Tests for Config.Schema (and antispam's AntispamOpts.Schema): every statement must name the
// configured schema explicitly. The connections' search_path points at a DECOY schema holding a
// full copy of the tables with sentinel values (or at no schema at all), so that any statement
// which is not schema-qualified either modifies the decoy or reads sentinels and visibly
// misbehaves. The schema names need quoting (mixed case, space, double quote, hyphen).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	storage "github.com/transparency-dev/tessera/storage/internal"
	"github.com/transparency-dev/tessera/storage/s3pg/antispam"
)

// exerciseReplicas runs initialisation, sequencing, integration, publication and garbage
// collection concurrently on several sequencer replicas (separate pools) of the log selected by
// cfg, and checks the resulting state of that log, reading schema (qualified) through db.
func exerciseReplicas(t *testing.T, ctx context.Context, db *sql.DB, cfg Config, schema string) {
	t.Helper()
	const replicas = 3
	seqs := make([]*pgSequencer, replicas)
	errs := make([]error, replicas)
	var wg sync.WaitGroup
	for i := range replicas {
		c := cfg
		c.MaxOpenConns = 4
		wg.Go(func() { seqs[i], errs[i] = newPGSequencer(ctx, c, 1000) })
	}
	wg.Wait()
	for i := range replicas {
		if errs[i] != nil {
			t.Fatalf("concurrent newPGSequencer[%d]: %v", i, errs[i])
		}
		t.Cleanup(func() { _ = seqs[i].close() })
	}
	if got := tablesIn(t, db, schema); !slices.Equal(got, wantTables) {
		t.Fatalf("tables in %q = %v, want %v", schema, got, wantTables)
	}
	for i, s := range seqs {
		if n, err := s.nextIndex(ctx); err != nil || n != 0 {
			t.Errorf("replica %d: nextIndex = (%d, %v), want (0, nil)", i, n, err)
		}
		if size, root, err := s.currentTree(ctx); err != nil || size != 0 || !bytes.Equal(root, rfc6962.DefaultHasher.EmptyRoot()) {
			t.Errorf("replica %d: currentTree = (%d, %x, %v), want empty tree", i, size, root, err)
		}
	}

	// Concurrent sequencing.
	const batches, batchSize = 4, 5
	const total = replicas * batches * batchSize
	var mu sync.Mutex
	byIndex := map[uint64][]byte{}
	for r := range replicas {
		wg.Go(func() {
			for b := range batches {
				entries := make([]*tessera.Entry, batchSize)
				for i := range entries {
					entries[i] = tessera.NewEntry(fmt.Appendf(nil, "replica %d batch %d entry %d", r, b, i))
				}
				if err := seqs[r].assignEntries(ctx, entries); err != nil {
					t.Errorf("replica %d: assignEntries: %v", r, err)
					return
				}
				mu.Lock()
				for i, e := range entries {
					byIndex[*e.Index()] = fmt.Appendf(nil, "replica %d batch %d entry %d", r, b, i)
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if len(byIndex) != total {
		t.Fatalf("%d distinct indices assigned, want %d", len(byIndex), total)
	}

	// Concurrent integration (2 Seq rows at a time) on every replica.
	var integrated uint64
	var got []storage.SequencedEntry
	f := func(_ context.Context, from uint64, es []storage.SequencedEntry) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		if from != integrated {
			return nil, fmt.Errorf("consumeFunc called from %d, want %d", from, integrated)
		}
		got = append(got, es...)
		integrated += uint64(len(es))
		return fakeRoot(integrated), nil
	}
	for r := range replicas {
		wg.Go(func() {
			for {
				mu.Lock()
				done := integrated == total
				mu.Unlock()
				if done || ctx.Err() != nil {
					return
				}
				if _, err := seqs[r].consumeEntries(ctx, 2, f, false); err != nil {
					t.Errorf("replica %d: consumeEntries: %v", r, err)
					return
				}
			}
		})
	}
	wg.Wait()
	if len(got) != total {
		t.Fatalf("integrated %d entries, want %d", len(got), total)
	}
	for i, e := range got {
		if want := wantSequenced(byIndex[uint64(i)], uint64(i)); !bytes.Equal(e.BundleData, want.BundleData) || !bytes.Equal(e.LeafHash, want.LeafHash) {
			t.Fatalf("integrated entry %d differs from the entry assigned that index", i)
		}
	}

	// Concurrent publication: exactly one replica publishes the integrated tree.
	var pubs []string
	before := time.Now()
	for r := range replicas {
		wg.Go(func() {
			if _, err := seqs[r].publishCheckpoint(ctx, 0, 0, func(_ context.Context, size uint64, root []byte) error {
				mu.Lock()
				defer mu.Unlock()
				pubs = append(pubs, fmt.Sprintf("%d/%s", size, root))
				return nil
			}); err != nil {
				t.Errorf("replica %d: publishCheckpoint: %v", r, err)
			}
		})
	}
	wg.Wait()
	after := time.Now()
	if want := []string{fmt.Sprintf("%d/%s", total, fakeRoot(total))}; !slices.Equal(pubs, want) {
		t.Errorf("publications = %q, want %q", pubs, want)
	}

	// Concurrent garbage collection: every obsolete prefix is deleted exactly once.
	const gcSize = 3*layout.EntryBundleWidth + 5
	deleted := map[string]int{}
	for r := range replicas {
		wg.Go(func() {
			if err := seqs[r].garbageCollect(ctx, gcSize, 100, func(_ context.Context, p string) error {
				mu.Lock()
				defer mu.Unlock()
				deleted[p]++
				return nil
			}, layout.EntriesPath); err != nil {
				t.Errorf("replica %d: garbageCollect: %v", r, err)
			}
		})
	}
	wg.Wait()
	wantDeleted := map[string]int{}
	for b := range uint64(3) {
		wantDeleted[layout.EntriesPath(b, 0)+".p/"] = 1
		wantDeleted[layout.TilePath(0, b, 0)+".p/"] = 1
	}
	if fmt.Sprint(deleted) != fmt.Sprint(wantDeleted) {
		t.Errorf("garbage collected prefixes = %v, want %v", deleted, wantDeleted)
	}

	st := readLogState(t, db, schema)
	if st.Next != total || st.IntSize != total || !bytes.Equal(st.IntRoot, fakeRoot(total)) || len(st.SeqKeys) != 0 {
		t.Errorf("state of %q: next %d, IntCoord (%d, %q), Seq %v; want %d, (%d, %q), []", schema, st.Next, st.IntSize, st.IntRoot, st.SeqKeys, total, total, fakeRoot(total))
	}
	if !st.PubSize.Valid || st.PubSize.Int64 != total || st.PubAt < before.UnixNano() || st.PubAt > after.UnixNano() {
		t.Errorf("PubCoord of %q = (%d, %v), want (in [%d, %d], %d)", schema, st.PubAt, st.PubSize, before.UnixNano(), after.UnixNano(), total)
	}
	if st.GCFrom != 3*layout.EntryBundleWidth {
		t.Errorf("GCCoord.fromSize of %q = %d, want %d", schema, st.GCFrom, 3*layout.EntryBundleWidth)
	}
	for i, s := range seqs {
		if n, err := s.nextIndex(ctx); err != nil || n != total {
			t.Errorf("replica %d: nextIndex = (%d, %v), want %d", i, n, err, total)
		}
		if size, root, err := s.currentTree(ctx); err != nil || size != total || !bytes.Equal(root, fakeRoot(total)) {
			t.Errorf("replica %d: currentTree = (%d, %q, %v), want (%d, %q)", i, size, root, err, total, fakeRoot(total))
		}
	}
}

// Sequencer replicas with Config.Schema use only that schema, never the search_path's.
func TestSchemaQualifiedReplicas(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	decoy := newDecoyEnv(t, 99)
	schema := newOddSchema(t)
	before := schemaFingerprint(t, decoy.DB, decoy.Schema, true)

	exerciseReplicas(t, ctx, decoy.DB, Config{DSN: decoy.DSN, Schema: schema}, schema)

	checkFingerprint(t, decoy.DB, decoy.Schema, true, before, "decoy schema")
	if got := tablesIn(t, decoy.DB, decoy.Schema); !slices.Equal(got, wantTables) {
		t.Errorf("decoy tables = %v, want %v", got, wantTables)
	}
}

// With Config.Schema, a connection without any search_path works.
func TestSchemaQualifiedWithoutSearchPath(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	requirePG(t)
	schema := newOddSchema(t)
	dsn := dsnWithParams(t, baseTestDSN(), map[string]string{"search_path": ""})
	db := openTestDB(t, dsn, 2)
	var schemas string
	if err := db.QueryRowContext(ctx, "SELECT current_schemas(false)::text").Scan(&schemas); err != nil || schemas != "{}" {
		t.Fatalf("current_schemas = %q (%v), want {} (no search_path)", schemas, err)
	}
	exerciseReplicas(t, ctx, db, Config{DSN: dsn, Schema: schema}, schema)

	// An appender (with its queue and background jobs, objects in memory) on another log, behind
	// the same search_path-less connection.
	schema = newOddSchema(t)
	cfg := Config{DSN: dsn, Schema: schema}
	const n = 10
	actx, acancel := context.WithCancel(ctx)
	defer acancel()
	sk, vk := mustGenerateKeys(t)
	seq := newTestSequencer(t, ctx, cfg, 1000)
	st := &Storage{cfg: cfg}
	a, lr, err := st.newAppender(actx, newMemObjStore(), seq, tessera.NewAppendOptions().WithCheckpointSigner(sk).WithCheckpointInterval(time.Second).WithBatching(8, 10*time.Millisecond))
	if err != nil {
		t.Fatalf("newAppender: %v", err)
	}
	aw := tessera.NewPublicationAwaiter(actx, lr.ReadCheckpoint, 20*time.Millisecond)
	var futures []tessera.IndexFuture
	for i := range n {
		futures = append(futures, a.Add(actx, tessera.NewEntry(fmt.Appendf(nil, "via appender %d", i))))
	}
	seen := map[uint64]bool{}
	for _, f := range futures {
		idx, cpRaw, err := aw.Await(actx, f)
		if err != nil {
			t.Fatalf("Await: %v", err)
		}
		if idx.Index >= n || seen[idx.Index] {
			t.Errorf("index %d assigned, want a distinct index in [0, %d)", idx.Index, n)
		}
		seen[idx.Index] = true
		if _, _, _, err := client.FetchCheckpoint(ctx, func(context.Context) ([]byte, error) { return cpRaw, nil }, vk, vk.Name()); err != nil {
			t.Errorf("published checkpoint does not verify: %v", err)
		}
	}
	if got, err := lr.IntegratedSize(ctx); err != nil || got != n {
		t.Errorf("LogReader.IntegratedSize = (%d, %v), want %d", got, err, n)
	}
	if got, err := lr.NextIndex(ctx); err != nil || got != n {
		t.Errorf("LogReader.NextIndex = (%d, %v), want %d", got, err, n)
	}

	// A MigrationWriter's LogReader on the same log.
	d, err := New(ctx, Config{SDKConfig: unusedS3, Bucket: "unused", DSN: dsn, Schema: schema})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.(*Storage).Close() })
	mw, mlr, err := d.(*Storage).MigrationWriter(ctx, tessera.NewMigrationOptions())
	if err != nil {
		t.Fatalf("MigrationWriter: %v", err)
	}
	if got, err := mlr.IntegratedSize(ctx); err != nil || got != n {
		t.Errorf("MigrationWriter LogReader.IntegratedSize = (%d, %v), want %d", got, err, n)
	}
	if got, err := mlr.NextIndex(ctx); err != nil || got != n {
		t.Errorf("MigrationWriter LogReader.NextIndex = (%d, %v), want %d", got, err, n)
	}
	if got, err := mw.IntegratedSize(ctx); err != nil || got != n {
		t.Errorf("MigrationWriter.IntegratedSize = (%d, %v), want %d", got, err, n)
	}
}

// A Config.Schema which doesn't exist makes construction fail cleanly (error, pool released),
// and never falls back to the search_path's schema.
func TestSchemaQualifiedMissingSchema(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		skip bool
		// decoy returns the search_path schema; with SkipSchemaInit it is a complete, valid log
		// (so falling back to it would "work"), otherwise it is empty (so falling back to it would
		// create tables there).
		decoy func(t *testing.T) *pgTestEnv
	}{
		{"SkipSchemaInit", true, func(t *testing.T) *pgTestEnv { return newDecoyEnv(t, SchemaCompatibilityVersion) }},
		{"with schema init", false, newPGTestEnv},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			decoy := tc.decoy(t)
			before := schemaFingerprint(t, decoy.DB, decoy.Schema, true)
			missing := fmt.Sprintf(`No "Such" schema-%s`, randHex(t, 4))
			dsn, app := dsnWithApp(t, decoy)
			cfg := Config{SDKConfig: unusedS3, Bucket: "unused", DSN: dsn, Schema: missing, SkipSchemaInit: tc.skip}

			if s, err := newPGSequencer(ctx, cfg, 10); err == nil {
				_ = s.close()
				t.Error("newPGSequencer succeeded with a non-existent Config.Schema")
			}
			d, err := New(ctx, cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if _, _, err := d.(*Storage).Appender(ctx, tessera.NewAppendOptions()); err == nil {
				t.Error("Appender succeeded with a non-existent Config.Schema")
			}
			if _, _, err := d.(*Storage).MigrationWriter(ctx, tessera.NewMigrationOptions()); err == nil {
				t.Error("MigrationWriter succeeded with a non-existent Config.Schema")
			}
			waitFor(t, 10*time.Second, "pools of failed constructions to be closed", func() bool { return countConns(t, app) == 0 })
			if err := d.(*Storage).Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			checkFingerprint(t, decoy.DB, decoy.Schema, true, before, "search_path schema")
			var n int
			if err := decoy.DB.QueryRowContext(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname = $1", missing).Scan(&n); err != nil || n != 0 {
				t.Errorf("schema %q exists after failed construction (n=%d, err=%v)", missing, n, err)
			}
		})
	}
}

// End to end with S3: an appender with Config.Schema (and a BucketPrefix) behind a decoy
// search_path produces a verifiable log, keeps all its state in the configured schema
// (including publication and garbage collection progress), and never touches the decoy.
func TestSchemaQualifiedE2E(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	decoy := newDecoyEnv(t, 99)
	schema := newOddSchema(t)
	before := schemaFingerprint(t, decoy.DB, decoy.Schema, true)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, vk := mustGenerateKeys(t)

	const prefix = "qualified"
	d, err := New(ctx, Config{SDKConfig: b.SDKConfig, S3Options: b.S3Options, Bucket: b.Name, BucketPrefix: prefix, DSN: decoy.DSN, Schema: schema})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.(*Storage).Close() })
	app, shutdown, lr, err := tessera.NewAppender(ctx, d, tessera.NewAppendOptions().
		WithCheckpointSigner(sk).
		WithCheckpointInterval(time.Second).
		WithBatching(32, 20*time.Millisecond).
		WithGarbageCollectionInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}
	a := &e2eAppender{appender: app, shutdown: shutdown, reader: lr, awaiter: tessera.NewPublicationAwaiter(ctx, lr.ReadCheckpoint, 50*time.Millisecond)}
	const n = 300
	want := mustAddConcurrently(t, ctx, a, "qualified", n, 8)
	checkIndices(t, want, 0, n)
	cp, _ := verifyLog(t, ctx, &s3Reader{client: b.Client, bucket: b.Name, prefix: prefix}, vk, want)

	if got, err := lr.IntegratedSize(ctx); err != nil || got != n {
		t.Errorf("LogReader.IntegratedSize = (%d, %v), want %d", got, err, n)
	}
	if got, err := lr.NextIndex(ctx); err != nil || got != n {
		t.Errorf("LogReader.NextIndex = (%d, %v), want %d", got, err, n)
	}
	waitFor(t, 30*time.Second, "publication and GC progress to be recorded in the configured schema", func() bool {
		st := readLogState(t, decoy.DB, schema)
		return st.PubSize.Valid && st.PubSize.Int64 == n && st.GCFrom == layout.EntryBundleWidth
	})
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	st := readLogState(t, decoy.DB, schema)
	if st.Next != n || st.IntSize != n || !bytes.Equal(st.IntRoot, cp.Hash) || len(st.SeqKeys) != 0 {
		t.Errorf("state of %q: next %d, IntCoord (%d, %x), Seq %v; want %d, (%d, %x), []", schema, st.Next, st.IntSize, st.IntRoot, st.SeqKeys, n, n, cp.Hash)
	}
	if d.(*Storage).CoordinationError() != nil {
		t.Errorf("CoordinationError = %v", d.(*Storage).CoordinationError())
	}
	checkFingerprint(t, decoy.DB, decoy.Schema, true, before, "decoy schema")
}

// synthLog is a synthetic tlog-tiles source log for migrations.
type synthLog struct {
	data [][]byte
	root []byte
}

func newSynthLog(t *testing.T, prefix string, n int) *synthLog {
	t.Helper()
	s := &synthLog{}
	for i := range n {
		s.data = append(s.data, fmt.Appendf(nil, "%s %d", prefix, i))
	}
	s.root = rootOf(t, s.data)
	return s
}

func (s *synthLog) size() uint64 { return uint64(len(s.data)) }

// readEntryBundle implements client.EntryBundleFetcherFunc.
func (s *synthLog) readEntryBundle(_ context.Context, idx uint64, p uint8) ([]byte, error) {
	n := uint64(p)
	if n == 0 {
		n = layout.EntryBundleWidth
	}
	first := idx * layout.EntryBundleWidth
	if first+n > s.size() {
		return nil, os.ErrNotExist
	}
	var b []byte
	for i := first; i < first+n; i++ {
		b = append(b, tessera.NewEntry(s.data[i]).MarshalBundleData(i)...)
	}
	return b, nil
}

// checkMigrated verifies, reading the bucket directly, that it holds the source log's entries and
// tiles from which inclusion proofs for the source root verify.
func checkMigrated(t *testing.T, ctx context.Context, r *s3Reader, src *synthLog) {
	t.Helper()
	for i := uint64(0); i*layout.EntryBundleWidth < src.size(); i++ {
		bun, err := client.GetEntryBundle(ctx, r.ReadEntryBundle, i, src.size())
		if err != nil {
			t.Fatalf("GetEntryBundle(%d): %v", i, err)
		}
		for j, e := range bun.Entries {
			if idx := i*layout.EntryBundleWidth + uint64(j); !bytes.Equal(e, src.data[idx]) {
				t.Fatalf("migrated entry %d = %q, want %q", idx, e, src.data[idx])
			}
		}
	}
	pb, err := client.NewProofBuilder(ctx, src.size(), r.ReadTile)
	if err != nil {
		t.Fatalf("NewProofBuilder: %v", err)
	}
	for _, idx := range proofSample(src.size()) {
		p, err := pb.InclusionProof(ctx, idx)
		if err != nil {
			t.Fatalf("InclusionProof(%d): %v", idx, err)
		}
		if err := proof.VerifyInclusion(rfc6962.DefaultHasher, idx, src.size(), rfc6962.DefaultHasher.HashLeaf(src.data[idx]), p, src.root); err != nil {
			t.Errorf("VerifyInclusion(%d): %v", idx, err)
		}
	}
}

// checkAntispamIndex checks that as reports the source entries sampled by proofSample as
// duplicates at their indices, and passes a new entry on to the delegate.
func checkAntispamIndex(t *testing.T, ctx context.Context, as *antispam.AntispamStorage, src *synthLog) {
	t.Helper()
	var delegated int
	add := as.Decorator()(func(context.Context, *tessera.Entry) tessera.IndexFuture {
		delegated++
		return func() (tessera.Index, error) { return tessera.Index{Index: 1 << 40}, nil }
	})
	for _, i := range proofSample(src.size()) {
		idx, err := add(ctx, tessera.NewEntry(src.data[i]))()
		if err != nil || !idx.IsDup || idx.Index != i {
			t.Errorf("antispam lookup of entry %d = (%+v, %v), want duplicate at %d", i, idx, err, i)
		}
	}
	if delegated != 0 {
		t.Errorf("%d known entries were passed on to the delegate", delegated)
	}
	if idx, err := add(ctx, tessera.NewEntry([]byte("never seen")))(); err != nil || idx.IsDup || idx.Index != 1<<40 || delegated != 1 {
		t.Errorf("new entry = (%+v, %v), delegated %d; want it passed on to the delegate", idx, err, delegated)
	}
}

// Migration into a target with Config.Schema, populating an antispam index with
// AntispamOpts.Schema, behind a decoy search_path holding both sets of tables.
func TestSchemaQualifiedMigrationWithAntispam(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	decoy := newDecoyEnv(t, 99)
	decoy.exec(t, antispam.SchemaSQL)
	decoy.exec(t, "UPDATE AntispamMeta SET compatibilityVersion = 99")
	decoy.exec(t, "UPDATE AntispamFollowCoord SET nextIdx = 999999")
	src := newSynthLog(t, "migrated", 600)
	for i := range src.data {
		h := sha256.Sum256(src.data[i])
		decoy.exec(t, "INSERT INTO AntispamIDSeq (h, idx) VALUES ($1, $2)", h[:], 424242+i)
	}
	logSchema, asSchema := newOddSchema(t), newOddSchema(t)
	before := schemaFingerprint(t, decoy.DB, decoy.Schema, true)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()

	d, err := New(ctx, Config{SDKConfig: b.SDKConfig, S3Options: b.S3Options, Bucket: b.Name, DSN: decoy.DSN, Schema: logSchema})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.(*Storage).Close() })
	mw, lr, err := d.(*Storage).MigrationWriter(ctx, tessera.NewMigrationOptions())
	if err != nil {
		t.Fatalf("MigrationWriter: %v", err)
	}
	for name, f := range map[string]func(context.Context) (uint64, error){"LogReader.IntegratedSize": lr.IntegratedSize, "LogReader.NextIndex": lr.NextIndex, "MigrationWriter.IntegratedSize": mw.IntegratedSize} {
		if got, err := f(ctx); err != nil || got != 0 {
			t.Errorf("%s before migration = (%d, %v), want 0", name, got, err)
		}
	}
	as, err := antispam.NewAntispam(ctx, decoy.DSN, antispam.AntispamOpts{Schema: asSchema})
	if err != nil {
		t.Fatalf("NewAntispam: %v", err)
	}
	t.Cleanup(func() { _ = as.Close() })
	mt, err := tessera.NewMigrationTarget(ctx, d, tessera.NewMigrationOptions().WithAntispam(as))
	if err != nil {
		t.Fatalf("NewMigrationTarget: %v", err)
	}
	if err := mt.Migrate(ctx, 4, src.size(), src.root, src.readEntryBundle); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if got, err := lr.IntegratedSize(ctx); err != nil || got != src.size() {
		t.Errorf("LogReader.IntegratedSize = (%d, %v), want %d", got, err, src.size())
	}
	// Migration doesn't sequence anything: NextIndex is SeqCoord.next, untouched.
	if got, err := lr.NextIndex(ctx); err != nil || got != 0 {
		t.Errorf("LogReader.NextIndex = (%d, %v), want 0", got, err)
	}
	st := readLogState(t, decoy.DB, logSchema)
	if st.IntSize != src.size() || !bytes.Equal(st.IntRoot, src.root) || st.Next != 0 {
		t.Errorf("state of %q: IntCoord (%d, %x), next %d; want (%d, %x), 0", logSchema, st.IntSize, st.IntRoot, st.Next, src.size(), src.root)
	}
	if got, want := tablesIn(t, decoy.DB, asSchema), []string{"antispamfollowcoord", "antispamidseq", "antispammeta"}; !slices.Equal(got, want) {
		t.Errorf("tables in %q = %v, want %v", asSchema, got, want)
	}
	if got, err := as.Follower(nil).EntriesProcessed(ctx); err != nil || got != src.size() {
		t.Errorf("antispam EntriesProcessed = (%d, %v), want %d", got, err, src.size())
	}
	checkAntispamIndex(t, ctx, as, src)
	checkMigrated(t, ctx, &s3Reader{client: b.Client, bucket: b.Name}, src)
	checkFingerprint(t, decoy.DB, decoy.Schema, true, before, "decoy schema")
}
