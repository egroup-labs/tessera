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

// End-to-end tests: s3pg.New + tessera.NewAppender against real PostgreSQL and a real
// S3-compatible endpoint ($TESSERA_S3PG_TEST_S3_ENDPOINT, skipped if unset). The resulting log is
// read back directly from the bucket (independently of the code under test) and verified with
// the client package.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/transparency-dev/formats/log"
	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/fsck"
	"golang.org/x/mod/sumdb/note"
)

const e2eTimeout = 2 * time.Minute

// e2eAppender is a running tessera appender backed by s3pg.
type e2eAppender struct {
	appender *tessera.Appender
	shutdown func(context.Context) error
	reader   tessera.LogReader
	awaiter  *tessera.PublicationAwaiter
}

// newE2EAppender creates an s3pg driver from cfg and starts an appender on it.
func newE2EAppender(t *testing.T, ctx context.Context, cfg Config, sk note.Signer) *e2eAppender {
	t.Helper()
	driver, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	a, shutdown, lr, err := tessera.NewAppender(ctx, driver, tessera.NewAppendOptions().
		WithCheckpointSigner(sk).
		WithCheckpointInterval(time.Second).
		WithBatching(32, 50*time.Millisecond).
		WithPushback(4096))
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}
	return &e2eAppender{
		appender: a,
		shutdown: shutdown,
		reader:   lr,
		awaiter:  tessera.NewPublicationAwaiter(ctx, lr.ReadCheckpoint, 50*time.Millisecond),
	}
}

// addConcurrently adds n entries "<prefix> <i>" using the given number of concurrent workers, waits
// for all of them to be published, and returns the assigned index -> data.
// It may be called from any goroutine.
func addConcurrently(ctx context.Context, a *e2eAppender, prefix string, n, workers int) (map[uint64][]byte, error) {
	futures := make([]tessera.IndexFuture, n)
	wg := sync.WaitGroup{}
	for w := range workers {
		wg.Go(func() {
			for i := w; i < n; i += workers {
				futures[i] = a.appender.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "%s %d", prefix, i)))
			}
		})
	}
	wg.Wait()

	indices := make([]tessera.Index, n)
	errs := make([]error, n)
	for w := range workers {
		wg.Go(func() {
			for i := w; i < n; i += workers {
				indices[i], _, errs[i] = a.awaiter.Await(ctx, futures[i])
			}
		})
	}
	wg.Wait()

	r := make(map[uint64][]byte, n)
	for i := range n {
		if errs[i] != nil {
			return nil, fmt.Errorf("Await(%s %d): %v", prefix, i, errs[i])
		}
		if indices[i].IsDup {
			return nil, fmt.Errorf("%s %d reported as a duplicate", prefix, i)
		}
		if prev, ok := r[indices[i].Index]; ok {
			return nil, fmt.Errorf("index %d assigned to both %q and \"%s %d\"", indices[i].Index, prev, prefix, i)
		}
		r[indices[i].Index] = fmt.Appendf(nil, "%s %d", prefix, i)
	}
	return r, nil
}

// mustAddConcurrently is addConcurrently, failing the test on error. Call it from the test goroutine only.
func mustAddConcurrently(t *testing.T, ctx context.Context, a *e2eAppender, prefix string, n, workers int) map[uint64][]byte {
	t.Helper()
	r, err := addConcurrently(ctx, a, prefix, n, workers)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// checkIndices checks that the keys of m are exactly [from, from+n).
func checkIndices(t *testing.T, m map[uint64][]byte, from uint64, n int) {
	t.Helper()
	got := make([]uint64, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	slices.Sort(got)
	want := make([]uint64, n)
	for i := range want {
		want[i] = from + uint64(i)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("assigned indices are not exactly [%d, %d): got %v", from, from+uint64(n), got)
	}
}

// proofSample returns a deterministic sample of leaf indices for a tree of the given size.
func proofSample(size uint64) []uint64 {
	r := []uint64{0, 1, 255, 256, 257, size / 2, size - 2, size - 1}
	rng := rand.New(rand.NewPCG(size, 1))
	for range 8 {
		r = append(r, rng.Uint64N(size))
	}
	r = slices.DeleteFunc(r, func(i uint64) bool { return i >= size })
	slices.Sort(r)
	return slices.Compact(r)
}

// verifyLog checks, reading directly from the bucket, that the published log contains exactly
// the entries in want (index -> data): the checkpoint signature verifies, its size is len(want),
// every entry bundle holds the expected data, the root hash recomputed from the bundles matches,
// sampled inclusion proofs built from the tiles verify, and fsck passes.
func verifyLog(t *testing.T, ctx context.Context, r *s3Reader, vk note.Verifier, want map[uint64][]byte) (*log.Checkpoint, []byte) {
	t.Helper()
	cp, cpRaw, _, err := client.FetchCheckpoint(ctx, r.ReadCheckpoint, vk, vk.Name())
	if err != nil {
		t.Fatalf("FetchCheckpoint: %v", err)
	}
	if cp.Origin != vk.Name() {
		t.Errorf("checkpoint origin %q, want %q", cp.Origin, vk.Name())
	}
	if cp.Size != uint64(len(want)) {
		t.Fatalf("checkpoint size %d, want %d", cp.Size, len(want))
	}

	rf := compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}
	cr := rf.NewEmptyRange(0)
	for i := uint64(0); i*layout.EntryBundleWidth < cp.Size; i++ {
		b, err := client.GetEntryBundle(ctx, r.ReadEntryBundle, i, cp.Size)
		if err != nil {
			t.Fatalf("GetEntryBundle(%d): %v", i, err)
		}
		if got, want := uint64(len(b.Entries)), min(layout.EntryBundleWidth, cp.Size-i*layout.EntryBundleWidth); got != want {
			t.Fatalf("entry bundle %d has %d entries, want %d", i, got, want)
		}
		for j, e := range b.Entries {
			idx := i*layout.EntryBundleWidth + uint64(j)
			if !bytes.Equal(e, want[idx]) {
				t.Errorf("entry %d = %q, want %q", idx, e, want[idx])
			}
			if err := cr.Append(rfc6962.DefaultHasher.HashLeaf(e), nil); err != nil {
				t.Fatalf("compact range Append: %v", err)
			}
		}
	}
	root, err := cr.GetRootHash(nil)
	if err != nil {
		t.Fatalf("GetRootHash: %v", err)
	}
	if !bytes.Equal(root, cp.Hash) {
		t.Errorf("root recomputed from entry bundles %x, checkpoint has %x", root, cp.Hash)
	}

	pb, err := client.NewProofBuilder(ctx, cp.Size, r.ReadTile)
	if err != nil {
		t.Fatalf("NewProofBuilder: %v", err)
	}
	for _, idx := range proofSample(cp.Size) {
		p, err := pb.InclusionProof(ctx, idx)
		if err != nil {
			t.Fatalf("InclusionProof(%d): %v", idx, err)
		}
		if err := proof.VerifyInclusion(rfc6962.DefaultHasher, idx, cp.Size, rfc6962.DefaultHasher.HashLeaf(want[idx]), p, cp.Hash); err != nil {
			t.Errorf("VerifyInclusion(%d): %v", idx, err)
		}
	}

	if err := fsck.New(vk.Name(), vk, r, defaultMerkleLeafHasher, fsck.Opts{N: 4}).Check(ctx); err != nil {
		t.Errorf("fsck: %v", err)
	}
	return cp, cpRaw
}

// checkDBIntegrated checks that the database agrees everything up to size has been sequenced and
// integrated, with nothing left in Seq.
func checkDBIntegrated(t *testing.T, env *pgTestEnv, size uint64, root []byte) {
	t.Helper()
	if got := env.seqCoordNext(t); got != size {
		t.Errorf("SeqCoord.next = %d, want %d", got, size)
	}
	if gotSize, gotRoot := env.intCoord(t); gotSize != size || !bytes.Equal(gotRoot, root) {
		t.Errorf("IntCoord = (%d, %x), want (%d, %x)", gotSize, gotRoot, size, root)
	}
	checkSeqKeys(t, env, []uint64{})
}

// TestE2EAppendAndVerify: a single appender adds a few hundred entries concurrently; the log
// published to S3 contains exactly those entries at the indices returned, and verifies.
func TestE2EAppendAndVerify(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, vk := mustGenerateKeys(t)

	const prefix = "logs/one"
	cfg := Config{
		SDKConfig:    b.SDKConfig,
		S3Options:    b.S3Options,
		Bucket:       b.Name,
		BucketPrefix: prefix,
		DSN:          env.DSN,
		MaxOpenConns: 10,
		MaxIdleConns: 2,
	}
	a := newE2EAppender(t, ctx, cfg, sk)

	const n = 300
	want := mustAddConcurrently(t, ctx, a, "single", n, 10)
	checkIndices(t, want, 0, n)

	r := &s3Reader{client: b.Client, bucket: b.Name, prefix: prefix}
	cp, cpRaw := verifyLog(t, ctx, r, vk, want)
	checkDBIntegrated(t, env, n, cp.Hash)

	// The appender's LogReader serves the same checkpoint and entries.
	lrCP, err := a.reader.ReadCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LogReader.ReadCheckpoint: %v", err)
	}
	if !bytes.Equal(lrCP, cpRaw) {
		t.Errorf("LogReader checkpoint %q differs from bucket checkpoint %q", lrCP, cpRaw)
	}
	if got, err := a.reader.IntegratedSize(ctx); err != nil || got != n {
		t.Errorf("LogReader.IntegratedSize = (%d, %v), want (%d, nil)", got, err, n)
	}
	if got, err := a.reader.NextIndex(ctx); err != nil || got != n {
		t.Errorf("LogReader.NextIndex = (%d, %v), want (%d, nil)", got, err, n)
	}

	// Everything lives under the bucket prefix.
	for _, k := range b.keys(t) {
		if !strings.HasPrefix(k, prefix+"/") {
			t.Errorf("object %q written outside of BucketPrefix %q", k, prefix)
		}
	}

	if err := a.shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

// TestE2ETwoAppenders: two independent Storage/Appender instances (separate pools: one opened
// from the DSN, one external Config.DB) on the same schema and bucket add entries concurrently;
// the log contains every entry exactly once and verifies.
func TestE2ETwoAppenders(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, vk := mustGenerateKeys(t)

	const prefix = "logs/two"
	cfgA := Config{
		SDKConfig:    b.SDKConfig,
		S3Options:    b.S3Options,
		Bucket:       b.Name,
		BucketPrefix: prefix,
		DSN:          env.DSN,
		MaxOpenConns: 10,
		MaxIdleConns: 2,
	}
	cfgB := cfgA
	cfgB.DSN = ""
	cfgB.DB = openTestDB(t, env.DSN, 10)

	aA := newE2EAppender(t, ctx, cfgA, sk)
	aB := newE2EAppender(t, ctx, cfgB, sk)

	const n = 200
	var wantA, wantB map[uint64][]byte
	var errA, errB error
	wg := sync.WaitGroup{}
	wg.Go(func() { wantA, errA = addConcurrently(ctx, aA, "A", n, 8) })
	wg.Go(func() { wantB, errB = addConcurrently(ctx, aB, "B", n, 8) })
	wg.Wait()
	if err := errors.Join(errA, errB); err != nil {
		t.Fatal(err)
	}

	want := make(map[uint64][]byte, 2*n)
	for _, m := range []map[uint64][]byte{wantA, wantB} {
		for k, v := range m {
			if prev, ok := want[k]; ok {
				t.Fatalf("index %d assigned to both %q and %q", k, prev, v)
			}
			want[k] = v
		}
	}
	checkIndices(t, want, 0, 2*n)

	r := &s3Reader{client: b.Client, bucket: b.Name, prefix: prefix}
	cp, _ := verifyLog(t, ctx, r, vk, want)
	checkDBIntegrated(t, env, 2*n, cp.Hash)

	for name, a := range map[string]*e2eAppender{"A": aA, "B": aB} {
		if err := a.shutdown(ctx); err != nil {
			t.Errorf("shutdown %s: %v", name, err)
		}
	}
}

// TestE2ERestart: entries added before an appender is shut down are kept when a new appender
// is started on the same schema and bucket, indices carry on, and the new checkpoint is
// consistent with the old one.
func TestE2ERestart(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, vk := mustGenerateKeys(t)

	cfg := Config{
		SDKConfig: b.SDKConfig,
		S3Options: b.S3Options,
		Bucket:    b.Name,
		DSN:       env.DSN,
	}
	r := &s3Reader{client: b.Client, bucket: b.Name}
	const n1, n2 = 150, 170

	ctx1, cancel1 := context.WithCancel(ctx)
	a1 := newE2EAppender(t, ctx1, cfg, sk)
	want := mustAddConcurrently(t, ctx1, a1, "first", n1, 6)
	checkIndices(t, want, 0, n1)
	cp1, cp1Raw := verifyLog(t, ctx, r, vk, want)
	if err := a1.shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	cancel1()
	if _, err := a1.appender.Add(ctx, tessera.NewEntry([]byte("after shutdown")))(); err == nil {
		t.Error("Add after shutdown succeeded, want error")
	}

	a2 := newE2EAppender(t, ctx, cfg, sk)
	want2 := mustAddConcurrently(t, ctx, a2, "second", n2, 6)
	checkIndices(t, want2, n1, n2)
	for k, v := range want2 {
		want[k] = v
	}
	cp2, _ := verifyLog(t, ctx, r, vk, want)
	checkDBIntegrated(t, env, n1+n2, cp2.Hash)

	pb, err := client.NewProofBuilder(ctx, cp2.Size, r.ReadTile)
	if err != nil {
		t.Fatalf("NewProofBuilder: %v", err)
	}
	cproof, err := pb.ConsistencyProof(ctx, cp1.Size, cp2.Size)
	if err != nil {
		t.Fatalf("ConsistencyProof(%d, %d): %v", cp1.Size, cp2.Size, err)
	}
	if err := proof.VerifyConsistency(rfc6962.DefaultHasher, cp1.Size, cp2.Size, cproof, cp1.Hash, cp2.Hash); err != nil {
		t.Errorf("VerifyConsistency(%d, %d): %v", cp1.Size, cp2.Size, err)
	}

	// A client which saw the old checkpoint accepts the new one.
	lst, err := client.NewLogStateTracker(ctx, r.ReadTile, cp1Raw, vk, vk.Name(), client.UnilateralConsensus(r.ReadCheckpoint))
	if err != nil {
		t.Fatalf("NewLogStateTracker: %v", err)
	}
	if _, _, _, err := lst.Update(ctx); err != nil {
		t.Fatalf("LogStateTracker.Update: %v", err)
	}
	if got := lst.Latest(); got.Size != cp2.Size || !bytes.Equal(got.Hash, cp2.Hash) {
		t.Errorf("LogStateTracker latest = (%d, %x), want (%d, %x)", got.Size, got.Hash, cp2.Size, cp2.Hash)
	}

	if err := a2.shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

// TestE2ES3ObjectStore checks the s3Storage object store semantics the driver relies on against
// the real S3-compatible endpoint, with and without a BucketPrefix: prefixing, Content-Type and
// Cache-Control, NoSuchKey on missing objects, idempotent If-None-Match writes, and prefix deletes
// (including of a prefix with no objects, as GC does for partials which never existed).
// It needs no database.
func TestE2ES3ObjectStore(t *testing.T) {
	t.Parallel()
	for _, prefix := range []string{"", "pfx"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			t.Parallel()
			b := newTestBucket(t)
			ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
			defer cancel()

			s := &s3Storage{s3Client: s3.NewFromConfig(*b.SDKConfig, b.S3Options), bucket: b.Name, bucketPrefix: prefix}
			key := func(k string) string { return path.Join(prefix, k) }
			raw := &s3Reader{client: b.Client, bucket: b.Name}

			if err := s.setObject(ctx, "a/one", []byte("one"), logContType, logCacheControl); err != nil {
				t.Fatalf("setObject: %v", err)
			}
			if got, err := s.getObject(ctx, "a/one"); err != nil || string(got) != "one" {
				t.Errorf("getObject(a/one) = (%q, %v), want (one, nil)", got, err)
			}
			if got, err := raw.get(ctx, key("a/one")); err != nil || string(got) != "one" {
				t.Errorf("raw object %s = (%q, %v), want (one, nil)", key("a/one"), got, err)
			}
			h, err := b.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b.Name), Key: aws.String(key("a/one"))})
			if err != nil {
				t.Fatalf("HeadObject: %v", err)
			}
			if got := aws.ToString(h.ContentType); got != logContType {
				t.Errorf("Content-Type = %q, want %q", got, logContType)
			}
			if got := aws.ToString(h.CacheControl); got != logCacheControl {
				t.Errorf("Cache-Control = %q, want %q", got, logCacheControl)
			}

			_, err = s.getObject(ctx, "a/missing")
			var nsk *types.NoSuchKey
			if !errors.As(err, &nsk) {
				t.Errorf("getObject(missing) = %v, want NoSuchKey", err)
			}
			lrs := &logResourceStore{objStore: s, entriesPath: layout.EntriesPath}
			if _, err := lrs.ReadCheckpoint(ctx); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("ReadCheckpoint on empty log = %v, want os.ErrNotExist", err)
			}

			if err := s.setObjectIfNoneMatch(ctx, "a/two", []byte("two"), logContType, logCacheControl); err != nil {
				t.Fatalf("setObjectIfNoneMatch: %v", err)
			}
			if err := s.setObjectIfNoneMatch(ctx, "a/two", []byte("two"), logContType, logCacheControl); err != nil {
				t.Errorf("idempotent setObjectIfNoneMatch with identical data: %v", err)
			}
			if err := s.setObjectIfNoneMatch(ctx, "a/two", []byte("different"), logContType, logCacheControl); err == nil {
				t.Error("setObjectIfNoneMatch with different data succeeded, want error")
			}
			if got, err := s.getObject(ctx, "a/two"); err != nil || string(got) != "two" {
				t.Errorf("getObject(a/two) = (%q, %v), want (two, nil)", got, err)
			}

			if err := s.setObject(ctx, "b/three", []byte("three"), logContType, logCacheControl); err != nil {
				t.Fatalf("setObject: %v", err)
			}
			if err := s.deleteObjectsWithPrefix(ctx, "a/"); err != nil {
				t.Fatalf("deleteObjectsWithPrefix(a/): %v", err)
			}
			if got, want := b.keys(t), []string{key("b/three")}; !slices.Equal(got, want) {
				t.Errorf("objects after deleting prefix a/ = %v, want %v", got, want)
			}
			if err := s.deleteObjectsWithPrefix(ctx, "nothing/here.p/"); err != nil {
				t.Errorf("deleteObjectsWithPrefix of a prefix with no objects = %v, want nil (no-op)", err)
			}
			if got, want := b.keys(t), []string{key("b/three")}; !slices.Equal(got, want) {
				t.Errorf("objects = %v, want %v", got, want)
			}
		})
	}
}

// TestE2EGarbageCollectS3: garbageCollect against real S3 removes the obsolete partial resources of
// completed bundles, including when some bundles never had partials (which is common: a bundle
// completed by a single integration), and records its progress in GCCoord.
func TestE2EGarbageCollectS3(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()

	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	s := &s3Storage{s3Client: s3.NewFromConfig(*b.SDKConfig, b.S3Options), bucket: b.Name}

	// Bundle 0 was built up in steps, so it (and its tile) has partials; bundle 1 was completed in
	// one go and has none; bundle 2 is the current partial right-hand edge and must be kept.
	keep := []string{
		layout.EntriesPath(0, 0), layout.TilePath(0, 0, 0),
		layout.EntriesPath(1, 0), layout.TilePath(0, 1, 0),
		layout.EntriesPath(2, 7), layout.TilePath(0, 2, 7), layout.TilePath(1, 0, 3),
		layout.CheckpointPath,
	}
	obsolete := []string{
		layout.EntriesPath(0, 10), layout.EntriesPath(0, 200), layout.TilePath(0, 0, 10), layout.TilePath(0, 0, 200),
	}
	for _, k := range append(slices.Clone(keep), obsolete...) {
		if err := s.setObject(ctx, k, []byte(k), logContType, logCacheControl); err != nil {
			t.Fatalf("setObject(%s): %v", k, err)
		}
	}

	treeSize := uint64(2*layout.EntryBundleWidth + 7)
	if err := seq.garbageCollect(ctx, treeSize, 100, s.deleteObjectsWithPrefix, layout.EntriesPath); err != nil {
		t.Fatalf("garbageCollect: %v", err)
	}
	got := b.keys(t)
	slices.Sort(got)
	slices.Sort(keep)
	if !slices.Equal(got, keep) {
		t.Errorf("objects after GC = %v, want %v", got, keep)
	}
	if got := env.gcFromSize(t); got != 2*layout.EntryBundleWidth {
		t.Errorf("GCCoord.fromSize = %d, want %d", got, 2*layout.EntryBundleWidth)
	}
}
