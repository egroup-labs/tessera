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

// Tests added after independent review / mutation testing, covering the non-mechanical
// differences from storage/aws: pool lifecycle (Storage.Close, close on failure, MaxIdleConns),
// S3Options handling, schema completeness checks and constraints, GC coordination between
// replicas, MigrationWriter, and the S3 delete path (via a fake S3 server).

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
)

// countConns returns the number of server sessions with the given application_name.
func countConns(t *testing.T, app string) int {
	t.Helper()
	var n int
	if err := requirePG(t).QueryRowContext(context.Background(),
		"SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", app).Scan(&n); err != nil {
		t.Fatalf("count connections: %v", err)
	}
	return n
}

// dsnWithApp returns the env DSN tagged with a unique application_name, and that name.
func dsnWithApp(t *testing.T, env *pgTestEnv) (string, string) {
	t.Helper()
	app := "s3pg-test-" + randHex(t, 6)
	return dsnWithParams(t, env.DSN, map[string]string{"application_name": app}), app
}

// unusedS3 is an S3 configuration for tests which construct, but never use, the S3 client.
var unusedS3 = &aws.Config{Region: "us-east-1"}

// Storage.Close releases the pools opened by the Storage (from DSN), is idempotent, and never
// closes an external Config.DB.
func TestStorageCloseReleasesPools(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	dsn, app := dsnWithApp(t, env)

	d, err := New(ctx, Config{SDKConfig: unusedS3, Bucket: "unused", DSN: dsn, MaxIdleConns: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s := d.(*Storage)
	for range 2 {
		if _, _, err := s.MigrationWriter(ctx, tessera.NewMigrationOptions()); err != nil {
			t.Fatalf("MigrationWriter: %v", err)
		}
	}
	if n := countConns(t, app); n == 0 {
		t.Fatalf("expected open connections for %s before Close", app)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitFor(t, 10*time.Second, "connections to be released by Close", func() bool { return countConns(t, app) == 0 })
	if err := s.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	ext := openTestDB(t, env.DSN, 2)
	d2, err := New(ctx, Config{SDKConfig: unusedS3, Bucket: "unused", DB: ext})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, _, err := d2.(*Storage).MigrationWriter(ctx, tessera.NewMigrationOptions()); err != nil {
		t.Fatalf("MigrationWriter: %v", err)
	}
	if err := d2.(*Storage).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := ext.PingContext(ctx); err != nil {
		t.Errorf("external Config.DB was closed by Storage.Close: %v", err)
	}
}

// If Appender fails after opening its pool, the pool is released immediately.
func TestAppenderFailureClosesPool(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	dsn, app := dsnWithApp(t, env)
	d, err := New(ctx, Config{SDKConfig: unusedS3, Bucket: "unused", DSN: dsn, MaxIdleConns: 2})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// A checkpoint interval below the minimum makes appender initialisation fail after the
	// sequencer (and its pool) has been created.
	opts := tessera.NewAppendOptions().WithCheckpointInterval(time.Millisecond)
	if _, _, err := d.(*Storage).Appender(ctx, opts); err == nil {
		t.Fatal("Appender succeeded, want error for too-short checkpoint interval")
	}
	waitFor(t, 10*time.Second, "pool of failed Appender to be closed", func() bool { return countConns(t, app) == 0 })
}

// A pool opened by newPGSequencer is closed again if construction fails after opening it.
func TestPGSequencerClosesPoolOnFailure(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	dsn, app := dsnWithApp(t, env)
	// Empty schema + SkipSchemaInit: fails after connecting.
	if _, err := newPGSequencer(ctx, Config{DSN: dsn, SkipSchemaInit: true, MaxIdleConns: 2}, 10); err == nil {
		t.Fatal("newPGSequencer succeeded on an empty schema with SkipSchemaInit")
	}
	waitFor(t, 10*time.Second, "pool of failed sequencer to be closed", func() bool { return countConns(t, app) == 0 })
}

// MaxIdleConns: 0 keeps the database/sql default (idle connections are kept), negative disables them.
func TestPGSequencerMaxIdleConns(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	for _, tc := range []struct {
		maxIdle  int
		wantIdle bool
	}{{0, true}, {3, true}, {-1, false}} {
		s := newTestSequencer(t, ctx, Config{DSN: env.DSN, MaxIdleConns: tc.maxIdle}, 10)
		if _, _, err := s.currentTree(ctx); err != nil {
			t.Fatalf("currentTree: %v", err)
		}
		if got := s.dbPool.Stats().Idle > 0; got != tc.wantIdle {
			t.Errorf("MaxIdleConns=%d: idle connections kept = %v, want %v", tc.maxIdle, got, tc.wantIdle)
		}
	}
}

// New honours a caller-supplied S3Options even when SDKConfig is nil (upstream overwrote it).
func TestNewHonoursS3OptionsWithoutSDKConfig(t *testing.T) {
	// Not parallel: uses t.Setenv.
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_REGION", "us-east-1")
	ctx := t.Context()
	env := newPGTestEnv(t)
	var called bool
	d, err := New(ctx, Config{Bucket: "unused", DSN: env.DSN, S3Options: func(*s3.Options) { called = true }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.(*Storage).Close() })
	if _, _, err := d.(*Storage).MigrationWriter(ctx, tessera.NewMigrationOptions()); err != nil {
		t.Fatalf("MigrationWriter: %v", err)
	}
	if !called {
		t.Error("S3Options was not applied when SDKConfig is nil")
	}
}

// A partially applied schema is rejected at start-up (with SkipSchemaInit) rather than failing later.
func TestPGSequencerIncompleteSchema(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for _, tc := range []struct {
		name  string
		setup func(e *pgTestEnv)
	}{
		{"only version table", func(e *pgTestEnv) {
			e.exec(t, "CREATE TABLE Tessera (id INTEGER PRIMARY KEY, compatibilityVersion BIGINT NOT NULL)")
			e.exec(t, "INSERT INTO Tessera VALUES (0, $1)", SchemaCompatibilityVersion)
		}},
		{"missing GCCoord row", func(e *pgTestEnv) {
			e.exec(t, SchemaSQL)
			e.exec(t, "DELETE FROM GCCoord")
		}},
		{"missing Seq table", func(e *pgTestEnv) {
			e.exec(t, SchemaSQL)
			e.exec(t, "DROP TABLE Seq")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newPGTestEnv(t)
			tc.setup(env)
			s, err := newPGSequencer(ctx, Config{DSN: env.DSN, SkipSchemaInit: true}, 10)
			if err == nil {
				_ = s.close()
				t.Fatal("newPGSequencer succeeded on an incomplete schema")
			}
		})
	}
}

// The schema rejects negative sizes, which would otherwise wrap to huge uint64 values.
func TestSchemaSQLRejectsNegativeSizes(t *testing.T) {
	t.Parallel()
	env := newPGTestEnv(t)
	env.exec(t, SchemaSQL)
	for _, q := range []string{
		"UPDATE PubCoord SET size = -1",
		"UPDATE PubCoord SET publishedAt = -1",
		"UPDATE GCCoord SET fromSize = -1",
		"UPDATE SeqCoord SET next = -1",
		"UPDATE IntCoord SET seq = -1",
	} {
		if _, err := env.DB.ExecContext(t.Context(), q); err == nil {
			t.Errorf("%q succeeded, want CHECK violation", q)
		}
	}
}

// Concurrent garbage collection by two replicas never deletes the same prefix twice, i.e. the
// GCCoord row lock serialises them and the second sees the first's progress.
func TestPGGarbageCollectConcurrent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	dsnA, _ := dsnWithApp(t, env)
	dsnB, appB := dsnWithApp(t, env)
	a := newTestSequencer(t, ctx, Config{DSN: dsnA}, 10)
	b := newTestSequencer(t, ctx, Config{DSN: dsnB}, 10)

	const treeSize = 10 * layout.EntryBundleWidth
	var mu sync.Mutex
	deleted := map[string]int{}
	var once sync.Once
	del := func(ctx context.Context, prefix string) error {
		// Hold the first call until B is blocked on a lock (or a timeout, so that a missing
		// lock shows up as duplicated deletes rather than a hang).
		once.Do(func() {
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				var waiting int
				_ = requirePG(t).QueryRowContext(ctx,
					"SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event_type = 'Lock'", appB).Scan(&waiting)
				if waiting > 0 {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
		mu.Lock()
		defer mu.Unlock()
		deleted[prefix]++
		return nil
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Go(func() { errs[0] = a.garbageCollect(ctx, treeSize, 100, del, layout.EntriesPath) })
	time.Sleep(50 * time.Millisecond)
	wg.Go(func() { errs[1] = b.garbageCollect(ctx, treeSize, 100, del, layout.EntriesPath) })
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("garbageCollect[%d]: %v", i, err)
		}
	}
	if len(deleted) == 0 {
		t.Fatal("nothing was garbage collected")
	}
	for p, n := range deleted {
		if n != 1 {
			t.Errorf("prefix %q deleted %d times, want 1", p, n)
		}
	}
	if got := env.gcFromSize(t); got != treeSize {
		t.Errorf("GCCoord.fromSize = %d, want %d", got, treeSize)
	}
}

// MigrationWriter (and MigrationStorage.buildTree's SQL) can import a log: migrating a source
// log into a fresh schema+bucket reproduces the same tree.
func TestE2EMigrate(t *testing.T) {
	t.Parallel()
	srcB, dstB := newTestBucket(t), newTestBucket(t)
	srcEnv, dstEnv := newPGTestEnv(t), newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, vk := mustGenerateKeys(t)

	src := newE2EAppender(t, ctx, Config{SDKConfig: srcB.SDKConfig, S3Options: srcB.S3Options, Bucket: srcB.Name, DSN: srcEnv.DSN}, sk)
	const n = 700 // Several full bundles plus a partial one.
	want := mustAddConcurrently(t, ctx, src, "migrate", n, 8)
	srcR := &s3Reader{client: srcB.Client, bucket: srcB.Name}
	cp, _, _, err := client.FetchCheckpoint(ctx, srcR.ReadCheckpoint, vk, vk.Name())
	if err != nil {
		t.Fatalf("FetchCheckpoint: %v", err)
	}
	if cp.Size != n {
		t.Fatalf("source checkpoint size %d, want %d", cp.Size, n)
	}

	const prefix = "migrated"
	dst, err := New(ctx, Config{SDKConfig: dstB.SDKConfig, S3Options: dstB.S3Options, Bucket: dstB.Name, BucketPrefix: prefix, DSN: dstEnv.DSN})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = dst.(*Storage).Close() })
	mt, err := tessera.NewMigrationTarget(ctx, dst, tessera.NewMigrationOptions())
	if err != nil {
		t.Fatalf("NewMigrationTarget: %v", err)
	}
	if err := mt.Migrate(ctx, 4, cp.Size, cp.Hash, srcR.ReadEntryBundle); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	if gotSize, gotRoot := dstEnv.intCoord(t); gotSize != cp.Size || !bytes.Equal(gotRoot, cp.Hash) {
		t.Fatalf("migrated IntCoord = (%d, %x), want (%d, %x)", gotSize, gotRoot, cp.Size, cp.Hash)
	}
	dstR := &s3Reader{client: dstB.Client, bucket: dstB.Name, prefix: prefix}
	for i := uint64(0); i*layout.EntryBundleWidth < cp.Size; i++ {
		b, err := client.GetEntryBundle(ctx, dstR.ReadEntryBundle, i, cp.Size)
		if err != nil {
			t.Fatalf("GetEntryBundle(%d): %v", i, err)
		}
		for j, e := range b.Entries {
			idx := i*layout.EntryBundleWidth + uint64(j)
			if !bytes.Equal(e, want[idx]) {
				t.Errorf("migrated entry %d = %q, want %q", idx, e, want[idx])
			}
		}
	}
	pb, err := client.NewProofBuilder(ctx, cp.Size, dstR.ReadTile)
	if err != nil {
		t.Fatalf("NewProofBuilder: %v", err)
	}
	for _, idx := range proofSample(cp.Size) {
		p, err := pb.InclusionProof(ctx, idx)
		if err != nil {
			t.Fatalf("InclusionProof(%d): %v", idx, err)
		}
		if err := proof.VerifyInclusion(rfc6962.DefaultHasher, idx, cp.Size, rfc6962.DefaultHasher.HashLeaf(want[idx]), p, cp.Hash); err != nil {
			t.Errorf("VerifyInclusion(%d) in migrated tiles: %v", idx, err)
		}
	}
	if err := src.shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

// fakeS3 is a minimal path-style S3 endpoint implementing ListObjectsV2 (with pagination) and
// DeleteObjects, used to exercise s3Storage.deleteObjectsWithPrefix deterministically.
type fakeS3 struct {
	mu        sync.Mutex
	keys      []string
	pageSize  int
	failKeys  map[string]bool
	prefixes  []string
	deletes   int
	deletedKs []string
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Get("list-type") == "2":
		prefix := q.Get("prefix")
		f.prefixes = append(f.prefixes, prefix)
		matching := []string{}
		for _, k := range f.keys {
			if strings.HasPrefix(k, prefix) {
				matching = append(matching, k)
			}
		}
		slices.Sort(matching)
		// Like S3, the continuation token is position-independent (here: the last key returned),
		// so deleting already-listed keys between pages doesn't skip any.
		start := 0
		if tok := q.Get("continuation-token"); tok != "" {
			start, _ = slices.BinarySearch(matching, tok+"\x00")
		}
		end := min(start+f.pageSize, len(matching))
		var b strings.Builder
		fmt.Fprintf(&b, `<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name><Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys>`, prefix, end-start, f.pageSize)
		if end < len(matching) {
			fmt.Fprintf(&b, `<IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken>`, matching[end-1])
		} else {
			b.WriteString(`<IsTruncated>false</IsTruncated>`)
		}
		for _, k := range matching[start:end] {
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>1</Size></Contents>`, k)
		}
		b.WriteString(`</ListBucketResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, b.String())
	case r.Method == http.MethodPost && q.Has("delete"):
		f.deletes++
		var req struct {
			Objects []struct {
				Key string `xml:"Key"`
			} `xml:"Object"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := xml.Unmarshal(body, &req); err != nil || len(req.Objects) == 0 {
			// Real S3 rejects an empty (or malformed) Delete request.
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `<Error><Code>MalformedXML</Code><Message>bad</Message></Error>`)
			return
		}
		var b strings.Builder
		b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">`)
		for _, o := range req.Objects {
			if f.failKeys[o.Key] {
				fmt.Fprintf(&b, `<Error><Key>%s</Key><Code>AccessDenied</Code><Message>denied</Message></Error>`, o.Key)
				continue
			}
			f.deletedKs = append(f.deletedKs, o.Key)
			f.keys = slices.DeleteFunc(f.keys, func(k string) bool { return k == o.Key })
		}
		b.WriteString(`</DeleteResult>`)
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, b.String())
	default:
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.String(), http.StatusNotImplemented)
	}
}

func newFakeS3Storage(t *testing.T, f *fakeS3, prefix string) *s3Storage {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c := s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("a", "b", "")}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.UsePathStyle = true
	})
	return &s3Storage{s3Client: c, bucket: "b", bucketPrefix: prefix}
}

func TestDeleteObjectsWithPrefix(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	t.Run("nothing to delete sends no DeleteObjects", func(t *testing.T) {
		f := &fakeS3{keys: []string{"pfx/tile/0/001.p/1"}, pageSize: 1000}
		s := newFakeS3Storage(t, f, "pfx")
		if err := s.deleteObjectsWithPrefix(ctx, "tile/0/000.p/"); err != nil {
			t.Fatalf("deleteObjectsWithPrefix: %v", err)
		}
		if f.deletes != 0 {
			t.Errorf("%d DeleteObjects requests sent for an empty listing, want 0", f.deletes)
		}
		if want := []string{"pfx/tile/0/000.p/"}; !slices.Equal(f.prefixes, want) {
			t.Errorf("listed prefixes %q, want %q (prefixed, trailing slash kept)", f.prefixes, want)
		}
	})

	t.Run("all pages are deleted", func(t *testing.T) {
		f := &fakeS3{pageSize: 2}
		for i := range 5 {
			f.keys = append(f.keys, fmt.Sprintf("tile/0/000.p/%d", i+1))
		}
		f.keys = append(f.keys, "tile/0/000", "tile/0/001.p/1")
		s := newFakeS3Storage(t, f, "")
		if err := s.deleteObjectsWithPrefix(ctx, "tile/0/000.p/"); err != nil {
			t.Fatalf("deleteObjectsWithPrefix: %v", err)
		}
		if want := []string{"tile/0/000", "tile/0/001.p/1"}; !slices.Equal(f.keys, want) {
			t.Errorf("remaining keys %q, want %q", f.keys, want)
		}
	})

	t.Run("per-key delete errors are returned", func(t *testing.T) {
		f := &fakeS3{pageSize: 1000, keys: []string{"tile/0/000.p/1", "tile/0/000.p/2"}, failKeys: map[string]bool{"tile/0/000.p/2": true}}
		s := newFakeS3Storage(t, f, "")
		err := s.deleteObjectsWithPrefix(ctx, "tile/0/000.p/")
		if err == nil || !strings.Contains(err.Error(), "tile/0/000.p/2") {
			t.Fatalf("deleteObjectsWithPrefix error = %v, want error naming the failed key", err)
		}
	})
}

// The driver must not depend on the server's default_transaction_isolation: platforms may default
// to REPEATABLE READ or SERIALIZABLE, under which the READ COMMITTED locking protocol would fail
// with serialization errors (SQLSTATE 40001) unless the isolation level is set explicitly.
func TestIsolationLevelIndependent(t *testing.T) {
	t.Parallel()
	for _, iso := range []string{"repeatable read", "serializable"} {
		t.Run(iso, func(t *testing.T) {
			t.Parallel()
			params := map[string]string{"default_transaction_isolation": iso}

			// Assign + consume from several replicas.
			t.Run("stress", func(t *testing.T) { runMultiReplicaStress(t, params) })

			t.Run("init publish gc", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
				defer cancel()
				env := newPGTestEnv(t)
				dsn := dsnWithParams(t, env.DSN, params)
				var verify int
				if err := openTestDB(t, dsn, 1).QueryRowContext(ctx, "SELECT count(*) FROM pg_settings WHERE name = 'default_transaction_isolation' AND setting = $1", iso).Scan(&verify); err != nil || verify != 1 {
					t.Fatalf("session default_transaction_isolation not %q (err=%v)", iso, err)
				}

				// Concurrent schema initialisation.
				const n = 6
				seqs := make([]*pgSequencer, n)
				errs := make([]error, n)
				var wg sync.WaitGroup
				for i := range n {
					wg.Go(func() { seqs[i], errs[i] = newPGSequencer(ctx, Config{DSN: dsn, MaxOpenConns: 4}, 10) })
				}
				wg.Wait()
				for i := range n {
					if errs[i] != nil {
						t.Fatalf("concurrent newPGSequencer[%d]: %v", i, errs[i])
					}
					t.Cleanup(func() { _ = seqs[i].close() })
				}
				env.exec(t, "UPDATE IntCoord SET seq = $1", 4*layout.EntryBundleWidth)

				// Concurrent checkpoint publication and garbage collection on every replica.
				var published sync.Map
				deadline := time.Now().Add(1500 * time.Millisecond)
				for i := range n {
					wg.Go(func() {
						for time.Now().Before(deadline) {
							if _, err := seqs[i].publishCheckpoint(ctx, 0, 0, func(_ context.Context, size uint64, _ []byte) error {
								published.Store(size, true)
								return nil
							}); err != nil {
								t.Errorf("replica %d publishCheckpoint: %v", i, err)
								return
							}
							if err := seqs[i].garbageCollect(ctx, 4*layout.EntryBundleWidth, 2, func(context.Context, string) error { return nil }, layout.EntriesPath); err != nil {
								t.Errorf("replica %d garbageCollect: %v", i, err)
								return
							}
						}
					})
				}
				wg.Wait()
				if _, ok := published.Load(uint64(4 * layout.EntryBundleWidth)); !ok {
					t.Error("checkpoint for the integrated size was never published")
				}
				if got := env.gcFromSize(t); got != 4*layout.EntryBundleWidth {
					t.Errorf("GCCoord.fromSize = %d, want %d", got, 4*layout.EntryBundleWidth)
				}
			})
		})
	}
}
