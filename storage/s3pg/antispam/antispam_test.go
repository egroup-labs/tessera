// Copyright 2025 The Tessera authors. All Rights Reserved.
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

package antispam

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/storage/s3pg"
	"github.com/transparency-dev/tessera/testonly"
)

// TestMain inits flags and runs tests.
func TestMain(m *testing.M) {
	// m.Run() will parse flags
	os.Exit(m.Run())
}

// antispamTables are the (lower-cased) names of the tables created by SchemaSQL.
var antispamTables = []string{"antispammeta", "antispamidseq", "antispamfollowcoord"}

// TestAntispam is a port of the upstream end-to-end test: entries added via the decorator are
// picked up by the follower, and re-adding one is reported as a duplicate at its original index.
func TestAntispam(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn, _ := mustCreateSchema(t)
	as, err := NewAntispam(ctx, dsn, AntispamOpts{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = as.Close() })
	fl, shutdown := testonly.NewTestLog(t, tessera.NewAppendOptions().WithCheckpointInterval(time.Second))
	defer func() {
		if err := shutdown(t.Context()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()
	addFn := as.Decorator()(fl.Appender.Add)
	follower := as.Follower(testBundleHasher)
	startFollower(t, follower, fl.LogReader)

	pos, err := follower.EntriesProcessed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 0 {
		t.Error("expected initial position to be 0")
	}

	a := tessera.NewPublicationAwaiter(t.Context(), fl.LogReader.ReadCheckpoint, time.Second)
	var idx1 tessera.Index
	idxf1 := addFn(ctx, tessera.NewEntry([]byte("one")))
	if _, _, err := a.Await(t.Context(), idxf1); err != nil {
		t.Fatalf("Await(1): %v", err)
	}

	idxf2 := addFn(ctx, tessera.NewEntry([]byte("two")))
	if _, _, err := a.Await(t.Context(), idxf2); err != nil {
		t.Fatalf("Await(2): %v", err)
	}

	if idx1, err = idxf1(); err != nil {
		t.Fatal(err)
	}
	if _, err := idxf2(); err != nil {
		t.Fatal(err)
	}
	waitForEntriesProcessed(t, follower, 2, time.Minute)
	dupIdx, err := addFn(ctx, tessera.NewEntry([]byte("one")))()
	if err != nil {
		t.Error(err)
	}
	if !dupIdx.IsDup {
		t.Error("expected dupe but it wasn't marked as such")
	}
	if dupIdx.Index != idx1.Index {
		t.Errorf("expected idx %d but got %d", idx1.Index, dupIdx.Index)
	}
}

// TestAntispamPushbackRecovers is a port of the upstream test: a follower which starts behind by more
// than PushbackThreshold leaves pushback mode once it has caught up.
func TestAntispamPushbackRecovers(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn, _ := mustCreateSchema(t)
	as, err := NewAntispam(ctx, dsn, AntispamOpts{
		PushbackThreshold: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = as.Close() })
	fl, shutdown := testonly.NewTestLog(t, tessera.NewAppendOptions().WithCheckpointInterval(time.Second))
	defer func() {
		if err := shutdown(t.Context()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	}()
	addFn := as.Decorator()(fl.Appender.Add)
	follower := as.Follower(testBundleHasher)
	a := tessera.NewPublicationAwaiter(t.Context(), fl.LogReader.ReadCheckpoint, time.Second)
	idxf1 := addFn(ctx, tessera.NewEntry([]byte("one")))
	if _, _, err := a.Await(t.Context(), idxf1); err != nil {
		t.Fatalf("Await(1): %v", err)
	}

	idxf2 := addFn(ctx, tessera.NewEntry([]byte("two")))
	if _, _, err := a.Await(t.Context(), idxf2); err != nil {
		t.Fatalf("Await(2): %v", err)
	}

	startFollower(t, follower, fl.LogReader)

	waitForEntriesProcessed(t, follower, 2, time.Minute)

	// Ensure that the follower gets itself _out_ of pushback mode once it's caught up.
	// We'll give the follower some time to do its thing and notice.
	// It runs onces a second, so this should be plenty of time.
	for i := range 5 {
		time.Sleep(time.Second)
		if !as.pushBack.Load() {
			t.Logf("Antispam caught up and out of pushback in %ds", i)
			return
		}
	}
	t.Fatalf("pushBack remains true after 5 seconds despite being caught up!")
}

// TestAntispamIndexError is a port of the upstream test: lookups on an empty index return nil, nil,
// and lookups against a closed pool return an error.
func TestAntispamIndexError(t *testing.T) {
	t.Parallel()
	dsn, _ := mustCreateSchema(t)
	as, err := NewAntispam(t.Context(), dsn, AntispamOpts{
		PushbackThreshold: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = as.Close() })

	// DB should be empty, try to read an index and expect no index and no error.
	i, err := as.index(t.Context(), []byte("hash"))
	if i != nil || err != nil {
		t.Fatalf("got %v, %v, want nil, nil", i, err)
	}

	// Close the DB, and try to read an index. Should see an error.
	if err := as.dbPool.Close(); err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}
	if _, err := as.index(t.Context(), []byte("hash")); err == nil {
		t.Fatal("Got no error, want error")
	}
}

// TestDecoratorPushback checks that the decorator rejects adds with ErrPushbackAntispam, without
// calling the delegate, while in pushback mode, and passes non-duplicates through otherwise.
func TestDecoratorPushback(t *testing.T) {
	t.Parallel()
	dsn, _ := mustCreateSchema(t)
	as := mustNewAntispam(t, dsn, AntispamOpts{})
	called := 0
	addFn := as.Decorator()(func(context.Context, *tessera.Entry) tessera.IndexFuture {
		called++
		return func() (tessera.Index, error) { return tessera.Index{Index: 123}, nil }
	})

	as.pushBack.Store(true)
	if _, err := addFn(t.Context(), tessera.NewEntry([]byte("x")))(); !errors.Is(err, tessera.ErrPushbackAntispam) {
		t.Errorf("in pushback: got err %v, want %v", err, tessera.ErrPushbackAntispam)
	}
	if called != 0 {
		t.Errorf("in pushback: delegate called %d times, want 0", called)
	}

	as.pushBack.Store(false)
	idx, err := addFn(t.Context(), tessera.NewEntry([]byte("x")))()
	if err != nil {
		t.Fatalf("not in pushback: %v", err)
	}
	if idx.IsDup || idx.Index != 123 || called != 1 {
		t.Errorf("not in pushback: got %+v (delegate called %d times), want {Index:123 IsDup:false} via delegate", idx, called)
	}
}

// TestConcurrentInit checks that many instances initialising the same empty schema concurrently all
// succeed, and that the schema ends up with exactly one copy of each initial row.
func TestConcurrentInit(t *testing.T) {
	t.Parallel()
	dsn, schema := mustCreateSchema(t)
	const n = 10
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make([]error, n)
		ass   = make([]*AntispamStorage, n)
	)
	for i := range n {
		wg.Go(func() {
			<-start
			ass[i], errs[i] = NewAntispam(t.Context(), dsn, AntispamOpts{})
		})
	}
	close(start)
	wg.Wait()
	for i := range n {
		if ass[i] != nil {
			t.Cleanup(func() { _ = ass[i].Close() })
		}
		if errs[i] != nil {
			t.Errorf("NewAntispam #%d: %v", i, errs[i])
		}
	}

	db := mustOpenDB(t, dsn)
	if got, want := schemaTables(t, db, schema), setOf(antispamTables...); !maps.Equal(got, want) {
		t.Errorf("tables = %v, want %v", got, want)
	}
	for _, tbl := range []string{"AntispamMeta", "AntispamFollowCoord"} {
		var c int
		if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+tbl).Scan(&c); err != nil {
			t.Fatalf("count %s: %v", tbl, err)
		}
		if c != 1 {
			t.Errorf("%s has %d rows, want 1", tbl, c)
		}
	}
}

// TestSkipSchemaInit checks that SkipSchemaInit neither creates nor tolerates a missing schema, and
// works once SchemaSQL has been applied externally.
func TestSkipSchemaInit(t *testing.T) {
	t.Parallel()
	dsn, schema := mustCreateSchema(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	if as, err := NewAntispam(ctx, dsn, AntispamOpts{SkipSchemaInit: true}); err == nil {
		_ = as.Close()
		t.Fatal("NewAntispam(SkipSchemaInit) on an empty schema succeeded, want error")
	} else {
		t.Logf("NewAntispam(SkipSchemaInit) on an empty schema: %v", err)
	}
	db := mustOpenDB(t, dsn)
	if got := schemaTables(t, db, schema); len(got) != 0 {
		t.Fatalf("SkipSchemaInit created tables: %v", got)
	}

	if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
		t.Fatalf("applying SchemaSQL: %v", err)
	}
	as := mustNewAntispam(t, dsn, AntispamOpts{SkipSchemaInit: true})
	if n, err := as.Follower(testBundleHasher).EntriesProcessed(ctx); err != nil || n != 0 {
		t.Errorf("EntriesProcessed() = %d, %v, want 0, nil", n, err)
	}
}

// TestCompatibilityMismatch checks that construction fails if the stored compatibility version
// differs from SchemaCompatibilityVersion, and that initialisation does not overwrite it.
func TestCompatibilityMismatch(t *testing.T) {
	t.Parallel()
	dsn, _ := mustCreateSchema(t)
	ctx := t.Context()
	db := mustOpenDB(t, dsn)
	if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
		t.Fatalf("applying SchemaSQL: %v", err)
	}
	const badVersion = SchemaCompatibilityVersion + 1
	if _, err := db.ExecContext(ctx, "UPDATE AntispamMeta SET compatibilityVersion = $1 WHERE id = 0", badVersion); err != nil {
		t.Fatalf("UPDATE AntispamMeta: %v", err)
	}

	for _, skip := range []bool{false, true} {
		as, err := NewAntispam(ctx, dsn, AntispamOpts{SkipSchemaInit: skip})
		if err == nil {
			_ = as.Close()
			t.Errorf("NewAntispam(SkipSchemaInit=%v) succeeded, want compatibility error", skip)
		} else if !strings.Contains(err.Error(), "compatib") {
			t.Errorf("NewAntispam(SkipSchemaInit=%v) = %v, want compatibility error", skip, err)
		}
		if as, err := NewAntispamWithDB(ctx, db, AntispamOpts{SkipSchemaInit: skip}); err == nil {
			_ = as.Close()
			t.Errorf("NewAntispamWithDB(SkipSchemaInit=%v) succeeded, want compatibility error", skip)
		}
	}

	var got uint64
	if err := db.QueryRowContext(ctx, "SELECT compatibilityVersion FROM AntispamMeta WHERE id = 0").Scan(&got); err != nil {
		t.Fatalf("read compatibilityVersion: %v", err)
	}
	if got != badVersion {
		t.Errorf("initialisation changed stored compatibilityVersion to %d, want %d", got, badVersion)
	}
}

// TestSchemaSQL checks that SchemaSQL is idempotent, seeds exactly one AntispamMeta row with
// SchemaCompatibilityVersion and one AntispamFollowCoord row at 0, and sets fillfactor on the
// coordination table.
func TestSchemaSQL(t *testing.T) {
	t.Parallel()
	dsn, schema := mustCreateSchema(t)
	ctx := t.Context()
	db := mustOpenDB(t, dsn)
	for i := range 2 {
		if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
			t.Fatalf("applying SchemaSQL (#%d): %v", i, err)
		}
	}
	if got, want := schemaTables(t, db, schema), setOf(antispamTables...); !maps.Equal(got, want) {
		t.Errorf("tables = %v, want %v", got, want)
	}

	var n int
	var version uint64
	if err := db.QueryRowContext(ctx, "SELECT count(*), max(compatibilityVersion) FROM AntispamMeta WHERE id = 0").Scan(&n, &version); err != nil {
		t.Fatalf("read AntispamMeta: %v", err)
	}
	if n != 1 || version != SchemaCompatibilityVersion {
		t.Errorf("AntispamMeta: %d rows with version %d, want 1 row with version %d", n, version, SchemaCompatibilityVersion)
	}
	var nextIdx uint64
	if err := db.QueryRowContext(ctx, "SELECT count(*), max(nextIdx) FROM AntispamFollowCoord WHERE id = 0").Scan(&n, &nextIdx); err != nil {
		t.Fatalf("read AntispamFollowCoord: %v", err)
	}
	if n != 1 || nextIdx != 0 {
		t.Errorf("AntispamFollowCoord: %d rows with nextIdx %d, want 1 row with nextIdx 0", n, nextIdx)
	}

	var relOpts sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT array_to_string(reloptions, ',') FROM pg_class WHERE oid = 'AntispamFollowCoord'::regclass").Scan(&relOpts); err != nil {
		t.Fatalf("read reloptions: %v", err)
	}
	if !strings.Contains(relOpts.String, "fillfactor=50") {
		t.Errorf("AntispamFollowCoord reloptions = %q, want fillfactor=50", relOpts.String)
	}
}

// TestReinitDoesNotReset checks that re-initialising an existing schema (via the constructors or by
// re-applying SchemaSQL) does not reset the follower position or the index contents.
func TestReinitDoesNotReset(t *testing.T) {
	t.Parallel()
	dsn, _ := mustCreateSchema(t)
	ctx := t.Context()
	_ = mustNewAntispam(t, dsn, AntispamOpts{})
	db := mustOpenDB(t, dsn)
	if _, err := db.ExecContext(ctx, "UPDATE AntispamFollowCoord SET nextIdx = 42 WHERE id = 0"); err != nil {
		t.Fatalf("UPDATE AntispamFollowCoord: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO AntispamIDSeq (h, idx) VALUES ($1, $2)", testIDHash([]byte("x")), 7); err != nil {
		t.Fatalf("INSERT AntispamIDSeq: %v", err)
	}

	as2 := mustNewAntispam(t, dsn, AntispamOpts{})
	if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
		t.Fatalf("re-applying SchemaSQL: %v", err)
	}
	as3, err := NewAntispamWithDB(ctx, db, AntispamOpts{})
	if err != nil {
		t.Fatalf("NewAntispamWithDB: %v", err)
	}
	for i, as := range []*AntispamStorage{as2, as3} {
		if n, err := as.Follower(testBundleHasher).EntriesProcessed(ctx); err != nil || n != 42 {
			t.Errorf("#%d: EntriesProcessed() = %d, %v, want 42, nil", i, n, err)
		}
		if idx, err := as.index(ctx, testIDHash([]byte("x"))); err != nil || idx == nil || *idx != 7 {
			t.Errorf("#%d: index(x) = %v, %v, want 7, nil", i, idx, err)
		}
	}
}

// TestFollowerDuplicatesKeepFirstIndex checks that when the log contains duplicate entries, within a
// single follower batch and/or across batches, the index keeps the FIRST index (INSERT IGNORE semantics),
// and that the follower advances in steps of MaxBatchSize.
func TestFollowerDuplicatesKeepFirstIndex(t *testing.T) {
	t.Parallel()
	skipIfNoPG(t)
	// Each of the 23 values appears 3 times in a row (duplicates within a batch), and recurs every 69
	// entries (duplicates across batches).
	const N = 150
	entryAt := func(i uint64) []byte { return fmt.Appendf(nil, "dup-%d", (i/3)%23) }
	want := make(map[string]uint64)
	for i := range uint64(N) {
		if k := hex.EncodeToString(testIDHash(entryAt(i))); !hasKey(want, k) {
			want[k] = i
		}
	}

	for _, bs := range []uint{1, 5, 0, 1000} {
		t.Run(fmt.Sprintf("MaxBatchSize=%d", bs), func(t *testing.T) {
			t.Parallel()
			dsn, _ := mustCreateSchema(t)
			as := mustNewAntispam(t, dsn, AntispamOpts{MaxBatchSize: bs})
			f := as.Follower(testBundleHasher)
			startFollower(t, f, newFakeLogReader(N, entryAt))
			seen := waitForEntriesProcessed(t, f, N, 2*time.Minute)

			step := uint64(bs)
			if step == 0 {
				step = DefaultMaxBatchSize
			}
			for _, v := range seen {
				if v != N && v%step != 0 {
					t.Errorf("observed EntriesProcessed=%d, want multiples of MaxBatchSize %d (seen %v)", v, step, seen)
				}
			}
			if got := readIDSeq(t, mustOpenDB(t, dsn)); !maps.Equal(got, want) {
				t.Errorf("AntispamIDSeq = %v, want %v", got, want)
			}

			addFn := as.Decorator()(func(context.Context, *tessera.Entry) tessera.IndexFuture {
				t.Error("delegate called for a duplicate")
				return func() (tessera.Index, error) { return tessera.Index{}, errors.New("unexpected") }
			})
			for v := range uint64(23) {
				e := entryAt(3 * v)
				idx, err := addFn(t.Context(), tessera.NewEntry(e))()
				if err != nil {
					t.Fatalf("add(%q): %v", e, err)
				}
				if !idx.IsDup || idx.Index != 3*v {
					t.Errorf("add(%q) = %+v, want {Index:%d IsDup:true}", e, idx, 3*v)
				}
			}
		})
	}
}

// TestFollowerLargeBatch checks that a single follower step can insert more entries than a multi-row
// VALUES statement could carry within PostgreSQL's 65535 bind parameter limit.
func TestFollowerLargeBatch(t *testing.T) {
	t.Parallel()
	const N = 40000
	dsn, _ := mustCreateSchema(t)
	as := mustNewAntispam(t, dsn, AntispamOpts{MaxBatchSize: N})
	entryAt := func(i uint64) []byte { return fmt.Appendf(nil, "large-%d", i) }
	f := as.Follower(testBundleHasher)
	// The whole log is integrated before the follower starts, so it should be consumed in one step.
	start := time.Now()
	startFollower(t, f, newFakeLogReader(N, entryAt))
	seen := waitForEntriesProcessed(t, f, N, 2*time.Minute)
	t.Logf("followed %d entries in %v", N, time.Since(start))
	for _, v := range seen {
		if v != 0 && v != N {
			t.Errorf("observed EntriesProcessed=%d, want a single step 0 -> %d (seen %v)", v, N, seen)
		}
	}

	got := readIDSeq(t, mustOpenDB(t, dsn))
	if len(got) != N {
		t.Fatalf("AntispamIDSeq has %d rows, want %d", len(got), N)
	}
	for i := range uint64(N) {
		if k := hex.EncodeToString(testIDHash(entryAt(i))); got[k] != i {
			t.Fatalf("AntispamIDSeq[entry %d] = %d, want %d", i, got[k], i)
		}
	}
}

// TestFollowerLargeIndices checks that indices beyond 2^32 round-trip through the bulk insert, the
// follow coordination row and lookups.
func TestFollowerLargeIndices(t *testing.T) {
	t.Parallel()
	const (
		from = uint64(1)<<40 + 10
		size = uint64(1)<<40 + 300
	)
	dsn, _ := mustCreateSchema(t)
	ctx := t.Context()
	as := mustNewAntispam(t, dsn, AntispamOpts{})
	db := mustOpenDB(t, dsn)
	// Pretend that everything before from has already been followed.
	if _, err := db.ExecContext(ctx, "UPDATE AntispamFollowCoord SET nextIdx = $1 WHERE id = 0", from); err != nil {
		t.Fatalf("UPDATE AntispamFollowCoord: %v", err)
	}
	entryAt := func(i uint64) []byte { return fmt.Appendf(nil, "big-%d", i) }
	f := as.Follower(testBundleHasher)
	startFollower(t, f, newFakeLogReader(size, entryAt))
	waitForEntriesProcessed(t, f, size, 2*time.Minute)

	got := readIDSeq(t, db)
	if len(got) != int(size-from) {
		t.Fatalf("AntispamIDSeq has %d rows, want %d", len(got), size-from)
	}
	for i := from; i < size; i++ {
		if k := hex.EncodeToString(testIDHash(entryAt(i))); got[k] != i {
			t.Fatalf("AntispamIDSeq[entry %d] = %d, want %d", i, got[k], i)
		}
	}
	addFn := as.Decorator()(func(context.Context, *tessera.Entry) tessera.IndexFuture {
		t.Error("delegate called for a duplicate")
		return func() (tessera.Index, error) { return tessera.Index{}, errors.New("unexpected") }
	})
	if idx, err := addFn(ctx, tessera.NewEntry(entryAt(size-1)))(); err != nil || !idx.IsDup || idx.Index != size-1 {
		t.Errorf("add(last) = %+v, %v, want {Index:%d IsDup:true}, nil", idx, err, size-1)
	}
}

// TestNewAntispamWithDB checks that an externally provided handle is used as-is: pool sizes are not
// applied, and it is closed neither on construction failure nor by Close().
func TestNewAntispamWithDB(t *testing.T) {
	t.Parallel()
	dsn, _ := mustCreateSchema(t)
	ctx := t.Context()
	db := mustOpenDB(t, dsn)

	if as, err := NewAntispamWithDB(ctx, db, AntispamOpts{SkipSchemaInit: true}); err == nil {
		_ = as.Close()
		t.Fatal("NewAntispamWithDB(SkipSchemaInit) on an empty schema succeeded, want error")
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("external db unusable after failed construction: %v", err)
	}

	as, err := NewAntispamWithDB(ctx, db, AntispamOpts{MaxOpenConns: 3, MaxIdleConns: 1})
	if err != nil {
		t.Fatalf("NewAntispamWithDB: %v", err)
	}
	if as.dbPool != db {
		t.Error("NewAntispamWithDB did not use the provided handle")
	}
	if got := db.Stats().MaxOpenConnections; got != 0 {
		t.Errorf("MaxOpenConns applied to external handle: MaxOpenConnections = %d, want 0", got)
	}
	if err := as.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("external db closed by Close(): %v", err)
	}
	if n, err := as.Follower(testBundleHasher).EntriesProcessed(ctx); err != nil || n != 0 {
		t.Errorf("after Close(): EntriesProcessed() = %d, %v, want 0, nil", n, err)
	}
}

// TestCloseClosesOwnedPool checks that pool sizes are applied to, and Close() closes, a pool opened
// from a DSN.
func TestCloseClosesOwnedPool(t *testing.T) {
	t.Parallel()
	dsn, _ := mustCreateSchema(t)
	ctx := t.Context()
	as, err := NewAntispam(ctx, dsn, AntispamOpts{MaxOpenConns: 3})
	if err != nil {
		t.Fatalf("NewAntispam: %v", err)
	}
	if got := as.dbPool.Stats().MaxOpenConnections; got != 3 {
		t.Errorf("MaxOpenConnections = %d, want 3", got)
	}
	if err := as.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := as.dbPool.PingContext(ctx); err == nil {
		t.Fatal("pool still usable after Close(), want closed")
	}
}

// TestCoexistsWithLogSchema checks that the antispam tables can share a PostgreSQL schema with the
// s3pg log tables, whichever is initialised first (or concurrently), without either affecting the other.
func TestCoexistsWithLogSchema(t *testing.T) {
	t.Parallel()
	skipIfNoPG(t)
	logTables := []string{"tessera", "seqcoord", "seq", "intcoord", "pubcoord", "gccoord"}
	applyLogSchema := func(t *testing.T, db *sql.DB) {
		t.Helper()
		if _, err := db.ExecContext(t.Context(), s3pg.SchemaSQL); err != nil {
			t.Errorf("applying s3pg.SchemaSQL: %v", err)
		}
	}
	for _, order := range []string{"log first", "antispam first", "concurrently"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			dsn, schema := mustCreateSchema(t)
			ctx := t.Context()
			db := mustOpenDB(t, dsn)
			var as *AntispamStorage
			switch order {
			case "log first":
				applyLogSchema(t, db)
				as = mustNewAntispam(t, dsn, AntispamOpts{})
			case "antispam first":
				as = mustNewAntispam(t, dsn, AntispamOpts{})
				applyLogSchema(t, db)
			case "concurrently":
				var wg sync.WaitGroup
				start := make(chan struct{})
				wg.Go(func() { <-start; applyLogSchema(t, db) })
				ass := make([]*AntispamStorage, 5)
				errs := make([]error, len(ass))
				for i := range ass {
					wg.Go(func() { <-start; ass[i], errs[i] = NewAntispam(ctx, dsn, AntispamOpts{}) })
				}
				close(start)
				wg.Wait()
				for i := range ass {
					if ass[i] != nil {
						t.Cleanup(func() { _ = ass[i].Close() })
					}
					if errs[i] != nil {
						t.Fatalf("NewAntispam #%d: %v", i, errs[i])
					}
				}
				as = ass[0]
			}
			// Re-initialise both: neither must interfere with the other's tables or rows.
			applyLogSchema(t, db)
			_ = mustNewAntispam(t, dsn, AntispamOpts{})

			if got, want := schemaTables(t, db, schema), setOf(append(logTables, antispamTables...)...); !maps.Equal(got, want) {
				t.Errorf("tables = %v, want %v", got, want)
			}
			var logVersion, asVersion uint64
			if err := db.QueryRowContext(ctx, "SELECT compatibilityVersion FROM Tessera WHERE id = 0").Scan(&logVersion); err != nil {
				t.Fatalf("read Tessera: %v", err)
			}
			if err := db.QueryRowContext(ctx, "SELECT compatibilityVersion FROM AntispamMeta WHERE id = 0").Scan(&asVersion); err != nil {
				t.Fatalf("read AntispamMeta: %v", err)
			}
			if logVersion != s3pg.SchemaCompatibilityVersion || asVersion != SchemaCompatibilityVersion {
				t.Errorf("compatibility versions: log %d, antispam %d; want %d, %d", logVersion, asVersion, s3pg.SchemaCompatibilityVersion, SchemaCompatibilityVersion)
			}

			// The antispam index works in the shared schema.
			const N = 10
			entryAt := func(i uint64) []byte { return fmt.Appendf(nil, "shared-%d", i) }
			f := as.Follower(testBundleHasher)
			startFollower(t, f, newFakeLogReader(N, entryAt))
			waitForEntriesProcessed(t, f, N, time.Minute)
			if got := readIDSeq(t, db); len(got) != N {
				t.Errorf("AntispamIDSeq has %d rows, want %d", len(got), N)
			}
		})
	}
}

func setOf(s ...string) map[string]bool {
	r := make(map[string]bool, len(s))
	for _, v := range s {
		r[v] = true
	}
	return r
}

func hasKey[K comparable, V any](m map[K]V, k K) bool {
	_, ok := m[k]
	return ok
}

func testIDHash(d []byte) []byte {
	r := sha256.Sum256(d)
	return r[:]
}

func testBundleHasher(b []byte) ([][]byte, error) {
	bun := &api.EntryBundle{}
	err := bun.UnmarshalText(b)
	if err != nil {
		return nil, err
	}
	r := make([][]byte, len(bun.Entries))
	for i, e := range bun.Entries {
		r[i] = testIDHash(e)
	}
	return r, err
}
