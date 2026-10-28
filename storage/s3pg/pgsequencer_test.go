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

// PostgreSQL-specific sequencer tests. Comments of the form "Spec N" refer to the numbered items
// in "Required behaviour of the PostgreSQL sequencer" of the s3pg port spec.

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	storage "github.com/transparency-dev/tessera/storage/internal"
)

// wantTables are the (lowercased) tables a schema initialised with SchemaSQL contains.
var wantTables = []string{"gccoord", "intcoord", "pubcoord", "seq", "seqcoord", "tessera"}

// fakeRoot is the deterministic "root hash" returned by test consumeFuncs for a tree of the given size.
func fakeRoot(size uint64) []byte {
	return fmt.Appendf(nil, "root-%d", size)
}

// failIfCalled returns a consumeFunc which fails the test if it's called.
func failIfCalled(t *testing.T) consumeFunc {
	return func(_ context.Context, from uint64, entries []storage.SequencedEntry) ([]byte, error) {
		t.Errorf("consumeFunc unexpectedly called with from=%d, %d entries", from, len(entries))
		return nil, errors.New("unexpected call")
	}
}

// assignData assigns entries with the provided data as one batch, returning the assigned indices.
func assignData(t *testing.T, ctx context.Context, s *pgSequencer, data ...string) []uint64 {
	t.Helper()
	entries := make([]*tessera.Entry, len(data))
	for i, d := range data {
		entries[i] = tessera.NewEntry([]byte(d))
	}
	if err := s.assignEntries(ctx, entries); err != nil {
		t.Fatalf("assignEntries: %v", err)
	}
	r := make([]uint64, len(entries))
	for i, e := range entries {
		if e.Index() == nil {
			t.Fatalf("entry %d has no index after assignEntries", i)
		}
		r[i] = *e.Index()
	}
	return r
}

// batchData returns n distinct data strings with the provided prefix.
func batchData(prefix string, n int) []string {
	r := make([]string, n)
	for i := range r {
		r[i] = fmt.Sprintf("%s %d", prefix, i)
	}
	return r
}

// seqRange returns [from, from+n).
func seqRange(from uint64, n int) []uint64 {
	r := make([]uint64, n)
	for i := range r {
		r[i] = from + uint64(i)
	}
	return r
}

// consumeOnce calls consumeEntries with a recording consumeFunc which returns fakeRoot, and
// returns the from/entries it was called with (calls == 0 if it wasn't called).
func consumeOnce(t *testing.T, ctx context.Context, s *pgSequencer, limit uint64, force bool) (more bool, calls int, from uint64, entries []storage.SequencedEntry) {
	t.Helper()
	more, err := s.consumeEntries(ctx, limit, func(_ context.Context, f uint64, e []storage.SequencedEntry) ([]byte, error) {
		calls++
		from, entries = f, slices.Clone(e)
		return fakeRoot(f + uint64(len(e))), nil
	}, force)
	if err != nil {
		t.Fatalf("consumeEntries: %v", err)
	}
	return more, calls, from, entries
}

// wantBatch returns the SequencedEntries expected for data assigned contiguously from idx.
func wantBatch(idx uint64, data ...string) []storage.SequencedEntry {
	r := make([]storage.SequencedEntry, len(data))
	for i, d := range data {
		r[i] = wantSequenced([]byte(d), idx+uint64(i))
	}
	return r
}

// checkTree checks the tree state as seen by both the sequencer and the IntCoord table.
func checkTree(t *testing.T, ctx context.Context, env *pgTestEnv, s *pgSequencer, wantSize uint64, wantRoot []byte) {
	t.Helper()
	size, root, err := s.currentTree(ctx)
	if err != nil {
		t.Fatalf("currentTree: %v", err)
	}
	if size != wantSize || !bytes.Equal(root, wantRoot) {
		t.Errorf("currentTree = (%d, %q), want (%d, %q)", size, root, wantSize, wantRoot)
	}
	dbSize, dbRoot := env.intCoord(t)
	if dbSize != wantSize || !bytes.Equal(dbRoot, wantRoot) {
		t.Errorf("IntCoord = (%d, %q), want (%d, %q)", dbSize, dbRoot, wantSize, wantRoot)
	}
}

// checkSeqKeys checks the keys of the rows in Seq.
func checkSeqKeys(t *testing.T, env *pgTestEnv, want []uint64) {
	t.Helper()
	if diff := cmp.Diff(want, env.seqKeys(t)); diff != "" {
		t.Errorf("Seq keys mismatch (-want +got):\n%s", diff)
	}
}

// checkNext checks the next index as seen by both the sequencer and the SeqCoord table.
func checkNext(t *testing.T, ctx context.Context, env *pgTestEnv, s *pgSequencer, want uint64) {
	t.Helper()
	got, err := s.nextIndex(ctx)
	if err != nil {
		t.Fatalf("nextIndex: %v", err)
	}
	if got != want {
		t.Errorf("nextIndex = %d, want %d", got, want)
	}
	if got := env.seqCoordNext(t); got != want {
		t.Errorf("SeqCoord.next = %d, want %d", got, want)
	}
}

// checkInitialState checks that the schema holds exactly the seed rows of a fresh log.
func checkInitialState(t *testing.T, env *pgTestEnv) {
	t.Helper()
	if diff := cmp.Diff(wantTables, env.tableNames(t)); diff != "" {
		t.Errorf("tables mismatch (-want +got):\n%s", diff)
	}
	for _, tbl := range []string{"Tessera", "SeqCoord", "IntCoord", "PubCoord", "GCCoord"} {
		if n := env.queryUint64(t, "SELECT count(*) FROM "+tbl); n != 1 {
			t.Errorf("%s has %d rows, want 1", tbl, n)
		}
	}
	if v := env.queryUint64(t, "SELECT compatibilityVersion FROM Tessera WHERE id = 0"); v != SchemaCompatibilityVersion {
		t.Errorf("Tessera.compatibilityVersion = %d, want %d", v, SchemaCompatibilityVersion)
	}
	if n := env.seqCoordNext(t); n != 0 {
		t.Errorf("SeqCoord.next = %d, want 0", n)
	}
	if size, root := env.intCoord(t); size != 0 || !bytes.Equal(root, rfc6962.DefaultHasher.EmptyRoot()) {
		t.Errorf("IntCoord = (%d, %x), want (0, %x)", size, root, rfc6962.DefaultHasher.EmptyRoot())
	}
	if at, size := env.pubCoord(t); at != 0 || !size.Valid || size.Int64 != 0 {
		t.Errorf("PubCoord = (%d, %v), want (0, 0)", at, size)
	}
	if n := env.gcFromSize(t); n != 0 {
		t.Errorf("GCCoord.fromSize = %d, want 0", n)
	}
	checkSeqKeys(t, env, []uint64{})
}

// Spec 1: construction on a fresh schema creates all tables (in the search_path schema) with
// the initial rows, and the sequencer reports an empty log.
func TestPGSequencerInitFreshSchema(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	if got := env.tableNames(t); len(got) != 0 {
		t.Fatalf("fresh schema already has tables %v", got)
	}

	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	checkInitialState(t, env)
	checkNext(t, ctx, env, s, 0)
	checkTree(t, ctx, env, s, 0, rfc6962.DefaultHasher.EmptyRoot())
	if err := s.checkDataCompatibility(ctx); err != nil {
		t.Errorf("checkDataCompatibility: %v", err)
	}
}

// Spec 3/4: SchemaSQL on its own creates the tables, seeds exactly SchemaCompatibilityVersion and
// is idempotent.
func TestSchemaSQL(t *testing.T) {
	t.Parallel()
	env := newPGTestEnv(t)

	env.exec(t, SchemaSQL)
	checkInitialState(t, env)
	// Applying it again must be a no-op.
	env.exec(t, SchemaSQL)
	checkInitialState(t, env)
}

// Spec 1: N processes initialising the same empty schema concurrently all succeed, and leave
// exactly one copy of the seed rows.
func TestPGSequencerConcurrentInit(t *testing.T) {
	t.Parallel()
	// Several independent rounds, as a missing lock only shows up as a race some of the time.
	const numInits = 10
	for round := range 5 {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			env := newPGTestEnv(t)

			seqs := make([]*pgSequencer, numInits)
			errs := make([]error, numInits)
			start := make(chan struct{})
			wg := sync.WaitGroup{}
			for i := range numInits {
				wg.Go(func() {
					<-start
					seqs[i], errs[i] = newPGSequencer(ctx, Config{DSN: env.DSN, MaxOpenConns: 2}, 1000)
				})
			}
			close(start)
			wg.Wait()
			for i, s := range seqs {
				if s != nil {
					t.Cleanup(func() { _ = s.close() })
				}
				if errs[i] != nil {
					t.Errorf("newPGSequencer #%d: %v", i, errs[i])
				}
			}
			if t.Failed() {
				return
			}
			checkInitialState(t, env)

			// All instances drive the same log.
			for i, s := range seqs {
				if got := assignData(t, ctx, s, fmt.Sprintf("from %d", i)); got[0] != uint64(i) {
					t.Errorf("sequencer #%d assigned index %d, want %d", i, got[0], i)
				}
			}
		})
	}
}

// pgState is a snapshot of all of the coordination state in a schema.
type pgState struct {
	Version   uint64
	Next      uint64
	IntSeq    uint64
	IntRoot   []byte
	PubAt     int64
	PubSize   sql.NullInt64
	GCFrom    uint64
	SeqKeys   []uint64
	SeqDigest string
}

func snapshotState(t *testing.T, env *pgTestEnv) pgState {
	t.Helper()
	s := pgState{
		Version: env.queryUint64(t, "SELECT compatibilityVersion FROM Tessera WHERE id = 0"),
		Next:    env.seqCoordNext(t),
		GCFrom:  env.gcFromSize(t),
		SeqKeys: env.seqKeys(t),
	}
	s.IntSeq, s.IntRoot = env.intCoord(t)
	s.PubAt, s.PubSize = env.pubCoord(t)
	if err := env.DB.QueryRow("SELECT coalesce(md5(string_agg(md5(v), ',' ORDER BY seq)), '') FROM Seq").Scan(&s.SeqDigest); err != nil {
		t.Fatalf("digest Seq: %v", err)
	}
	return s
}

// Spec 4: re-initialising an existing log (via initDB, a new sequencer instance, or SchemaSQL)
// never resets indices, the integrated tree, pending entries, or the publish/GC coordination rows.
func TestPGSequencerReinitPreservesState(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s1 := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	a := batchData("a", 5)
	b := batchData("b", 7)
	c := batchData("c", 9)
	assignData(t, ctx, s1, a...)
	assignData(t, ctx, s1, b...)
	assignData(t, ctx, s1, c...)
	if more, calls, from, got := consumeOnce(t, ctx, s1, 2, false); !more || calls != 1 || from != 0 || len(got) != 12 {
		t.Fatalf("consumeEntries: more=%t calls=%d from=%d len=%d, want true 1 0 12", more, calls, from, len(got))
	}
	pubAt := time.Now().Add(-time.Minute).UnixNano()
	env.exec(t, "UPDATE PubCoord SET publishedAt = $1, size = $2 WHERE id = 0", pubAt, 12)
	env.exec(t, "UPDATE GCCoord SET fromSize = $1 WHERE id = 0", 7)

	want := pgState{
		Version: SchemaCompatibilityVersion,
		Next:    21,
		IntSeq:  12,
		IntRoot: fakeRoot(12),
		PubAt:   pubAt,
		PubSize: sql.NullInt64{Int64: 12, Valid: true},
		GCFrom:  7,
		SeqKeys: []uint64{12},
	}
	want.SeqDigest = snapshotState(t, env).SeqDigest
	check := func(when string) {
		t.Helper()
		if diff := cmp.Diff(want, snapshotState(t, env)); diff != "" {
			t.Errorf("%s: state mismatch (-want +got):\n%s", when, diff)
		}
	}
	check("before re-init")

	if err := s1.initDB(ctx); err != nil {
		t.Fatalf("initDB: %v", err)
	}
	check("after initDB")

	s2 := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	check("after newPGSequencer")

	env.exec(t, SchemaSQL)
	check("after SchemaSQL")

	// The new instance carries on where the old one left off.
	checkNext(t, ctx, env, s2, 21)
	checkTree(t, ctx, env, s2, 12, fakeRoot(12))
	if got := assignData(t, ctx, s2, "d"); got[0] != 21 {
		t.Errorf("assigned index %d, want 21", got[0])
	}
	more, calls, from, got := consumeOnce(t, ctx, s2, 10, false)
	if !more || calls != 1 || from != 12 {
		t.Fatalf("consumeEntries: more=%t calls=%d from=%d, want true 1 12", more, calls, from)
	}
	wantEntries := append(wantBatch(12, c...), wantBatch(21, "d")...)
	if diff := cmp.Diff(wantEntries, got); diff != "" {
		t.Errorf("consumed entries mismatch (-want +got):\n%s", diff)
	}
}

// Spec 2: with SkipSchemaInit, construction against a schema without the tables fails (rather
// than hanging or creating them); once SchemaSQL has been applied externally it succeeds.
func TestPGSequencerSkipSchemaInit(t *testing.T) {
	t.Parallel()
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	s, err := newPGSequencer(ctx, Config{DSN: env.DSN, SkipSchemaInit: true}, 1000)
	if err == nil {
		_ = s.close()
		t.Fatal("newPGSequencer(SkipSchemaInit) on empty schema succeeded, want error")
	}
	if ctx.Err() != nil {
		t.Fatalf("newPGSequencer(SkipSchemaInit) on empty schema only returned after the context expired: %v", err)
	}
	if got := env.tableNames(t); len(got) != 0 {
		t.Errorf("SkipSchemaInit created tables %v, want none", got)
	}

	env.exec(t, SchemaSQL)
	s = newTestSequencer(t, ctx, Config{DSN: env.DSN, SkipSchemaInit: true}, 1000)
	if diff := cmp.Diff(seqRange(0, 3), assignData(t, ctx, s, "x", "y", "z")); diff != "" {
		t.Errorf("assigned indices mismatch (-want +got):\n%s", diff)
	}
	checkNext(t, ctx, env, s, 3)
}

// Spec 3: the stored compatibility version must equal SchemaCompatibilityVersion, both at
// construction time (with and without schema init) and via checkDataCompatibility.
func TestPGSequencerCompatibilityVersion(t *testing.T) {
	t.Parallel()

	t.Run("mismatch at construction", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		env := newPGTestEnv(t)
		env.exec(t, SchemaSQL)
		env.exec(t, "UPDATE Tessera SET compatibilityVersion = $1 WHERE id = 0", SchemaCompatibilityVersion+1)
		for _, skip := range []bool{false, true} {
			s, err := newPGSequencer(ctx, Config{DSN: env.DSN, SkipSchemaInit: skip}, 1000)
			if err == nil {
				_ = s.close()
				t.Errorf("newPGSequencer(SkipSchemaInit=%t) with compatibilityVersion %d succeeded, want error", skip, SchemaCompatibilityVersion+1)
			}
		}
		// Init must not "fix" the stored version.
		if v := env.queryUint64(t, "SELECT compatibilityVersion FROM Tessera WHERE id = 0"); v != SchemaCompatibilityVersion+1 {
			t.Errorf("compatibilityVersion = %d after failed construction, want %d", v, SchemaCompatibilityVersion+1)
		}
	})

	t.Run("missing version row", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		env := newPGTestEnv(t)
		env.exec(t, SchemaSQL)
		env.exec(t, "DELETE FROM Tessera")
		s, err := newPGSequencer(ctx, Config{DSN: env.DSN, SkipSchemaInit: true}, 1000)
		if err == nil {
			_ = s.close()
			t.Error("newPGSequencer(SkipSchemaInit) with no Tessera row succeeded, want error")
		}
	})

	t.Run("checkDataCompatibility", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		env := newPGTestEnv(t)
		s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
		if err := s.checkDataCompatibility(ctx); err != nil {
			t.Errorf("checkDataCompatibility: %v", err)
		}
		env.exec(t, "UPDATE Tessera SET compatibilityVersion = 0 WHERE id = 0")
		if err := s.checkDataCompatibility(ctx); err == nil {
			t.Error("checkDataCompatibility with version 0 succeeded, want error")
		}
		env.exec(t, "UPDATE Tessera SET compatibilityVersion = $1 WHERE id = 0", SchemaCompatibilityVersion)
		if err := s.checkDataCompatibility(ctx); err != nil {
			t.Errorf("checkDataCompatibility after restoring version: %v", err)
		}
	})
}

// Spec 1 / Config.DB: an external handle is used as-is (DSN and pool sizes ignored) and is never
// closed by the sequencer.
func TestPGSequencerExternalDB(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	db := openTestDB(t, env.DSN, 7)

	s, err := newPGSequencer(ctx, Config{
		DB:           db,
		DSN:          "postgres://nobody:nothing@127.0.0.1:1/nowhere?sslmode=disable&connect_timeout=1",
		MaxOpenConns: 1,
		MaxIdleConns: 0,
	}, 1000)
	if err != nil {
		t.Fatalf("newPGSequencer with external DB: %v", err)
	}
	if s.dbPool != db {
		t.Errorf("sequencer dbPool is not the provided Config.DB")
	}
	if got := db.Stats().MaxOpenConnections; got != 7 {
		t.Errorf("external DB MaxOpenConnections = %d, want 7 (Config.MaxOpenConns must not be applied)", got)
	}
	if diff := cmp.Diff(wantTables, env.tableNames(t)); diff != "" {
		t.Errorf("tables mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(seqRange(0, 2), assignData(t, ctx, s, "p", "q")); diff != "" {
		t.Errorf("assigned indices mismatch (-want +got):\n%s", diff)
	}
	// MaxIdleConns=0 must not have been applied either: a released connection stays idle.
	if got := db.Stats().Idle; got == 0 {
		t.Errorf("external DB has no idle connections; Config.MaxIdleConns must not be applied")
	}

	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("external DB unusable after sequencer close (it must not be closed): %v", err)
	}
	if n := env.queryUint64(t, "SELECT next FROM SeqCoord WHERE id = 0"); n != 2 {
		t.Errorf("SeqCoord.next = %d, want 2", n)
	}
}

// Spec 1: a pool opened from the DSN has MaxOpenConns applied, and is closed by close().
func TestPGSequencerOwnedPool(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)

	s, err := newPGSequencer(ctx, Config{DSN: env.DSN, MaxOpenConns: 3, MaxIdleConns: 1}, 1000)
	if err != nil {
		t.Fatalf("newPGSequencer: %v", err)
	}
	if s.dbPool == nil {
		t.Fatal("dbPool is nil")
	}
	if got := s.dbPool.Stats().MaxOpenConnections; got != 3 {
		t.Errorf("MaxOpenConnections = %d, want 3", got)
	}
	if err := s.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := s.dbPool.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Errorf("PingContext after close = %v, want 'database is closed' error", err)
	}
}

// Spec 1: an unreachable database is reported by the constructor.
func TestPGSequencerUnreachableDB(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	s, err := newPGSequencer(ctx, Config{DSN: "postgres://postgres:postgres@127.0.0.1:1/tessera_test?sslmode=disable&connect_timeout=2"}, 1000)
	if err == nil {
		_ = s.close()
		t.Fatal("newPGSequencer with unreachable DB succeeded, want error")
	}
}

// Spec 6: if the consumeFunc fails, neither IntCoord nor Seq change and the error is returned;
// the same entries are offered again on the next call.
func TestPGConsumeEntriesFuncError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	a, b := batchData("a", 4), batchData("b", 6)
	assignData(t, ctx, s, a...)
	assignData(t, ctx, s, b...)
	before := snapshotState(t, env)

	errBoom := errors.New("boom")
	calls := 0
	for _, force := range []bool{false, true} {
		more, err := s.consumeEntries(ctx, 10, func(_ context.Context, from uint64, entries []storage.SequencedEntry) ([]byte, error) {
			calls++
			if from != 0 || len(entries) != 10 {
				t.Errorf("consumeFunc called with from=%d, %d entries, want 0, 10", from, len(entries))
			}
			return []byte("should not be stored"), errBoom
		}, force)
		if !errors.Is(err, errBoom) {
			t.Errorf("consumeEntries(force=%t) = %v, want %v", force, err, errBoom)
		}
		if more {
			t.Errorf("consumeEntries(force=%t) returned more=true on failure", force)
		}
		if diff := cmp.Diff(before, snapshotState(t, env)); diff != "" {
			t.Errorf("state changed after failed consume (-before +after):\n%s", diff)
		}
	}
	if calls != 2 {
		t.Errorf("consumeFunc called %d times, want 2", calls)
	}

	// The entries are still there to be consumed.
	more, calls, from, got := consumeOnce(t, ctx, s, 10, false)
	if !more || calls != 1 || from != 0 {
		t.Fatalf("consumeEntries: more=%t calls=%d from=%d, want true 1 0", more, calls, from)
	}
	if diff := cmp.Diff(append(wantBatch(0, a...), wantBatch(4, b...)...), got); diff != "" {
		t.Errorf("consumed entries mismatch (-want +got):\n%s", diff)
	}
	checkTree(t, ctx, env, s, 10, fakeRoot(10))
	checkSeqKeys(t, env, []uint64{})

	// A failing forced update with nothing to consume doesn't change the root either.
	if _, err := s.consumeEntries(ctx, 10, func(context.Context, uint64, []storage.SequencedEntry) ([]byte, error) {
		return []byte("nope"), errBoom
	}, true); !errors.Is(err, errBoom) {
		t.Errorf("forced consumeEntries = %v, want %v", err, errBoom)
	}
	checkTree(t, ctx, env, s, 10, fakeRoot(10))
}

// Spec 5/6: each assignEntries call is stored as one Seq row keyed by its first index;
// consumeEntries reads at most limit rows, in order, from IntCoord.seq, and deletes exactly the
// consumed rows (Seq is empty once everything is integrated).
func TestPGConsumeEntriesLimit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	want := []storage.SequencedEntry{}
	for i, n := range []int{3, 1, 4, 1, 5} {
		d := batchData(fmt.Sprintf("batch %d", i), n)
		idx := assignData(t, ctx, s, d...)
		if diff := cmp.Diff(seqRange(uint64(len(want)), n), idx); diff != "" {
			t.Fatalf("batch %d indices mismatch (-want +got):\n%s", i, diff)
		}
		want = append(want, wantBatch(uint64(len(want)), d...)...)
	}
	checkSeqKeys(t, env, []uint64{0, 3, 4, 8, 9})
	checkNext(t, ctx, env, s, 14)

	for _, step := range []struct {
		limit    uint64
		wantFrom uint64
		wantN    int
		wantKeys []uint64
	}{
		{limit: 2, wantFrom: 0, wantN: 4, wantKeys: []uint64{4, 8, 9}},
		{limit: 1, wantFrom: 4, wantN: 4, wantKeys: []uint64{8, 9}},
		{limit: 1, wantFrom: 8, wantN: 1, wantKeys: []uint64{9}},
		{limit: 10, wantFrom: 9, wantN: 5, wantKeys: []uint64{}},
	} {
		more, calls, from, got := consumeOnce(t, ctx, s, step.limit, false)
		if !more || calls != 1 || from != step.wantFrom {
			t.Fatalf("consumeEntries(limit=%d): more=%t calls=%d from=%d, want true 1 %d", step.limit, more, calls, from, step.wantFrom)
		}
		end := step.wantFrom + uint64(step.wantN)
		if diff := cmp.Diff(want[step.wantFrom:end], got); diff != "" {
			t.Errorf("consumeEntries(limit=%d) entries mismatch (-want +got):\n%s", step.limit, diff)
		}
		checkTree(t, ctx, env, s, end, fakeRoot(end))
		checkSeqKeys(t, env, step.wantKeys)
	}

	// Nothing left: f isn't called and nothing changes.
	more, err := s.consumeEntries(ctx, 10, failIfCalled(t), false)
	if err != nil || more {
		t.Errorf("consumeEntries on empty Seq = (%t, %v), want (false, nil)", more, err)
	}
	checkTree(t, ctx, env, s, 14, fakeRoot(14))
	checkNext(t, ctx, env, s, 14)
}

// Spec 6: forceUpdate calls f even when there is nothing to consume (with an empty slice, from
// the current size), and stores the returned root; without force nothing happens.
func TestPGConsumeEntriesForceUpdate(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	more, err := s.consumeEntries(ctx, 10, failIfCalled(t), false)
	if err != nil || more {
		t.Errorf("unforced consumeEntries on empty log = (%t, %v), want (false, nil)", more, err)
	}
	checkTree(t, ctx, env, s, 0, rfc6962.DefaultHasher.EmptyRoot())

	forced := func(size uint64) {
		t.Helper()
		calls := 0
		more, err := s.consumeEntries(ctx, 10, func(_ context.Context, from uint64, entries []storage.SequencedEntry) ([]byte, error) {
			calls++
			if from != size || len(entries) != 0 {
				t.Errorf("forced consumeFunc called with from=%d, %d entries, want %d, 0", from, len(entries), size)
			}
			return fmt.Appendf(nil, "forced-%d", size), nil
		}, true)
		if err != nil || !more {
			t.Errorf("forced consumeEntries = (%t, %v), want (true, nil)", more, err)
		}
		if calls != 1 {
			t.Errorf("forced consumeFunc called %d times, want 1", calls)
		}
		checkTree(t, ctx, env, s, size, fmt.Appendf(nil, "forced-%d", size))
	}
	forced(0)

	d := batchData("x", 3)
	assignData(t, ctx, s, d...)
	if more, calls, from, got := consumeOnce(t, ctx, s, 10, false); !more || calls != 1 || from != 0 || len(got) != 3 {
		t.Fatalf("consumeEntries: more=%t calls=%d from=%d len=%d, want true 1 0 3", more, calls, from, len(got))
	}
	checkTree(t, ctx, env, s, 3, fakeRoot(3))
	forced(3)

	// Forcing with pending entries consumes them as normal.
	d = batchData("y", 2)
	assignData(t, ctx, s, d...)
	more, calls, from, got := consumeOnce(t, ctx, s, 10, true)
	if !more || calls != 1 || from != 3 {
		t.Fatalf("forced consumeEntries: more=%t calls=%d from=%d, want true 1 3", more, calls, from)
	}
	if diff := cmp.Diff(wantBatch(3, d...), got); diff != "" {
		t.Errorf("entries mismatch (-want +got):\n%s", diff)
	}
	checkTree(t, ctx, env, s, 5, fakeRoot(5))
	checkSeqKeys(t, env, []uint64{})
}

// Spec 6: a non-contiguous Seq row is an integrity error; f is not called and nothing changes.
func TestPGConsumeEntriesGap(t *testing.T) {
	t.Parallel()

	insertBatch := func(t *testing.T, env *pgTestEnv, key uint64, data ...string) {
		t.Helper()
		env.exec(t, "INSERT INTO Seq (id, seq, v) VALUES (0, $1, $2)", key, encodeBatch(t, wantBatch(key, data...)))
		env.exec(t, "UPDATE SeqCoord SET next = $1 WHERE id = 0", key+uint64(len(data)))
	}

	t.Run("gap between batches", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		env := newPGTestEnv(t)
		s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

		d := batchData("ok", 3)
		assignData(t, ctx, s, d...)
		// Entries 3 and 4 are missing.
		insertBatch(t, env, 5, "gap a", "gap b")
		before := snapshotState(t, env)

		more, err := s.consumeEntries(ctx, 10, failIfCalled(t), false)
		if err == nil || more {
			t.Errorf("consumeEntries across a gap = (%t, %v), want (false, error)", more, err)
		}
		if diff := cmp.Diff(before, snapshotState(t, env)); diff != "" {
			t.Errorf("state changed after failed consume (-before +after):\n%s", diff)
		}

		// Reading only the first (contiguous) row is fine...
		more, calls, from, got := consumeOnce(t, ctx, s, 1, false)
		if !more || calls != 1 || from != 0 {
			t.Fatalf("consumeEntries(limit=1): more=%t calls=%d from=%d, want true 1 0", more, calls, from)
		}
		if diff := cmp.Diff(wantBatch(0, d...), got); diff != "" {
			t.Errorf("entries mismatch (-want +got):\n%s", diff)
		}
		checkTree(t, ctx, env, s, 3, fakeRoot(3))
		checkSeqKeys(t, env, []uint64{5})

		// ...but the next batch doesn't start at the integrated size.
		before = snapshotState(t, env)
		more, err = s.consumeEntries(ctx, 10, failIfCalled(t), false)
		if err == nil || more {
			t.Errorf("consumeEntries with gap at start = (%t, %v), want (false, error)", more, err)
		}
		if diff := cmp.Diff(before, snapshotState(t, env)); diff != "" {
			t.Errorf("state changed after failed consume (-before +after):\n%s", diff)
		}
	})

	t.Run("first batch beyond integrated size", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		env := newPGTestEnv(t)
		s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
		insertBatch(t, env, 2, "late a", "late b")
		before := snapshotState(t, env)

		for _, force := range []bool{false, true} {
			more, err := s.consumeEntries(ctx, 10, failIfCalled(t), force)
			if err == nil || more {
				t.Errorf("consumeEntries(force=%t) = (%t, %v), want (false, error)", force, more, err)
			}
		}
		if diff := cmp.Diff(before, snapshotState(t, env)); diff != "" {
			t.Errorf("state changed after failed consume (-before +after):\n%s", diff)
		}
	})
}

// Spec 5: pushback happens iff next - IntCoord.seq > maxOutstanding at the time of the call; a
// pushed-back batch is not stored and gets no index.
func TestPGSequencerPushbackBoundary(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	const maxOutstanding = 10
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, maxOutstanding)

	// 0 outstanding: a batch of exactly maxOutstanding entries is accepted.
	if diff := cmp.Diff(seqRange(0, 10), assignData(t, ctx, s, batchData("first", 10)...)); diff != "" {
		t.Errorf("indices mismatch (-want +got):\n%s", diff)
	}
	// Exactly maxOutstanding outstanding: still accepted.
	if diff := cmp.Diff(seqRange(10, 1), assignData(t, ctx, s, "at limit")); diff != "" {
		t.Errorf("indices mismatch (-want +got):\n%s", diff)
	}
	// maxOutstanding+1 outstanding: pushback.
	before := snapshotState(t, env)
	e := tessera.NewEntry([]byte("over limit"))
	err := s.assignEntries(ctx, []*tessera.Entry{e})
	if !errors.Is(err, tessera.ErrPushbackIntegration) {
		t.Fatalf("assignEntries with %d outstanding = %v, want %v", maxOutstanding+1, err, tessera.ErrPushbackIntegration)
	}
	if !errors.Is(err, tessera.ErrPushback) {
		t.Errorf("pushback error %v does not wrap tessera.ErrPushback", err)
	}
	if diff := cmp.Diff(before, snapshotState(t, env)); diff != "" {
		t.Errorf("state changed after pushback (-before +after):\n%s", diff)
	}

	// Integrating relieves the pressure.
	if more, calls, _, got := consumeOnce(t, ctx, s, 100, false); !more || calls != 1 || len(got) != 11 {
		t.Fatalf("consumeEntries: more=%t calls=%d len=%d, want true 1 11", more, calls, len(got))
	}
	if diff := cmp.Diff(seqRange(11, 1), assignData(t, ctx, s, "after integration")); diff != "" {
		t.Errorf("indices mismatch (-want +got):\n%s", diff)
	}
	checkNext(t, ctx, env, s, 12)
}

// Spec 5/6: large entries and batches (a single 8MiB entry, a single >16MiB batch row) survive the
// Seq round trip byte-for-byte.
func TestPGSequencerLargeEntries(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	randBytes := func(n int) []byte {
		b := make([]byte, n)
		if _, err := crand.Read(b); err != nil {
			t.Fatalf("rand.Read: %v", err)
		}
		return b
	}
	big := randBytes(8 << 20)
	batch := [][]byte{randBytes(6 << 20), randBytes(6 << 20), randBytes(6 << 20)}

	assign := func(data ...[]byte) []uint64 {
		entries := make([]*tessera.Entry, len(data))
		for i, d := range data {
			entries[i] = tessera.NewEntry(d)
		}
		if err := s.assignEntries(ctx, entries); err != nil {
			t.Fatalf("assignEntries: %v", err)
		}
		r := []uint64{}
		for _, e := range entries {
			r = append(r, *e.Index())
		}
		return r
	}
	if diff := cmp.Diff(seqRange(0, 1), assign(big)); diff != "" {
		t.Errorf("indices mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(seqRange(1, 3), assign(batch...)); diff != "" {
		t.Errorf("indices mismatch (-want +got):\n%s", diff)
	}
	checkSeqKeys(t, env, []uint64{0, 1})
	if n := env.queryUint64(t, "SELECT octet_length(v) FROM Seq WHERE seq = 1"); n <= 16<<20 {
		t.Errorf("Seq row for the 18MiB batch is %d bytes, want > 16MiB", n)
	}

	check := func(from uint64, data ...[]byte) {
		t.Helper()
		more, calls, gotFrom, got := consumeOnce(t, ctx, s, 1, false)
		if !more || calls != 1 || gotFrom != from || len(got) != len(data) {
			t.Fatalf("consumeEntries: more=%t calls=%d from=%d len=%d, want true 1 %d %d", more, calls, gotFrom, len(got), from, len(data))
		}
		for i, d := range data {
			w := wantSequenced(d, from+uint64(i))
			if !bytes.Equal(got[i].BundleData, w.BundleData) || !bytes.Equal(got[i].LeafHash, w.LeafHash) {
				t.Errorf("entry %d differs from what was assigned", from+uint64(i))
			}
		}
	}
	check(0, big)
	check(1, batch...)
	checkTree(t, ctx, env, s, 4, fakeRoot(4))
	checkSeqKeys(t, env, []uint64{})
}

// Spec 8: indices beyond 2^32 are assigned, stored and consumed correctly.
func TestPGSequencerIndicesBeyond2To32(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	const base = uint64(1)<<33 + 5
	env.exec(t, "UPDATE SeqCoord SET next = $1 WHERE id = 0", int64(base))
	env.exec(t, "UPDATE IntCoord SET seq = $1 WHERE id = 0", int64(base))
	checkNext(t, ctx, env, s, base)

	a, b := batchData("a", 3), batchData("b", 4)
	if diff := cmp.Diff(seqRange(base, 3), assignData(t, ctx, s, a...)); diff != "" {
		t.Errorf("indices mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(seqRange(base+3, 4), assignData(t, ctx, s, b...)); diff != "" {
		t.Errorf("indices mismatch (-want +got):\n%s", diff)
	}
	checkSeqKeys(t, env, []uint64{base, base + 3})
	checkNext(t, ctx, env, s, base+7)

	more, calls, from, got := consumeOnce(t, ctx, s, 10, false)
	if !more || calls != 1 || from != base {
		t.Fatalf("consumeEntries: more=%t calls=%d from=%d, want true 1 %d", more, calls, from, base)
	}
	if diff := cmp.Diff(append(wantBatch(base, a...), wantBatch(base+3, b...)...), got); diff != "" {
		t.Errorf("entries mismatch (-want +got):\n%s", diff)
	}
	checkTree(t, ctx, env, s, base+7, fakeRoot(base+7))
	checkSeqKeys(t, env, []uint64{})
}

// Spec 7: several replicas (separate pools) concurrently assigning and integrating against the
// same schema never lose, duplicate, reorder or gap entries, and never integrate concurrently.
func TestPGSequencerMultiReplicaStress(t *testing.T) {
	t.Parallel()
	runMultiReplicaStress(t, nil)
}

// runMultiReplicaStress runs the multi-replica stress scenario, with dsnParams (e.g. session
// settings such as default_transaction_isolation) added to the replicas' DSN.
func runMultiReplicaStress(t *testing.T, dsnParams map[string]string) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	env := newPGTestEnv(t)
	dsn := env.DSN
	if dsnParams != nil {
		dsn = dsnWithParams(t, dsn, dsnParams)
	}

	const (
		numReplicas         = 3
		assignersPerReplica = 3
		batchesPerAssigner  = 40
		maxBatchSize        = 16
	)
	seqs := make([]*pgSequencer, numReplicas)
	for i := range seqs {
		seqs[i] = newTestSequencer(t, ctx, Config{DSN: dsn, MaxOpenConns: 8, MaxIdleConns: 4}, 1<<30)
	}

	// Assigners record what they were given.
	type assigned struct {
		data string
		idx  uint64
	}
	var assignMu sync.Mutex
	allAssigned := []assigned{}

	// Consumers record every consumeFunc invocation, in invocation order.
	type call struct {
		from    uint64
		entries []storage.SequencedEntry
	}
	var callsMu sync.Mutex
	calls := []call{}
	var inF atomic.Int32
	consume := func(_ context.Context, from uint64, entries []storage.SequencedEntry) ([]byte, error) {
		if n := inF.Add(1); n != 1 {
			t.Errorf("consumeFunc invoked concurrently (%d in flight)", n)
		}
		defer inF.Add(-1)
		if len(entries) == 0 {
			t.Errorf("unforced consumeFunc called with no entries (from=%d)", from)
		}
		callsMu.Lock()
		calls = append(calls, call{from: from, entries: slices.Clone(entries)})
		callsMu.Unlock()
		return fakeRoot(from + uint64(len(entries))), nil
	}

	var stopConsumers atomic.Bool
	consumersDone := sync.WaitGroup{}
	// Make sure the consumers are gone before the test completes, however it ends.
	t.Cleanup(func() {
		stopConsumers.Store(true)
		consumersDone.Wait()
	})
	for r, s := range seqs {
		consumersDone.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(r), 99))
			for !stopConsumers.Load() && ctx.Err() == nil {
				more, err := s.consumeEntries(ctx, uint64(1+rng.IntN(4)), consume, false)
				if err != nil {
					if ctx.Err() == nil {
						t.Errorf("replica %d: consumeEntries: %v", r, err)
					}
					return
				}
				if !more {
					// Nothing to do right now; back off briefly rather than hammering the DB.
					time.Sleep(time.Millisecond)
				}
			}
		})
	}

	assigners := sync.WaitGroup{}
	for r, s := range seqs {
		for a := range assignersPerReplica {
			assigners.Go(func() {
				rng := rand.New(rand.NewPCG(uint64(r), uint64(a)))
				for b := range batchesPerAssigner {
					n := 1 + rng.IntN(maxBatchSize)
					entries := make([]*tessera.Entry, n)
					for i := range entries {
						entries[i] = tessera.NewEntry(fmt.Appendf(nil, "r%d/a%d/b%d/e%d", r, a, b, i))
					}
					if err := s.assignEntries(ctx, entries); err != nil {
						t.Errorf("replica %d assigner %d: assignEntries: %v", r, a, err)
						return
					}
					first := *entries[0].Index()
					assignMu.Lock()
					for i, e := range entries {
						if got := *e.Index(); got != first+uint64(i) {
							t.Errorf("batch r%d/a%d/b%d: entry %d has index %d, want %d (non-contiguous batch)", r, a, b, i, got, first+uint64(i))
						}
						allAssigned = append(allAssigned, assigned{data: string(e.Data()), idx: *e.Index()})
					}
					assignMu.Unlock()
				}
			})
		}
	}
	assigners.Wait()
	total := uint64(len(allAssigned))
	if t.Failed() {
		t.FailNow()
	}

	// Let the consumers finish integrating everything.
	waitFor(t, 30*time.Second, "all entries to be integrated", func() bool {
		size, _, err := seqs[0].currentTree(ctx)
		return err == nil && size == total
	})
	stopConsumers.Store(true)
	consumersDone.Wait()

	// Every index in [0, total) was assigned exactly once.
	byIndex := make(map[uint64]string, total)
	for _, a := range allAssigned {
		if prev, ok := byIndex[a.idx]; ok {
			t.Fatalf("index %d assigned to both %q and %q", a.idx, prev, a.data)
		}
		if a.idx >= total {
			t.Fatalf("index %d assigned to %q is beyond the %d assigned entries", a.idx, a.data, total)
		}
		byIndex[a.idx] = a.data
	}

	// The consumeFunc calls, in the order they happened across all replicas, form one contiguous
	// sequence covering [0, total), and each position holds the entry assigned that index.
	next := uint64(0)
	for i, c := range calls {
		if c.from != next {
			t.Fatalf("consumeFunc call %d had from=%d, want %d (gap, duplicate or reorder)", i, c.from, next)
		}
		for j, e := range c.entries {
			idx := c.from + uint64(j)
			if diff := cmp.Diff(wantSequenced([]byte(byIndex[idx]), idx), e); diff != "" {
				t.Fatalf("integrated entry at index %d is not the entry assigned that index (-want +got):\n%s", idx, diff)
			}
		}
		next += uint64(len(c.entries))
	}
	if next != total {
		t.Errorf("integrated %d entries, want %d", next, total)
	}
	t.Logf("integrated %d entries in %d consumeFunc calls", total, len(calls))

	checkTree(t, ctx, env, seqs[1], total, fakeRoot(total))
	checkNext(t, ctx, env, seqs[2], total)
	checkSeqKeys(t, env, []uint64{})
}

// Spec 10: two schemas in the same database are completely independent logs.
func TestPGSequencerSchemasIndependent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	envA, envB := newPGTestEnv(t), newPGTestEnv(t)
	sA := newTestSequencer(t, ctx, Config{DSN: envA.DSN}, 1000)
	sB := newTestSequencer(t, ctx, Config{DSN: envB.DSN}, 1000)

	if diff := cmp.Diff(seqRange(0, 5), assignData(t, ctx, sA, batchData("A", 5)...)); diff != "" {
		t.Errorf("A indices mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(seqRange(0, 3), assignData(t, ctx, sB, batchData("B", 3)...)); diff != "" {
		t.Errorf("B indices mismatch (-want +got):\n%s", diff)
	}
	checkNext(t, ctx, envA, sA, 5)
	checkNext(t, ctx, envB, sB, 3)

	more, calls, from, got := consumeOnce(t, ctx, sA, 10, false)
	if !more || calls != 1 || from != 0 {
		t.Fatalf("A consumeEntries: more=%t calls=%d from=%d, want true 1 0", more, calls, from)
	}
	if diff := cmp.Diff(wantBatch(0, batchData("A", 5)...), got); diff != "" {
		t.Errorf("A entries mismatch (-want +got):\n%s", diff)
	}
	checkTree(t, ctx, envA, sA, 5, fakeRoot(5))
	checkSeqKeys(t, envA, []uint64{})
	checkTree(t, ctx, envB, sB, 0, rfc6962.DefaultHasher.EmptyRoot())
	checkSeqKeys(t, envB, []uint64{0})

	pubCalls := 0
	if _, err := sA.publishCheckpoint(ctx, 0, 0, func(_ context.Context, size uint64, root []byte) error {
		pubCalls++
		if size != 5 || !bytes.Equal(root, fakeRoot(5)) {
			t.Errorf("A publish f(%d, %q), want (5, %q)", size, root, fakeRoot(5))
		}
		return nil
	}); err != nil {
		t.Fatalf("A publishCheckpoint: %v", err)
	}
	if pubCalls != 1 {
		t.Errorf("A publish f called %d times, want 1", pubCalls)
	}
	if _, size := envA.pubCoord(t); !size.Valid || size.Int64 != 5 {
		t.Errorf("A PubCoord.size = %v, want 5", size)
	}
	if at, size := envB.pubCoord(t); at != 0 || !size.Valid || size.Int64 != 0 {
		t.Errorf("B PubCoord = (%d, %v), want (0, 0)", at, size)
	}

	// B integrates its own entries from its own starting point.
	more, calls, from, got = consumeOnce(t, ctx, sB, 10, false)
	if !more || calls != 1 || from != 0 {
		t.Fatalf("B consumeEntries: more=%t calls=%d from=%d, want true 1 0", more, calls, from)
	}
	if diff := cmp.Diff(wantBatch(0, batchData("B", 3)...), got); diff != "" {
		t.Errorf("B entries mismatch (-want +got):\n%s", diff)
	}
	checkTree(t, ctx, envA, sA, 5, fakeRoot(5))
	checkTree(t, ctx, envB, sB, 3, fakeRoot(3))
}

// Spec 9: replicas racing publishCheckpoint within the same staleness window publish exactly once
// (PubCoord row lock); the losers see the fresh publishedAt and skip.
func TestPGPublishCheckpointConcurrent(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	env := newPGTestEnv(t)

	const numReplicas = 3
	const minStale = time.Minute
	appNames := make([]string, numReplicas)
	seqs := make([]*pgSequencer, numReplicas)
	for i := range seqs {
		appNames[i] = fmt.Sprintf("%s_pub%d", env.Schema, i)
		dsn := dsnWithParams(t, env.DSN, map[string]string{"application_name": appNames[i]})
		seqs[i] = newTestSequencer(t, ctx, Config{DSN: dsn}, 1000)
	}
	// Grow the tree so that there's something to publish.
	assignData(t, ctx, seqs[0], batchData("x", 3)...)
	if more, calls, _, _ := consumeOnce(t, ctx, seqs[0], 10, false); !more || calls != 1 {
		t.Fatalf("consumeEntries: more=%t calls=%d, want true 1", more, calls)
	}

	var fCalls, inF, returned atomic.Int32
	f := func(ctx context.Context, size uint64, root []byte) error {
		fCalls.Add(1)
		inF.Add(1)
		defer inF.Add(-1)
		if size != 3 || !bytes.Equal(root, fakeRoot(3)) {
			t.Errorf("publish f(%d, %q), want (3, %q)", size, root, fakeRoot(3))
		}
		// Hold the lock until every other replica is observed blocked on a lock, done, or
		// (wrongly) publishing too, so that the race is real rather than sequential.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var blocked int
			if err := env.DB.QueryRowContext(ctx,
				"SELECT count(*) FROM pg_stat_activity WHERE application_name = ANY($1) AND wait_event_type = 'Lock'", appNames).Scan(&blocked); err != nil {
				t.Errorf("query pg_stat_activity: %v", err)
				return nil
			}
			if blocked+int(returned.Load())+int(inF.Load()-1) >= numReplicas-1 {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Logf("other replicas were not observed blocking on the publish lock")
		return nil
	}

	before := time.Now()
	start := make(chan struct{})
	nextPubs := make([]time.Time, numReplicas)
	errs := make([]error, numReplicas)
	wg := sync.WaitGroup{}
	for i, s := range seqs {
		wg.Go(func() {
			<-start
			nextPubs[i], errs[i] = s.publishCheckpoint(ctx, minStale, minStale, f)
			returned.Add(1)
		})
	}
	close(start)
	wg.Wait()
	after := time.Now()

	if got := fCalls.Load(); got != 1 {
		t.Errorf("publish f called %d times, want exactly 1", got)
	}
	for i := range seqs {
		if errs[i] != nil {
			t.Errorf("replica %d: publishCheckpoint: %v", i, errs[i])
		}
		if lo, hi := before.Add(minStale), after.Add(minStale); nextPubs[i].Before(lo) || nextPubs[i].After(hi) {
			t.Errorf("replica %d: nextPubAt = %v, want within [%v, %v]", i, nextPubs[i], lo, hi)
		}
	}
	at, size := env.pubCoord(t)
	if !size.Valid || size.Int64 != 3 {
		t.Errorf("PubCoord.size = %v, want 3", size)
	}
	if at < before.UnixNano() || at > after.UnixNano() {
		t.Errorf("PubCoord.publishedAt = %d, want within [%d, %d] (nanoseconds)", at, before.UnixNano(), after.UnixNano())
	}

	// Still within the staleness window: nobody publishes.
	for i, s := range seqs {
		if _, err := s.publishCheckpoint(ctx, minStale, minStale, f); err != nil {
			t.Errorf("replica %d: publishCheckpoint: %v", i, err)
		}
	}
	if got := fCalls.Load(); got != 1 {
		t.Errorf("publish f called %d times in total, want 1", got)
	}
}

// Spec 9: PubCoord staleness rules match the MySQL implementation: legacy second-resolution
// timestamps, growth-triggered publishing, republish interval, NULL size self-healing, and no
// PubCoord change when f fails.
func TestPGPublishCheckpointRules(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T, integrated int) (context.Context, *pgTestEnv, *pgSequencer) {
		t.Helper()
		ctx := t.Context()
		env := newPGTestEnv(t)
		s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
		if integrated > 0 {
			assignData(t, ctx, s, batchData("e", integrated)...)
			if more, calls, _, _ := consumeOnce(t, ctx, s, 10, false); !more || calls != 1 {
				t.Fatalf("consumeEntries: more=%t calls=%d, want true 1", more, calls)
			}
		}
		return ctx, env, s
	}
	recorder := func(n *int, size *uint64, root *[]byte) func(context.Context, uint64, []byte) error {
		return func(_ context.Context, s uint64, r []byte) error {
			*n++
			*size, *root = s, r
			return nil
		}
	}

	t.Run("legacy seconds timestamp is recent", func(t *testing.T) {
		t.Parallel()
		ctx, env, s := setup(t, 2)
		sec := time.Now().Unix()
		env.exec(t, "UPDATE PubCoord SET publishedAt = $1, size = 0 WHERE id = 0", sec)
		next, err := s.publishCheckpoint(ctx, time.Hour, 0, func(context.Context, uint64, []byte) error {
			t.Error("publish f called for a checkpoint published within minStaleActive")
			return nil
		})
		if err != nil {
			t.Fatalf("publishCheckpoint: %v", err)
		}
		if want := time.Unix(sec, 0).Add(time.Hour); !next.Equal(want) {
			t.Errorf("nextPubAt = %v, want %v", next, want)
		}
		if at, _ := env.pubCoord(t); at != sec {
			t.Errorf("PubCoord.publishedAt = %d, want unchanged %d", at, sec)
		}
	})

	t.Run("nanosecond timestamp is recent", func(t *testing.T) {
		t.Parallel()
		ctx, env, s := setup(t, 2)
		ns := time.Now().Add(-time.Second).UnixNano()
		env.exec(t, "UPDATE PubCoord SET publishedAt = $1, size = 0 WHERE id = 0", ns)
		next, err := s.publishCheckpoint(ctx, time.Hour, 0, func(context.Context, uint64, []byte) error {
			t.Error("publish f called for a checkpoint published within minStaleActive")
			return nil
		})
		if err != nil {
			t.Fatalf("publishCheckpoint: %v", err)
		}
		if want := time.Unix(0, ns).Add(time.Hour); !next.Equal(want) {
			t.Errorf("nextPubAt = %v, want %v", next, want)
		}
	})

	t.Run("growth publishes, f failure changes nothing", func(t *testing.T) {
		t.Parallel()
		ctx, env, s := setup(t, 2)
		errBoom := errors.New("boom")
		if _, err := s.publishCheckpoint(ctx, time.Millisecond, 0, func(context.Context, uint64, []byte) error { return errBoom }); !errors.Is(err, errBoom) {
			t.Errorf("publishCheckpoint with failing f = %v, want %v", err, errBoom)
		}
		if at, size := env.pubCoord(t); at != 0 || !size.Valid || size.Int64 != 0 {
			t.Errorf("PubCoord = (%d, %v) after failed publish, want (0, 0)", at, size)
		}

		var n int
		var size uint64
		var root []byte
		before := time.Now()
		next, err := s.publishCheckpoint(ctx, time.Millisecond, 0, recorder(&n, &size, &root))
		after := time.Now()
		if err != nil {
			t.Fatalf("publishCheckpoint: %v", err)
		}
		if n != 1 || size != 2 || !bytes.Equal(root, fakeRoot(2)) {
			t.Errorf("publish f called %d times with (%d, %q), want once with (2, %q)", n, size, root, fakeRoot(2))
		}
		at, dbSize := env.pubCoord(t)
		if !dbSize.Valid || dbSize.Int64 != 2 || at < before.UnixNano() || at > after.UnixNano() {
			t.Errorf("PubCoord = (%d, %v), want (within [%d, %d], 2)", at, dbSize, before.UnixNano(), after.UnixNano())
		}
		if next.Before(before.Add(time.Millisecond)) || next.After(after.Add(time.Millisecond)) {
			t.Errorf("nextPubAt = %v, want within [%v, %v]", next, before.Add(time.Millisecond), after.Add(time.Millisecond))
		}

		// No growth and republishing disabled: no publish, however stale.
		env.exec(t, "UPDATE PubCoord SET publishedAt = $1 WHERE id = 0", time.Now().Add(-24*time.Hour).UnixNano())
		if _, err := s.publishCheckpoint(ctx, time.Millisecond, 0, recorder(&n, &size, &root)); err != nil {
			t.Fatalf("publishCheckpoint: %v", err)
		}
		if n != 1 {
			t.Errorf("publish f called without growth and with republish disabled")
		}
	})

	t.Run("republish without growth", func(t *testing.T) {
		t.Parallel()
		ctx, env, s := setup(t, 2)
		env.exec(t, "UPDATE PubCoord SET publishedAt = $1, size = 2 WHERE id = 0", time.Now().Add(-2*time.Hour).UnixNano())
		var n int
		var size uint64
		var root []byte
		if _, err := s.publishCheckpoint(ctx, time.Minute, time.Hour, recorder(&n, &size, &root)); err != nil {
			t.Fatalf("publishCheckpoint: %v", err)
		}
		if n != 1 || size != 2 {
			t.Errorf("publish f called %d times with size %d, want once with 2", n, size)
		}
	})

	t.Run("unknown last size self-heals", func(t *testing.T) {
		t.Parallel()
		ctx, env, s := setup(t, 0)
		env.exec(t, "UPDATE PubCoord SET publishedAt = 0, size = NULL WHERE id = 0")
		var n int
		var size uint64
		var root []byte
		if _, err := s.publishCheckpoint(ctx, time.Millisecond, 0, recorder(&n, &size, &root)); err != nil {
			t.Fatalf("publishCheckpoint: %v", err)
		}
		if n != 1 || size != 0 || !bytes.Equal(root, rfc6962.DefaultHasher.EmptyRoot()) {
			t.Errorf("publish f called %d times with (%d, %x), want once with (0, %x)", n, size, root, rfc6962.DefaultHasher.EmptyRoot())
		}
		if _, dbSize := env.pubCoord(t); !dbSize.Valid || dbSize.Int64 != 0 {
			t.Errorf("PubCoord.size = %v, want 0", dbSize)
		}
	})
}

// gcRecorder records the prefixes passed to garbageCollect's delete function.
type gcRecorder struct {
	mu       sync.Mutex
	prefixes []string
	failOn   string
}

func (g *gcRecorder) delete(_ context.Context, prefix string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prefixes = append(g.prefixes, prefix)
	if prefix == g.failOn {
		return fmt.Errorf("failed to delete %q", prefix)
	}
	return nil
}

func (g *gcRecorder) take() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	r := append([]string{}, g.prefixes...)
	g.prefixes = nil
	slices.Sort(r)
	return r
}

func partialDirs(paths ...string) []string {
	r := make([]string, len(paths))
	for i, p := range paths {
		r[i] = p + ".p/"
	}
	slices.Sort(r)
	return r
}

// Spec 9: garbageCollect tracks progress in GCCoord exactly like the MySQL implementation: it
// visits full bundles from GCCoord.fromSize (up to maxBundles+1 per call), removes partials of the
// bundle, its level-0 tile and completed parent tiles, and stops at the first partial bundle.
func TestPGGarbageCollectProgress(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	ep := layout.EntriesPath
	tp := func(l, i uint64) string { return layout.TilePath(l, i, 0) }

	g := &gcRecorder{}
	treeSize := uint64(3*layout.EntryBundleWidth + 10)
	for _, step := range []struct {
		treeSize   uint64
		wantDel    []string
		wantFromSz uint64
	}{
		{treeSize: treeSize, wantDel: partialDirs(ep(0, 0), tp(0, 0), ep(1, 0), tp(0, 1)), wantFromSz: 512},
		{treeSize: treeSize, wantDel: partialDirs(ep(2, 0), tp(0, 2)), wantFromSz: 768},
		// Only the partial bundle 3 remains: nothing to do.
		{treeSize: treeSize, wantDel: partialDirs(), wantFromSz: 768},
		// fromSize == treeSize: nothing to do.
		{treeSize: 768, wantDel: partialDirs(), wantFromSz: 768},
	} {
		if err := s.garbageCollect(ctx, step.treeSize, 1, g.delete, ep); err != nil {
			t.Fatalf("garbageCollect(%d): %v", step.treeSize, err)
		}
		if diff := cmp.Diff(step.wantDel, g.take()); diff != "" {
			t.Errorf("garbageCollect(%d) deleted prefixes mismatch (-want +got):\n%s", step.treeSize, diff)
		}
		if got := env.gcFromSize(t); got != step.wantFromSz {
			t.Errorf("GCCoord.fromSize = %d, want %d", got, step.wantFromSz)
		}
	}

	// Completing bundle 255 also completes the level-1 tile above it.
	env.exec(t, "UPDATE GCCoord SET fromSize = $1 WHERE id = 0", 255*layout.EntryBundleWidth)
	if err := s.garbageCollect(ctx, 256*layout.EntryBundleWidth+5, 10, g.delete, ep); err != nil {
		t.Fatalf("garbageCollect: %v", err)
	}
	if diff := cmp.Diff(partialDirs(ep(255, 0), tp(0, 255), tp(1, 0)), g.take()); diff != "" {
		t.Errorf("deleted prefixes mismatch (-want +got):\n%s", diff)
	}
	if got := env.gcFromSize(t); got != 256*layout.EntryBundleWidth {
		t.Errorf("GCCoord.fromSize = %d, want %d", got, 256*layout.EntryBundleWidth)
	}
}

// Spec 9: if any delete fails, garbageCollect returns an error and GCCoord doesn't advance.
func TestPGGarbageCollectDeleteError(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	s := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)

	g := &gcRecorder{failOn: layout.TilePath(0, 1, 0) + ".p/"}
	if err := s.garbageCollect(ctx, 3*layout.EntryBundleWidth, 10, g.delete, layout.EntriesPath); err == nil {
		t.Error("garbageCollect with failing delete succeeded, want error")
	}
	if got := env.gcFromSize(t); got != 0 {
		t.Errorf("GCCoord.fromSize = %d after failed GC, want 0", got)
	}

	// Once deletes work again, GC catches up.
	g.failOn = ""
	g.take()
	if err := s.garbageCollect(ctx, 3*layout.EntryBundleWidth, 10, g.delete, layout.EntriesPath); err != nil {
		t.Fatalf("garbageCollect: %v", err)
	}
	if got := env.gcFromSize(t); got != 3*layout.EntryBundleWidth {
		t.Errorf("GCCoord.fromSize = %d, want %d", got, 3*layout.EntryBundleWidth)
	}
}
