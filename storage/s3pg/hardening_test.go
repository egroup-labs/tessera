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

// Tests for the hardening of the driver against inconsistent or failing coordination state:
// sticky guards, startup/integration/publication refusals, missing or corrupt rows, commit
// failures, missing DDL privileges, and the MigrationWriter's LogReader.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/smithy-go"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	storage "github.com/transparency-dev/tessera/storage/internal"
	"github.com/transparency-dev/tessera/storage/s3pg/antispam"
)

// accessDenied is a non-NotFound S3 error.
var accessDenied = &smithy.GenericAPIError{Code: "AccessDenied", Message: "access denied"}

func failCheckpointReads(key string) error {
	if key == layout.CheckpointPath {
		return accessDenied
	}
	return nil
}

// Once a guard has tripped (here: the database was restored to an old snapshot under a running
// appender), the appender refuses new entries: Add futures fail with ErrCoordinationBehind,
// CoordinationError reports it, and nothing more is sequenced.
func TestGuardStickyAfterDatabaseRestore(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, _ := mustGenerateKeys(t)
	r := &s3Reader{client: b.Client, bucket: b.Name}
	d, err := New(ctx, Config{SDKConfig: b.SDKConfig, S3Options: b.S3Options, Bucket: b.Name, DSN: env.DSN})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	st := d.(*Storage)
	t.Cleanup(func() { _ = st.Close() })
	app, shutdown, lr, err := tessera.NewAppender(ctx, d, tessera.NewAppendOptions().
		WithCheckpointSigner(sk).
		WithCheckpointInterval(time.Second).
		WithBatching(16, 20*time.Millisecond).
		WithGarbageCollectionInterval(0))
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}
	a := &e2eAppender{appender: app, shutdown: shutdown, reader: lr, awaiter: tessera.NewPublicationAwaiter(ctx, lr.ReadCheckpoint, 50*time.Millisecond)}
	mustAddConcurrently(t, ctx, a, "before", 100, 4)
	restore := snapshotDB(t, env)
	mustAddConcurrently(t, ctx, a, "after", 100, 4)
	if s, _ := readCP(t, ctx, r); s != 200 {
		t.Fatalf("published size %d, want 200", s)
	}
	cpBefore, err := r.ReadCheckpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CoordinationError(); err != nil {
		t.Fatalf("CoordinationError before the restore = %v", err)
	}

	restore()
	// The first entry after the restore is (unavoidably) handed an index which has already been
	// published; trying to integrate it trips the guard. (If the snapshot caught PubCoord before
	// the commit of the last publication, the publication job may trip the guard first.)
	idx, err := app.Add(ctx, tessera.NewEntry([]byte("first after restore")))()
	if (err == nil && idx.Index != 100) || (err != nil && !errors.Is(err, ErrCoordinationBehind)) {
		t.Fatalf("Add after restore = (%+v, %v), want index 100 or ErrCoordinationBehind", idx, err)
	}
	waitFor(t, 30*time.Second, "the guard to trip", func() bool { return st.CoordinationError() != nil })
	tripped := st.CoordinationError()
	if !errors.Is(tripped, ErrCoordinationBehind) {
		t.Fatalf("CoordinationError = %v, want ErrCoordinationBehind", tripped)
	}

	next, keys := env.seqCoordNext(t), env.seqKeys(t)
	futures := make([]tessera.IndexFuture, 10)
	for i := range futures {
		futures[i] = app.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "refused %d", i)))
	}
	for i, f := range futures {
		if idx, err := f(); !errors.Is(err, ErrCoordinationBehind) {
			t.Errorf("Add #%d after the guard tripped = (%+v, %v), want ErrCoordinationBehind", i, idx, err)
		}
	}
	if got := env.seqCoordNext(t); got != next {
		t.Errorf("SeqCoord.next moved from %d to %d after the guard tripped", next, got)
	}
	if got := env.seqKeys(t); !slices.Equal(got, keys) {
		t.Errorf("Seq rows changed from %v to %v after the guard tripped", keys, got)
	}
	if size, _ := env.intCoord(t); size != 100 {
		t.Errorf("IntCoord.seq = %d, want 100", size)
	}
	if cp, err := r.ReadCheckpoint(ctx); err != nil || !bytes.Equal(cp, cpBefore) {
		t.Errorf("checkpoint changed after the guard tripped: %q (%v), was %q", cp, err, cpBefore)
	}
	if got := st.CoordinationError(); got != tripped {
		t.Errorf("CoordinationError changed from %v to %v", tripped, got)
	}
}

// T1: startup against an unparseable checkpoint, or a checkpoint which can't be read (other than
// "not found"), fails without writing anything to the bucket or the database.
func TestStartupUnreadableCheckpoint(t *testing.T) {
	t.Parallel()
	sk, _ := mustGenerateKeys(t)
	for _, tc := range []struct {
		name    string
		setup   func(m *memObjStore, spy *spyStore)
		wantErr string
	}{
		{"unparseable", func(m *memObjStore, _ *spyStore) {
			_ = m.setObject(context.Background(), layout.CheckpointPath, []byte("bananas"), "", "")
		}, "parse"},
		{"empty", func(m *memObjStore, _ *spyStore) {
			_ = m.setObject(context.Background(), layout.CheckpointPath, []byte{}, "", "")
		}, "parse"},
		{"read error", func(_ *memObjStore, spy *spyStore) { spy.getErr = failCheckpointReads }, "failed to read checkpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			env := newPGTestEnv(t)
			seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
			m := newMemObjStore()
			spy := &spyStore{objStore: m}
			tc.setup(m, spy)
			memBefore := memContents(m)
			dbBefore := schemaFingerprint(t, env.DB, env.Schema, false)
			st := &Storage{}
			_, _, err := st.newAppender(ctx, spy, seq, tessera.NewAppendOptions().WithCheckpointSigner(sk))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("newAppender = %v, want error containing %q", err, tc.wantErr)
			}
			if _, _, w := spy.take(); len(w) != 0 {
				t.Errorf("failed startup wrote %q", w)
			}
			checkMemUnchanged(t, m, memBefore, "failed startup")
			checkFingerprint(t, env.DB, env.Schema, false, dbBefore, "database after failed startup")
		})
	}
}

// T2: startup refuses a database whose tree differs from the published checkpoint at the same
// size (or is smaller), tripping the guards, and accepts consistent states.
func TestStartupChecksPublishedCheckpoint(t *testing.T) {
	t.Parallel()
	sk, _ := mustGenerateKeys(t)
	empty := rfc6962.DefaultHasher.EmptyRoot()
	for _, tc := range []struct {
		name          string
		dbSize        uint64
		dbRoot        []byte
		pubSize       uint64
		pubRoot       []byte
		wantRefused   bool
		wantErrDetail string
	}{
		{"same size and root", 0, empty, 0, empty, false, ""},
		{"same size, different root", 0, empty, 0, []byte("a different root hash of 32 byte"), true, "size 0"},
		{"published is larger", 0, empty, 5, empty, true, "size 5"},
		{"database is larger", 7, fakeRoot(7), 0, empty, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			env := newPGTestEnv(t)
			seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
			env.exec(t, "UPDATE IntCoord SET seq = $1, rootHash = $2", tc.dbSize, tc.dbRoot)
			env.exec(t, "UPDATE SeqCoord SET next = $1", tc.dbSize)
			m := newMemObjStore()
			if err := m.setObject(ctx, layout.CheckpointPath, mustTestCP(tc.pubSize, tc.pubRoot), "", ""); err != nil {
				t.Fatal(err)
			}
			spy := &spyStore{objStore: m}
			memBefore := memContents(m)
			dbBefore := schemaFingerprint(t, env.DB, env.Schema, false)
			st := &Storage{}
			_, _, err := st.newAppender(ctx, spy, seq, tessera.NewAppendOptions().WithCheckpointSigner(sk).WithCheckpointInterval(time.Hour))
			if !tc.wantRefused {
				if err != nil || st.CoordinationError() != nil {
					t.Fatalf("newAppender = %v, CoordinationError = %v; want both nil", err, st.CoordinationError())
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), ErrCoordinationBehind.Error()) || !strings.Contains(err.Error(), tc.wantErrDetail) {
				t.Fatalf("newAppender = %v, want ErrCoordinationBehind mentioning %q", err, tc.wantErrDetail)
			}
			if ce := st.CoordinationError(); !errors.Is(ce, ErrCoordinationBehind) {
				t.Errorf("CoordinationError = %v, want ErrCoordinationBehind", ce)
			}
			// Start-up refusals write nothing, not even the tripped marker: start-up re-checks every
			// time, and a misconfigured instance must not block correctly configured ones.
			if _, _, w := spy.take(); len(w) != 0 {
				t.Errorf("refused startup wrote %q", w)
			}
			checkMemUnchanged(t, m, memBefore, "refused startup")
			checkFingerprint(t, env.DB, env.Schema, false, dbBefore, "database after refused startup")
		})
	}
}

// T3: integration with an unreadable or unparseable published checkpoint fails before writing
// anything; IntCoord and Seq are unchanged, and (transient) read errors don't trip the guards.
func TestIntegrationUnreadableCheckpoint(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	m := newMemObjStore()
	spy := &spyStore{objStore: m}
	a, st := newBareAppender(spy, seq, layout.EntriesPath)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	all := addAndIntegrate(t, ctx, seq, a, "one", 10, 5)
	if _, err := seq.publishCheckpoint(ctx, 0, 0, a.updateCheckpoint); err != nil {
		t.Fatal(err)
	}
	pending := batchData("pending", 5)
	assignData(t, ctx, seq, pending...)
	goodCP, err := m.getObject(ctx, layout.CheckpointPath)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name          string
		break_, fix   func()
		mayTripGuards bool
	}{
		{"read error", func() { spy.getErr = failCheckpointReads }, func() { spy.getErr = nil }, false},
		{"unparseable", func() { _ = m.setObject(ctx, layout.CheckpointPath, []byte("bananas"), "", "") }, func() { _ = m.setObject(ctx, layout.CheckpointPath, goodCP, "", "") }, true},
	} {
		tc.break_()
		memBefore := memContents(m)
		dbBefore := schemaFingerprint(t, env.DB, env.Schema, false)
		spy.take()
		if _, err := seq.consumeEntries(ctx, DefaultIntegrationSizeLimit, a.integrateEntries, false); err == nil {
			t.Errorf("%s: consumeEntries succeeded", tc.name)
		}
		if _, _, w := spy.take(); len(w) != 0 {
			t.Errorf("%s: failed integration wrote %q", tc.name, w)
		}
		checkMemUnchanged(t, m, memBefore, tc.name)
		checkFingerprint(t, env.DB, env.Schema, false, dbBefore, tc.name+": database")
		if err := st.CoordinationError(); err != nil && !tc.mayTripGuards {
			t.Errorf("%s: tripped the guards: %v", tc.name, err)
		}
		tc.fix()
	}
	integrateAll(t, ctx, seq, a)
	for _, d := range pending {
		all = append(all, []byte(d))
	}
	if size, root := env.intCoord(t); size != 15 || !bytes.Equal(root, rootOf(t, all)) {
		t.Errorf("IntCoord = (%d, %x) after recovery, want (15, %x)", size, root, rootOf(t, all))
	}
}

// T4: publishing a larger tree fails if a tile needed for the consistency proof from the
// published checkpoint is missing; checkpoint and PubCoord are unchanged, and it recovers once
// the tile is back.
func TestPublicationMissingTile(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	m := newMemObjStore()
	a, st := newBareAppender(m, seq, layout.EntriesPath)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	publish := func() error { _, err := seq.publishCheckpoint(ctx, 0, 0, a.updateCheckpoint); return err }
	all := addAndIntegrate(t, ctx, seq, a, "one", 300, 50)
	if err := publish(); err != nil {
		t.Fatal(err)
	}
	all = append(all, addAndIntegrate(t, ctx, seq, a, "two", 400, 100)...)

	tile := layout.TilePath(0, 1, 0)
	tileData, err := m.getObject(ctx, tile)
	if err != nil {
		t.Fatalf("tile %s: %v", tile, err)
	}
	if err := m.deleteObjectsWithPrefix(ctx, tile); err != nil {
		t.Fatal(err)
	}
	cpBefore, _ := m.getObject(ctx, layout.CheckpointPath)
	pubAt, pubSize := env.pubCoord(t)
	err = publish()
	if err == nil || !strings.Contains(err.Error(), "consistency proof") {
		t.Fatalf("publication without tile %s = %v, want consistency proof failure", tile, err)
	}
	if cp, _ := m.getObject(ctx, layout.CheckpointPath); !bytes.Equal(cp, cpBefore) {
		t.Errorf("checkpoint changed to %q", cp)
	}
	if at, size := env.pubCoord(t); at != pubAt || size != pubSize {
		t.Errorf("PubCoord = (%d, %v), want unchanged (%d, %v)", at, size, pubAt, pubSize)
	}
	if err := st.CoordinationError(); err != nil {
		t.Errorf("a missing tile tripped the guards: %v", err)
	}

	if err := m.setObject(ctx, tile, tileData, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := publish(); err != nil {
		t.Fatalf("publication with the tile back: %v", err)
	}
	if cp, _ := m.getObject(ctx, layout.CheckpointPath); !bytes.Equal(cp, mustTestCP(700, rootOf(t, all))) {
		t.Errorf("checkpoint = %q, want size 700", cp)
	}
}

// T5: after the database is restored to a snapshot taken when PubCoord.size < IntCoord.seq,
// publishCheckpoint wants to publish the old tree, and the guard refuses: checkpoint and PubCoord
// stay unchanged.
func TestPublicationRefusedAfterRestore(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	m := newMemObjStore()
	a, st := newBareAppender(m, seq, layout.EntriesPath)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	publish := func() error { _, err := seq.publishCheckpoint(ctx, 0, 0, a.updateCheckpoint); return err }
	addAndIntegrate(t, ctx, seq, a, "one", 200, 50)
	if err := publish(); err != nil {
		t.Fatal(err)
	}
	addAndIntegrate(t, ctx, seq, a, "two", 30, 10)
	restore := snapshotDB(t, env) // PubCoord.size 200 < IntCoord.seq 230.
	addAndIntegrate(t, ctx, seq, a, "three", 230, 50)
	if err := publish(); err != nil {
		t.Fatal(err)
	}
	restore()
	cpBefore, _ := m.getObject(ctx, layout.CheckpointPath)
	pubAt, pubSize := env.pubCoord(t)
	if !pubSize.Valid || pubSize.Int64 != 200 {
		t.Fatalf("restored PubCoord.size = %v, want 200", pubSize)
	}

	err := publish()
	if !errors.Is(err, ErrCoordinationBehind) || !strings.Contains(err.Error(), ErrCoordinationBehind.Error()) || !strings.Contains(err.Error(), "size 230 over published size 460") {
		t.Fatalf("publication of the restored tree = %v, want ErrCoordinationBehind (230 over 460)", err)
	}
	if cp, _ := m.getObject(ctx, layout.CheckpointPath); !bytes.Equal(cp, cpBefore) {
		t.Errorf("checkpoint changed to %q", cp)
	}
	if at, size := env.pubCoord(t); at != pubAt || size != pubSize {
		t.Errorf("PubCoord = (%d, %v), want unchanged (%d, %v)", at, size, pubAt, pubSize)
	}
	if ce := st.CoordinationError(); !errors.Is(ce, ErrCoordinationBehind) {
		t.Errorf("CoordinationError = %v, want ErrCoordinationBehind", ce)
	}
}

// T6: without the IntCoord row, sequencing and integration fail and change nothing (upstream
// silently drops batches / does nothing).
func TestMissingIntCoordRow(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	assignData(t, ctx, seq, batchData("a", 3)...)
	env.exec(t, "DELETE FROM IntCoord")
	before := schemaFingerprint(t, env.DB, env.Schema, false)

	if err := seq.assignEntries(ctx, []*tessera.Entry{tessera.NewEntry([]byte("x"))}); err == nil {
		t.Error("assignEntries without IntCoord row succeeded")
	}
	for _, force := range []bool{false, true} {
		if _, err := seq.consumeEntries(ctx, 10, failIfCalled(t), force); err == nil {
			t.Errorf("consumeEntries(force=%v) without IntCoord row succeeded", force)
		}
	}
	if _, err := seq.publishCheckpoint(ctx, 0, 0, func(context.Context, uint64, []byte) error {
		t.Error("publication callback called without IntCoord row")
		return nil
	}); err == nil {
		t.Error("publishCheckpoint without IntCoord row succeeded")
	}
	if _, _, err := seq.currentTree(ctx); err == nil {
		t.Error("currentTree without IntCoord row succeeded")
	}
	checkFingerprint(t, env.DB, env.Schema, false, before, "database")
}

// T7: if fewer Seq rows are deleted than were consumed (here: a trigger skips one), integration
// fails and nothing changes.
func TestConsumeEntriesDeleteIntegrity(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	assignData(t, ctx, seq, batchData("a", 3)...)
	assignData(t, ctx, seq, batchData("b", 2)...)
	q := quoteIdent(env.Schema)
	env.exec(t, fmt.Sprintf(`CREATE FUNCTION %[1]s.skip_one() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF OLD.seq = 3 THEN RETURN NULL; END IF;
	RETURN OLD;
END $$`, q))
	env.exec(t, fmt.Sprintf("CREATE TRIGGER skip_one BEFORE DELETE ON %[1]s.Seq FOR EACH ROW EXECUTE FUNCTION %[1]s.skip_one()", q))
	before := schemaFingerprint(t, env.DB, env.Schema, false)

	calls := 0
	_, err := seq.consumeEntries(ctx, 10, func(_ context.Context, from uint64, es []storage.SequencedEntry) ([]byte, error) {
		calls++
		return fakeRoot(from + uint64(len(es))), nil
	}, false)
	if err == nil || !strings.Contains(err.Error(), "integrity fail - deleted 1 Seq rows, expected 2") {
		t.Fatalf("consumeEntries = %v, want integrity failure", err)
	}
	if calls != 1 {
		t.Errorf("consumeFunc called %d times, want 1", calls)
	}
	checkFingerprint(t, env.DB, env.Schema, false, before, "database")
	checkSeqKeys(t, env, []uint64{0, 3})
}

// T8: when the commit of an integration (or publication) fails, the database is unchanged; the
// retry writes exactly the same objects, and the guard accepts republishing the checkpoint which
// the failed publication had already written.
func TestCommitFailureThenRetry(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	m := newMemObjStore()
	spy := &spyStore{objStore: m}
	a, st := newBareAppender(spy, seq, layout.EntriesPath)
	if err := a.init(ctx); err != nil {
		t.Fatal(err)
	}
	publish := func() error { _, err := seq.publishCheckpoint(ctx, 0, 0, a.updateCheckpoint); return err }
	all := addAndIntegrate(t, ctx, seq, a, "one", 100, 20)
	if err := publish(); err != nil {
		t.Fatal(err)
	}

	// Deferred constraint triggers fail the next commit which updates the table, once.
	q := quoteIdent(env.Schema)
	failNextCommit := func(table string) {
		env.exec(t, fmt.Sprintf("CREATE SEQUENCE %s.calls_%s", q, table))
		env.exec(t, fmt.Sprintf(`CREATE FUNCTION %[1]s.fail_%[2]s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF nextval('%[1]s.calls_%[2]s') = 1 THEN RAISE EXCEPTION 'injected commit failure'; END IF;
	RETURN NULL;
END $$`, q, table))
		env.exec(t, fmt.Sprintf("CREATE CONSTRAINT TRIGGER fail_%[2]s AFTER UPDATE ON %[1]s.%[2]s DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %[1]s.fail_%[2]s()", q, table))
	}
	failNextCommit("IntCoord")
	for i := range 3 {
		data := batchData(fmt.Sprintf("two/%d", i), 100)
		assignData(t, ctx, seq, data...)
		for _, d := range data {
			all = append(all, []byte(d))
		}
	}
	dbBefore := schemaFingerprint(t, env.DB, env.Schema, false)
	spy.take()
	if _, err := seq.consumeEntries(ctx, DefaultIntegrationSizeLimit, a.integrateEntries, false); err == nil || !strings.Contains(err.Error(), "injected commit failure") {
		t.Fatalf("integration with failing commit = %v, want injected commit failure", err)
	}
	checkFingerprint(t, env.DB, env.Schema, false, dbBefore, "database after failed integration commit")
	_, _, writes1 := spy.take()
	afterFailed := memContents(m)
	if len(writes1) == 0 {
		t.Fatal("the failed integration wrote no objects")
	}
	if more, err := seq.consumeEntries(ctx, DefaultIntegrationSizeLimit, a.integrateEntries, false); err != nil || !more {
		t.Fatalf("retried integration = (%v, %v), want (true, nil)", more, err)
	}
	_, _, writes2 := spy.take()
	slices.Sort(writes1)
	slices.Sort(writes2)
	if !slices.Equal(writes1, writes2) {
		t.Errorf("the retry wrote %q, the failed attempt %q", writes2, writes1)
	}
	checkMemUnchanged(t, m, afterFailed, "retried integration")
	if size, root := env.intCoord(t); size != 400 || !bytes.Equal(root, rootOf(t, all)) {
		t.Fatalf("IntCoord = (%d, %x), want (400, %x)", size, root, rootOf(t, all))
	}

	failNextCommit("PubCoord")
	pubAt, pubSize := env.pubCoord(t)
	if err := publish(); err == nil || !strings.Contains(err.Error(), "injected commit failure") {
		t.Fatalf("publication with failing commit = %v, want injected commit failure", err)
	}
	if at, size := env.pubCoord(t); at != pubAt || size != pubSize {
		t.Errorf("PubCoord = (%d, %v) after failed commit, want unchanged (%d, %v)", at, size, pubAt, pubSize)
	}
	cp400 := mustTestCP(400, rootOf(t, all))
	if cp, _ := m.getObject(ctx, layout.CheckpointPath); !bytes.Equal(cp, cp400) {
		t.Fatalf("checkpoint = %q, want %q (written before the failed commit)", cp, cp400)
	}
	if err := publish(); err != nil {
		t.Fatalf("republication of the same checkpoint: %v", err)
	}
	if _, size := env.pubCoord(t); !size.Valid || size.Int64 != 400 {
		t.Errorf("PubCoord.size = %v, want 400", size)
	}
	if cp, _ := m.getObject(ctx, layout.CheckpointPath); !bytes.Equal(cp, cp400) {
		t.Errorf("checkpoint = %q, want %q", cp, cp400)
	}
	if err := st.CoordinationError(); err != nil {
		t.Errorf("CoordinationError = %v", err)
	}
}

// T9: an undecodable Seq row makes integration fail without calling the consumeFunc.
func TestConsumeEntriesCorruptSeqRow(t *testing.T) {
	t.Parallel()
	for name, v := range map[string]any{"garbage": []byte{0xde, 0xad, 0xbe, 0xef}, "NULL": nil, "empty": []byte{}} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			env := newPGTestEnv(t)
			seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
			env.exec(t, "INSERT INTO Seq (id, seq, v) VALUES (0, 0, $1)", v)
			env.exec(t, "UPDATE SeqCoord SET next = 1")
			before := schemaFingerprint(t, env.DB, env.Schema, false)
			for _, force := range []bool{false, true} {
				if _, err := seq.consumeEntries(ctx, 10, failIfCalled(t), force); err == nil || !strings.Contains(err.Error(), "deserialise") {
					t.Errorf("consumeEntries(force=%v) = %v, want deserialisation error", force, err)
				}
			}
			checkFingerprint(t, env.DB, env.Schema, false, before, "database")
		})
	}
}

// newRoleDSN creates a login role (dropped on cleanup) and returns a DSN for it with the given
// extra parameters.
func newRoleDSN(t *testing.T, params map[string]string) (role, dsn string) {
	t.Helper()
	admin := requirePG(t)
	role = "s3pg_t11_" + randHex(t, 6)
	pw := randHex(t, 12)
	if _, err := admin.ExecContext(t.Context(), fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", role, pw)); err != nil {
		t.Fatalf("CREATE ROLE: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, q := range []string{
			"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + role + "'",
			"DROP OWNED BY " + role,
			"DROP ROLE " + role,
		} {
			if _, err := admin.ExecContext(ctx, q); err != nil {
				t.Errorf("cleanup %q: %v", q, err)
			}
		}
	})
	u, err := url.Parse(baseTestDSN())
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, pw)
	return role, dsnWithParams(t, u.String(), params)
}

// T11: a role without CREATE privilege on the schema can't initialise it: construction fails
// with "permission denied" (whether or not the tables already exist) and the pool is closed.
// With SkipSchemaInit it works on an existing schema.
func TestNoCreatePrivilege(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for _, preinit := range []bool{false, true} {
		t.Run(fmt.Sprintf("tables exist=%v", preinit), func(t *testing.T) {
			t.Parallel()
			env := newPGTestEnv(t)
			if preinit {
				env.exec(t, SchemaSQL)
			}
			app := "s3pg-t11-" + randHex(t, 4)
			role, dsn := newRoleDSN(t, map[string]string{"search_path": env.Schema, "application_name": app})
			schemaDSN := dsnWithParams(t, dsn, map[string]string{"search_path": ""})
			env.exec(t, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", quoteIdent(env.Schema), role))
			env.exec(t, fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %s TO %s", quoteIdent(env.Schema), role))
			before := schemaFingerprint(t, env.DB, env.Schema, false)
			for name, cfg := range map[string]Config{
				"search_path":   {DSN: dsn},
				"Config.Schema": {DSN: schemaDSN, Schema: env.Schema},
			} {
				s, err := newPGSequencer(ctx, cfg, 10)
				if err == nil {
					_ = s.close()
					t.Fatalf("%s: newPGSequencer succeeded without CREATE privilege", name)
				}
				if !strings.Contains(err.Error(), "permission denied") {
					t.Errorf("%s: newPGSequencer = %v, want permission denied", name, err)
				}
			}
			waitFor(t, 10*time.Second, "pools of failed constructions to be closed", func() bool { return countConns(t, app) == 0 })
			checkFingerprint(t, env.DB, env.Schema, false, before, "schema")
			if preinit {
				s, err := newPGSequencer(ctx, Config{DSN: dsn, SkipSchemaInit: true}, 10)
				if err != nil {
					t.Fatalf("newPGSequencer with SkipSchemaInit: %v", err)
				}
				defer func() { _ = s.close() }()
				if n, err := s.nextIndex(ctx); err != nil || n != 0 {
					t.Errorf("nextIndex = (%d, %v)", n, err)
				}
			}
		})
	}
}

// The MigrationWriter's LogReader works (upstream: IntegratedSize/NextIndex panic), and a
// migration populating an antispam index (MigrationOptions.WithAntispam) works end to end.
func TestMigrationWriterLogReaderAndAntispam(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	d, err := New(ctx, Config{SDKConfig: b.SDKConfig, S3Options: b.S3Options, Bucket: b.Name, DSN: env.DSN})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	st := d.(*Storage)
	t.Cleanup(func() { _ = st.Close() })
	mw, lr, err := st.MigrationWriter(ctx, tessera.NewMigrationOptions())
	if err != nil {
		t.Fatalf("MigrationWriter: %v", err)
	}
	sizes := func() (uint64, uint64, uint64) {
		t.Helper()
		a, err1 := lr.IntegratedSize(ctx)
		b, err2 := lr.NextIndex(ctx)
		c, err3 := mw.IntegratedSize(ctx)
		if err := errors.Join(err1, err2, err3); err != nil {
			t.Fatalf("sizes: %v", err)
		}
		return a, b, c
	}
	if i, n, w := sizes(); i != 0 || n != 0 || w != 0 {
		t.Errorf("fresh log: IntegratedSize %d, NextIndex %d, MigrationWriter.IntegratedSize %d; want 0, 0, 0", i, n, w)
	}
	as, err := antispam.NewAntispam(ctx, env.DSN, antispam.AntispamOpts{})
	if err != nil {
		t.Fatalf("NewAntispam: %v", err)
	}
	t.Cleanup(func() { _ = as.Close() })
	src := newSynthLog(t, "src", 700)
	for range 2 { // The second migration finds everything done.
		mt, err := tessera.NewMigrationTarget(ctx, d, tessera.NewMigrationOptions().WithAntispam(as))
		if err != nil {
			t.Fatalf("NewMigrationTarget: %v", err)
		}
		if err := mt.Migrate(ctx, 4, src.size(), src.root, src.readEntryBundle); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		if i, n, w := sizes(); i != src.size() || n != env.seqCoordNext(t) || w != src.size() {
			t.Errorf("after migration: IntegratedSize %d, NextIndex %d, MigrationWriter.IntegratedSize %d; want %d, %d, %d", i, n, w, src.size(), env.seqCoordNext(t), src.size())
		}
	}
	if size, root := env.intCoord(t); size != src.size() || !bytes.Equal(root, src.root) {
		t.Errorf("IntCoord = (%d, %x), want (%d, %x)", size, root, src.size(), src.root)
	}
	checkAntispamIndex(t, ctx, as, src)
	checkMigrated(t, ctx, &s3Reader{client: b.Client, bucket: b.Name}, src)
}
