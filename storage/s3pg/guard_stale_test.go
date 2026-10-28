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

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/transparency-dev/tessera"
	"strings"
	"testing"
	"time"

	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
	storage "github.com/transparency-dev/tessera/storage/internal"
)

// A guard comparison which only fails because this instance's view is stale (its connection, and
// so its row lock, was lost while another instance integrated and published) must not trip: the
// fresh integrated size from the database resolves it. Regression test for a false positive found
// by fault injection (connection resets while the guard's checkpoint read was slow).
func TestGuardStaleViewDoesNotTrip(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	publish := func(m *memObjStore, size uint64) {
		t.Helper()
		cp := fmt.Appendf(nil, "test\n%d\n%s\n", size, base64.StdEncoding.EncodeToString(rfc6962.DefaultHasher.EmptyRoot()))
		if err := m.setObject(ctx, layout.CheckpointPath, cp, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name      string
		freshSize uint64 // What the database says now.
		wantTrip  bool
	}{
		{"stale view: database has moved on", 700, false},
		{"stale view: database exactly at published size", 500, false},
		{"database really behind", 400, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := newMemObjStore()
			publish(m, 500)
			a, st := newBareAppender(m, fixedTreeSequencer{size: tc.freshSize}, layout.EntriesPath)

			// Integration with a stale fromSeq (300) below the published size (500).
			errI := a.checkIntegrationAllowed(ctx, 300, []storage.SequencedEntry{{}})
			// Publication of a stale size (300) below the published size (500).
			pubRaw, _, err := a.logStore.readCheckpointWithETag(ctx)
			if err != nil {
				t.Fatal(err)
			}
			errP := a.checkPublicationAllowed(ctx, pubRaw, 300, rfc6962.DefaultHasher.EmptyRoot())

			for what, err := range map[string]error{"integration": errI, "publication": errP} {
				if err == nil {
					t.Fatalf("%s check succeeded with a stale size, want error", what)
				}
				if got := errors.Is(err, ErrCoordinationBehind); got != tc.wantTrip {
					t.Errorf("%s: errors.Is(%v, ErrCoordinationBehind) = %v, want %v", what, err, got, tc.wantTrip)
				}
				if !tc.wantTrip && !errors.Is(err, errStaleView) {
					t.Errorf("%s: %v, want errStaleView", what, err)
				}
			}
			if got := st.CoordinationError() != nil; got != tc.wantTrip {
				t.Errorf("tripped = %v, want %v", got, tc.wantTrip)
			}
			if _, err := m.getObject(ctx, trippedMarkerPath); (err == nil) != tc.wantTrip {
				t.Errorf("tripped marker present = %v, want %v", err == nil, tc.wantTrip)
			}
		})
	}
}

// latencyStore adds a fixed delay to every object store call.
type latencyStore struct {
	objStore
	d time.Duration
}

func (l latencyStore) wait(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-time.After(l.d):
	}
}

func (l latencyStore) getObject(ctx context.Context, k string) ([]byte, error) {
	l.wait(ctx)
	return l.objStore.getObject(ctx, k)
}

func (l latencyStore) listObjects(ctx context.Context, p string) ([]string, error) {
	l.wait(ctx)
	return l.objStore.listObjects(ctx, p)
}

// The slow path of the bucket consistency check (a retry after a failed commit, with the bucket
// holding a large integrated-but-uncommitted batch) must comfortably fit in the integration
// timeout even with realistic object store latency. Regression test for integration getting stuck
// for good: the sequential version needed ~4 round trips per bundle.
func TestCheckBucketConsistentSlowPathLatency(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	const W = layout.EntryBundleWidth
	// A large retry: integration batches are bounded by Seq rows and, in practice, by pushback
	// (e.g. 40960 entries = 160 bundles in the conformance server). 80 bundles here.
	const bundles = 80
	E := seqEntries("e", 0, bundles*W)
	m := newMemObjStore()
	for b := uint64(0); b < bundles; b++ {
		if err := m.setObject(ctx, layout.EntriesPath(b, 0), concatResource(E[b*W:(b+1)*W], false), "", ""); err != nil {
			t.Fatal(err)
		}
		if err := m.setObject(ctx, layout.TilePath(0, b, 0), concatResource(E[b*W:(b+1)*W], true), "", ""); err != nil {
			t.Fatal(err)
		}
	}
	const latency = 20 * time.Millisecond
	a, st := newBareAppender(latencyStore{objStore: m, d: latency}, nil, layout.EntriesPath)
	start := time.Now()
	if err := a.checkBucketConsistent(ctx, 0, E); err != nil {
		t.Fatalf("checkBucketConsistent: %v", err)
	}
	took := time.Since(start)
	// Sequentially this is 2 resources x 80 bundles x 2 calls x 20ms = 6.4s; the integration
	// timeout is 10s and the integration itself needs time too. (Batches that still don't fit are
	// handled by the integration job shrinking its batch; see TestIntegrationLimitAdapts.)
	if limit := defaultIntegrationTimeout / 4; took > limit {
		t.Errorf("slow path took %v with %v per call, want < %v", took, latency, limit)
	}
	if st.CoordinationError() != nil {
		t.Errorf("tripped: %v", st.CoordinationError())
	}
	t.Logf("slow path over %d bundles with %v latency: %v", bundles, latency, took)
}

// While the tripped marker exists, instances refuse to start even against a healthy database; once
// the operator has deleted it (the last step of the recovery procedure), they start.
func TestTrippedMarkerBlocksStartup(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	sk, _ := mustGenerateKeys(t)
	m := newMemObjStore()
	opts := func() *tessera.AppendOptions {
		return tessera.NewAppendOptions().WithCheckpointSigner(sk).WithCheckpointInterval(time.Hour)
	}
	if err := m.setObject(ctx, trippedMarkerPath, []byte("2026-10-01T00:00:00Z\nsome earlier trip\n"), "", ""); err != nil {
		t.Fatal(err)
	}
	st := &Storage{}
	actx, cancel := context.WithCancel(ctx)
	defer cancel()
	if _, _, err := st.newAppender(actx, m, seq, opts()); err == nil || !errors.Is(st.CoordinationError(), ErrCoordinationBehind) {
		t.Fatalf("start with marker: err = %v, CoordinationError = %v; want refusal", err, st.CoordinationError())
	} else if !strings.Contains(err.Error(), trippedMarkerPath) || !strings.Contains(err.Error(), "some earlier trip") {
		t.Errorf("start with marker: %v, want an error citing the marker and its contents", err)
	}
	if _, err := m.getObject(ctx, layout.CheckpointPath); err == nil {
		t.Error("refused start wrote a checkpoint")
	}
	if err := m.deleteObjectsWithPrefix(ctx, trippedMarkerPath); err != nil {
		t.Fatal(err)
	}
	st2 := &Storage{}
	if _, _, err := st2.newAppender(actx, m, seq, opts()); err != nil || st2.CoordinationError() != nil {
		t.Fatalf("start after deleting marker: err = %v, CoordinationError = %v", err, st2.CoordinationError())
	}
}

// Sequencing refuses to hand out indices below what this instance has seen published or
// integrated (which can only happen if the database lost commits), and trips.
func TestAssignEntriesRefusesBelowHighWater(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
	if err := seq.assignEntries(ctx, newEntriesN("a", 5)); err != nil {
		t.Fatal(err)
	}
	hw := uint64(5)
	seq.minNext = func() uint64 { return hw }
	if err := seq.assignEntries(ctx, newEntriesN("b", 3)); err != nil {
		t.Fatalf("assignEntries at next == high water: %v", err)
	}
	hw = 100
	before := env.seqCoordNext(t)
	err := seq.assignEntries(ctx, newEntriesN("c", 3))
	if !errors.Is(err, ErrCoordinationBehind) {
		t.Fatalf("assignEntries below high water = %v, want ErrCoordinationBehind", err)
	}
	if got := env.seqCoordNext(t); got != before {
		t.Errorf("SeqCoord.next changed from %d to %d on refusal", before, got)
	}
}

func newEntriesN(prefix string, n int) []*tessera.Entry {
	r := make([]*tessera.Entry, n)
	for i := range r {
		r[i] = tessera.NewEntry(fmt.Appendf(nil, "%s %d", prefix, i))
	}
	return r
}

// A transaction-bound read (as used by the guards inside consumeEntries/publishCheckpoint) works
// with a pool of a single connection: confirming a suspected lag must not need a second one.
func TestConfirmBehindSingleConnectionPool(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	env := newPGTestEnv(t)
	seq := newTestSequencer(t, ctx, Config{DSN: env.DSN, MaxOpenConns: 1}, 1000)
	m := newMemObjStore()
	a, st := newBareAppender(m, seq, layout.EntriesPath)
	// Integrate 10 entries and publish a checkpoint for them.
	if err := seq.assignEntries(ctx, newEntriesN("x", 10)); err != nil {
		t.Fatal(err)
	}
	integrateAll(t, ctx, seq, a)
	size, root, err := seq.currentTree(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.setObject(ctx, layout.CheckpointPath, fmt.Appendf(nil, "test\n%d\n%s\n", size, base64.StdEncoding.EncodeToString(root)), "", ""); err != nil {
		t.Fatal(err)
	}
	// The database loses commits: back to 5 integrated, then new (different) entries are sequenced.
	env.exec(t, "UPDATE IntCoord SET seq = 5")
	env.exec(t, "UPDATE SeqCoord SET next = 5")
	if err := seq.assignEntries(ctx, newEntriesN("y", 3)); err != nil {
		t.Fatal(err)
	}
	if _, err := seq.consumeEntries(ctx, DefaultIntegrationSizeLimit, a.integrateEntries, false); !errors.Is(err, ErrCoordinationBehind) {
		t.Fatalf("consumeEntries = %v, want ErrCoordinationBehind (confirmed on the transaction's own connection)", err)
	}
	if !errors.Is(st.CoordinationError(), ErrCoordinationBehind) {
		t.Errorf("not tripped: %v", st.CoordinationError())
	}
}

// limitSequencer is a sequencer whose consumeEntries fails whenever it is asked for more than max
// rows (like an integration of a too-large batch hitting its timeout), and records the limits used.
type limitSequencer struct {
	sequencer
	max    uint64
	limits chan uint64
}

func (l *limitSequencer) consumeEntries(_ context.Context, limit uint64, _ consumeFunc, _ bool) (bool, error) {
	l.limits <- limit
	if limit > l.max {
		return false, context.DeadlineExceeded
	}
	return true, nil
}

// The integration job shrinks the batch after failed attempts until attempts succeed (each one
// then makes progress), and grows it again afterwards.
func TestIntegrationLimitAdapts(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ls := &limitSequencer{max: 4, limits: make(chan uint64, 100)}
	a := &Appender{sequencer: ls, treeUpdated: make(chan struct{}, 100), entriesAssigned: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() { defer close(done); a.integrateEntriesJob(ctx) }()
	defer func() { cancel(); <-done }()

	var got []uint64
	for len(got) < 20 {
		a.entriesAssigned <- struct{}{}
		select {
		case l := <-ls.limits:
			got = append(got, l)
		case <-time.After(10 * time.Second):
			t.Fatalf("integration job stopped attempting; limits so far %v", got)
		}
	}
	if got[0] != DefaultIntegrationSizeLimit {
		t.Errorf("first attempt used limit %d, want %d", got[0], DefaultIntegrationSizeLimit)
	}
	succeeded := 0
	for i, l := range got {
		if l <= ls.max {
			succeeded++
		}
		if i > 0 && got[i-1] > ls.max && l != max(1, got[i-1]/2) {
			t.Errorf("after a failure with limit %d the next limit was %d, want %d", got[i-1], l, max(1, got[i-1]/2))
		}
		if i > 0 && got[i-1] <= ls.max && l != min(DefaultIntegrationSizeLimit, got[i-1]*2) {
			t.Errorf("after a success with limit %d the next limit was %d, want %d", got[i-1], l, got[i-1]*2)
		}
	}
	if succeeded == 0 {
		t.Errorf("no attempt was small enough to succeed: %v", got)
	}
}

// After the database loses commits, sequencing refuses at once to hand out indices this instance
// has already seen published (a) or integrated (b) — before any integration attempt could notice.
func TestHighWaterRefusesStaleIndices(t *testing.T) {
	t.Parallel()
	sk, _ := mustGenerateKeys(t)
	add := func(t *testing.T, ctx context.Context, a *Appender, prefix string, n int) []tessera.IndexFuture {
		t.Helper()
		fs := make([]tessera.IndexFuture, n)
		for i := range n {
			fs[i] = a.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "%s %d", prefix, i)))
		}
		for _, f := range fs {
			if _, err := f(); err != nil {
				t.Fatalf("Add: %v", err)
			}
		}
		return fs
	}
	opts := func(interval time.Duration) *tessera.AppendOptions {
		return tessera.NewAppendOptions().WithCheckpointSigner(sk).WithCheckpointInterval(interval).WithBatching(1, time.Millisecond)
	}

	t.Run("published size seen by an instance which never integrated it", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		env := newPGTestEnv(t)
		m := newMemObjStore()
		// Instance A builds and publishes a log of 20 entries, then stops.
		seqA := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
		restore := snapshotDB(t, env) // The empty log.
		ctxA, cancelA := context.WithCancel(ctx)
		a, _, err := (&Storage{}).newAppender(ctxA, m, seqA, opts(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		add(t, ctxA, a, "a", 20)
		waitFor(t, 30*time.Second, "checkpoint for 20 entries", func() bool {
			raw, err := m.getObject(ctx, layout.CheckpointPath)
			return err == nil && strings.Contains(string(raw), "\n20\n")
		})
		cancelA()
		// Instance B starts (and sees the published checkpoint), then the database loses everything.
		seqB := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
		st := &Storage{}
		b, _, err := st.newAppender(ctx, m, seqB, opts(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		restore()
		if _, err := b.Add(ctx, tessera.NewEntry([]byte("after loss")))(); !errors.Is(err, ErrCoordinationBehind) {
			t.Fatalf("Add after the database lost commits = %v, want ErrCoordinationBehind", err)
		}
		if got := env.seqCoordNext(t); got != 0 {
			t.Errorf("SeqCoord.next = %d after refusal, want 0 (nothing handed out)", got)
		}
		if !errors.Is(st.CoordinationError(), ErrCoordinationBehind) {
			t.Errorf("not tripped: %v", st.CoordinationError())
		}
	})

	t.Run("integrated but not yet published", func(t *testing.T) {
		t.Parallel()
		ctx := t.Context()
		env := newPGTestEnv(t)
		m := newMemObjStore()
		seq := newTestSequencer(t, ctx, Config{DSN: env.DSN}, 1000)
		st := &Storage{}
		// With an hour-long checkpoint interval, only the initial empty checkpoint is published.
		a, _, err := st.newAppender(ctx, m, seq, opts(time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		add(t, ctx, a, "first", 10)
		waitFor(t, 30*time.Second, "integration of 10 entries", func() bool { s, _ := env.intCoord(t); return s == 10 })
		restore := snapshotDB(t, env)
		add(t, ctx, a, "second", 10)
		waitFor(t, 30*time.Second, "integration of 20 entries", func() bool { s, _ := env.intCoord(t); return s == 20 })
		restore() // Back to 10 integrated, while this instance integrated up to 20.
		if _, err := a.Add(ctx, tessera.NewEntry([]byte("after loss")))(); !errors.Is(err, ErrCoordinationBehind) {
			t.Fatalf("Add after the database lost commits = %v, want ErrCoordinationBehind", err)
		}
		if got := env.seqCoordNext(t); got != 10 {
			t.Errorf("SeqCoord.next = %d after refusal, want 10 (nothing handed out)", got)
		}
	})
}
