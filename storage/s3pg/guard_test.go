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

// Tests for the fail-closed guards in guard.go: a coordination database which is behind the
// published log (lost commits after failover, restore from an old backup) must never cause a
// rollback or fork of the published checkpoint, nor writes of conflicting tiles/bundles.

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/parse"
)

var coordTables = []string{"Tessera", "SeqCoord", "Seq", "IntCoord", "PubCoord", "GCCoord"}

// snapshotDB copies the coordination tables, returning a function which restores them, i.e.
// simulates losing every commit made after the snapshot.
func snapshotDB(t *testing.T, env *pgTestEnv) func() {
	t.Helper()
	tag := randHex(t, 4)
	for _, tb := range coordTables {
		env.exec(t, fmt.Sprintf("CREATE TABLE snap_%s_%s AS SELECT * FROM %s", tb, tag, tb))
	}
	return func() {
		for _, tb := range coordTables {
			env.exec(t, fmt.Sprintf("DELETE FROM %s", tb))
			env.exec(t, fmt.Sprintf("INSERT INTO %s SELECT * FROM snap_%s_%s", tb, tb, tag))
		}
	}
}

// readCP returns the size and root of the checkpoint in the bucket.
func readCP(t *testing.T, ctx context.Context, r *s3Reader) (uint64, []byte) {
	t.Helper()
	raw, err := r.ReadCheckpoint(ctx)
	if err != nil {
		t.Fatalf("ReadCheckpoint: %v", err)
	}
	_, size, root, err := parse.CheckpointUnsafe(raw)
	if err != nil {
		t.Fatalf("parse checkpoint: %v", err)
	}
	return size, root
}

// An instance started against a database which is behind the published checkpoint refuses to start.
func TestGuardStartupRefusesStaleDatabase(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, _ := mustGenerateKeys(t)
	cfg := Config{SDKConfig: b.SDKConfig, S3Options: b.S3Options, Bucket: b.Name, DSN: env.DSN}

	ctx1, cancel1 := context.WithCancel(ctx)
	a1 := newE2EAppender(t, ctx1, cfg, sk)
	mustAddConcurrently(t, ctx1, a1, "before", 100, 4)
	restore := snapshotDB(t, env)
	mustAddConcurrently(t, ctx1, a1, "after", 100, 4)
	if err := a1.shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	cancel1()

	restore()
	d, err := New(ctx, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.(*Storage).Close() })
	_, _, _, err = tessera.NewAppender(ctx, d, tessera.NewAppendOptions().WithCheckpointSigner(sk).WithCheckpointInterval(time.Second))
	if err == nil || !strings.Contains(err.Error(), ErrCoordinationBehind.Error()) {
		t.Fatalf("NewAppender against stale database: err = %v, want %v", err, ErrCoordinationBehind)
	}
}

// A running instance whose database loses commits underneath it (e.g. failover to a lagging
// replica) never publishes a smaller or inconsistent checkpoint, and writes no new log objects.
func TestGuardRunningInstanceStaleDatabase(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, _ := mustGenerateKeys(t)
	r := &s3Reader{client: b.Client, bucket: b.Name}
	a := newE2EAppender(t, ctx, Config{SDKConfig: b.SDKConfig, S3Options: b.S3Options, Bucket: b.Name, DSN: env.DSN}, sk)

	// 230 entries before the snapshot, 230 after: the lost range straddles bundle boundaries, and
	// re-sequenced entries would land both in already-full and in partial bundles.
	mustAddConcurrently(t, ctx, a, "before", 230, 4)
	restore := snapshotDB(t, env)
	mustAddConcurrently(t, ctx, a, "after", 230, 4)
	waitFor(t, 30*time.Second, "checkpoint for 460 entries", func() bool { s, _ := readCP(t, ctx, r); return s == 460 })
	pubSize, pubRoot := readCP(t, ctx, r)
	keysBefore := b.keys(t)

	// Watch the published checkpoint for regressions or root changes from here on.
	var mu sync.Mutex
	var bad []string
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
			}
			if s, root := readCP(t, ctx, r); s != pubSize || !bytes.Equal(root, pubRoot) {
				mu.Lock()
				bad = append(bad, fmt.Sprintf("size %d root %x", s, root))
				mu.Unlock()
			}
		}
	})

	restore() // The database now believes only 230 entries exist.

	// New entries are still sequenced (that can't be prevented without a DB round trip to S3 on
	// every add), but must never be integrated or published.
	for i := range 20 {
		// The returned indices (from 230) collide with published entries; the guard's job is to
		// make sure they are never integrated or published.
		_, _ = a.appender.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "forked %d", i)))()
	}
	time.Sleep(5 * time.Second) // Several integration and publication cycles.
	close(stop)
	wg.Wait()

	if len(bad) > 0 {
		t.Errorf("published checkpoint changed after the database lost commits: %q", bad)
	}
	// (Only additions matter: garbage collection may legitimately remove partial objects.)
	added := []string{}
	for _, k := range b.keys(t) {
		if !slices.Contains(keysBefore, k) && k != trippedMarkerPath {
			added = append(added, k)
		}
	}
	if !slices.Contains(b.keys(t), trippedMarkerPath) {
		t.Errorf("tripped marker %q not written", trippedMarkerPath)
	}
	if len(added) > 0 {
		t.Errorf("objects written after the database lost commits: %q", added)
	}
	if size, _ := env.intCoord(t); size != 230 {
		t.Errorf("IntCoord advanced to %d on a stale database, want 230", size)
	}
}

// checkPublicationAllowed refuses regressions, same-size forks and larger-but-inconsistent trees,
// and accepts republication and consistent growth.
func TestGuardPublicationRules(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := newMemObjStore()
	// The database's (fresh) integrated size is 300: smaller than some published sizes below.
	a := &Appender{logStore: &logResourceStore{objStore: m, entriesPath: layout.EntriesPath}, sequencer: fixedTreeSequencer{size: 300}}

	leaves := func(prefix string, n int) [][]byte {
		r := make([][]byte, n)
		for i := range n {
			r[i] = rfc6962.DefaultHasher.HashLeaf(fmt.Appendf(nil, "%s %d", prefix, i))
		}
		return r
	}
	rootOf := func(lh [][]byte) []byte {
		rf := compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}
		cr := rf.NewEmptyRange(0)
		for _, h := range lh {
			if err := cr.Append(h, nil); err != nil {
				t.Fatal(err)
			}
		}
		r, err := cr.GetRootHash(nil)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	publish := func(size uint64, root []byte) {
		if err := m.setObject(ctx, layout.CheckpointPath, fmt.Appendf(nil, "test\n%d\n%s\n", size, base64.StdEncoding.EncodeToString(root)), "", ""); err != nil {
			t.Fatal(err)
		}
	}

	// The bucket's tiles describe tree "real" of 700 leaves.
	real := leaves("real", 700)
	if _, err := integrate(ctx, 0, real, a.logStore); err != nil {
		t.Fatalf("integrate: %v", err)
	}
	other := leaves("other", 300)

	// No checkpoint yet: anything goes.
	if err := a.checkPublicationAllowed(ctx, nil, 700, rootOf(real)); err != nil {
		t.Errorf("first publication: %v", err)
	}
	for _, tc := range []struct {
		name             string
		pubSize, newSize uint64
		pubRoot, newRoot []byte
		wantErr          bool
	}{
		{"republish same", 300, 300, rootOf(real[:300]), rootOf(real[:300]), false},
		{"consistent growth", 300, 700, rootOf(real[:300]), rootOf(real), false},
		{"growth from empty", 0, 700, rootOf(nil), rootOf(real), false},
		{"regression", 700, 300, rootOf(real), rootOf(real[:300]), true},
		{"same size fork", 300, 300, rootOf(real[:300]), rootOf(other), true},
		{"growth from a forked checkpoint", 300, 700, rootOf(other), rootOf(real), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a.behind.Store(nil) // Each case starts untripped.
			publish(tc.pubSize, tc.pubRoot)
			pubRaw, _, err := a.logStore.readCheckpointWithETag(ctx)
			if err != nil {
				t.Fatal(err)
			}
			err = a.checkPublicationAllowed(ctx, pubRaw, tc.newSize, tc.newRoot)
			if gotErr := err != nil; gotErr != tc.wantErr {
				t.Fatalf("checkPublicationAllowed(%d) = %v, wantErr %v", tc.newSize, err, tc.wantErr)
			}
			if tc.wantErr && !errors.Is(err, ErrCoordinationBehind) {
				t.Errorf("error %v is not ErrCoordinationBehind", err)
			}
		})
	}

	// An unparseable published checkpoint fails closed.
	if err := m.setObject(ctx, layout.CheckpointPath, []byte("bananas"), "", ""); err != nil {
		t.Fatal(err)
	}
	a.behind.Store(nil)
	if err := a.checkPublicationAllowed(ctx, []byte("bananas"), 700, rootOf(real)); err == nil {
		t.Error("publication over an unparseable checkpoint succeeded, want error")
	}
}

// The LogReader falls back from a missing partial resource to the full one (upstream mapped
// S3's NoSuchKey to a generic error, so the fallback never happened).
func TestLogReaderPartialFallsBackToFull(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	m := newMemObjStore()
	lr := &logResourceStore{objStore: m, entriesPath: layout.EntriesPath}
	if err := m.setObject(ctx, layout.EntriesPath(3, 0), []byte("full bundle"), "", ""); err != nil {
		t.Fatal(err)
	}
	if err := m.setObject(ctx, layout.TilePath(1, 2, 0), []byte("full tile"), "", ""); err != nil {
		t.Fatal(err)
	}
	if got, err := lr.ReadEntryBundle(ctx, 3, 17); err != nil || string(got) != "full bundle" {
		t.Errorf("ReadEntryBundle(3, p=17) = %q, %v; want full bundle", got, err)
	}
	if got, err := lr.ReadTile(ctx, 1, 2, 9); err != nil || string(got) != "full tile" {
		t.Errorf("ReadTile(1, 2, p=9) = %q, %v; want full tile", got, err)
	}
	if _, err := lr.ReadEntryBundle(ctx, 4, 0); err == nil {
		t.Error("ReadEntryBundle of a missing bundle succeeded")
	}
}

// fixedTreeSequencer is a sequencer whose currentTree always reports the given size. Other methods
// must not be called.
type fixedTreeSequencer struct {
	sequencer
	size uint64
}

func (f fixedTreeSequencer) currentTree(context.Context) (uint64, []byte, error) {
	return f.size, nil, nil
}
