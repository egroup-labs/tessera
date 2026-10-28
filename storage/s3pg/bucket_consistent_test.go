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

// Tests for checkBucketConsistent ("F3"): integration refuses to write entry bundles / level-0
// tiles over a bucket which already holds DIFFERENT data for the indices being integrated (the
// database lost commits whose objects had been written), while a legitimate retry after a
// failed database commit (identical data) goes through.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	storage "github.com/transparency-dev/tessera/storage/internal"
)

func TestCheckBucketConsistent(t *testing.T) {
	t.Parallel()
	const W = layout.EntryBundleWidth
	// E is the log; F a fork of it (different entries at the same indices).
	E, F := seqEntries("e", 0, 3*W), seqEntries("f", 0, 3*W)
	ct := tessera.NewAppendOptions().WithCTLayout().EntriesPath()
	std := layout.EntriesPath
	type obj struct {
		bundle bool
		b      uint64
		p      uint8 // 0: full
		data   []byte
	}
	// bun/til describe a bundle/tile b of width p (0: full) holding the given entries.
	bun := func(b uint64, p uint8, es ...[]storage.SequencedEntry) obj {
		return obj{true, b, p, concatResource(slices.Concat(es...), false)}
	}
	til := func(b uint64, p uint8, es ...[]storage.SequencedEntry) obj {
		return obj{false, b, p, concatResource(slices.Concat(es...), true)}
	}
	junk := func(o obj) obj { o.data = append(o.data, "junk"...); return o }
	// both returns the bundle and tile b of width p.
	both := func(b uint64, p uint8, es ...[]storage.SequencedEntry) []obj {
		return []obj{bun(b, p, es...), til(b, p, es...)}
	}

	for _, tc := range []struct {
		name        string
		entriesPath func(uint64, uint8) string
		bucket      []obj
		from, n     uint64
		// wantErr is "" for success, else a substring of the expected ErrCoordinationBehind error.
		wantErr string
		// fast: the check must decide from the two listings alone, reading no objects.
		fast bool
		// checkOnly: the bucket lacks the upper tiles integration needs, only run the check.
		checkOnly bool
	}{
		{name: "empty bucket", from: 0, n: 10, fast: true},
		{name: "normal partial", bucket: slices.Concat(both(0, 3, E[:3]), both(0, 7, E[:7]), both(0, 10, E[:10])), from: 10, n: 5, fast: true},
		{name: "normal, at bundle boundary", bucket: slices.Concat(both(0, 200, E[:200]), both(0, 0, E[:W])), from: W, n: 20, fast: true, checkOnly: true},
		{name: "normal, CT layout", entriesPath: ct, bucket: both(0, 10, E[:10]), from: 10, n: 5, fast: true},

		{name: "retry: wider partial, identical", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 15, E[:15])), from: 10, n: 5},
		{name: "retry: existing wider than batch, identical", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 20, E[:20])), from: 10, n: 5},
		{name: "retry: batch wider than existing, identical", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 12, E[:12])), from: 10, n: 5},
		{name: "retry: from 0, identical", bucket: both(0, 5, E[:5]), from: 0, n: 10},
		{name: "retry: full bundle, identical", bucket: slices.Concat(both(0, 200, E[:200]), both(0, 0, E[:W])), from: 200, n: 56},
		{name: "retry: full bundle, batch into the next, identical", bucket: slices.Concat(both(0, 200, E[:200]), both(0, 0, E[:W]), both(1, 30, E[W:W+30])), from: 200, n: 100},
		{name: "retry: several bundles, identical", bucket: slices.Concat(both(0, 250, E[:250]), both(0, 0, E[:W]), both(1, 0, E[W:2*W]), both(2, 20, E[2*W:2*W+20])), from: 250, n: 2*W + 20 - 250},
		{name: "retry: more entries than the failed attempt, into an absent bundle", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 15, E[:15])), from: 10, n: 300},
		{name: "retry: tile ahead of bundle, identical", bucket: slices.Concat(both(0, 10, E[:10]), []obj{til(0, 15, E[:15])}), from: 10, n: 5},
		{name: "retry: CT layout, identical", entriesPath: ct, bucket: slices.Concat(both(0, 10, E[:10]), both(0, 15, E[:15])), from: 10, n: 5},
		{name: "fork at the standard path is ignored with the CT layout", entriesPath: ct, bucket: slices.Concat(both(0, 10, E[:10]), []obj{{true, 0, 15, nil}}), from: 10, n: 5, fast: true},

		{name: "wider partial differs", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 15, E[:10], F[10:15])), from: 10, n: 5, wantErr: " 0 in the bucket"},
		{name: "wider partial differs, narrower partials sort after it", bucket: slices.Concat(both(0, 3, E[:3]), both(0, 7, E[:7]), both(0, 10, E[:10]), both(0, 15, E[:10], F[10:15])), from: 10, n: 5, wantErr: " 0 in the bucket"},
		{name: "existing wider than batch differs", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 20, E[:10], F[10:20])), from: 10, n: 5, wantErr: " 0 in the bucket"},
		{name: "existing differs only beyond the batch", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 20, E[:15], F[15:20])), from: 10, n: 5},
		{name: "batch wider than existing, existing differs", bucket: slices.Concat(both(0, 10, E[:10]), both(0, 12, E[:10], F[10:12])), from: 10, n: 5, wantErr: " 0 in the bucket"},
		{name: "existing has trailing garbage", bucket: slices.Concat(both(0, 10, E[:10]), []obj{junk(bun(0, 12, E[:12])), til(0, 12, E[:12])}), from: 10, n: 5, wantErr: "entry bundle 0"},
		{name: "from 0, differs", bucket: both(0, 5, F[:5]), from: 0, n: 10, wantErr: " 0 in the bucket"},
		{name: "full bundle differs", bucket: slices.Concat(both(0, 200, E[:200]), both(0, 0, E[:200], F[200:W])), from: 200, n: 56, wantErr: " 0 in the bucket"},
		{name: "only a full bundle, differs", bucket: both(0, 0, F[:W]), from: 0, n: 10, wantErr: " 0 in the bucket"},
		{name: "only the tile is ahead, and differs", bucket: slices.Concat(both(0, 10, E[:10]), []obj{til(0, 15, E[:10], F[10:15])}), from: 10, n: 5, wantErr: "level 0 tile 0"},
		{name: "only the bundle is ahead, and differs", bucket: slices.Concat(both(0, 10, E[:10]), []obj{bun(0, 15, E[:10], F[10:15])}), from: 10, n: 5, wantErr: "entry bundle 0"},
		{name: "tile differs while bundle is identical", bucket: slices.Concat(both(0, 10, E[:10]), []obj{bun(0, 15, E[:15]), til(0, 15, E[:10], F[10:15])}), from: 10, n: 5, wantErr: "level 0 tile 0"},
		{name: "several bundles, a later one differs", bucket: slices.Concat(both(0, 250, E[:250]), both(0, 0, E[:W]), both(1, 0, F[W:2*W]), both(2, 20, E[2*W:2*W+20])), from: 250, n: 2*W + 20 - 250, wantErr: " 1 in the bucket"},
		{name: "several bundles, the last tile differs", bucket: slices.Concat(both(0, 250, E[:250]), both(0, 0, E[:W]), both(1, 0, E[W:2*W]), []obj{bun(2, 20, E[2*W:2*W+20]), til(2, 20, F[2*W:2*W+20])}), from: 250, n: 2*W + 20 - 250, wantErr: "level 0 tile 2"},
		{name: "CT layout, differs", entriesPath: ct, bucket: slices.Concat(both(0, 10, E[:10]), both(0, 15, E[:10], F[10:15])), from: 10, n: 5, wantErr: " 0 in the bucket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			ep := tc.entriesPath
			if ep == nil {
				ep = std
			}
			m := newMemObjStore()
			for _, o := range tc.bucket {
				key := layout.TilePath(0, o.b, o.p)
				if o.bundle {
					// A nil-data bundle is placed at the standard path, whatever the layout.
					if key = ep(o.b, o.p); o.data == nil {
						key, o.data = std(o.b, o.p), concatResource(F[:o.p], false)
					}
				}
				if err := m.setObject(ctx, key, o.data, "", ""); err != nil {
					t.Fatal(err)
				}
			}
			before := memContents(m)
			spy := &spyStore{objStore: m}
			a, st := newBareAppender(spy, nil, ep)
			entries := E[tc.from : tc.from+tc.n]

			err := a.checkBucketConsistent(ctx, tc.from, entries)
			gets, lists, writes := spy.take()
			if tc.wantErr == "" && err != nil {
				t.Fatalf("checkBucketConsistent = %v, want nil", err)
			}
			if tc.wantErr != "" && (!errors.Is(err, ErrCoordinationBehind) || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("checkBucketConsistent = %v, want ErrCoordinationBehind about %q", err, tc.wantErr)
			}
			if len(writes) != 0 {
				t.Errorf("checkBucketConsistent wrote %q", writes)
			}
			if tc.fast && (len(gets) != 0 || len(lists) != 2) {
				t.Errorf("fast path: read %q and listed %q; want no reads and exactly 2 listings", gets, lists)
			}
			if !tc.fast && len(lists) <= 2 {
				t.Errorf("listed only %q; the bucket is ahead, so each resource must be checked", lists)
			}

			if tc.checkOnly {
				return
			}
			// Integration as a whole: refused without writing anything (and tripping the guards),
			// or writes the same objects as the bucket already has.
			_, err = a.integrateEntries(ctx, tc.from, entries)
			if tc.wantErr != "" {
				if !errors.Is(err, ErrCoordinationBehind) {
					t.Fatalf("integrateEntries = %v, want ErrCoordinationBehind", err)
				}
				checkMemTripped(t, m, before, "refused integration")
				if ce := st.CoordinationError(); !errors.Is(ce, ErrCoordinationBehind) {
					t.Errorf("CoordinationError = %v, want ErrCoordinationBehind", ce)
				}
				return
			}
			if err != nil {
				t.Fatalf("integrateEntries = %v, want nil", err)
			}
			after := memContents(m)
			for k, v := range before {
				if string(after[k]) != string(v) {
					t.Errorf("integration replaced %q", k)
				}
			}
			end := tc.from + tc.n
			if want := concatResource(E[(end-1)/W*W:end], false); string(after[ep((end-1)/W, uint8(end%W))]) != string(want) {
				t.Errorf("integration didn't write the expected right-most entry bundle")
			}
			if st.CoordinationError() != nil {
				t.Errorf("CoordinationError = %v, want nil", st.CoordinationError())
			}
		})
	}
}

// A wider resource whose narrower prefix can't be read fails (closed), without writing anything.
func TestCheckBucketConsistentUnreadablePrefix(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	E := seqEntries("e", 0, 20)
	m := newMemObjStore()
	for _, k := range []string{layout.EntriesPath(0, 15), layout.TilePath(0, 0, 15)} {
		if err := m.setObject(ctx, k, []byte("x"), "", ""); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := newBareAppender(m, nil, layout.EntriesPath)
	// The bucket has a .p/15 but not the .p/10 which integrating from 10 implies.
	if err := a.checkBucketConsistent(ctx, 10, E[10:15]); err == nil {
		t.Error("checkBucketConsistent with an unreadable narrower resource succeeded")
	}
	before := memContents(m)
	if _, err := a.integrateEntries(ctx, 10, E[10:15]); err == nil {
		t.Error("integrateEntries succeeded")
	}
	checkMemUnchanged(t, m, before, "failed integration")
}

// End to end (PostgreSQL + S3): an integration whose IntCoord UPDATE fails after its objects
// were written is retried through checkBucketConsistent's slow path (the bucket is "ahead" with
// identical data) and succeeds; the log verifies and no guard trips.
func TestE2EFailedIntegrationCommitRetry(t *testing.T) {
	t.Parallel()
	b := newTestBucket(t)
	env := newPGTestEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), e2eTimeout)
	defer cancel()
	sk, vk := mustGenerateKeys(t)

	d, err := New(ctx, Config{SDKConfig: b.SDKConfig, S3Options: b.S3Options, Bucket: b.Name, DSN: env.DSN})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = d.(*Storage).Close() })
	app, shutdown, lr, err := tessera.NewAppender(ctx, d, tessera.NewAppendOptions().
		WithCheckpointSigner(sk).
		WithCheckpointInterval(time.Second).
		WithBatching(16, 20*time.Millisecond).
		WithGarbageCollectionInterval(0))
	if err != nil {
		t.Fatalf("NewAppender: %v", err)
	}
	a := &e2eAppender{appender: app, shutdown: shutdown, reader: lr, awaiter: tessera.NewPublicationAwaiter(ctx, lr.ReadCheckpoint, 50*time.Millisecond)}
	want := mustAddConcurrently(t, ctx, a, "before", 200, 4)

	// Fail the next IntCoord update which grows the tree, once, recording its new size in a
	// sequence (sequences are not transactional).
	q := quoteIdent(env.Schema)
	env.exec(t, fmt.Sprintf("CREATE SEQUENCE %s.fail_calls", q))
	env.exec(t, fmt.Sprintf("CREATE SEQUENCE %s.failed_size MINVALUE 0 START 0", q))
	env.exec(t, fmt.Sprintf(`CREATE FUNCTION %[1]s.fail_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF NEW.seq > OLD.seq THEN
		IF nextval('%[1]s.fail_calls') = 1 THEN
			PERFORM setval('%[1]s.failed_size', NEW.seq);
			RAISE EXCEPTION 'injected IntCoord update failure';
		END IF;
	END IF;
	RETURN NEW;
END $$`, q))
	env.exec(t, fmt.Sprintf("CREATE TRIGGER fail_once BEFORE UPDATE ON %s.IntCoord FOR EACH ROW EXECUTE FUNCTION %s.fail_once()", q, q))

	more := mustAddConcurrently(t, ctx, a, "after", 150, 4)
	for k, v := range more {
		want[k] = v
	}
	checkIndices(t, want, 0, 350)
	calls := env.queryUint64(t, fmt.Sprintf("SELECT last_value FROM %s.fail_calls", q))
	failedSize := env.queryUint64(t, fmt.Sprintf("SELECT last_value FROM %s.failed_size", q))
	if calls < 2 || failedSize <= 200 {
		t.Fatalf("the injected failure didn't happen as intended: trigger calls %d, failed update to size %d", calls, failedSize)
	}
	r := &s3Reader{client: b.Client, bucket: b.Name}
	// The failed attempt wrote the resources for [200, failedSize), so the retry from 200 found
	// the bucket ahead of the database.
	if _, err := r.get(ctx, layout.EntriesPath((failedSize-1)/layout.EntryBundleWidth, uint8(failedSize%layout.EntryBundleWidth))); err != nil {
		t.Errorf("entry bundle written by the failed attempt (size %d): %v", failedSize, err)
	}
	cp, _ := verifyLog(t, ctx, r, vk, want)
	checkDBIntegrated(t, env, 350, cp.Hash)
	if err := d.(*Storage).CoordinationError(); err != nil {
		t.Errorf("CoordinationError = %v, want nil", err)
	}
	if err := shutdown(ctx); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}
