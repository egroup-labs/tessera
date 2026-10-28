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

// Fail-closed guards against the coordination database being BEHIND the log in object storage.
//
// This driver (like storage/aws) treats PostgreSQL as the source of truth for the log's state.
// If PostgreSQL loses recently committed transactions while object storage keeps the newer
// state - e.g. a failover to an asynchronously replicated standby, a crash with
// synchronous_commit=off, or a restore from a backup that is older than the bucket - the unguarded
// upstream code would publish a checkpoint for the older (smaller) tree over the newer one (a
// rollback visible to clients), and could integrate different entries at indices which have
// already been published or written, publishing a checkpoint inconsistent with one clients have
// already seen (a fork) and leaving tiles/bundles inconsistent with each other in the bucket.
//
// These checks make the driver refuse to do any of that. Instead, integration, publication and
// sequencing stop with ErrCoordinationBehind, and new instances refuse to start, until an operator
// has reconciled the database with the bucket (see README.md, "Recovering from a lost commit").
//
// They are not a substitute for synchronous replication: entries which were sequenced but not
// yet integrated when the commits were lost cannot be recovered.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"

	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/internal/parse"
	storage "github.com/transparency-dev/tessera/storage/internal"
)

// ErrCoordinationBehind is returned when the coordination database is behind, or inconsistent
// with, the log already written to object storage.
var ErrCoordinationBehind = errors.New("s3pg: coordination database is behind or inconsistent with the published checkpoint")

// errStaleView is returned (without tripping) when a guard's comparison only failed because this
// instance's view of the database became stale while it was checking: e.g. its connection (and so
// its row lock) was lost and another instance has integrated and published in the meantime. The
// operation is retried as usual.
var errStaleView = errors.New("s3pg: coordination state moved on while checking (lost connection or lock?); retrying")

// trippedMarkerPath is the object, relative to the bucket prefix, recording that an instance's
// guards have tripped. While it exists, instances refuse to start; it is removed by the operator
// as the last step of the recovery procedure (see README.md, "Recovering from a lost commit").
const trippedMarkerPath = "s3pg-coordination-tripped"

// markerWriteTimeout bounds the write of the tripped marker.
const markerWriteTimeout = 10 * time.Second

// trip records err if it is an ErrCoordinationBehind, and returns it. The first trip also writes
// the tripped marker, so that the condition survives restarts of this and other instances.
func (a *Appender) trip(ctx context.Context, err error) error {
	return a.tripWith(ctx, err, true)
}

// tripWith is trip, writing the marker only if persist is set.
func (a *Appender) tripWith(ctx context.Context, err error, persist bool) error {
	if errors.Is(err, ErrCoordinationBehind) {
		if a.behind.CompareAndSwap(nil, &err) {
			slog.ErrorContext(ctx, "Coordination database is behind or inconsistent with the log in object storage: refusing to integrate, publish or sequence until recovered (see README.md, \"Recovering from a lost commit\")", slog.Any("error", err))
			if a.onBehind != nil {
				a.onBehind(err)
			}
			if persist {
				mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), markerWriteTimeout)
				defer cancel()
				marker := fmt.Appendf(nil, "%s\n%s\n", time.Now().UTC().Format(time.RFC3339Nano), err)
				if mErr := a.logStore.objStore.setObject(mctx, trippedMarkerPath, marker, "text/plain; charset=utf-8", "no-cache"); mErr != nil {
					slog.ErrorContext(ctx, "Failed to write tripped marker", slog.String("path", trippedMarkerPath), slog.Any("error", mErr))
				}
			}
		}
	}
	return err
}

// observe records that the log has (at least) size n: published, or integrated (and so also
// sequenced). Sequencing never hands out indices below the largest size observed.
func (a *Appender) observe(n uint64) {
	for {
		cur := a.highWater.Load()
		if n <= cur || a.highWater.CompareAndSwap(cur, n) {
			return
		}
	}
}

// checkTrippedMarker fails if an instance has previously tripped (see trippedMarkerPath).
func (a *Appender) checkTrippedMarker(ctx context.Context) error {
	m, err := a.logStore.objStore.getObject(ctx, trippedMarkerPath)
	if err != nil {
		var nske *types.NoSuchKey
		if errors.As(err, &nske) {
			return nil
		}
		return fmt.Errorf("failed to check for tripped marker %q: %v", trippedMarkerPath, err)
	}
	err = fmt.Errorf("%w: marker %q exists (written when an instance's guard tripped: %q); complete the recovery procedure, then delete the marker", ErrCoordinationBehind, trippedMarkerPath, strings.TrimSpace(string(m)))
	if a.behind.CompareAndSwap(nil, &err) && a.onBehind != nil {
		a.onBehind(err)
	}
	return err
}

// confirmBehind re-reads the integrated tree size with a fresh statement (on the caller's
// transaction connection, if ctx carries one: see withTx, so it can't starve the pool, and fails if
// that connection has been lost), and returns ErrCoordinationBehind (tripping) only if it is still
// smaller than pubSize. In a healthy system the published size never exceeds the committed
// integrated size, which only grows, so a fresh read resolves comparisons made with a stale view.
func (a *Appender) confirmBehind(ctx context.Context, pubSize uint64, behindErr error) error {
	size, _, err := a.sequencer.currentTree(ctx)
	if err != nil {
		return fmt.Errorf("could not confirm a suspected coordination lag (published size %d), will retry: failed to re-read the integrated size: %v", pubSize, err)
	}
	if size >= pubSize {
		return fmt.Errorf("%w (fresh integrated size %d >= published size %d)", errStaleView, size, pubSize)
	}
	return a.trip(ctx, behindErr)
}

// behindErr returns the error which tripped the appender's guards, if any.
func (a *Appender) behindErr() error {
	if e := a.behind.Load(); e != nil {
		return *e
	}
	return nil
}

// publishedCheckpoint returns the size and root hash of the checkpoint currently published in
// object storage, and false if there is none. The signature is not verified: anyone able to write
// to the bucket could replace the checkpoint wholesale anyway.
func (a *Appender) publishedCheckpoint(ctx context.Context) (uint64, []byte, bool, error) {
	raw, err := a.logStore.ReadCheckpoint(ctx)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, fmt.Errorf("failed to read published checkpoint: %v", err)
	}
	size, root, ok, err := parsePublished(raw)
	if ok {
		a.observe(size)
	}
	return size, root, ok, err
}

func parsePublished(raw []byte) (uint64, []byte, bool, error) {
	if raw == nil {
		return 0, nil, false, nil
	}
	_, size, root, err := parse.CheckpointUnsafe(raw)
	if err != nil {
		return 0, nil, false, fmt.Errorf("failed to parse published checkpoint: %v", err)
	}
	return size, root, true, nil
}

// checkIntegrationAllowed fails if the published checkpoint is larger than the tree the database
// says has been integrated (fromSeq), or if the bucket already holds tiles or entry bundles which
// differ from the entries about to be integrated (see checkBucketConsistent).
func (a *Appender) checkIntegrationAllowed(ctx context.Context, fromSeq uint64, entries []storage.SequencedEntry) error {
	if err := a.behindErr(); err != nil {
		return err
	}
	pubSize, _, ok, err := a.publishedCheckpoint(ctx)
	if err != nil {
		return err
	}
	if ok && pubSize > fromSeq {
		return a.confirmBehind(ctx, pubSize, fmt.Errorf("%w: database integrated size %d < published checkpoint size %d; refusing to integrate", ErrCoordinationBehind, fromSeq, pubSize))
	}
	return a.trip(ctx, a.checkBucketConsistent(ctx, fromSeq, entries))
}

// checkPublicationAllowed fails unless a checkpoint for (size, root) is an append-only extension of
// the published checkpoint pubRaw (nil if none): it must not be smaller, must have the same root at
// the same size, and must be provably consistent (via a consistency proof built from the log's own
// tiles) when larger.
//
// It is called while holding the PubCoord row lock; the subsequent write is additionally
// conditional on the published checkpoint not having changed (see updateCheckpoint).
func (a *Appender) checkPublicationAllowed(ctx context.Context, pubRaw []byte, size uint64, root []byte) error {
	if err := a.behindErr(); err != nil {
		return err
	}
	pubSize, pubRoot, ok, err := parsePublished(pubRaw)
	if err != nil || !ok {
		return err
	}
	a.observe(pubSize)
	switch {
	case size < pubSize:
		return a.confirmBehind(ctx, pubSize, fmt.Errorf("%w: refusing to publish checkpoint of size %d over published size %d", ErrCoordinationBehind, size, pubSize))
	case size == pubSize:
		if !bytes.Equal(root, pubRoot) {
			return a.trip(ctx, fmt.Errorf("%w: refusing to publish root %x for size %d, published root is %x", ErrCoordinationBehind, root, size, pubRoot))
		}
		return nil
	case pubSize == 0:
		return nil
	}
	pb, err := client.NewProofBuilder(ctx, size, a.logStore.ReadTile)
	if err != nil {
		return fmt.Errorf("failed to create proof builder for consistency check: %v", err)
	}
	p, err := pb.ConsistencyProof(ctx, pubSize, size)
	if err != nil {
		return fmt.Errorf("failed to build consistency proof %d -> %d: %v", pubSize, size, err)
	}
	if err := proof.VerifyConsistency(rfc6962.DefaultHasher, pubSize, size, p, pubRoot, root); err != nil {
		return a.trip(ctx, fmt.Errorf("%w: new checkpoint (size %d) is not consistent with published checkpoint (size %d): %v", ErrCoordinationBehind, size, pubSize, err))
	}
	return nil
}

// checkStartup fails if the database is behind (or inconsistent with) the published checkpoint,
// so that an instance pointed at a stale database refuses to start rather than stalling later.
func (a *Appender) checkStartup(ctx context.Context) error {
	pubSize, pubRoot, ok, err := a.publishedCheckpoint(ctx)
	if err != nil || !ok {
		return err
	}
	size, root, err := a.sequencer.currentTree(ctx)
	if err != nil {
		return err
	}
	if size < pubSize || (size == pubSize && !bytes.Equal(root, pubRoot)) {
		// No marker: start-up re-checks every time, and a misconfigured instance (e.g. pointed at
		// the wrong database) must not block correctly configured ones.
		return a.tripWith(ctx, fmt.Errorf("%w: database tree (size %d, root %x) vs published checkpoint (size %d, root %x)", ErrCoordinationBehind, size, root, pubSize, pubRoot), false)
	}
	return nil
}

// checkBucketConsistent fails if the bucket already holds entry bundles or level-0 tiles covering
// indices from fromSeq onwards whose contents differ from entries.
//
// Data beyond the database's integrated size is normal after an integration attempt whose
// database commit failed (the retry then writes identical objects). Different data means the
// database has lost commits whose tiles and bundles had already been written: integrating other
// entries at those indices would leave partial and full resources which contradict each other.
//
// Fast path: if the bucket is not ahead in the bundle (or level-0 tile) containing fromSeq, which
// is checked with one listing each, nothing else is compared. A failed earlier attempt which wrote
// later bundles but not that first one (they are written concurrently), followed by the database
// also losing those Seq rows, is not detected here; writing different data to those later
// objects then fails on the conditional write (a stall, never a fork).
func (a *Appender) checkBucketConsistent(ctx context.Context, fromSeq uint64, entries []storage.SequencedEntry) error {
	if len(entries) == 0 {
		return nil
	}
	const width = layout.EntryBundleWidth
	first := fromSeq / width
	bundlePath := func(b uint64) func(uint8) string {
		return func(p uint8) string { return a.logStore.entriesPath(b, p) }
	}
	tilePath := func(b uint64) func(uint8) string {
		return func(p uint8) string { return layout.TilePath(0, b, p) }
	}
	var widths [2]uint64
	eg, ectx := errgroup.WithContext(ctx)
	for i, path := range []func(uint8) string{bundlePath(first), tilePath(first)} {
		eg.Go(func() error {
			w, _, err := a.widest(ectx, path)
			widths[i] = w
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	if widths[0] <= fromSeq%width && widths[1] <= fromSeq%width {
		return nil
	}

	// Slow path: compare every bundle and level-0 tile touched by the batch, concurrently, so that
	// even a large batch fits comfortably within the integration timeout.
	const concurrency = 16
	eg, ectx = errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	end := fromSeq + uint64(len(entries))
	for b := first; b*width < end; b++ {
		lo, hi := max(fromSeq, b*width), min(end, (b+1)*width)
		items := entries[lo-fromSeq : hi-fromSeq]
		eg.Go(func() error {
			return a.checkResourceConsistent(ectx, "entry bundle", b, bundlePath(b), lo-b*width, items, func(e storage.SequencedEntry) []byte { return e.BundleData })
		})
		eg.Go(func() error {
			return a.checkResourceConsistent(ectx, "level 0 tile", b, tilePath(b), lo-b*width, items, func(e storage.SequencedEntry) []byte { return e.LeafHash })
		})
	}
	return eg.Wait()
}

// checkResourceConsistent checks one entry bundle or level-0 tile b: if the bucket already has a
// version of it wider than base (the number of items already integrated), the items it holds from
// base onwards must equal the items about to be written.
func (a *Appender) checkResourceConsistent(ctx context.Context, kind string, b uint64, path func(uint8) string, base uint64, items []storage.SequencedEntry, data func(storage.SequencedEntry) []byte) error {
	w, key, err := a.widest(ctx, path)
	if err != nil || w <= base {
		return err
	}
	existing, err := a.logStore.objStore.getObject(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to read %s %s: %v", kind, key, err)
	}
	// What the resource should look like up to the overlap: the already integrated prefix
	// followed by the new items.
	want := []byte{}
	if base > 0 {
		if want, err = a.logStore.objStore.getObject(ctx, path(uint8(base))); err != nil {
			return fmt.Errorf("failed to read %s %s: %v", kind, path(uint8(base)), err)
		}
	}
	overlap := min(w-base, uint64(len(items)))
	for _, it := range items[:overlap] {
		want = append(want, data(it)...)
	}
	ok := bytes.HasPrefix(existing, want)
	if w-base == overlap {
		// The existing resource ends within (or exactly at the end of) the new items.
		ok = bytes.Equal(existing, want)
	}
	if !ok {
		return fmt.Errorf("%w: %s %d in the bucket (%s, %d items) differs from the entries being integrated from index %d", ErrCoordinationBehind, kind, b, key, w, b*layout.EntryBundleWidth+base)
	}
	return nil
}

// widest returns the width (number of items; 256 if full) and name of the widest existing version
// of the resource whose full and partial names are given by path, or 0 if none exists.
func (a *Appender) widest(ctx context.Context, path func(uint8) string) (uint64, string, error) {
	full := path(0)
	keys, err := a.logStore.objStore.listObjects(ctx, full)
	if err != nil {
		return 0, "", fmt.Errorf("failed to list %s*: %v", full, err)
	}
	var w uint64
	var key string
	var partials map[string]uint64
	for _, k := range keys {
		if k == full {
			return layout.EntryBundleWidth, k, nil
		}
		if !strings.HasPrefix(k, full) {
			continue
		}
		if partials == nil {
			partials = make(map[string]uint64, layout.EntryBundleWidth-1)
			for p := uint64(1); p < layout.EntryBundleWidth; p++ {
				partials[path(uint8(p))] = p
			}
		}
		if p, ok := partials[k]; ok && p > w {
			w, key = p, k
		}
	}
	return w, key, nil
}
