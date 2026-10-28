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

package main

// The deterministic workload.
//
// The log is grown in phases. Each phase runs a fresh Appender (i.e. a restart of the
// personality) configured with WithBatching(batch, 1h), and performs `steps` steps of exactly
// `batch` entries each. A step adds its entries from a single goroutine, so the sequencer queue
// flushes exactly once, when the batch is full: every step is one Seq row with contiguous
// indices. The step then waits until the entries have been integrated before the next step is
// started, so each integration run consumes exactly one step. Integration points (and hence the
// set of partial tiles and bundles written) are therefore identical for both drivers.
//
// Entry contents are a pure function of the entry's index (see contentAt), so the expected
// index of every entry, and of the first occurrence of every duplicate, is known in advance.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/s3pg"
)

type phaseSpec struct {
	name  string
	kind  string // "tiny", "large" or "big"
	batch int
	steps int
}

type plan struct {
	phases []phaseSpec
	starts []uint64 // starting index of each phase
	size   uint64   // final log size
}

func newPlan(phases []phaseSpec) *plan {
	p := &plan{phases: phases}
	for _, ph := range phases {
		p.starts = append(p.starts, p.size)
		p.size += uint64(ph.batch * ph.steps)
	}
	return p
}

// fullPlan crosses entry bundle (256) and tile boundaries, lands exactly on multiples of 256 and of
// 256*256 (where the first level-1 tile becomes full), crosses 2*256*256 within one batch, and
// includes ~60KB entries and entries of the maximum permitted size.
func fullPlan() *plan {
	return newPlan([]phaseSpec{
		{"tiny-b1", "tiny", 1, 300},       // -> 300: one integration per entry
		{"tiny-b7", "tiny", 7, 100},       // -> 1000: unaligned batches crossing bundles
		{"align-1024", "tiny", 24, 1},     // -> 1024: lands exactly on a bundle boundary
		{"full-bundles", "tiny", 256, 4},  // -> 2048: whole bundles, no partials
		{"tiny-b300", "tiny", 300, 3},     // -> 2948: batches larger than a bundle
		{"large", "large", 50, 6},         // -> 3248: ~60KB entries (and some of 65535 bytes)
		{"to-65536", "big", 62288, 1},     // -> 65536: exactly 256*256
		{"after-65536", "tiny", 1, 3},     // -> 65539
		{"cross-131072", "big", 66050, 1}, // -> 131589: crosses 2*256*256 in one batch
		{"tail", "tiny", 5, 2},            // -> 131599
	})
}

// smallPlan is a quick variant of fullPlan.
func smallPlan() *plan {
	return newPlan([]phaseSpec{
		{"tiny-b1", "tiny", 1, 40},    // -> 40
		{"tiny-b7", "tiny", 7, 40},    // -> 320
		{"align-512", "tiny", 192, 1}, // -> 512
		{"large", "large", 20, 3},     // -> 572
		{"big-1000", "big", 1000, 1},  // -> 1572
		{"after", "tiny", 1, 2},       // -> 1574
	})
}

func (p *plan) phaseOf(idx uint64) phaseSpec {
	for i := len(p.phases) - 1; i >= 0; i-- {
		if idx >= p.starts[i] {
			return p.phases[i]
		}
	}
	panic("unreachable")
}

// contentAt returns the data of the entry at index idx. It includes duplicates:
//   - consecutive duplicates (idx%10 == 9 repeats idx-1): within one antispam follower batch,
//     and (for batch size 1) across integration batches,
//   - a shared value repeated every 50 entries, across all phases,
//   - entry 5 repeated every 97 entries, across phases,
//   - empty entries every 1000 entries.
func (p *plan) contentAt(idx uint64) []byte {
	kind := p.phaseOf(idx).kind
	switch {
	case kind != "large" && idx%1000 == 500:
		return []byte{}
	case kind != "large" && idx%50 == 25:
		return []byte("dup-shared-A")
	case kind != "large" && idx > 5 && idx%97 == 0:
		return p.contentAt(5)
	case idx%10 == 9:
		return p.contentAt(idx - 1)
	}
	switch kind {
	case "tiny":
		return fmt.Appendf(nil, "tiny-%d", idx)
	case "big":
		return append(fmt.Appendf(nil, "big-%d-", idx), bytes.Repeat([]byte{'x'}, int(idx%37))...)
	case "large":
		n := 59000 + int(idx%2000)
		if idx%100 == 33 {
			n = int(tessera.DefaultEntrySizeLimit)
		}
		r := rand.New(rand.NewPCG(idx, 0x5eed))
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(r.Uint32())
		}
		return b
	}
	panic("unknown kind " + kind)
}

// firstIndex returns, for every distinct entry identity in the log, the index of its first occurrence.
func (p *plan) firstIndex() map[[32]byte]uint64 {
	m := map[[32]byte]uint64{}
	for i := uint64(0); i < p.size; i++ {
		h := sha256.Sum256(p.contentAt(i))
		if _, ok := m[h]; !ok {
			m[h] = i
		}
	}
	return m
}

// runPhase runs one phase of the workload against a fresh Appender on the given driver.
//
// It returns once the Appender has been shut down, i.e. once all of the phase's entries have
// been integrated and a checkpoint committing to them has been published.
func runPhase(ctx context.Context, s *side, p *plan, pi int, gcInterval time.Duration) error {
	ph := p.phases[pi]
	in, err := startInstance(ctx, s, ph.batch, gcInterval)
	if err != nil {
		return err
	}
	defer in.kill()
	next := p.starts[pi]
	s.maxSeq = 0
	for step := 0; step < ph.steps; step++ {
		start := time.Now()
		if err := in.add(p, next, ph.batch); err != nil {
			return fmt.Errorf("phase %s step %d (%s, after %v): %v", ph.name, step, s.name, time.Since(start).Round(time.Millisecond), err)
		}
		s.maxSeq = max(s.maxSeq, time.Since(start))
		next += uint64(ph.batch)
		if err := awaitIntegrated(in.ctx, in.lr, next); err != nil {
			return fmt.Errorf("phase %s step %d (%s): %v", ph.name, step, s.name, err)
		}
	}
	return in.stop(ctx, next)
}

// instance is a running Appender (a personality process) on one side.
type instance struct {
	s        *side
	ctx      context.Context // cancelled by kill
	drv      tessera.Driver
	app      *tessera.Appender
	lr       tessera.LogReader
	shutdown func(context.Context) error
	cancel   context.CancelFunc
	closeFn  func()
	killOnce sync.Once
}

// startInstance starts an Appender on a new driver instance, batching exactly batch entries.
func startInstance(ctx context.Context, s *side, batch int, gcInterval time.Duration) (*instance, error) {
	actx, cancel := context.WithCancel(ctx)
	drv, closeFn, err := s.newDriver(actx)
	if err != nil {
		cancel()
		return nil, err
	}
	app, shutdown, lr, err := tessera.NewAppender(actx, drv, appendOptions(batch, gcInterval))
	if err != nil {
		cancel()
		closeFn()
		return nil, fmt.Errorf("NewAppender: %v", err)
	}
	return &instance{s: s, ctx: actx, drv: drv, app: app, lr: lr, shutdown: shutdown, cancel: cancel, closeFn: closeFn}, nil
}

// add adds the n entries starting at index from, from a single goroutine (so that they are
// sequenced as one batch if n is the batch size), and waits until they have been sequenced.
func (in *instance) add(p *plan, from uint64, n int) error {
	futures := make([]tessera.IndexFuture, n)
	for i := range futures {
		futures[i] = in.app.Add(in.ctx, tessera.NewEntry(p.contentAt(from+uint64(i))))
	}
	for i, f := range futures {
		idx, err := f()
		if err != nil {
			return fmt.Errorf("entry %d: %v", from+uint64(i), err)
		}
		if want := from + uint64(i); idx.Index != want || idx.IsDup {
			return fmt.Errorf("got index %+v, want %d (not a duplicate)", idx, want)
		}
	}
	return nil
}

// kill stops the instance without shutting it down, as if the process had crashed.
func (in *instance) kill() {
	in.killOnce.Do(func() {
		in.cancel()
		time.Sleep(100 * time.Millisecond)
		in.closeFn()
	})
}

// stop shuts the instance down cleanly, once a checkpoint of the given size has been published.
// For s3pg it also checks that no coordination guard has tripped.
func (in *instance) stop(ctx context.Context, size uint64) error {
	defer in.kill()
	sctx, scancel := context.WithTimeout(ctx, time.Minute)
	defer scancel()
	if err := in.shutdown(sctx); err != nil {
		return fmt.Errorf("shutdown: %v", err)
	}
	if st, ok := in.drv.(*s3pg.Storage); ok {
		if err := st.CoordinationError(); err != nil {
			return fmt.Errorf("s3pg coordination guard tripped: %v", err)
		}
	}
	// Shutdown returns as soon as the checkpoint object is visible, but the publisher updates
	// PubCoord (in the same transaction) only after writing the object. Cancelling the context
	// before that commit rolls the PubCoord update back (with either driver), leaving
	// PubCoord.size behind the published checkpoint. Wait for the commit so that the
	// coordination state compared after the phase does not depend on this timing.
	for {
		var pub sql.NullInt64
		if err := in.s.insp.QueryRowContext(sctx, "SELECT size FROM PubCoord WHERE id = 0").Scan(&pub); err != nil {
			return fmt.Errorf("reading PubCoord: %v", err)
		}
		if pub.Int64 == int64(size) {
			return nil
		}
		select {
		case <-sctx.Done():
			return fmt.Errorf("PubCoord.size %d != published size %d: %v", pub.Int64, size, sctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// appendOptions returns the AppendOptions used for both drivers.
func appendOptions(batch int, gcInterval time.Duration) *tessera.AppendOptions {
	return tessera.NewAppendOptions().
		WithCheckpointSigner(signer).
		WithCheckpointInterval(time.Second).
		WithBatching(uint(batch), time.Hour).
		WithPushback(tessera.DefaultPushbackMaxOutstanding).
		WithGarbageCollectionInterval(gcInterval)
}

// awaitIntegrated waits until the integrated tree has exactly the given size.
func awaitIntegrated(ctx context.Context, lr tessera.LogReader, size uint64) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	for {
		sz, err := lr.IntegratedSize(ctx)
		if err == nil && sz >= size {
			if sz != size {
				return fmt.Errorf("integrated size %d overshot expected %d", sz, size)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for integration of size %d (have %d, err %v): %v", size, sz, err, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
