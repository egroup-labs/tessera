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

// chaos writes unique entries to one or more s3pg conformance servers while failures are
// injected externally (see run.sh), records every acknowledged write and every published
// checkpoint, and then verifies the log's durability and consistency invariants:
//
//   - every acknowledged entry is integrated, exactly once, at the index it was acknowledged with;
//   - no two checkpoints commit to different roots for the same size (no fork), and every
//     observed checkpoint is consistent with the final one;
//   - inclusion proofs verify for a sample of entries.
//
// It exits non-zero if any invariant is violated.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	f_log "github.com/transparency-dev/formats/log"
	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/client"
)

var (
	writeURLs = flag.String("write_urls", "", "comma separated conformance server URLs")
	logURL    = flag.String("log_url", "", "public read URL of the log")
	pubKey    = flag.String("public_key", "", "log public key (vkey)")
	writers   = flag.Int("writers", 32, "concurrent writers")
	duration  = flag.Duration("duration", 2*time.Minute, "how long to write for")
	settle    = flag.Duration("settle", 3*time.Minute, "max time to wait for acked entries to be integrated")
	outFile   = flag.String("out", "", "write JSON summary here")
)

type acked struct {
	idx  uint64
	data []byte
}

type cp struct {
	Size uint64
	Hash []byte
}

func main() {
	flag.Parse()
	ctx := context.Background()
	urls := strings.Split(*writeURLs, ",")
	v, err := f_note.NewVerifier(*pubKey)
	must(err)
	lu, err := url.Parse(*logURL)
	must(err)
	fetcher, err := client.NewHTTPFetcher(lu, &http.Client{Timeout: 30 * time.Second})
	must(err)

	var (
		mu        sync.Mutex
		acks      []acked
		errCount  atomic.Uint64
		pushbacks atomic.Uint64
		cps       = map[uint64]cp{}
	)

	// Checkpoint watcher: records every distinct checkpoint observed. A size seen with two
	// different hashes is a fork.
	stopWatch := make(chan struct{})
	watchDone := make(chan struct{})
	forks := 0
	go func() {
		defer close(watchDone)
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-t.C:
			}
			c, _, _, err := client.FetchCheckpoint(ctx, fetcher.ReadCheckpoint, v, v.Name())
			if err != nil {
				continue
			}
			mu.Lock()
			if old, ok := cps[c.Size]; ok && !bytes.Equal(old.Hash, c.Hash) {
				forks++
				slog.ErrorContext(ctx, "FORK: same size, different root", slog.Uint64("size", c.Size))
			}
			cps[c.Size] = cp{Size: c.Size, Hash: c.Hash}
			mu.Unlock()
		}
	}()

	hc := &http.Client{Timeout: 20 * time.Second}
	deadline := time.Now().Add(*duration)
	var wg sync.WaitGroup
	for w := range *writers {
		wg.Go(func() {
			for n := 0; time.Now().Before(deadline); n++ {
				r := make([]byte, 16)
				_, _ = rand.Read(r)
				data := fmt.Appendf(nil, "chaos/%d/%d/%s", w, n, hex.EncodeToString(r))
				ui, _ := rand.Int(rand.Reader, big.NewInt(int64(len(urls))))
				resp, err := hc.Post(urls[ui.Int64()]+"/add", "application/octet-stream", bytes.NewReader(data))
				if err != nil {
					errCount.Add(1)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusServiceUnavailable {
					pushbacks.Add(1)
					time.Sleep(200 * time.Millisecond)
					continue
				}
				if resp.StatusCode != http.StatusOK {
					errCount.Add(1)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				idx, err := strconv.ParseUint(strings.TrimSpace(string(body)), 10, 64)
				if err != nil {
					errCount.Add(1)
					continue
				}
				mu.Lock()
				acks = append(acks, acked{idx: idx, data: data})
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	var maxIdx uint64
	for _, a := range acks {
		maxIdx = max(maxIdx, a.idx)
	}
	slog.InfoContext(ctx, "writing finished", slog.Int("acked", len(acks)), slog.Uint64("errors", errCount.Load()), slog.Uint64("pushbacks", pushbacks.Load()), slog.Uint64("maxIdx", maxIdx))

	// Every acknowledged entry must eventually be integrated, even if the replica which
	// accepted it has been killed in the meantime.
	var final *f_log.Checkpoint
	settleBy := time.Now().Add(*settle)
	for {
		c, _, _, err := client.FetchCheckpoint(ctx, fetcher.ReadCheckpoint, v, v.Name())
		if err == nil && c.Size > maxIdx {
			final = c
			break
		}
		if time.Now().After(settleBy) {
			size := uint64(0)
			if c != nil {
				size = c.Size
			}
			fail("acked entries not integrated within settle period: checkpoint size %d, max acked index %d", size, maxIdx)
		}
		time.Sleep(500 * time.Millisecond)
	}
	close(stopWatch)
	<-watchDone
	slog.InfoContext(ctx, "final checkpoint", slog.Uint64("size", final.Size))

	problems := []string{}
	if forks > 0 {
		problems = append(problems, fmt.Sprintf("%d forks observed", forks))
	}

	// All observed checkpoints must be consistent with the final one.
	pb, err := client.NewProofBuilder(ctx, final.Size, fetcher.ReadTile)
	must(err)
	consistencyChecked := 0
	for _, c := range cps {
		if c.Size == 0 || c.Size > final.Size {
			continue
		}
		p, err := pb.ConsistencyProof(ctx, c.Size, final.Size)
		if err != nil {
			problems = append(problems, fmt.Sprintf("consistency proof %d->%d: %v", c.Size, final.Size, err))
			continue
		}
		if err := proof.VerifyConsistency(rfc6962.DefaultHasher, c.Size, final.Size, p, c.Hash, final.Hash); err != nil {
			problems = append(problems, fmt.Sprintf("checkpoint %d inconsistent with final %d: %v", c.Size, final.Size, err))
		}
		consistencyChecked++
	}

	// Read the whole log, check each acked entry is at its acked index, and that no chaos
	// entry appears more than once (which would mean an entry was sequenced twice).
	seen := map[string][]uint64{}
	for b := uint64(0); b*layout.EntryBundleWidth < final.Size; b++ {
		eb, err := client.GetEntryBundle(ctx, fetcher.ReadEntryBundle, b, final.Size)
		if err != nil {
			fail("bundle %d: %v", b, err)
		}
		for i, e := range eb.Entries {
			idx := b*layout.EntryBundleWidth + uint64(i)
			seen[string(e)] = append(seen[string(e)], idx)
			// Spot-check inclusion proofs for a sample of entries.
			if idx%97 == 0 {
				ip, err := pb.InclusionProof(ctx, idx)
				if err != nil {
					problems = append(problems, fmt.Sprintf("inclusion proof %d: %v", idx, err))
				} else if err := proof.VerifyInclusion(rfc6962.DefaultHasher, idx, final.Size, rfc6962.DefaultHasher.HashLeaf(e), ip, final.Hash); err != nil {
					problems = append(problems, fmt.Sprintf("inclusion %d: %v", idx, err))
				}
			}
		}
	}
	lost, moved, dups := 0, 0, 0
	for _, a := range acks {
		got, ok := seen[string(a.data)]
		switch {
		case !ok:
			lost++
		case len(got) != 1 || got[0] != a.idx:
			moved++
		}
	}
	unacked := 0
	ackedSet := map[string]bool{}
	for _, a := range acks {
		ackedSet[string(a.data)] = true
	}
	for d, idxs := range seen {
		if len(idxs) > 1 {
			dups++
		}
		if strings.HasPrefix(d, "chaos/") && !ackedSet[d] {
			unacked++
		}
	}
	if lost > 0 {
		problems = append(problems, fmt.Sprintf("%d acked entries LOST", lost))
	}
	if moved > 0 {
		problems = append(problems, fmt.Sprintf("%d acked entries at wrong index", moved))
	}
	if dups > 0 {
		problems = append(problems, fmt.Sprintf("%d entries appear more than once", dups))
	}
	if uint64(len(seen))+0 > final.Size {
		problems = append(problems, "more distinct entries than log size")
	}

	summary := map[string]any{
		"acked":                len(acks),
		"write_errors":         errCount.Load(),
		"pushbacks":            pushbacks.Load(),
		"final_size":           final.Size,
		"checkpoints_observed": len(cps),
		"consistency_checked":  consistencyChecked,
		"unacked_but_present":  unacked,
		"lost":                 lost,
		"wrong_index":          moved,
		"duplicates":           dups,
		"forks":                forks,
		"problems":             problems,
	}
	j, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(j))
	if *outFile != "" {
		_ = os.WriteFile(*outFile, j, 0o644)
	}
	if len(problems) > 0 {
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(f string, a ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+f+"\n", a...)
	os.Exit(1)
}
