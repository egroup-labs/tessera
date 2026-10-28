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

// cpwatch polls a log's published checkpoint (directly from object storage, never through a fault
// proxy) and records every distinct checkpoint. It flags:
//
//   - REGRESSION: a checkpoint smaller than one observed earlier (a rollback visible to clients);
//   - FORK: two different roots for the same size;
//   - INCONSISTENT: a larger checkpoint whose consistency proof (built from the log's own tiles)
//     from the largest earlier checkpoint does not verify.
//
// Runs until SIGTERM/SIGINT, then prints a JSON summary (exit 1 on any violation).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	f_note "github.com/transparency-dev/formats/note"
	"github.com/transparency-dev/merkle/proof"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/client"
)

var (
	logURL   = flag.String("log_url", "", "direct read URL of the log")
	pubKey   = flag.String("public_key", "", "log public key (vkey)")
	interval = flag.Duration("interval", 100*time.Millisecond, "poll interval")
	outFile  = flag.String("out", "", "write JSON summary here")
	evFile   = flag.String("events", "", "append events here")
)

type cpt struct {
	Size uint64
	Hash []byte
}

func main() {
	flag.Parse()
	ctx := context.Background()
	v, err := f_note.NewVerifier(*pubKey)
	if err != nil {
		panic(err)
	}
	lu, err := url.Parse(*logURL)
	if err != nil {
		panic(err)
	}
	f, err := client.NewHTTPFetcher(lu, &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}})
	if err != nil {
		panic(err)
	}
	var ev io.Writer = os.Stderr
	if *evFile != "" {
		if fh, err := os.OpenFile(*evFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			ev = fh
		}
	}
	logf := func(format string, a ...any) {
		_, _ = fmt.Fprintf(ev, "%s %s\n", time.Now().UTC().Format("15:04:05.000"), fmt.Sprintf(format, a...))
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)

	seen := map[uint64][]byte{}
	var maxCP *cpt    // largest checkpoint observed
	var lastCP *cpt   // most recently observed checkpoint
	var verified *cpt // largest checkpoint verified consistent with all earlier ones
	pending := []cpt{}
	regressions, forks, inconsistent, polls, errs, distinct := 0, 0, 0, 0, 0, 0
	var regressionDetails []string

	verify := func() {
		// Verify pending larger checkpoints against the latest verified one, in order of observation.
		for len(pending) > 0 {
			c := pending[0]
			if verified == nil || verified.Size == 0 || c.Size <= verified.Size {
				if verified == nil || c.Size >= verified.Size {
					verified = &c
				}
				pending = pending[1:]
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			pb, err := client.NewProofBuilder(cctx, c.Size, f.ReadTile)
			var p [][]byte
			if err == nil {
				p, err = pb.ConsistencyProof(cctx, verified.Size, c.Size)
			}
			cancel()
			if err != nil {
				logf("cannot build consistency proof %d->%d yet: %v", verified.Size, c.Size, err)
				return
			}
			if err := proof.VerifyConsistency(rfc6962.DefaultHasher, verified.Size, c.Size, p, verified.Hash, c.Hash); err != nil {
				inconsistent++
				logf("INCONSISTENT: checkpoint %d/%x not consistent with %d/%x: %v", c.Size, c.Hash, verified.Size, verified.Hash, err)
			}
			verified = &c
			pending = pending[1:]
		}
	}

	t := time.NewTicker(*interval)
loop:
	for {
		select {
		case <-sig:
			break loop
		case <-t.C:
		}
		polls++
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		c, _, _, err := client.FetchCheckpoint(cctx, f.ReadCheckpoint, v, v.Name())
		cancel()
		if err != nil {
			errs++
			continue
		}
		cur := cpt{Size: c.Size, Hash: c.Hash}
		if lastCP != nil && lastCP.Size == cur.Size && bytes.Equal(lastCP.Hash, cur.Hash) {
			continue
		}
		lastCP = &cur
		if old, ok := seen[cur.Size]; ok {
			if !bytes.Equal(old, cur.Hash) {
				forks++
				logf("FORK: size %d root %x, earlier root %x", cur.Size, cur.Hash, old)
			}
		} else {
			distinct++
			seen[cur.Size] = cur.Hash
		}
		switch {
		case maxCP == nil || cur.Size > maxCP.Size:
			logf("checkpoint size=%d root=%x", cur.Size, cur.Hash)
			maxCP = &cur
			pending = append(pending, cur)
		case cur.Size < maxCP.Size:
			regressions++
			d := fmt.Sprintf("REGRESSION: checkpoint size %d (root %x) published after size %d", cur.Size, cur.Hash, maxCP.Size)
			regressionDetails = append(regressionDetails, time.Now().UTC().Format("15:04:05.000")+" "+d)
			logf("%s", d)
		default:
			logf("checkpoint size=%d root=%x (re-observed after change)", cur.Size, cur.Hash)
		}
		verify()
	}
	// Final attempt at verifying anything pending.
	for i := 0; i < 10 && len(pending) > 0; i++ {
		verify()
		if len(pending) > 0 {
			time.Sleep(time.Second)
		}
	}
	var maxSize uint64
	if maxCP != nil {
		maxSize = maxCP.Size
	}
	summary := map[string]any{
		"polls": polls, "poll_errors": errs, "distinct_checkpoints": distinct, "max_size": maxSize,
		"regressions": regressions, "regression_details": regressionDetails, "forks": forks, "inconsistent": inconsistent,
		"unverified": len(pending),
	}
	j, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(j))
	if *outFile != "" {
		_ = os.WriteFile(*outFile, j, 0o644)
	}
	if regressions+forks+inconsistent > 0 {
		os.Exit(1)
	}
}
