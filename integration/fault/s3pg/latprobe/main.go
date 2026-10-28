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

// latprobe measures end-to-end latencies of an s3pg log under load: every -interval it adds one
// entry through a conformance server (-add_url) and records how long it took until the entry was
// acknowledged (sequenced), integrated (IntCoord.seq > index, read directly from PostgreSQL) and
// published (checkpoint size > index, read directly from object storage). Prints a JSON summary
// with percentiles when the duration is over (or on SIGTERM).
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	addURL   = flag.String("add_url", "", "conformance server base URL (POST <url>/add)")
	cpURL    = flag.String("checkpoint_url", "", "direct URL of the published checkpoint")
	dsn      = flag.String("dsn", "", "PostgreSQL DSN selecting the log's schema (search_path)")
	interval = flag.Duration("interval", 500*time.Millisecond, "time between probes")
	duration = flag.Duration("duration", time.Minute, "how long to probe")
	timeout  = flag.Duration("timeout", 60*time.Second, "give up on a probe after this long")
	outFile  = flag.String("out", "", "write JSON summary here")
)

var hc = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}

func add(ctx context.Context, n int) (uint64, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(*addURL, "/")+"/add", strings.NewReader(fmt.Sprintf("latprobe-%d-%d", time.Now().UnixNano(), n)))
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, b)
	}
	return strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
}

func cpSize(ctx context.Context) (uint64, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, *cpURL, nil)
	resp, err := hc.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	l := strings.Split(string(b), "\n")
	if resp.StatusCode != 200 || len(l) < 2 {
		return 0, fmt.Errorf("checkpoint: HTTP %d", resp.StatusCode)
	}
	return strconv.ParseUint(l[1], 10, 64)
}

type sample struct{ ack, integ, pub float64 }

func pct(v []float64, p float64) float64 {
	if len(v) == 0 {
		return 0
	}
	sort.Float64s(v)
	i := int(p * float64(len(v)-1))
	return float64(int(v[i]*1000)) / 1000
}

func main() {
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	db, err := sql.Open("pgx", *dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	var mu sync.Mutex
	var samples []sample
	errs, timeouts := 0, 0
	var wg sync.WaitGroup
	t := time.NewTicker(*interval)
	defer t.Stop()
	for n := 0; ; n++ {
		select {
		case <-ctx.Done():
		case <-t.C:
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				pctx, pcancel := context.WithTimeout(context.Background(), *timeout)
				defer pcancel()
				start := time.Now()
				idx, err := add(pctx, n)
				if err != nil {
					mu.Lock()
					errs++
					mu.Unlock()
					return
				}
				s := sample{ack: time.Since(start).Seconds()}
				for s.integ == 0 || s.pub == 0 {
					if pctx.Err() != nil {
						mu.Lock()
						timeouts++
						mu.Unlock()
						return
					}
					if s.integ == 0 {
						var seq uint64
						if err := db.QueryRowContext(pctx, "SELECT seq FROM IntCoord WHERE id = 0").Scan(&seq); err == nil && seq > idx {
							s.integ = time.Since(start).Seconds()
						}
					}
					if s.pub == 0 {
						if size, err := cpSize(pctx); err == nil && size > idx {
							s.pub = time.Since(start).Seconds()
							if s.integ == 0 {
								s.integ = s.pub
							}
						}
					}
					time.Sleep(10 * time.Millisecond)
				}
				mu.Lock()
				samples = append(samples, s)
				mu.Unlock()
			}(n)
			continue
		}
		break
	}
	wg.Wait()
	var a, i, p []float64
	for _, s := range samples {
		a, i, p = append(a, s.ack), append(i, s.integ), append(p, s.pub)
	}
	res := map[string]any{"samples": len(samples), "add_errors": errs, "timeouts": timeouts,
		"ack_p50": pct(a, 0.5), "ack_p95": pct(a, 0.95), "integrated_p50": pct(i, 0.5), "integrated_p95": pct(i, 0.95), "integrated_max": pct(i, 1),
		"published_p50": pct(p, 0.5), "published_p95": pct(p, 0.95), "published_max": pct(p, 1)}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	if *outFile != "" {
		_ = os.WriteFile(*outFile, b, 0o644)
	}
}
