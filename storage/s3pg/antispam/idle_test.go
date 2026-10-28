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

package antispam

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// MaxIdleConns: 0 keeps the database/sql default (idle connections are kept between lookups),
// a negative value disables idle connections. This intentionally differs from storage/aws.
func TestMaxIdleConnsSemantics(t *testing.T) {
	t.Parallel()
	dsn, _ := mustCreateSchema(t)
	for _, tc := range []struct {
		maxIdle  int
		wantIdle bool
	}{{0, true}, {3, true}, {-1, false}} {
		as := mustNewAntispam(t, dsn, AntispamOpts{MaxIdleConns: tc.maxIdle})
		if _, err := as.index(t.Context(), make([]byte, 32)); err != nil {
			t.Fatalf("index: %v", err)
		}
		if got := as.dbPool.Stats().Idle > 0; got != tc.wantIdle {
			t.Errorf("MaxIdleConns=%d: idle connections kept = %v, want %v", tc.maxIdle, got, tc.wantIdle)
		}
	}
}

// The antispam store must not depend on the server's default_transaction_isolation.
func TestIsolationLevelIndependent(t *testing.T) {
	t.Parallel()
	for _, iso := range []string{"repeatable read", "serializable"} {
		t.Run(iso, func(t *testing.T) {
			t.Parallel()
			dsn, _ := mustCreateSchema(t)
			u, err := url.Parse(dsn)
			if err != nil {
				t.Fatal(err)
			}
			q := u.Query()
			q.Set("default_transaction_isolation", iso)
			// pgx does not decode '+' as a space in DSN query parameters.
			u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
			dsn = u.String()

			// Concurrent initialisation, then two followers racing over the same log.
			const n = 6
			stores := make([]*AntispamStorage, n)
			errs := make([]error, n)
			var wg sync.WaitGroup
			for i := range n {
				wg.Go(func() { stores[i], errs[i] = NewAntispam(t.Context(), dsn, AntispamOpts{MaxBatchSize: 16}) })
			}
			wg.Wait()
			for i := range n {
				if errs[i] != nil {
					t.Fatalf("concurrent NewAntispam[%d]: %v", i, errs[i])
				}
				t.Cleanup(func() { _ = stores[i].Close() })
			}
			const size = 500
			lr := newFakeLogReader(size, func(i uint64) []byte { return fmt.Appendf(nil, "entry %d", i) })
			fA, fB := stores[0].Follower(testBundleHasher), stores[1].Follower(testBundleHasher)
			startFollower(t, fA, lr)
			startFollower(t, fB, lr)
			waitForEntriesProcessed(t, fA, size, 30*time.Second)
			waitForEntriesProcessed(t, fB, size, 30*time.Second)
			for i := uint64(0); i < size; i += 37 {
				idx, err := stores[2].index(t.Context(), testIDHash(fmt.Appendf(nil, "entry %d", i)))
				if err != nil || idx == nil || *idx != i {
					t.Errorf("index(entry %d) = %v, %v; want %d", i, idx, err, i)
				}
			}
		})
	}
}
