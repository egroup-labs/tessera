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
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"log/slog"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
)

const defaultPGTestDSN = "postgres://postgres:postgres@127.0.0.1:5432/tessera_test?sslmode=disable"

var isPGTestOptional = flag.Bool("pg_test_optional", true, "Boolean value to control whether the PostgreSQL tests are optional; if true, tests are skipped when PostgreSQL is unavailable")

// pgTestDSN returns the DSN of the PostgreSQL database used for tests.
func pgTestDSN() string {
	if dsn := os.Getenv("TESSERA_S3PG_TEST_DSN"); dsn != "" {
		return dsn
	}
	return defaultPGTestDSN
}

// canSkipPGTest checks if the test PostgreSQL db is available and, if not, if the test can be skipped.
//
// If pg_test_optional is set to false and the PostgreSQL database cannot be opened or pinged,
// the test will fail immediately. Otherwise, the test will be skipped if the test is optional
// and the database is not available.
func canSkipPGTest(t *testing.T, ctx context.Context) bool {
	t.Helper()

	db, err := sql.Open("pgx", pgTestDSN())
	if err != nil {
		if *isPGTestOptional {
			return true
		}
		t.Fatalf("failed to open PostgreSQL test db: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Fatalf("failed to close PostgreSQL database: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		if *isPGTestOptional {
			return true
		}
		t.Fatalf("failed to ping PostgreSQL test db: %v", err)
	}
	return false
}

// skipIfNoPG skips the test if PostgreSQL is unavailable and the tests are optional (see canSkipPGTest).
func skipIfNoPG(t *testing.T) {
	t.Helper()
	if canSkipPGTest(t, t.Context()) {
		slog.WarnContext(t.Context(), "PostgreSQL not available, skipping", slog.String("name", t.Name()))
		t.Skip("PostgreSQL not available, skipping test")
	}
}

// mustCreateSchema skips the test if PostgreSQL is unavailable (and optional), otherwise creates a fresh
// schema for the calling test and returns a DSN which selects it via search_path.
//
// The schema is dropped (CASCADE) when the test and all of its cleanups registered later have finished.
func mustCreateSchema(t *testing.T) (dsn, schema string) {
	t.Helper()
	ctx := t.Context()
	skipIfNoPG(t)

	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	schema = "s3pg_test_" + hex.EncodeToString(b[:])

	admin, err := sql.Open("pgx", pgTestDSN())
	if err != nil {
		t.Fatalf("failed to open PostgreSQL test db: %v", err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		_ = admin.Close()
		t.Fatalf("CREATE SCHEMA %s: %v", schema, err)
	}
	// Registered first, so runs last: after any pools opened by the test have been closed.
	t.Cleanup(func() {
		// t.Context() is already cancelled when cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
			t.Errorf("DROP SCHEMA %s: %v", schema, err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close admin db: %v", err)
		}
	})
	return withSearchPath(t, pgTestDSN(), schema), schema
}

// withSearchPath returns dsn with the search_path runtime parameter set to schema.
func withSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("invalid DSN: %v", err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		return u.String()
	}
	return dsn + " search_path=" + schema
}

// mustOpenDB opens a pool for the given DSN which is closed when the test finishes.
func mustOpenDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	})
	return db
}

// mustNewAntispam creates an AntispamStorage which is closed when the test finishes.
func mustNewAntispam(t *testing.T, dsn string, opts AntispamOpts) *AntispamStorage {
	t.Helper()
	as, err := NewAntispam(t.Context(), dsn, opts)
	if err != nil {
		t.Fatalf("NewAntispam: %v", err)
	}
	t.Cleanup(func() {
		if err := as.Close(); err != nil {
			t.Errorf("as.Close: %v", err)
		}
	})
	return as
}

// startFollower runs f.Follow in the background until the test finishes, and waits for it to exit
// before any previously registered cleanups (e.g. closing the antispam storage) run.
func startFollower(t *testing.T, f tessera.Follower, lr tessera.LogReader) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Follow(ctx, lr)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// waitForEntriesProcessed polls the follower until it reports at least want processed entries,
// failing the test after timeout. It returns all distinct values observed while polling, in order.
func waitForEntriesProcessed(t *testing.T, f tessera.Follower, want uint64, timeout time.Duration) []uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	var seen []uint64
	for {
		got, err := f.EntriesProcessed(ctx)
		if err != nil {
			t.Fatalf("EntriesProcessed: %v (seen so far: %v)", err, seen)
		}
		if len(seen) == 0 || seen[len(seen)-1] != got {
			seen = append(seen, got)
		}
		if got >= want {
			return seen
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for EntriesProcessed >= %d; seen %v", want, seen)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// fakeLogReader is an in-memory tessera.LogReader whose entries are generated by entryAt.
//
// Only IntegratedSize and ReadEntryBundle are meaningful; the rest exist to satisfy the interface.
type fakeLogReader struct {
	size    atomic.Uint64
	entryAt func(i uint64) []byte
}

func newFakeLogReader(size uint64, entryAt func(i uint64) []byte) *fakeLogReader {
	r := &fakeLogReader{entryAt: entryAt}
	r.size.Store(size)
	return r
}

func (r *fakeLogReader) ReadCheckpoint(context.Context) ([]byte, error) { return nil, os.ErrNotExist }

func (r *fakeLogReader) ReadTile(context.Context, uint64, uint64, uint8) ([]byte, error) {
	return nil, os.ErrNotExist
}

func (r *fakeLogReader) NextIndex(context.Context) (uint64, error) { return r.size.Load(), nil }

func (r *fakeLogReader) IntegratedSize(context.Context) (uint64, error) { return r.size.Load(), nil }

// ReadEntryBundle returns a tlog-tiles encoded entry bundle.
func (r *fakeLogReader) ReadEntryBundle(_ context.Context, index uint64, p uint8) ([]byte, error) {
	n := uint64(p)
	if n == 0 {
		n = layout.EntryBundleWidth
	}
	first := index * layout.EntryBundleWidth
	if first+n > r.size.Load() {
		return nil, os.ErrNotExist
	}
	var b []byte
	for i := first; i < first+n; i++ {
		e := r.entryAt(i)
		b = binary.BigEndian.AppendUint16(b, uint16(len(e)))
		b = append(b, e...)
	}
	return b, nil
}

// readIDSeq returns the full contents of AntispamIDSeq as a map of hex(hash) -> index.
func readIDSeq(t *testing.T, db *sql.DB) map[string]uint64 {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT h, idx FROM AntispamIDSeq")
	if err != nil {
		t.Fatalf("query AntispamIDSeq: %v", err)
	}
	defer func() { _ = rows.Close() }()
	r := make(map[string]uint64)
	for rows.Next() {
		var h []byte
		var idx uint64
		if err := rows.Scan(&h, &idx); err != nil {
			t.Fatalf("scan AntispamIDSeq: %v", err)
		}
		r[hex.EncodeToString(h)] = idx
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate AntispamIDSeq: %v", err)
	}
	return r
}

// schemaTables returns the (lower-cased) names of the tables in the given schema.
func schemaTables(t *testing.T, db *sql.DB, schema string) map[string]bool {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT table_name FROM information_schema.tables WHERE table_schema = $1", schema)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	defer func() { _ = rows.Close() }()
	r := make(map[string]bool)
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		r[n] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}
	return r
}
