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

// Tests for AntispamOpts.Schema (every statement names the schema; the connection's search_path
// points at a decoy copy of the tables with sentinel values, or at nothing), a follower whose
// position is beyond the log, and missing DDL privileges.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/transparency-dev/tessera"
)

func randName(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// adminDB returns a pool on the test database (no search_path), closed when the test finishes.
func adminDB(t *testing.T) *sql.DB {
	t.Helper()
	skipIfNoPG(t)
	return mustOpenDB(t, pgTestDSN())
}

// newOddSchema creates an empty schema whose name needs quoting, dropped when the test finishes
// (after cleanups registered later, e.g. closing pools).
func newOddSchema(t *testing.T) string {
	t.Helper()
	skipIfNoPG(t)
	admin, err := sql.Open("pgx", pgTestDSN())
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf(`Anti Spam "Q"-%s`, randName(t, 6))
	if _, err := admin.ExecContext(t.Context(), "CREATE SCHEMA "+quoteIdent(name)); err != nil {
		_ = admin.Close()
		t.Fatalf("CREATE SCHEMA: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteIdent(name)+" CASCADE"); err != nil {
			t.Errorf("DROP SCHEMA: %v", err)
		}
		_ = admin.Close()
	})
	return name
}

func dsnParams(t *testing.T, dsn string, params map[string]string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	return u.String()
}

// fingerprint returns every row (with xmin, xmax and ctid) of every table in schema.
func fingerprint(t *testing.T, db *sql.DB, schema string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	r := map[string][]string{}
	for tbl := range schemaTables(t, db, schema) {
		rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT concat_ws(' ', xmin, xmax, ctid, t::text) FROM %s.%s AS t ORDER BY 1", quoteIdent(schema), quoteIdent(tbl)))
		if err != nil {
			t.Fatalf("read %s: %v", tbl, err)
		}
		vals := []string{}
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatal(err)
			}
			vals = append(vals, v)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		r[tbl] = vals
	}
	return r
}

// readQualifiedIDSeq returns AntispamIDSeq in schema as hex(hash) -> index.
func readQualifiedIDSeq(t *testing.T, db *sql.DB, schema string) map[string]uint64 {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT h, idx FROM "+quoteIdent(schema)+".AntispamIDSeq")
	if err != nil {
		t.Fatalf("read AntispamIDSeq: %v", err)
	}
	defer func() { _ = rows.Close() }()
	r := map[string]uint64{}
	for rows.Next() {
		var h []byte
		var idx uint64
		if err := rows.Scan(&h, &idx); err != nil {
			t.Fatal(err)
		}
		r[hex.EncodeToString(h)] = idx
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return r
}

func connCount(t *testing.T, db *sql.DB, app string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", app).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// checkIndex checks, via the decorator, that entries "entry i" (i in idxs) are known at index i,
// and that a new entry is passed on to the delegate.
func checkIndex(t *testing.T, ctx context.Context, as *AntispamStorage, idxs ...uint64) {
	t.Helper()
	delegated := 0
	add := as.Decorator()(func(context.Context, *tessera.Entry) tessera.IndexFuture {
		delegated++
		return func() (tessera.Index, error) { return tessera.Index{Index: 1 << 40}, nil }
	})
	for _, i := range idxs {
		if got, err := add(ctx, tessera.NewEntry(fmt.Appendf(nil, "entry %d", i)))(); err != nil || !got.IsDup || got.Index != i {
			t.Errorf("lookup of entry %d = (%+v, %v), want duplicate at %d", i, got, err, i)
		}
	}
	if delegated != 0 {
		t.Errorf("%d known entries passed on to the delegate", delegated)
	}
	if got, err := add(ctx, tessera.NewEntry([]byte("never seen")))(); err != nil || got.IsDup || delegated != 1 {
		t.Errorf("new entry = (%+v, %v), delegated %d; want passed on", got, err, delegated)
	}
}

func entryAt(i uint64) []byte { return fmt.Appendf(nil, "entry %d", i) }

// exerciseAntispam initialises several instances concurrently, runs two followers over a log of
// size entries, and checks the resulting index in schema (read through db).
func exerciseAntispam(t *testing.T, dsn, schema string, db *sql.DB) {
	t.Helper()
	ctx := t.Context()
	const n = 4
	stores := make([]*AntispamStorage, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { stores[i], errs[i] = NewAntispam(ctx, dsn, AntispamOpts{Schema: schema, MaxBatchSize: 16}) })
	}
	wg.Wait()
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("concurrent NewAntispam[%d]: %v", i, errs[i])
		}
		t.Cleanup(func() { _ = stores[i].Close() })
	}
	got := []string{}
	for tbl := range schemaTables(t, db, schema) {
		got = append(got, tbl)
	}
	slices.Sort(got)
	want := slices.Sorted(slices.Values(antispamTables))
	if !slices.Equal(got, want) {
		t.Fatalf("tables in %q = %v, want %v", schema, got, want)
	}
	const size = 300
	lr := newFakeLogReader(size, entryAt)
	fA, fB := stores[0].Follower(testBundleHasher), stores[1].Follower(testBundleHasher)
	startFollower(t, fA, lr)
	startFollower(t, fB, lr)
	waitForEntriesProcessed(t, fA, size, time.Minute)
	waitForEntriesProcessed(t, fB, size, time.Minute)
	if got, err := stores[3].Follower(nil).EntriesProcessed(ctx); err != nil || got != size {
		t.Errorf("EntriesProcessed = (%d, %v), want %d", got, err, size)
	}
	idseq := readQualifiedIDSeq(t, db, schema)
	if len(idseq) != size {
		t.Errorf("AntispamIDSeq has %d rows, want %d", len(idseq), size)
	}
	for i := range uint64(size) {
		if got, ok := idseq[hex.EncodeToString(testIDHash(entryAt(i)))]; !ok || got != i {
			t.Errorf("AntispamIDSeq[entry %d] = (%d, %v), want %d", i, got, ok, i)
		}
	}
	checkIndex(t, ctx, stores[2], 0, 1, 5, 17, 255, 256, 299)
}

// With AntispamOpts.Schema, the antispam store never touches the search_path's schema, which here
// holds a decoy copy of its tables with sentinel values.
func TestSchemaQualifiedAntispam(t *testing.T) {
	t.Parallel()
	dsn, decoy := mustCreateSchema(t)
	db := mustOpenDB(t, dsn)
	for _, q := range []string{
		SchemaSQL,
		"UPDATE AntispamMeta SET compatibilityVersion = 99",
		"UPDATE AntispamFollowCoord SET nextIdx = 999999",
	} {
		if _, err := db.ExecContext(t.Context(), q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for i := range uint64(20) {
		if _, err := db.ExecContext(t.Context(), "INSERT INTO AntispamIDSeq (h, idx) VALUES ($1, $2)", testIDHash(entryAt(i)), 424242+i); err != nil {
			t.Fatal(err)
		}
	}
	schema := newOddSchema(t)
	before := fingerprint(t, db, decoy)

	exerciseAntispam(t, dsn, schema, db)

	if diff := cmp.Diff(before, fingerprint(t, db, decoy)); diff != "" {
		t.Errorf("decoy schema changed (-before +after):\n%s", diff)
	}
}

// With AntispamOpts.Schema, a connection without any search_path works.
func TestSchemaQualifiedAntispamWithoutSearchPath(t *testing.T) {
	t.Parallel()
	schema := newOddSchema(t)
	dsn := dsnParams(t, pgTestDSN(), map[string]string{"search_path": ""})
	db := mustOpenDB(t, dsn)
	var schemas string
	if err := db.QueryRowContext(t.Context(), "SELECT current_schemas(false)::text").Scan(&schemas); err != nil || schemas != "{}" {
		t.Fatalf("current_schemas = %q (%v), want {}", schemas, err)
	}
	exerciseAntispam(t, dsn, schema, db)
}

// A non-existent AntispamOpts.Schema makes construction fail cleanly, without falling back to the
// search_path's schema, and an owned pool is closed.
func TestSchemaQualifiedAntispamMissingSchema(t *testing.T) {
	t.Parallel()
	for _, skip := range []bool{true, false} {
		t.Run(fmt.Sprintf("SkipSchemaInit=%v", skip), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			dsn, decoy := mustCreateSchema(t)
			db := mustOpenDB(t, dsn)
			if skip {
				// A valid index in the search_path schema: falling back to it would "work".
				if _, err := db.ExecContext(ctx, SchemaSQL); err != nil {
					t.Fatal(err)
				}
			}
			before := fingerprint(t, db, decoy)
			app := "as-missing-" + randName(t, 4)
			missing := `No "Such" schema-` + randName(t, 4)
			opts := AntispamOpts{Schema: missing, SkipSchemaInit: skip}
			if as, err := NewAntispam(ctx, dsnParams(t, dsn, map[string]string{"application_name": app}), opts); err == nil {
				_ = as.Close()
				t.Error("NewAntispam succeeded with a non-existent schema")
			}
			ext := mustOpenDB(t, dsn)
			if as, err := NewAntispamWithDB(ctx, ext, opts); err == nil {
				_ = as.Close()
				t.Error("NewAntispamWithDB succeeded with a non-existent schema")
			}
			if err := ext.PingContext(ctx); err != nil {
				t.Errorf("external pool closed by failed NewAntispamWithDB: %v", err)
			}
			admin := adminDB(t)
			eventually(t, 10*time.Second, "pool of failed NewAntispam to be closed", func() bool { return connCount(t, admin, app) == 0 })
			if diff := cmp.Diff(before, fingerprint(t, db, decoy)); diff != "" {
				t.Errorf("search_path schema changed (-before +after):\n%s", diff)
			}
			var n int
			if err := admin.QueryRowContext(ctx, "SELECT count(*) FROM pg_namespace WHERE nspname = $1", missing).Scan(&n); err != nil || n != 0 {
				t.Errorf("schema %q exists after failed construction (n=%d, err=%v)", missing, n, err)
			}
		})
	}
}

// countingLogReader counts IntegratedSize and ReadEntryBundle calls.
type countingLogReader struct {
	*fakeLogReader
	sizeCalls, bundleCalls atomic.Int64
}

func (r *countingLogReader) IntegratedSize(ctx context.Context) (uint64, error) {
	r.sizeCalls.Add(1)
	return r.fakeLogReader.IntegratedSize(ctx)
}

func (r *countingLogReader) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	r.bundleCalls.Add(1)
	return r.fakeLogReader.ReadEntryBundle(ctx, i, p)
}

// T10: a follower whose position (nextIdx) is beyond the log's integrated size inserts nothing,
// keeps its position and doesn't push back; it resumes once the log has grown past it.
func TestFollowerAheadOfLog(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn, _ := mustCreateSchema(t)
	as := mustNewAntispam(t, dsn, AntispamOpts{PushbackThreshold: 1})
	db := mustOpenDB(t, dsn)
	if _, err := db.ExecContext(ctx, "UPDATE AntispamFollowCoord SET nextIdx = 50"); err != nil {
		t.Fatal(err)
	}
	lr := &countingLogReader{fakeLogReader: newFakeLogReader(10, entryAt)}
	f := as.Follower(testBundleHasher)
	startFollower(t, f, lr)
	// The follower has completed at least one full round once it starts a second one.
	eventually(t, 30*time.Second, "two rounds of the follower", func() bool { return lr.sizeCalls.Load() >= 2 })
	if got, err := f.EntriesProcessed(ctx); err != nil || got != 50 {
		t.Errorf("EntriesProcessed = (%d, %v), want 50", got, err)
	}
	if got := readIDSeq(t, db); len(got) != 0 {
		t.Errorf("AntispamIDSeq = %v, want empty", got)
	}
	if n := lr.bundleCalls.Load(); n != 0 {
		t.Errorf("%d entry bundles read, want 0", n)
	}
	if as.pushBack.Load() {
		t.Error("follower ahead of the log turned on pushback")
	}

	lr.size.Store(60)
	waitForEntriesProcessed(t, f, 60, time.Minute)
	got := readIDSeq(t, db)
	if len(got) != 10 {
		t.Errorf("AntispamIDSeq has %d rows, want 10", len(got))
	}
	for i := uint64(50); i < 60; i++ {
		if idx, ok := got[hex.EncodeToString(testIDHash(entryAt(i)))]; !ok || idx != i {
			t.Errorf("AntispamIDSeq[entry %d] = (%d, %v), want %d", i, idx, ok, i)
		}
	}
}

// T11: a role without CREATE privilege can't initialise the schema: NewAntispam fails with
// "permission denied" (whether or not the tables exist) and closes its pool.
func TestAntispamNoCreatePrivilege(t *testing.T) {
	t.Parallel()
	for _, preinit := range []bool{false, true} {
		t.Run(fmt.Sprintf("tables exist=%v", preinit), func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			admin := adminDB(t)
			ownerDSN, schema := mustCreateSchema(t)
			if preinit {
				if _, err := mustOpenDB(t, ownerDSN).ExecContext(ctx, SchemaSQL); err != nil {
					t.Fatal(err)
				}
			}
			role, pw := "as_t11_"+randName(t, 6), randName(t, 12)
			if _, err := admin.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", role, pw)); err != nil {
				t.Fatalf("CREATE ROLE: %v", err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				for _, q := range []string{
					"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '" + role + "'",
					"DROP OWNED BY " + role,
					"DROP ROLE " + role,
				} {
					if _, err := admin.ExecContext(ctx, q); err != nil {
						t.Errorf("cleanup %q: %v", q, err)
					}
				}
			})
			for _, q := range []string{
				fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", quoteIdent(schema), role),
				fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %s TO %s", quoteIdent(schema), role),
			} {
				if _, err := admin.ExecContext(ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			u, err := url.Parse(pgTestDSN())
			if err != nil {
				t.Fatal(err)
			}
			u.User = url.UserPassword(role, pw)
			app := "as-t11-" + randName(t, 4)
			for name, tc := range map[string]struct {
				dsn  string
				opts AntispamOpts
			}{
				"search_path":         {dsnParams(t, u.String(), map[string]string{"search_path": schema, "application_name": app}), AntispamOpts{}},
				"AntispamOpts.Schema": {dsnParams(t, u.String(), map[string]string{"search_path": "", "application_name": app}), AntispamOpts{Schema: schema}},
			} {
				as, err := NewAntispam(ctx, tc.dsn, tc.opts)
				if err == nil {
					_ = as.Close()
					t.Fatalf("%s: NewAntispam succeeded without CREATE privilege", name)
				}
				if !strings.Contains(err.Error(), "permission denied") {
					t.Errorf("%s: NewAntispam = %v, want permission denied", name, err)
				}
			}
			eventually(t, 10*time.Second, "pools of failed NewAntispam to be closed", func() bool { return connCount(t, admin, app) == 0 })
			if preinit {
				as, err := NewAntispam(ctx, dsnParams(t, u.String(), map[string]string{"search_path": schema}), AntispamOpts{SkipSchemaInit: true})
				if err != nil {
					t.Fatalf("NewAntispam with SkipSchemaInit: %v", err)
				}
				_ = as.Close()
			}
		})
	}
}
