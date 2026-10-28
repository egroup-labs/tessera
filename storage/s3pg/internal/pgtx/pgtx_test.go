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

package pgtx

// Tests for the transaction helpers (durable READ COMMITTED transactions) and the schema
// Qualifier. PostgreSQL-backed tests use $TESSERA_S3PG_TEST_DSN (default: the local test
// database) and are skipped if it is unreachable, unless -pg_test_optional=false.

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var pgTestOptional = flag.Bool("pg_test_optional", true, "If true, PostgreSQL-backed tests are skipped when the test database is unreachable; if false, an unreachable database fails the tests (CI mode)")

const defaultTestDSN = "postgres://postgres:postgres@127.0.0.1:5432/tessera_test?sslmode=disable"

func testDSN() string {
	if v := os.Getenv("TESSERA_S3PG_TEST_DSN"); v != "" {
		return v
	}
	return defaultTestDSN
}

// withParams returns dsn (URL form) with the given runtime parameters set.
func withParams(t *testing.T, dsn string, params map[string]string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	// pgx does not decode '+' as a space in DSN query parameters.
	u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
	return u.String()
}

// openDB opens a single-connection pool (so that session settings are observable) for dsn,
// skipping or failing the test if PostgreSQL is unreachable.
func openDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		if *pgTestOptional {
			t.Skipf("PostgreSQL unavailable (%v); run with -pg_test_optional=false to fail instead", err)
		}
		t.Fatalf("PostgreSQL unavailable: %v", err)
	}
	return db
}

func show(t *testing.T, ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, setting string) string {
	t.Helper()
	var v string
	if err := q.QueryRowContext(ctx, "SELECT current_setting($1)", setting).Scan(&v); err != nil {
		t.Fatalf("current_setting(%s): %v", setting, err)
	}
	return v
}

// Begin raises synchronous_commit to "on" for the transaction only if the session's setting
// could lose an acknowledged commit (off, local, remote_write); stronger settings are kept.
func TestBeginDurableCommit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		session, wantInTx string
	}{
		{"off", "on"},
		{"local", "on"},
		{"remote_write", "on"},
		{"on", "on"},
		{"remote_apply", "remote_apply"},
	} {
		t.Run(tc.session, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db := openDB(t, withParams(t, testDSN(), map[string]string{"synchronous_commit": tc.session}))
			if got := show(t, ctx, db, "synchronous_commit"); got != tc.session {
				t.Fatalf("session synchronous_commit = %q, want %q (DSN parameter not applied)", got, tc.session)
			}
			for _, commit := range []bool{true, false} {
				tx, err := Begin(ctx, db)
				if err != nil {
					t.Fatalf("Begin: %v", err)
				}
				if got := show(t, ctx, tx, "synchronous_commit"); got != tc.wantInTx {
					t.Errorf("synchronous_commit in transaction = %q, want %q", got, tc.wantInTx)
				}
				if commit {
					err = tx.Commit()
				} else {
					err = tx.Rollback()
				}
				if err != nil {
					t.Fatalf("commit=%v: %v", commit, err)
				}
				// The change is transaction-local: the session keeps its own setting.
				if got := show(t, ctx, db, "synchronous_commit"); got != tc.session {
					t.Errorf("session synchronous_commit after the transaction (commit=%v) = %q, want %q", commit, got, tc.session)
				}
			}
		})
	}
}

// Begin always starts READ COMMITTED transactions, whatever default_transaction_isolation is.
func TestBeginReadCommitted(t *testing.T) {
	t.Parallel()
	for _, iso := range []string{"serializable", "repeatable read", "read committed", "read uncommitted"} {
		t.Run(iso, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db := openDB(t, withParams(t, testDSN(), map[string]string{"default_transaction_isolation": iso}))
			if got := show(t, ctx, db, "default_transaction_isolation"); got != iso {
				t.Fatalf("default_transaction_isolation = %q, want %q (DSN parameter not applied)", got, iso)
			}
			tx, err := Begin(ctx, db)
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			defer Rollback(ctx, tx)
			if got := show(t, ctx, tx, "transaction_isolation"); got != "read committed" {
				t.Errorf("transaction_isolation = %q, want %q", got, "read committed")
			}
			var ro string
			if err := tx.QueryRowContext(ctx, "SELECT current_setting('transaction_read_only')").Scan(&ro); err != nil || ro != "off" {
				t.Errorf("transaction_read_only = %q (%v), want off", ro, err)
			}
		})
	}
}

// Begin fails cleanly (no transaction, no panic) on a closed pool or a cancelled context.
func TestBeginErrors(t *testing.T) {
	t.Parallel()
	db := openDB(t, testDSN())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if tx, err := Begin(ctx, db); err == nil || tx != nil {
		t.Errorf("Begin(cancelled ctx) = (%v, %v), want (nil, error)", tx, err)
	}
	// The pool remains usable afterwards (the failed Begin didn't leak the only connection).
	tx, err := Begin(t.Context(), db)
	if err != nil {
		t.Fatalf("Begin after a failed Begin: %v", err)
	}
	Rollback(t.Context(), tx)

	closed, err := sql.Open("pgx", testDSN())
	if err != nil {
		t.Fatal(err)
	}
	_ = closed.Close()
	if tx, err := Begin(t.Context(), closed); err == nil || tx != nil {
		t.Errorf("Begin(closed pool) = (%v, %v), want (nil, error)", tx, err)
	}
}

// Rollback undoes an open transaction, and is silent for one which has already finished.
// Not parallel: captures the default slog logger.
func TestRollback(t *testing.T) {
	ctx := t.Context()
	db := openDB(t, testDSN())
	var buf bytes.Buffer
	var mu sync.Mutex
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&lockedWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	tx, err := Begin(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "CREATE TEMP TABLE rb (x int)"); err != nil {
		t.Fatal(err)
	}
	Rollback(ctx, tx)
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_class WHERE relname = 'rb' AND relpersistence = 't' AND pg_table_is_visible(oid)").Scan(&n); err != nil || n != 0 {
		t.Errorf("table created in a rolled back transaction exists (n=%d, err=%v)", n, err)
	}
	// Second rollback, and rollback after commit: sql.ErrTxDone is expected and not logged.
	Rollback(ctx, tx)
	tx2, err := Begin(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	Rollback(ctx, tx2)
	mu.Lock()
	defer mu.Unlock()
	if buf.Len() != 0 {
		t.Errorf("Rollback of a finished transaction logged: %s", buf.String())
	}
}

type lockedWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

var testTables = []string{"Tessera", "SeqCoord", "Seq", "IntCoord", "PubCoord", "GCCoord", "AntispamIDSeq"}

func TestQualifierSQL(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, schema, stmt, want string
	}{
		{"empty schema leaves statements unchanged", "", "SELECT seq FROM Seq WHERE id = $1", "SELECT seq FROM Seq WHERE id = $1"},
		{"simple", "log", "SELECT seq, rootHash FROM IntCoord WHERE id = $1 FOR UPDATE", `SELECT seq, rootHash FROM "log".IntCoord WHERE id = $1 FOR UPDATE`},
		{"columns named like tables (case differs) are untouched", "log", "SELECT seq, v FROM Seq WHERE id = $1 AND seq >= $2 ORDER BY seq", `SELECT seq, v FROM "log".Seq WHERE id = $1 AND seq >= $2 ORDER BY seq`},
		{"longer names sharing a prefix are matched whole", "log", "UPDATE SeqCoord SET next = $1; INSERT INTO Seq(id, seq, v) VALUES($1,$2,$3)", `UPDATE "log".SeqCoord SET next = $1; INSERT INTO "log".Seq(id, seq, v) VALUES($1,$2,$3)`},
		{"name as suffix of an unknown word is untouched", "log", "SELECT * FROM AntispamIDSeq, MySeq, Seq_old, SeqX", `SELECT * FROM "log".AntispamIDSeq, MySeq, Seq_old, SeqX`},
		{"lower-case spelling is untouched", "log", "SELECT * FROM seq, seqcoord, intcoord", "SELECT * FROM seq, seqcoord, intcoord"},
		{"every occurrence", "log", "SELECT (SELECT count(*) FROM PubCoord) + (SELECT count(*) FROM PubCoord) + (SELECT count(*) FROM GCCoord)", `SELECT (SELECT count(*) FROM "log".PubCoord) + (SELECT count(*) FROM "log".PubCoord) + (SELECT count(*) FROM "log".GCCoord)`},
		{"adjacent punctuation", "log", "DELETE FROM Seq WHERE id=$1 AND seq = ANY($2);(Tessera)", `DELETE FROM "log".Seq WHERE id=$1 AND seq = ANY($2);("log".Tessera)`},
		{"mixed case schema is quoted", "MyLog", "SELECT 1 FROM Tessera", `SELECT 1 FROM "MyLog".Tessera`},
		{"space and hyphen", "my log-1", "SELECT 1 FROM Tessera", `SELECT 1 FROM "my log-1".Tessera`},
		{"embedded double quote is doubled", `a"b`, "SELECT 1 FROM Tessera", `SELECT 1 FROM "a""b".Tessera`},
		{"injection attempt stays an identifier", `x".Tessera; DROP TABLE Seq; --`, "SELECT 1 FROM Tessera", `SELECT 1 FROM "x"".Tessera; DROP TABLE Seq; --".Tessera`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := NewQualifier(tc.schema, testTables...)
			for i := range 2 { // The second call is served from the cache.
				if got := q.SQL(tc.stmt); got != tc.want {
					t.Errorf("call %d: SQL(%q) =\n %q, want\n %q", i, tc.stmt, got, tc.want)
				}
			}
		})
	}

	// The order in which table names are given doesn't matter.
	q := NewQualifier("s", "SeqCoord", "Seq")
	if got, want := q.SQL("Seq SeqCoord"), `"s".Seq "s".SeqCoord`; got != want {
		t.Errorf("reversed table order: got %q, want %q", got, want)
	}
	q = NewQualifier("s", "Seq", "SeqCoord")
	if got, want := q.SQL("SeqCoord Seq"), `"s".SeqCoord "s".Seq`; got != want {
		t.Errorf("table order: got %q, want %q", got, want)
	}
	// Distinct statements are cached independently.
	q = NewQualifier("s", "Seq")
	if a, b := q.SQL("Seq 1"), q.SQL("Seq 2"); a != `"s".Seq 1` || b != `"s".Seq 2` {
		t.Errorf("cache mixes up statements: %q, %q", a, b)
	}
}

// The cache is safe for concurrent use (run with -race).
func TestQualifierConcurrent(t *testing.T) {
	t.Parallel()
	q := NewQualifier("s", testTables...)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 200 {
				stmt := fmt.Sprintf("SELECT %d FROM Seq JOIN IntCoord ON true", (g*i)%17)
				if got, want := q.SQL(stmt), fmt.Sprintf(`SELECT %d FROM "s".Seq JOIN "s".IntCoord ON true`, (g*i)%17); got != want {
					t.Errorf("SQL(%q) = %q, want %q", stmt, got, want)
					return
				}
			}
		})
	}
	wg.Wait()
}

// Against PostgreSQL: SetSearchPath selects the (quoted) schema for the rest of the transaction
// only, and qualified statements reach the schema's tables whatever the session's search_path.
func TestQualifierPostgres(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	admin := openDB(t, testDSN())
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	schema := `Pg Tx "Q"-` + hex.EncodeToString(b)
	decoy := "pgtx_decoy_" + hex.EncodeToString(b)
	quoted := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	for _, s := range []string{quoted, decoy} {
		if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+s); err != nil {
			t.Fatalf("CREATE SCHEMA %s: %v", s, err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, s := range []string{quoted, decoy} {
			if _, err := admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+s+" CASCADE"); err != nil {
				t.Errorf("DROP SCHEMA %s: %v", s, err)
			}
		}
	})
	// The decoy (selected by the session's search_path) has a table of the same name.
	db := openDB(t, withParams(t, testDSN(), map[string]string{"search_path": decoy}))
	if _, err := db.ExecContext(ctx, "CREATE TABLE Seq (v TEXT); INSERT INTO Seq VALUES ('decoy')"); err != nil {
		t.Fatal(err)
	}
	q := NewQualifier(schema, "Seq")

	tx, err := Begin(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.SetSearchPath(ctx, tx); err != nil {
		t.Fatalf("SetSearchPath: %v", err)
	}
	if got := show(t, ctx, tx, "search_path"); got != quoted {
		t.Errorf("search_path in transaction = %q, want %q", got, quoted)
	}
	// Unqualified DDL now lands in the schema.
	if _, err := tx.ExecContext(ctx, "CREATE TABLE Seq (v TEXT); INSERT INTO Seq VALUES ('real')"); err != nil {
		t.Fatalf("DDL after SetSearchPath: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := show(t, ctx, db, "search_path"); got != decoy {
		t.Errorf("session search_path after the transaction = %q, want %q (SetSearchPath must be transaction-local)", got, decoy)
	}
	var v string
	if err := db.QueryRowContext(ctx, q.SQL("SELECT v FROM Seq")).Scan(&v); err != nil || v != "real" {
		t.Errorf("qualified SELECT = (%q, %v), want real", v, err)
	}
	if err := db.QueryRowContext(ctx, "SELECT v FROM Seq").Scan(&v); err != nil || v != "decoy" {
		t.Errorf("unqualified SELECT = (%q, %v), want decoy", v, err)
	}

	// An empty Qualifier leaves search_path alone.
	tx, err = Begin(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer Rollback(ctx, tx)
	if err := NewQualifier("", "Seq").SetSearchPath(ctx, tx); err != nil {
		t.Fatalf("SetSearchPath(empty): %v", err)
	}
	if got := show(t, ctx, tx, "search_path"); got != decoy {
		t.Errorf("search_path after empty SetSearchPath = %q, want %q", got, decoy)
	}
}
