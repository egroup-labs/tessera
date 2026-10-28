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

// Tests which keep the versioned migrations in migrations/ in sync with schema.sql.
//
// Conventions (see README.md):
//   - migrations/NNNNNN_description.up.sql, versions contiguous from 000001 (golang-migrate
//     naming; also usable by any tool which applies plain SQL files in order).
//   - 000001_initial.up.sql is an exact copy of schema.sql as of SchemaCompatibilityVersion 1.
//   - Every later change to schema.sql gets a new migration, such that applying all migrations
//     in order yields exactly the same database objects and initial rows as applying schema.sql.
//
// These tests deliberately don't depend on the other test helpers in this package.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	_ "github.com/jackc/pgx/v5/stdlib" // Registers the "pgx" database/sql driver.
)

const migDir = "migrations"

var migFileRE = regexp.MustCompile(`^([0-9]{6})_([a-z0-9_]+)\.up\.sql$`)

type migFile struct {
	version int
	path    string
}

// migLoad returns the migrations in version order, failing the test if the directory
// doesn't follow the conventions above.
func migLoad(t *testing.T) []migFile {
	t.Helper()
	des, err := os.ReadDir(migDir)
	if err != nil {
		t.Fatalf("ReadDir(%q): %v", migDir, err)
	}
	var ms []migFile
	for _, de := range des {
		m := migFileRE.FindStringSubmatch(de.Name())
		if de.IsDir() || m == nil {
			t.Errorf("unexpected entry %q in %s/: want files named NNNNNN_description.up.sql", de.Name(), migDir)
			continue
		}
		v, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("Atoi(%q): %v", m[1], err)
		}
		ms = append(ms, migFile{version: v, path: filepath.Join(migDir, de.Name())})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].version < ms[j].version })
	if len(ms) == 0 {
		t.Fatalf("no migrations found in %s/", migDir)
	}
	for i, m := range ms {
		if m.version != i+1 {
			t.Fatalf("migration versions must be contiguous from 1 without duplicates: found %s at position %d", m.path, i+1)
		}
	}
	return ms
}

func migRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q): %v", path, err)
	}
	return string(b)
}

func TestMigrationsLayout(t *testing.T) {
	ms := migLoad(t)
	if got, want := ms[0].path, filepath.Join(migDir, "000001_initial.up.sql"); got != want {
		t.Errorf("first migration is %q, want %q", got, want)
	}
}

// TestMigrationsMatchSchemaSQL checks, without needing a database, that the initial migration is
// an exact copy of schema.sql for as long as it is the only migration.
func TestMigrationsMatchSchemaSQL(t *testing.T) {
	ms := migLoad(t)
	if len(ms) > 1 {
		t.Skip("more than one migration: equivalence is checked by TestMigrationsEquivalentToSchemaSQL")
	}
	if got := migRead(t, ms[0].path); got != SchemaSQL {
		t.Errorf("%s differs from schema.sql. If schema.sql changed, either update the copy (only if no "+
			"released version used the old schema) or add a new migration, see storage/s3pg/README.md.\n%s",
			ms[0].path, cmp.Diff(SchemaSQL, got))
	}
}

// TestMigrationsEquivalentToSchemaSQL applies all migrations to one fresh schema, and SchemaSQL to
// another, and checks that the resulting tables, columns, constraints, indexes, storage options and
// initial rows are the same. It also checks that SchemaSQL is a no-op on a migrated schema (so servers
// which don't set SkipSchemaInit can run against it) and that the migrated schema is compatible with
// this version of the library.
func TestMigrationsEquivalentToSchemaSQL(t *testing.T) {
	ms := migLoad(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db := migTestDB(t, ctx)

	viaMigrations := migNewSchema(t, ctx, db)
	for _, m := range ms {
		migApply(t, ctx, db, viaMigrations, m.path, migRead(t, m.path))
	}
	viaSchemaSQL := migNewSchema(t, ctx, db)
	migApply(t, ctx, db, viaSchemaSQL, "schema.sql", SchemaSQL)

	got := migSnapshot(t, ctx, db, viaMigrations)
	if diff := cmp.Diff(migSnapshot(t, ctx, db, viaSchemaSQL), got); diff != "" {
		t.Errorf("applying migrations/ in order is not equivalent to applying schema.sql (-schema.sql +migrations):\n%s", diff)
	}

	migApply(t, ctx, db, viaMigrations, "schema.sql", SchemaSQL)
	if diff := cmp.Diff(got, migSnapshot(t, ctx, db, viaMigrations)); diff != "" {
		t.Errorf("applying schema.sql on top of the migrations changed the schema (-before +after):\n%s", diff)
	}

	var v uint64
	if err := db.QueryRowContext(ctx, "SELECT compatibilityVersion FROM "+migQuoteIdent(viaMigrations)+".Tessera WHERE id = 0").Scan(&v); err != nil {
		t.Fatalf("failed to read compatibilityVersion: %v", err)
	}
	if v != SchemaCompatibilityVersion {
		t.Errorf("migrated schema has compatibilityVersion %d, want SchemaCompatibilityVersion (%d)", v, SchemaCompatibilityVersion)
	}
}

// migTestDB returns a pool for the test database, skipping or failing the test if it's
// unreachable depending on the package's -pg_test_optional flag.
func migTestDB(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TESSERA_S3PG_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@127.0.0.1:5432/tessera_test?sslmode=disable"
	}
	optional := true
	if f := flag.Lookup("pg_test_optional"); f != nil {
		if g, ok := f.Value.(flag.Getter); ok {
			if b, ok := g.Get().(bool); ok {
				optional = b
			}
		}
	}
	db, err := sql.Open("pgx", dsn)
	if err == nil {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = db.PingContext(pctx)
		cancel()
		if err != nil {
			_ = db.Close()
		}
	}
	if err != nil {
		if optional {
			t.Skipf("PostgreSQL test database unavailable: %v", err)
		}
		t.Fatalf("PostgreSQL test database unavailable: %v", err)
	}
	db.SetMaxOpenConns(2)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return db
}

// migNewSchema creates a uniquely named schema which is dropped when the test completes.
func migNewSchema(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	name := "s3pg_test_" + hex.EncodeToString(b)
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+migQuoteIdent(name)); err != nil {
		t.Fatalf("CREATE SCHEMA %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+migQuoteIdent(name)+" CASCADE"); err != nil {
			t.Errorf("DROP SCHEMA %s: %v", name, err)
		}
	})
	return name
}

// migApply runs the given SQL in a single transaction with search_path set to schema, as
// migration tools typically do.
func migApply(t *testing.T, ctx context.Context, db *sql.DB, schema, what, sqlText string) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "SET LOCAL search_path TO "+migQuoteIdent(schema)); err != nil {
		t.Fatalf("SET search_path: %v", err)
	}
	if _, err := tx.ExecContext(ctx, sqlText); err != nil {
		t.Fatalf("applying %s: %v", what, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("applying %s: Commit: %v", what, err)
	}
}

// migSnapshot returns a normalised, schema-name independent description of the objects and rows
// in the given schema.
func migSnapshot(t *testing.T, ctx context.Context, db *sql.DB, schema string) []string {
	t.Helper()
	queries := []struct{ kind, q string }{
		{"relation", `SELECT c.relname::text, c.relkind::text, c.relpersistence::text, coalesce(array_to_string(c.reloptions, ','), '')
			FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 ORDER BY 1`},
		{"column", `SELECT table_name::text, column_name::text, data_type::text, is_nullable::text, coalesce(column_default::text, '')
			FROM information_schema.columns WHERE table_schema = $1 ORDER BY 1, 2`},
		{"constraint", `SELECT cl.relname::text, co.conname::text, co.contype::text, pg_get_constraintdef(co.oid)
			FROM pg_constraint co JOIN pg_class cl ON cl.oid = co.conrelid JOIN pg_namespace n ON n.oid = cl.relnamespace
			WHERE n.nspname = $1 ORDER BY 1, 2`},
		{"index", `SELECT indexname::text, indexdef::text FROM pg_indexes WHERE schemaname = $1 ORDER BY 1`},
	}
	var out []string
	for _, q := range queries {
		out = append(out, migQueryLines(t, ctx, db, q.kind, q.q, schema)...)
	}

	// Initial rows of every table.
	var tables []string
	rows, err := db.QueryContext(ctx, "SELECT table_name::text FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE' ORDER BY 1", schema)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		tables = append(tables, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("list tables: %v", err)
	}
	_ = rows.Close()
	for _, tbl := range tables {
		q := fmt.Sprintf("SELECT to_jsonb(t)::text FROM %s.%s t ORDER BY 1", migQuoteIdent(schema), migQuoteIdent(tbl))
		out = append(out, migQueryLines(t, ctx, db, "row "+tbl, q)...)
	}

	for i := range out {
		out[i] = strings.ReplaceAll(out[i], schema, "<schema>")
	}
	return out
}

// migQueryLines runs q and returns one line per row, consisting of kind followed by the row's
// (text) columns.
func migQueryLines(t *testing.T, ctx context.Context, db *sql.DB, kind, q string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		t.Fatalf("%s query: %v", kind, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("%s columns: %v", kind, err)
	}
	var out []string
	for rows.Next() {
		vals := make([]string, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("%s scan: %v", kind, err)
		}
		out = append(out, kind+": "+strings.Join(vals, " | "))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s rows: %v", kind, err)
	}
	return out
}

func migQuoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
