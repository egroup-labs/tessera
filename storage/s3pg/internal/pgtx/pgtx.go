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

// Package pgtx contains PostgreSQL helpers shared by the s3pg log and antispam stores: how
// transactions are started, and schema-qualification of the SQL they run.
package pgtx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
)

// txOptions is used for every transaction. The stores' locking protocols (SELECT ... FOR UPDATE on
// single coordination rows, then fresh reads) rely on READ COMMITTED semantics, so the isolation
// level is set explicitly rather than inherited from default_transaction_isolation: under
// REPEATABLE READ or SERIALIZABLE, concurrent instances fail with serialization errors (40001).
var txOptions = &sql.TxOptions{Isolation: sql.LevelReadCommitted}

// durableCommitSQL raises synchronous_commit to at least "on" for the current transaction only.
//
// A commit which is acknowledged but later lost (synchronous_commit = off/local/remote_write, set
// at server, database, role or session level) can make sequence numbers which were already handed
// out to clients be reassigned to other entries. Stronger settings (remote_apply) are left alone.
const durableCommitSQL = `SELECT set_config('synchronous_commit', 'on', true) WHERE current_setting('synchronous_commit') IN ('off', 'local', 'remote_write')`

// Begin starts a READ COMMITTED transaction whose commit is durable (see durableCommitSQL).
func Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := db.BeginTx(ctx, txOptions)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, durableCommitSQL); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to set synchronous_commit: %v", err)
	}
	return tx, nil
}

// Rollback rolls back tx unless it has already been committed or rolled back, logging any failure.
func Rollback(ctx context.Context, tx *sql.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		slog.WarnContext(ctx, "failed to rollback Tx", slog.Any("error", err))
	}
}

// Qualifier rewrites the table names in SQL statements to schema-qualified names.
//
// Selecting the schema with search_path alone is fragile: connection poolers may drop, ignore or
// (with some PgBouncer versions) even leak the search_path startup parameter between clients, so
// statements could silently run against another schema. With an explicit schema, every statement
// names its tables fully.
type Qualifier struct {
	schema string // Quoted identifier, or "" for unqualified names.
	re     *regexp.Regexp
	cache  sync.Map
}

// NewQualifier returns a Qualifier for the given (unquoted) schema name and table names. Table names
// are matched case-sensitively as whole words, exactly as written in the SQL statements. An empty
// schema returns statements unchanged (tables are resolved via search_path).
func NewQualifier(schema string, tables ...string) *Qualifier {
	q := &Qualifier{}
	if schema == "" {
		return q
	}
	q.schema = pgx.Identifier{schema}.Sanitize()
	alts := make([]string, len(tables))
	for i, t := range tables {
		alts[i] = regexp.QuoteMeta(t)
	}
	q.re = regexp.MustCompile(`\b(` + strings.Join(alts, "|") + `)\b`)
	return q
}

// SQL returns stmt with its table names qualified by the schema.
func (q *Qualifier) SQL(stmt string) string {
	if q.schema == "" {
		return stmt
	}
	if r, ok := q.cache.Load(stmt); ok {
		return r.(string)
	}
	// Literal replacement: the schema name may contain '$', which must not be expanded.
	r := q.re.ReplaceAllStringFunc(stmt, func(table string) string { return q.schema + "." + table })
	q.cache.Store(stmt, r)
	return r
}

// SetSearchPath makes unqualified table names in the rest of tx resolve to the schema (if any).
// It is used to apply DDL files which use unqualified names.
func (q *Qualifier) SetSearchPath(ctx context.Context, tx *sql.Tx) error {
	if q.schema == "" {
		return nil
	}
	_, err := tx.ExecContext(ctx, "SELECT set_config('search_path', $1, true)", q.schema)
	return err
}
