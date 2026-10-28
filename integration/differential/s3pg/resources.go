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

// Per-run resources: S3 buckets, MySQL databases and PostgreSQL schemas, all uniquely named
// after the run ID and removed again by cleanup.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"

	sdkaws "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/go-sql-driver/mysql" // Registers the "mysql" database/sql driver.
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // Registers the "pgx" database/sql driver.
	"github.com/transparency-dev/tessera/storage/s3pg"
	s3pgantispam "github.com/transparency-dev/tessera/storage/s3pg/antispam"
)

type resources struct {
	runID string

	sdkCfg *sdkaws.Config
	s3Opts func(*s3.Options)
	s3c    *s3.Client

	mysqlRoot string // MySQL DSN without database name, e.g. "root:root@tcp(127.0.0.1:3306)/"
	myAdmin   *sql.DB
	pgBase    string // PostgreSQL URL DSN without search_path
	pgAdmin   *sql.DB

	// qualified selects --pg_schema_mode=qualified: the s3pg stores are configured with
	// Config.Schema / AntispamOpts.Schema, and their DSNs' search_path selects decoySchema.
	qualified   bool
	decoySchema string            // quoted identifier
	decoyState  map[string]string // table -> digest of its rows, right after initialisation

	mu        sync.Mutex
	buckets   []string
	myDBs     []string
	pgSchemas []string
	dbs       []*sql.DB
}

func newResources(ctx context.Context, s3Endpoint, mysqlRoot, pgBase string, qualified bool) (*resources, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	r := &resources{
		runID:     hex.EncodeToString(b),
		mysqlRoot: mysqlRoot,
		pgBase:    pgBase,
		qualified: qualified,
	}
	r.sdkCfg = &sdkaws.Config{Region: "us-east-1"}
	r.s3Opts = func(o *s3.Options) {
		o.BaseEndpoint = sdkaws.String(s3Endpoint)
		o.Credentials = credentials.NewStaticCredentialsProvider("test", "test", "")
		o.Region = "us-east-1"
		o.UsePathStyle = true
	}
	r.s3c = s3.NewFromConfig(*r.sdkCfg, r.s3Opts)

	var err error
	if r.myAdmin, err = sql.Open("mysql", mysqlRoot); err != nil {
		return nil, err
	}
	if err := r.myAdmin.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("MySQL: %v", err)
	}
	if r.pgAdmin, err = sql.Open("pgx", pgBase); err != nil {
		return nil, err
	}
	r.pgAdmin.SetMaxOpenConns(2)
	if err := r.pgAdmin.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("PostgreSQL: %v", err)
	}
	if qualified {
		if err := r.createDecoy(ctx); err != nil {
			return nil, fmt.Errorf("decoy schema: %v", err)
		}
	}
	return r, nil
}

func (r *resources) name(tag, sep string) string {
	return strings.Join([]string{"diff", r.runID, tag}, sep)
}

// bucket creates a new, empty bucket.
func (r *resources) bucket(ctx context.Context, tag string) (string, error) {
	name := r.name(tag, "-")
	if _, err := r.s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: sdkaws.String(name)}); err != nil {
		return "", fmt.Errorf("CreateBucket(%q): %v", name, err)
	}
	r.mu.Lock()
	r.buckets = append(r.buckets, name)
	r.mu.Unlock()
	return name, nil
}

// mysqlDB creates a new MySQL database, and returns its DSN and an inspection handle.
func (r *resources) mysqlDB(ctx context.Context, tag string) (string, *sql.DB, error) {
	name := r.name(tag, "_")
	if _, err := r.myAdmin.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		return "", nil, err
	}
	r.mu.Lock()
	r.myDBs = append(r.myDBs, name)
	r.mu.Unlock()
	dsn := r.mysqlRoot + name
	db, err := r.open("mysql", dsn)
	return dsn, db, err
}

// pgSchema creates a new PostgreSQL schema for an s3pg log or antispam store. It returns the DSN
// and the Config.Schema (AntispamOpts.Schema) value to configure the store with, and an inspection
// handle whose search_path selects the new schema.
//
// By default (--pg_schema_mode=search_path) the DSN selects the schema via search_path, and the
// returned schema is "". With --pg_schema_mode=qualified the schema's name needs quoting (upper
// case and hyphens) and is returned for Config.Schema, while the DSN's search_path selects the
// decoy schema, which holds an initialised copy of every table: a statement which was not
// schema-qualified would silently read or write the decoy (see checkDecoy).
func (r *resources) pgSchema(ctx context.Context, tag string) (dsn, schema string, insp *sql.DB, err error) {
	name := r.name(tag, "_")
	if r.qualified {
		name = "Diff-" + r.runID + "-" + tag
	}
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := r.pgAdmin.ExecContext(ctx, "CREATE SCHEMA "+quoted); err != nil {
		return "", "", nil, err
	}
	r.mu.Lock()
	r.pgSchemas = append(r.pgSchemas, quoted)
	r.mu.Unlock()
	inspDSN, err := r.pgDSN(quoted)
	if err != nil {
		return "", "", nil, err
	}
	if insp, err = r.open("pgx", inspDSN); err != nil {
		return "", "", nil, err
	}
	if !r.qualified {
		return inspDSN, "", insp, nil
	}
	dsn, err = r.pgDSN(r.decoySchema)
	return dsn, name, insp, err
}

// pgDSN returns the base DSN with search_path set to the given (quoted) schema.
func (r *resources) pgDSN(searchPath string) (string, error) {
	u, err := url.Parse(r.pgBase)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("search_path", searchPath)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// decoyTables are all tables of the s3pg log and antispam schemas.
var decoyTables = []string{"Tessera", "SeqCoord", "Seq", "IntCoord", "PubCoord", "GCCoord", "AntispamMeta", "AntispamIDSeq", "AntispamFollowCoord"}

// createDecoy creates the decoy schema with initialised log and antispam tables, and records
// their contents.
func (r *resources) createDecoy(ctx context.Context) error {
	r.decoySchema = pgx.Identifier{r.name("decoy", "_")}.Sanitize()
	if _, err := r.pgAdmin.ExecContext(ctx, "CREATE SCHEMA "+r.decoySchema); err != nil {
		return err
	}
	r.mu.Lock()
	r.pgSchemas = append(r.pgSchemas, r.decoySchema)
	r.mu.Unlock()
	dsn, err := r.pgDSN(r.decoySchema)
	if err != nil {
		return err
	}
	db, err := r.open("pgx", dsn)
	if err != nil {
		return err
	}
	for _, ddl := range []string{s3pg.SchemaSQL, s3pgantispam.SchemaSQL} {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return err
		}
	}
	r.decoyState, err = r.decoyDigest(ctx)
	return err
}

// decoyDigest returns the number of rows and a digest of the contents of each decoy table.
func (r *resources) decoyDigest(ctx context.Context) (map[string]string, error) {
	res := map[string]string{}
	for _, t := range decoyTables {
		var n int64
		var d string
		q := fmt.Sprintf("SELECT count(*), coalesce(md5(string_agg(t::text, '|' ORDER BY t::text)), '') FROM %s.%s t", r.decoySchema, t)
		if err := r.pgAdmin.QueryRowContext(ctx, q).Scan(&n, &d); err != nil {
			return nil, fmt.Errorf("%s: %v", q, err)
		}
		res[t] = fmt.Sprintf("%d rows, md5 %s", n, d)
	}
	return res, nil
}

// checkDecoy checks that no s3pg store wrote to the decoy schema. (Reads of the decoy would show
// up as differences from upstream.)
func (r *resources) checkDecoy(ctx context.Context) error {
	if !r.qualified {
		return nil
	}
	now, err := r.decoyDigest(ctx)
	if err != nil {
		return err
	}
	var changed []string
	for _, t := range decoyTables {
		if now[t] != r.decoyState[t] {
			changed = append(changed, fmt.Sprintf("%s: %s -> %s", t, r.decoyState[t], now[t]))
		}
	}
	check(len(changed) == 0, "decoy schema %s (the search_path of every s3pg DSN) unchanged: %d tables, changed: %v", r.decoySchema, len(decoyTables), changed)
	return nil
}

func (r *resources) open(driver, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	r.mu.Lock()
	r.dbs = append(r.dbs, db)
	r.mu.Unlock()
	return db, nil
}

// cleanup removes every bucket, database and schema created by this run.
func (r *resources) cleanup(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, db := range r.dbs {
		_ = db.Close()
	}
	for _, b := range r.buckets {
		if err := emptyAndDeleteBucket(ctx, r.s3c, b); err != nil {
			slog.ErrorContext(ctx, "cleanup: bucket", slog.String("bucket", b), slog.Any("error", err))
		}
	}
	for _, d := range r.myDBs {
		if _, err := r.myAdmin.ExecContext(ctx, "DROP DATABASE IF EXISTS "+d); err != nil {
			slog.ErrorContext(ctx, "cleanup: MySQL database", slog.String("db", d), slog.Any("error", err))
		}
	}
	for _, s := range r.pgSchemas {
		if _, err := r.pgAdmin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+s+" CASCADE"); err != nil {
			slog.ErrorContext(ctx, "cleanup: PostgreSQL schema", slog.String("schema", s), slog.Any("error", err))
		}
	}
	_ = r.myAdmin.Close()
	_ = r.pgAdmin.Close()
	slog.InfoContext(ctx, "Cleaned up", slog.Int("buckets", len(r.buckets)), slog.Int("mysql_dbs", len(r.myDBs)), slog.Int("pg_schemas", len(r.pgSchemas)))
}

func emptyAndDeleteBucket(ctx context.Context, c *s3.Client, bucket string) error {
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: sdkaws.String(bucket)})
	for p.HasMorePages() {
		l, err := p.NextPage(ctx)
		if err != nil {
			return err
		}
		if len(l.Contents) == 0 {
			continue
		}
		objs := make([]types.ObjectIdentifier, 0, len(l.Contents))
		for _, o := range l.Contents {
			objs = append(objs, types.ObjectIdentifier{Key: o.Key})
		}
		if _, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: sdkaws.String(bucket), Delete: &types.Delete{Objects: objs, Quiet: sdkaws.Bool(true)}}); err != nil {
			return err
		}
	}
	_, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: sdkaws.String(bucket)})
	return err
}

// coordState is the coordination state of a log, read with identical SQL from MySQL (upstream) and
// PostgreSQL (s3pg). PubCoord.publishedAt is a wall-clock timestamp and deliberately not included.
type coordState struct {
	Version int64
	Next    int64
	IntSize int64
	IntRoot []byte
	SeqRows int64
	PubSize sql.NullInt64
	GCFrom  int64
}

func (c coordState) String() string {
	return fmt.Sprintf("version=%d SeqCoord.next=%d IntCoord=(%d, %x) Seq rows=%d PubCoord.size=%v GCCoord.fromSize=%d",
		c.Version, c.Next, c.IntSize, c.IntRoot, c.SeqRows, c.PubSize.Int64, c.GCFrom)
}

func readCoord(ctx context.Context, db *sql.DB) (coordState, error) {
	var c coordState
	qs := []struct {
		q    string
		dest []any
	}{
		{"SELECT compatibilityVersion FROM Tessera WHERE id = 0", []any{&c.Version}},
		{"SELECT next FROM SeqCoord WHERE id = 0", []any{&c.Next}},
		{"SELECT seq, rootHash FROM IntCoord WHERE id = 0", []any{&c.IntSize, &c.IntRoot}},
		{"SELECT COUNT(*) FROM Seq", []any{&c.SeqRows}},
		{"SELECT size FROM PubCoord WHERE id = 0", []any{&c.PubSize}},
		{"SELECT fromSize FROM GCCoord WHERE id = 0", []any{&c.GCFrom}},
	}
	var errs []error
	for _, q := range qs {
		if err := db.QueryRowContext(ctx, q.q).Scan(q.dest...); err != nil {
			errs = append(errs, fmt.Errorf("%s: %v", q.q, err))
		}
	}
	return c, errors.Join(errs...)
}
