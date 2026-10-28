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

// Shared helpers for the s3pg tests.
//
// PostgreSQL-backed tests each get their own schema (s3pg_test_<random hex>) in the test
// database, which makes them safe to run in parallel with each other and with other users
// of the same database. Tests needing real S3 get their own bucket.
//
// Environment:
//   - TESSERA_S3PG_TEST_DSN: PostgreSQL DSN of the test database (default defaultTestDSN).
//   - TESSERA_S3PG_TEST_S3_ENDPOINT: S3-compatible endpoint (e.g. http://127.0.0.1:9000);
//     tests needing S3 are skipped if unset.
//
// Flags:
//   - -pg_test_optional (default true): skip PostgreSQL-backed tests if the database is
//     unreachable; with -pg_test_optional=false an unreachable database is a test failure.

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/jackc/pgx/v5/stdlib" // Registers the "pgx" database/sql driver.
	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/api/layout"
	"github.com/transparency-dev/tessera/internal/fetcher"
	storage "github.com/transparency-dev/tessera/storage/internal"
)

const (
	// defaultTestDSN is used when TESSERA_S3PG_TEST_DSN is not set.
	defaultTestDSN = "postgres://postgres:postgres@127.0.0.1:5432/tessera_test?sslmode=disable"
	// testDSNEnv names the environment variable holding the test database DSN.
	testDSNEnv = "TESSERA_S3PG_TEST_DSN"
	// s3EndpointEnv names the environment variable holding the S3 endpoint for tests needing real S3.
	s3EndpointEnv = "TESSERA_S3PG_TEST_S3_ENDPOINT"
	// Credentials for the S3 test endpoint (default "test"/"test", which moto accepts; MinIO
	// needs real ones, e.g. its root user).
	s3AccessKeyEnv = "TESSERA_S3PG_TEST_S3_ACCESS_KEY"
	s3SecretEnv    = "TESSERA_S3PG_TEST_S3_SECRET"
	// testS3Region is the region used for the test S3 endpoint.
	testS3Region = "us-east-1"
)

var pgTestOptional = flag.Bool("pg_test_optional", true, "If true, PostgreSQL-backed tests are skipped when the test database is unreachable; if false, an unreachable database fails the tests (CI mode)")

// baseTestDSN returns the DSN of the test database, without any search_path.
func baseTestDSN() string {
	if v := os.Getenv(testDSNEnv); v != "" {
		return v
	}
	return defaultTestDSN
}

// dsnWithParams returns dsn with the provided connection parameters added (or replaced).
// Both URL ("postgres://...") and keyword/value ("host=... user=...") DSN forms are supported.
func dsnWithParams(t *testing.T, dsn string, params map[string]string) string {
	t.Helper()
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("failed to parse DSN: %v", err)
		}
		q := u.Query()
		for k, v := range params {
			q.Set(k, v)
		}
		// pgx does not decode '+' as a space in DSN query parameters.
		u.RawQuery = strings.ReplaceAll(q.Encode(), "+", "%20")
		return u.String()
	}
	for k, v := range params {
		dsn += fmt.Sprintf(" %s=%s", k, v)
	}
	return dsn
}

var (
	adminOnce sync.Once
	adminDB   *sql.DB
	adminErr  error
)

// adminPool returns a process-wide pool connected to the test database (no search_path), used to
// create and drop per-test schemas. It returns an error if the database cannot be reached.
func adminPool() (*sql.DB, error) {
	adminOnce.Do(func() {
		db, err := sql.Open("pgx", baseTestDSN())
		if err != nil {
			adminErr = err
			return
		}
		db.SetMaxOpenConns(4)
		db.SetMaxIdleConns(2)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			adminErr = err
			return
		}
		adminDB = db
	})
	return adminDB, adminErr
}

// requirePG returns the shared admin pool, or skips (or, with -pg_test_optional=false, fails)
// the test if the test database is unavailable.
func requirePG(t *testing.T) *sql.DB {
	t.Helper()
	db, err := adminPool()
	if err != nil {
		if *pgTestOptional {
			t.Skipf("PostgreSQL test database unavailable (%v), skipping test; run with -pg_test_optional=false to fail instead", err)
		}
		t.Fatalf("PostgreSQL test database unavailable: %v", err)
	}
	return db
}

// randHex returns a random lowercase hex string of 2*n characters.
func randHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return hex.EncodeToString(b)
}

// quoteIdent quotes a PostgreSQL identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// pgTestEnv is a per-test PostgreSQL schema.
type pgTestEnv struct {
	// Schema is the name of the per-test schema.
	Schema string
	// DSN connects to the test database with search_path set to Schema.
	DSN string
	// DB is a small pool using DSN, for direct SQL from the tests. It is closed on cleanup.
	DB *sql.DB
}

// newPGTestEnv creates a fresh, empty schema for the calling test.
//
// The schema is dropped (CASCADE) when the test finishes. Cleanup functions registered later
// (e.g. closing sequencers) run before the drop.
func newPGTestEnv(t *testing.T) *pgTestEnv {
	t.Helper()
	admin := requirePG(t)
	schema := "s3pg_test_" + randHex(t, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quoteIdent(schema)); err != nil {
		t.Fatalf("CREATE SCHEMA %s: %v", schema, err)
	}
	t.Cleanup(func() { dropSchema(t, admin, schema) })

	dsn := dsnWithParams(t, baseTestDSN(), map[string]string{"search_path": schema})
	return &pgTestEnv{
		Schema: schema,
		DSN:    dsn,
		DB:     openTestDB(t, dsn, 4),
	}
}

// dropSchema drops the named schema and everything in it.
func dropSchema(t *testing.T, admin *sql.DB, schema string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tx, err := admin.BeginTx(ctx, nil)
	if err != nil {
		t.Errorf("cleanup: BeginTx: %v", err)
		return
	}
	defer func() { _ = tx.Rollback() }()
	// Don't wait forever for locks held by leaked connections/transactions.
	if _, err := tx.ExecContext(ctx, "SET LOCAL lock_timeout = '30s'"); err != nil {
		t.Errorf("cleanup: SET lock_timeout: %v", err)
		return
	}
	if _, err := tx.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteIdent(schema)+" CASCADE"); err != nil {
		t.Errorf("cleanup: DROP SCHEMA %s: %v", schema, err)
		return
	}
	if err := tx.Commit(); err != nil {
		t.Errorf("cleanup: commit DROP SCHEMA %s: %v", schema, err)
	}
}

// openTestDB opens a pgx pool for dsn which is closed when the test finishes.
func openTestDB(t *testing.T, dsn string, maxOpen int) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxOpen)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("cleanup: close test DB: %v", err)
		}
	})
	return db
}

// newTestSequencer creates a pgSequencer, failing the test on error, and closes it on cleanup.
func newTestSequencer(t *testing.T, ctx context.Context, cfg Config, maxOutstanding uint64) *pgSequencer {
	t.Helper()
	s, err := newPGSequencer(ctx, cfg, maxOutstanding)
	if err != nil {
		t.Fatalf("newPGSequencer: %v", err)
	}
	if s == nil {
		t.Fatal("newPGSequencer returned nil sequencer and nil error")
	}
	t.Cleanup(func() {
		if err := s.close(); err != nil {
			t.Errorf("cleanup: pgSequencer.close: %v", err)
		}
	})
	return s
}

// exec runs a statement against the test schema, failing the test on error.
func (e *pgTestEnv) exec(t *testing.T, q string, args ...any) {
	t.Helper()
	if _, err := e.DB.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

// queryUint64 runs a query returning a single non-negative integer.
func (e *pgTestEnv) queryUint64(t *testing.T, q string, args ...any) uint64 {
	t.Helper()
	var v uint64
	if err := e.DB.QueryRowContext(context.Background(), q, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	return v
}

// intCoord returns the integrated tree size and root hash stored in IntCoord.
func (e *pgTestEnv) intCoord(t *testing.T) (uint64, []byte) {
	t.Helper()
	var seq uint64
	var root []byte
	if err := e.DB.QueryRowContext(context.Background(), "SELECT seq, rootHash FROM IntCoord WHERE id = 0").Scan(&seq, &root); err != nil {
		t.Fatalf("read IntCoord: %v", err)
	}
	return seq, root
}

// seqCoordNext returns SeqCoord.next.
func (e *pgTestEnv) seqCoordNext(t *testing.T) uint64 {
	t.Helper()
	return e.queryUint64(t, "SELECT next FROM SeqCoord WHERE id = 0")
}

// seqKeys returns the keys (first index of each batch) of all rows in Seq, in order.
func (e *pgTestEnv) seqKeys(t *testing.T) []uint64 {
	t.Helper()
	rows, err := e.DB.QueryContext(context.Background(), "SELECT seq FROM Seq ORDER BY seq")
	if err != nil {
		t.Fatalf("read Seq: %v", err)
	}
	defer func() { _ = rows.Close() }()
	r := []uint64{}
	for rows.Next() {
		var k uint64
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("scan Seq: %v", err)
		}
		r = append(r, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate Seq: %v", err)
	}
	return r
}

// pubCoord returns PubCoord.publishedAt and PubCoord.size.
func (e *pgTestEnv) pubCoord(t *testing.T) (int64, sql.NullInt64) {
	t.Helper()
	var at int64
	var size sql.NullInt64
	if err := e.DB.QueryRowContext(context.Background(), "SELECT publishedAt, size FROM PubCoord WHERE id = 0").Scan(&at, &size); err != nil {
		t.Fatalf("read PubCoord: %v", err)
	}
	return at, size
}

// gcFromSize returns GCCoord.fromSize.
func (e *pgTestEnv) gcFromSize(t *testing.T) uint64 {
	t.Helper()
	return e.queryUint64(t, "SELECT fromSize FROM GCCoord WHERE id = 0")
}

// tableNames returns the (lowercase) names of the tables in the test schema, sorted.
func (e *pgTestEnv) tableNames(t *testing.T) []string {
	t.Helper()
	rows, err := e.DB.QueryContext(context.Background(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name", e.Schema)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer func() { _ = rows.Close() }()
	r := []string{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		r = append(r, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tables: %v", err)
	}
	return r
}

// wantSequenced returns the SequencedEntry the sequencer is expected to store for data at idx.
func wantSequenced(data []byte, idx uint64) storage.SequencedEntry {
	e := tessera.NewEntry(data)
	return storage.SequencedEntry{
		BundleData: e.MarshalBundleData(idx),
		LeafHash:   e.LeafHash(),
	}
}

// encodeBatch gob-encodes a batch in the format stored in Seq.v.
func encodeBatch(t *testing.T, b []storage.SequencedEntry) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(b); err != nil {
		t.Fatalf("gob encode: %v", err)
	}
	return buf.Bytes()
}

// waitFor polls cond every 10ms until it returns true, failing the test if it doesn't within timeout.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// testBucket is a per-test bucket on the S3-compatible test endpoint.
type testBucket struct {
	Name      string
	SDKConfig *aws.Config
	S3Options func(*s3.Options)
	Client    *s3.Client
}

// newTestBucket creates a uniquely named bucket for the test, skipping the test if
// TESSERA_S3PG_TEST_S3_ENDPOINT is not set. The bucket and its contents are removed on cleanup.
//
// The client is configured the same way as cmd/conformance/aws configures a custom (MinIO) endpoint.
func newTestBucket(t *testing.T) *testBucket {
	t.Helper()
	endpoint := os.Getenv(s3EndpointEnv)
	if endpoint == "" {
		t.Skipf("%s not set, skipping test needing S3", s3EndpointEnv)
	}
	sdkConfig := &aws.Config{Region: testS3Region}
	s3Opts := func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.Credentials = credentials.NewStaticCredentialsProvider(envOr(s3AccessKeyEnv, "test"), envOr(s3SecretEnv, "test"), "")
		o.Region = testS3Region
		o.UsePathStyle = true
	}
	c := s3.NewFromConfig(*sdkConfig, s3Opts)
	name := "s3pg-test-" + randHex(t, 8)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Fatalf("CreateBucket(%q): %v", name, err)
	}
	t.Cleanup(func() { deleteBucket(t, c, name) })
	return &testBucket{Name: name, SDKConfig: sdkConfig, S3Options: s3Opts, Client: c}
}

// deleteBucket removes all objects in the bucket, and the bucket itself.
func deleteBucket(t *testing.T, c *s3.Client, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	p := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(name)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Logf("cleanup: list objects in %q: %v", name, err)
			return
		}
		if len(page.Contents) == 0 {
			continue
		}
		objs := make([]types.ObjectIdentifier, 0, len(page.Contents))
		for _, o := range page.Contents {
			objs = append(objs, types.ObjectIdentifier{Key: o.Key})
		}
		if _, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(name), Delete: &types.Delete{Objects: objs}}); err != nil {
			t.Logf("cleanup: delete objects in %q: %v", name, err)
			return
		}
	}
	if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)}); err != nil {
		t.Logf("cleanup: delete bucket %q: %v", name, err)
	}
}

// keys returns all object keys in the bucket.
func (b *testBucket) keys(t *testing.T) []string {
	t.Helper()
	r := []string{}
	p := s3.NewListObjectsV2Paginator(b.Client, &s3.ListObjectsV2Input{Bucket: aws.String(b.Name)})
	for p.HasMorePages() {
		page, err := p.NextPage(context.Background())
		if err != nil {
			t.Fatalf("list objects: %v", err)
		}
		for _, o := range page.Contents {
			r = append(r, aws.ToString(o.Key))
		}
	}
	return r
}

// s3Reader reads tlog-tiles resources directly from a bucket, independently of the code under test.
// It implements fsck.Fetcher and the client package fetcher function signatures.
type s3Reader struct {
	client *s3.Client
	bucket string
	prefix string
}

func (r *s3Reader) get(ctx context.Context, key string) ([]byte, error) {
	if r.prefix != "" {
		key = path.Join(r.prefix, key)
	}
	o, err := r.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(r.bucket), Key: aws.String(key)})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("%s: %w", key, os.ErrNotExist)
		}
		return nil, fmt.Errorf("GetObject(%q): %v", key, err)
	}
	defer func() { _ = o.Body.Close() }()
	return io.ReadAll(o.Body)
}

func (r *s3Reader) ReadCheckpoint(ctx context.Context) ([]byte, error) {
	return r.get(ctx, layout.CheckpointPath)
}

func (r *s3Reader) ReadTile(ctx context.Context, l, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return r.get(ctx, layout.TilePath(l, i, p))
	})
}

func (r *s3Reader) ReadEntryBundle(ctx context.Context, i uint64, p uint8) ([]byte, error) {
	return fetcher.PartialOrFullResource(ctx, p, func(ctx context.Context, p uint8) ([]byte, error) {
		return r.get(ctx, layout.EntriesPath(i, p))
	})
}

// envOr returns the value of the environment variable k, or def if it is unset or empty.
func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
