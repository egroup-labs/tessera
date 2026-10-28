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

// Helpers shared by the hardening tests: schema fingerprints, decoy schemas, an instrumented
// objStore, and an Appender without background jobs which tests drive step by step.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
	storage "github.com/transparency-dev/tessera/storage/internal"
)

// testCP is a deterministic, parseable (unsigned) checkpoint for (size, root).
func testCP(_ context.Context, size uint64, root []byte) ([]byte, error) {
	return fmt.Appendf(nil, "test\n%d\n%s\n", size, base64.StdEncoding.EncodeToString(root)), nil
}

func mustTestCP(size uint64, root []byte) []byte {
	cp, _ := testCP(context.Background(), size, root)
	return cp
}

// qtable returns the schema-qualified name of a table.
func qtable(schema, table string) string { return quoteIdent(schema) + "." + table }

// newOddSchema creates an empty schema whose name needs quoting (mixed case, a space, a double
// quote and a hyphen), dropped when the test finishes.
func newOddSchema(t *testing.T) string {
	t.Helper()
	admin := requirePG(t)
	name := fmt.Sprintf(`S3pg Log "Q"-%s`, randHex(t, 6))
	if _, err := admin.ExecContext(t.Context(), "CREATE SCHEMA "+quoteIdent(name)); err != nil {
		t.Fatalf("CREATE SCHEMA %s: %v", quoteIdent(name), err)
	}
	t.Cleanup(func() { dropSchema(t, admin, name) })
	return name
}

// schemaFingerprint returns every relation in schema and, for each table, all of its rows
// including the system columns xmin, ctid and (if withXmax) xmax. Any committed INSERT, UPDATE
// or DELETE changes it, even one writing identical values; with xmax, so does a row lock
// (SELECT ... FOR UPDATE) or an aborted update.
func schemaFingerprint(t *testing.T, db *sql.DB, schema string, withXmax bool) map[string][]string {
	t.Helper()
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, `SELECT c.relname, c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 ORDER BY 1`, schema)
	if err != nil {
		t.Fatalf("list relations of %q: %v", schema, err)
	}
	type rel struct{ name, kind string }
	var rels []rel
	for rows.Next() {
		var r rel
		if err := rows.Scan(&r.name, &r.kind); err != nil {
			t.Fatalf("scan: %v", err)
		}
		rels = append(rels, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	_ = rows.Close()
	r := map[string][]string{}
	sys := "xmin, ctid"
	if withXmax {
		sys = "xmin, xmax, ctid"
	}
	for _, rl := range rels {
		key := rl.kind + ":" + rl.name
		if rl.kind != "r" {
			r[key] = nil
			continue
		}
		q := fmt.Sprintf("SELECT concat_ws(' ', %s, t::text) FROM %s.%s AS t ORDER BY 1", sys, quoteIdent(schema), quoteIdent(rl.name))
		rr, err := db.QueryContext(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		var vals []string
		for rr.Next() {
			var v string
			if err := rr.Scan(&v); err != nil {
				t.Fatalf("scan: %v", err)
			}
			vals = append(vals, v)
		}
		if err := rr.Err(); err != nil {
			t.Fatalf("iterate: %v", err)
		}
		_ = rr.Close()
		r[key] = vals
	}
	return r
}

// checkFingerprint fails the test if schema's fingerprint differs from want.
func checkFingerprint(t *testing.T, db *sql.DB, schema string, withXmax bool, want map[string][]string, what string) {
	t.Helper()
	if diff := cmp.Diff(want, schemaFingerprint(t, db, schema, withXmax)); diff != "" {
		t.Errorf("%s changed (-before +after):\n%s", what, diff)
	}
}

// tablesIn returns the (lower-case) names of the tables in schema, sorted.
func tablesIn(t *testing.T, db *sql.DB, schema string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY 1", schema)
	if err != nil {
		t.Fatalf("list tables: %v", err)
	}
	defer func() { _ = rows.Close() }()
	r := []string{}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		r = append(r, n)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return r
}

// Sentinel values of decoy schemas.
const (
	decoyNext        = 777777
	decoyIntSize     = 555555
	decoyGCFrom      = 256
	decoyPublishedAt = 4102444800 * int64(time.Second) // 2100-01-01: "published in the future".
)

var decoyRoot = []byte("decoy root")

// newDecoyEnv returns a per-test schema, selected by the returned DSN's search_path, holding a
// complete copy of the log tables with sentinel values: a statement which is not
// schema-qualified either changes it (detected by schemaFingerprint) or reads values which make
// the operation misbehave visibly (sizes, indices, publication times, GC progress).
func newDecoyEnv(t *testing.T, compatVersion int) *pgTestEnv {
	t.Helper()
	env := newPGTestEnv(t)
	env.exec(t, SchemaSQL)
	env.exec(t, "UPDATE Tessera SET compatibilityVersion = $1", compatVersion)
	env.exec(t, "UPDATE SeqCoord SET next = $1", decoyNext)
	env.exec(t, "UPDATE IntCoord SET seq = $1, rootHash = $2", decoyIntSize, decoyRoot)
	env.exec(t, "UPDATE PubCoord SET publishedAt = $1, size = $2", decoyPublishedAt, decoyIntSize)
	env.exec(t, "UPDATE GCCoord SET fromSize = $1", decoyGCFrom)
	env.exec(t, "INSERT INTO Seq (id, seq, v) VALUES (0, $1, $2)", decoyIntSize, encodeBatch(t, []storage.SequencedEntry{wantSequenced([]byte("decoy entry"), decoyIntSize)}))
	return env
}

// logState is the state of the coordination tables of a log.
type logState struct {
	Next    uint64
	IntSize uint64
	IntRoot []byte
	PubAt   int64
	PubSize sql.NullInt64
	GCFrom  uint64
	SeqKeys []uint64
}

// readLogState reads the coordination tables in schema (fully qualified, whatever db's search_path).
func readLogState(t *testing.T, db *sql.DB, schema string) logState {
	t.Helper()
	ctx := context.Background()
	var s logState
	for q, dst := range map[string][]any{
		"SELECT next FROM " + qtable(schema, "SeqCoord"):              {&s.Next},
		"SELECT seq, rootHash FROM " + qtable(schema, "IntCoord"):     {&s.IntSize, &s.IntRoot},
		"SELECT publishedAt, size FROM " + qtable(schema, "PubCoord"): {&s.PubAt, &s.PubSize},
		"SELECT fromSize FROM " + qtable(schema, "GCCoord"):           {&s.GCFrom},
	} {
		if err := db.QueryRowContext(ctx, q).Scan(dst...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	rows, err := db.QueryContext(ctx, "SELECT seq FROM "+qtable(schema, "Seq")+" ORDER BY seq")
	if err != nil {
		t.Fatalf("read Seq: %v", err)
	}
	defer func() { _ = rows.Close() }()
	s.SeqKeys = []uint64{}
	for rows.Next() {
		var k uint64
		if err := rows.Scan(&k); err != nil {
			t.Fatalf("scan: %v", err)
		}
		s.SeqKeys = append(s.SeqKeys, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}
	return s
}

// spyStore wraps an objStore, recording calls and optionally injecting read failures or running
// a hook after the checkpoint has been read with its ETag.
type spyStore struct {
	objStore

	mu     sync.Mutex
	gets   []string // getObject and getObjectWithETag
	lists  []string
	writes []string // setObject, setObjectIfNoneMatch, setObjectIfETag
	// getErr, if set, is consulted before every read; a non-nil result is returned instead.
	getErr func(key string) error
	// afterCPRead, if set, is called (once; it is then cleared) after getObjectWithETag of the checkpoint.
	afterCPRead func(ctx context.Context)
}

func (s *spyStore) recordGet(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = append(s.gets, key)
	if s.getErr != nil {
		return s.getErr(key)
	}
	return nil
}

func (s *spyStore) recordWrite(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, key)
}

func (s *spyStore) getObject(ctx context.Context, key string) ([]byte, error) {
	if err := s.recordGet(key); err != nil {
		return nil, err
	}
	return s.objStore.getObject(ctx, key)
}

func (s *spyStore) getObjectWithETag(ctx context.Context, key string) ([]byte, string, error) {
	if err := s.recordGet(key); err != nil {
		return nil, "", err
	}
	d, e, err := s.objStore.getObjectWithETag(ctx, key)
	if key == layout.CheckpointPath {
		s.mu.Lock()
		hook := s.afterCPRead
		s.afterCPRead = nil
		s.mu.Unlock()
		if hook != nil {
			hook(ctx)
		}
	}
	return d, e, err
}

func (s *spyStore) listObjects(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	s.lists = append(s.lists, prefix)
	s.mu.Unlock()
	return s.objStore.listObjects(ctx, prefix)
}

func (s *spyStore) setObject(ctx context.Context, key string, data []byte, ct, cc string) error {
	s.recordWrite(key)
	return s.objStore.setObject(ctx, key, data, ct, cc)
}

func (s *spyStore) setObjectIfNoneMatch(ctx context.Context, key string, data []byte, ct, cc string) error {
	s.recordWrite(key)
	return s.objStore.setObjectIfNoneMatch(ctx, key, data, ct, cc)
}

func (s *spyStore) setObjectIfETag(ctx context.Context, key string, data []byte, ct, cc, etag string) error {
	s.recordWrite(key)
	return s.objStore.setObjectIfETag(ctx, key, data, ct, cc, etag)
}

// counts returns the number of reads, listings and writes so far, and resets them.
func (s *spyStore) take() (gets, lists, writes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gets, lists, writes = s.gets, s.lists, s.writes
	s.gets, s.lists, s.writes = nil, nil, nil
	return gets, lists, writes
}

// memContents returns a copy of the contents of a memObjStore.
func memContents(m *memObjStore) map[string][]byte {
	m.RLock()
	defer m.RUnlock()
	r := make(map[string][]byte, len(m.mem))
	for k, v := range m.mem {
		r[k] = bytes.Clone(v)
	}
	return r
}

// checkMemUnchanged fails the test if the store's contents differ from want.
func checkMemUnchanged(t *testing.T, m *memObjStore, want map[string][]byte, what string) {
	t.Helper()
	got := memContents(m)
	if !maps.EqualFunc(got, want, bytes.Equal) {
		var diffs []string
		for k := range got {
			if w, ok := want[k]; !ok {
				diffs = append(diffs, "+"+k)
			} else if !bytes.Equal(w, got[k]) {
				diffs = append(diffs, "~"+k)
			}
		}
		for k := range want {
			if _, ok := got[k]; !ok {
				diffs = append(diffs, "-"+k)
			}
		}
		slices.Sort(diffs)
		t.Errorf("%s: object store changed: %s", what, strings.Join(diffs, " "))
	}
}

// newBareAppender returns an Appender on store and seq without background jobs or queue, which
// reports tripped guards to the returned Storage (like Storage.newAppender does).
func newBareAppender(store objStore, seq sequencer, entriesPath func(uint64, uint8) string) (*Appender, *Storage) {
	st := &Storage{}
	a := &Appender{
		logStore:  &logResourceStore{objStore: store, entriesPath: entriesPath},
		sequencer: seq,
		newCP:     testCP,
		onBehind:  st.setBehind,
	}
	if seq != nil {
		a.logStore.integratedSize = func(ctx context.Context) (uint64, error) { s, _, err := seq.currentTree(ctx); return s, err }
		a.logStore.nextIndex = seq.nextIndex
	}
	return a, st
}

// integrateAll consumes all sequenced entries via a.integrateEntries.
func integrateAll(t *testing.T, ctx context.Context, seq *pgSequencer, a *Appender) {
	t.Helper()
	for {
		more, err := seq.consumeEntries(ctx, DefaultIntegrationSizeLimit, a.integrateEntries, false)
		if err != nil {
			t.Fatalf("consumeEntries: %v", err)
		}
		if !more {
			return
		}
	}
}

// addAndIntegrate assigns entries "<prefix> i" for i in [0, n) in batches of batch, and integrates them.
func addAndIntegrate(t *testing.T, ctx context.Context, seq *pgSequencer, a *Appender, prefix string, n, batch int) [][]byte {
	t.Helper()
	var r [][]byte
	for i := 0; i < n; i += batch {
		data := batchData(fmt.Sprintf("%s/%d", prefix, i), min(batch, n-i))
		assignData(t, ctx, seq, data...)
		for _, d := range data {
			r = append(r, []byte(d))
		}
	}
	integrateAll(t, ctx, seq, a)
	return r
}

// rootOf returns the RFC 6962 root hash of a tree with the given entries.
func rootOf(t *testing.T, data [][]byte) []byte {
	t.Helper()
	rf := compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}
	cr := rf.NewEmptyRange(0)
	for _, d := range data {
		if err := cr.Append(rfc6962.DefaultHasher.HashLeaf(d), nil); err != nil {
			t.Fatal(err)
		}
	}
	r, err := cr.GetRootHash(nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// seqEntries returns the SequencedEntries for data "<prefix> i" at indices [from, from+n).
func seqEntries(prefix string, from, n uint64) []storage.SequencedEntry {
	r := make([]storage.SequencedEntry, n)
	for i := range n {
		r[i] = wantSequenced(fmt.Appendf(nil, "%s %d", prefix, from+i), from+i)
	}
	return r
}

// concatBundle returns the entry bundle (or, with tile, level-0 tile) bytes for entries.
func concatResource(entries []storage.SequencedEntry, tile bool) []byte {
	var b []byte
	for _, e := range entries {
		if tile {
			b = append(b, e.LeafHash...)
		} else {
			b = append(b, e.BundleData...)
		}
	}
	return b
}

// checkMemTripped checks that the only change to the object store is the tripped marker written
// when a guard trips (see guard.go), and that it was written.
func checkMemTripped(t *testing.T, m *memObjStore, want map[string][]byte, what string) {
	t.Helper()
	got := memContents(m)
	if _, ok := got[trippedMarkerPath]; !ok {
		t.Errorf("%s: tripped marker %q not written", what, trippedMarkerPath)
	}
	m.Lock()
	marker, had := m.mem[trippedMarkerPath]
	delete(m.mem, trippedMarkerPath)
	m.Unlock()
	checkMemUnchanged(t, m, want, what)
	if had {
		m.Lock()
		m.mem[trippedMarkerPath] = marker
		m.Unlock()
	}
}
