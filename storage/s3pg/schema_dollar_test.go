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

import (
	"context"
	"testing"

	"github.com/transparency-dev/tessera"
)

// Config.Schema "logs_<x>$prod" (a valid PostgreSQL schema name) is initialised correctly (DDL
// goes through search_path), but every qualified statement is rewritten by a regexp TEMPLATE, so
// "$prod" is expanded as a (non-existent) capture group: all statements then run against schema
// "logs_<x>" -- here another log, whose state is read and modified.
func TestSchemaWithDollarUsesAnotherLog(t *testing.T) {
	ctx := context.Background()
	admin := requirePG(t)
	other := "logs_" + randHex(t, 6)
	schema := other + "$prod"
	for _, s := range []string{other, schema} {
		if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+quoteIdent(s)); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { dropSchema(t, admin, s) })
	}
	// "other" is another, existing log.
	otherDB := openTestDB(t, dsnWithParams(t, baseTestDSN(), map[string]string{"search_path": other}), 2)
	if _, err := otherDB.ExecContext(ctx, SchemaSQL+"; UPDATE SeqCoord SET next = 12345"); err != nil {
		t.Fatal(err)
	}
	before := schemaFingerprint(t, admin, other, false)

	s, err := newPGSequencer(ctx, Config{DSN: baseTestDSN(), Schema: schema}, 1<<30)
	if err != nil {
		t.Fatalf("newPGSequencer: %v", err)
	}
	t.Cleanup(func() { _ = s.close() })
	if n, err := s.nextIndex(ctx); err != nil || n != 0 {
		t.Errorf("nextIndex of the fresh log %q = (%d, %v), want 0", schema, n, err)
	}
	if err := s.assignEntries(ctx, []*tessera.Entry{tessera.NewEntry([]byte("x"))}); err != nil {
		t.Fatalf("assignEntries: %v", err)
	}
	checkFingerprint(t, admin, other, false, before, "the other log")
	if st := readLogState(t, admin, schema); st.Next != 1 {
		t.Errorf("SeqCoord.next of %q = %d, want 1", schema, st.Next)
	}
}
