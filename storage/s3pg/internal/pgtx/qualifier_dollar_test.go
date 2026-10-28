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

import "testing"

// PostgreSQL identifiers may contain '$' (even unquoted: CREATE SCHEMA log$1 is valid), but
// Qualifier.SQL passes the quoted schema name as a regexp *template* to ReplaceAllString, so "$1",
// "$name" or "${x}" inside the schema name are expanded as capture-group references: statements
// are silently pointed at a different (possibly existing) schema, or become invalid.
func TestQualifierSchemaWithDollar(t *testing.T) {
	for _, tc := range []struct{ schema, want string }{
		{"log$1", `SELECT 1 FROM "log$1".Tessera`},
		{"a$b", `SELECT 1 FROM "a$b".Tessera`},
		{"x${1}y", `SELECT 1 FROM "x${1}y".Tessera`},
		{"$$", `SELECT 1 FROM "$$".Tessera`},
	} {
		if got := NewQualifier(tc.schema, "Tessera").SQL("SELECT 1 FROM Tessera"); got != tc.want {
			t.Errorf("schema %q: SQL = %q, want %q", tc.schema, got, tc.want)
		}
	}
}
