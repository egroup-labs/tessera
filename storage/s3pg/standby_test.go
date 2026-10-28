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
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/transparency-dev/tessera/storage/s3pg/internal/pgtx"
)

// standbyDSNEnv names a DSN for a streaming-replication standby of a primary on which the s3pg
// schema exists in the DSN's search_path (e.g. replicated from a test run). Skipped if unset.
const standbyDSNEnv = "TESSERA_S3PG_TEST_STANDBY_DSN"

// Reads of the integrated tree size served by a standby (e.g. via a load-balancing pooler) must
// fail rather than return a possibly lagging size, which the guards could mistake for lost commits.
func TestCurrentTreeRefusesStandby(t *testing.T) {
	dsn := os.Getenv(standbyDSNEnv)
	if dsn == "" {
		t.Skipf("%s not set", standbyDSNEnv)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	s := &pgSequencer{dbPool: db, q: pgtx.NewQualifier("")}
	if _, _, err := s.currentTree(t.Context()); err == nil || !strings.Contains(err.Error(), "standby") {
		t.Fatalf("currentTree on a standby = %v, want an error about the standby", err)
	}
}
