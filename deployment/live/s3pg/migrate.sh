#!/bin/sh
# Copyright 2026 The Tessera authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Applies the Tessera s3pg schemas as the owning role, standing in for your migration tooling.
#
# Both schema files are idempotent and never reset existing state, so this is safe to run on
# every start-up. Connection settings come from the standard PG* environment variables
# (PGHOST, PGDATABASE, PGUSER, PGPASSWORD, ...).
#
# Usage: migrate.sh <schema> <file.sql> [<schema> <file.sql> ...]
set -eu

for i in $(seq 1 30); do
  pg_isready -q && break
  sleep 1
done

while [ "$#" -ge 2 ]; do
  schema="$1"
  file="$2"
  shift 2
  echo "Applying ${file} to schema ${schema}"
  psql -v ON_ERROR_STOP=1 -X -q -v schema="${schema}" -v file="${file}" <<'EOSQL'
BEGIN;
SET LOCAL client_min_messages TO warning;
SET LOCAL search_path TO :"schema";
\i :file
-- Normally covered by ALTER DEFAULT PRIVILEGES (see postgres-init/), repeated here so that
-- the runtime role's grants are correct even for tables created before those defaults existed.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA :"schema" TO tessera_app;
COMMIT;
EOSQL
done
