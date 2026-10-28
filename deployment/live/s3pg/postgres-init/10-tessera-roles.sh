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

# Bootstraps roles and schemas for a Tessera s3pg log. Run once, as a superuser, by the
# postgres image's entrypoint when the data directory is first initialised
# (/docker-entrypoint-initdb.d). In production, do the equivalent with your usual
# provisioning tooling; see storage/s3pg/README.md.
#
#   tessera_owner  owns the schemas and tables; used only to apply schema.sql (DDL).
#   tessera_app    used by the Tessera servers at runtime (--skip_schema_init): DML only.
#
# The tables themselves are created by the "migrate" service in compose.yaml (as tessera_owner).
#
# No `set -eu`: the postgres entrypoint sources (rather than executes) non-executable *.sh
# files, and already runs with `set -e`.
: "${TESSERA_OWNER_PASSWORD:?must be set}" "${TESSERA_APP_PASSWORD:?must be set}"

psql -v ON_ERROR_STOP=1 -X --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v db="$POSTGRES_DB" \
  -v owner_pw="$TESSERA_OWNER_PASSWORD" \
  -v app_pw="$TESSERA_APP_PASSWORD" <<'EOSQL'
CREATE ROLE tessera_owner LOGIN PASSWORD :'owner_pw';
CREATE ROLE tessera_app LOGIN PASSWORD :'app_pw';

REVOKE ALL ON DATABASE :"db" FROM PUBLIC;
-- Before PostgreSQL 15, every role may create objects in the public schema; the runtime role must not.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE :"db" TO tessera_owner, tessera_app;

-- One schema per log; the antispam index gets a schema of its own.
CREATE SCHEMA tessera_log AUTHORIZATION tessera_owner;
CREATE SCHEMA tessera_antispam AUTHORIZATION tessera_owner;

-- Runtime role: may use the tables, but not create, alter or drop anything.
GRANT USAGE ON SCHEMA tessera_log, tessera_antispam TO tessera_app;
ALTER DEFAULT PRIVILEGES FOR ROLE tessera_owner IN SCHEMA tessera_log, tessera_antispam
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO tessera_app;

-- Server-side guard rails for the runtime role. They must comfortably exceed the longest
-- transaction Tessera holds open on purpose (garbage collection: up to 30s), otherwise
-- healthy integration/GC transactions get killed. See storage/s3pg/README.md.
ALTER ROLE tessera_app SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE tessera_app SET statement_timeout = '60s';
ALTER ROLE tessera_app SET lock_timeout = '60s';
EOSQL
