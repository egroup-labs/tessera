#!/bin/bash
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

# Chaos test for the s3pg driver: two conformance replicas share one log (PostgreSQL schema +
# bucket) while the chaos tool writes continuously and this script injects failures. See README.md.
#
# Required environment:
#   PG_ADMIN_DSN   superuser URL DSN for a DISPOSABLE cluster, e.g. postgres://postgres:pw@127.0.0.1:5432/postgres?sslmode=disable
#   S3_ENDPOINT    e.g. http://127.0.0.1:9000
#   S3_ACCESS_KEY, S3_SECRET
#   BUCKET         an existing, EMPTY bucket that is anonymously readable at $LOG_URL
# Optional:
#   LOG_URL        public read URL of the bucket (default $S3_ENDPOINT/$BUCKET/)
#   PG_RESTART_CMD command that restarts PostgreSQL (e.g. "sudo systemctl restart postgresql"); step skipped if unset
#   WRITE_SECS     write duration (default 150)
#   OUT            output directory (default ./chaos-out)
set -uo pipefail
: "${PG_ADMIN_DSN:?}" "${S3_ENDPOINT:?}" "${S3_ACCESS_KEY:?}" "${S3_SECRET:?}" "${BUCKET:?}"
LOG_URL=${LOG_URL:-$S3_ENDPOINT/$BUCKET/}
WRITE_SECS=${WRITE_SECS:-150}
OUT=${OUT:-$PWD/chaos-out}
ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
mkdir -p "$OUT" && rm -f "$OUT"/*.log "$OUT"/summary.json

SUFFIX=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n')
DB=s3pg_chaos_$SUFFIX
APP_ROLE=s3pg_chaos_app_$SUFFIX   # unique: never touches roles of an existing deployment
APP_PW=$(head -c12 /dev/urandom | od -An -tx1 | tr -d ' \n')
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
declare -A PIDS PORTS=([a]=2941 [b]=2942)

admin() { psql -X -q -v ON_ERROR_STOP=1 "$PG_ADMIN_DSN" "$@"; }
# Same server/credentials as PG_ADMIN_DSN, but connected to database $1.
dsn_for_db() { python3 -c 'import sys,urllib.parse as u; p=u.urlsplit(sys.argv[1]); print(u.urlunsplit(p._replace(path="/"+sys.argv[2])))' "$PG_ADMIN_DSN" "$1"; }
app_dsn() { python3 -c 'import sys,urllib.parse as u; p=u.urlsplit(sys.argv[1]); h=p.hostname+(":%d"%p.port if p.port else ""); q=dict(u.parse_qsl(p.query)); q.update(application_name=sys.argv[5]); print(u.urlunsplit(p._replace(netloc="%s:%s@%s"%(sys.argv[3],sys.argv[4],h), path="/"+sys.argv[2], query=u.urlencode(q))))' "$PG_ADMIN_DSN" "$DB" "$APP_ROLE" "$APP_PW" "$1"; }
log() { echo "[$(date +%T)] $*" | tee -a "$OUT/events.log"; }

cleanup() {
  for r in a b; do [ -n "${PIDS[$r]:-}" ] && { kill -CONT "${PIDS[$r]}"; kill -9 "${PIDS[$r]}"; } 2>/dev/null; done
  admin -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" -c "DROP ROLE IF EXISTS $APP_ROLE" 2>/dev/null
}
trap cleanup EXIT

cd "$ROOT"
go build -o "$OUT/conformance-s3pg" ./cmd/conformance/s3pg && go build -o "$OUT/chaos" ./integration/chaos/s3pg && go build -o "$OUT/fsck" ./cmd/fsck || exit 1

# Fresh database, schema applied by the superuser, runtime role with DML only (--skip_schema_init).
admin -c "CREATE DATABASE $DB" -c "CREATE ROLE $APP_ROLE LOGIN PASSWORD '$APP_PW'" \
  -c "ALTER ROLE $APP_ROLE SET idle_in_transaction_session_timeout = '60s'" -c "ALTER ROLE $APP_ROLE SET statement_timeout = '60s'"
psql -X -q -v ON_ERROR_STOP=1 "$(dsn_for_db "$DB")" \
  -c "CREATE SCHEMA tessera_log" -c "SET search_path TO tessera_log" -f storage/s3pg/schema.sql \
  -c "GRANT USAGE ON SCHEMA tessera_log TO $APP_ROLE" \
  -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA tessera_log TO $APP_ROLE" || exit 1

start() {
  local r=$1
  TESSERA_PG_DSN="$(app_dsn "replica-$r")" TESSERA_S3_ACCESS_KEY="$S3_ACCESS_KEY" TESSERA_S3_SECRET="$S3_SECRET" TESSERA_SIGNER="$KEY" \
    "$OUT/conformance-s3pg" --bucket="$BUCKET" --s3_endpoint="$S3_ENDPOINT" --skip_schema_init --pg_schema=tessera_log \
      --listen=127.0.0.1:${PORTS[$r]} --pg_max_conns=16 --pg_max_idle_conns=4 >> "$OUT/replica-$r.log" 2>&1 &
  PIDS[$r]=$!
  log "started replica $r (pid ${PIDS[$r]})"
}
wait_healthy() { for _ in $(seq 1 120); do curl -sf "http://127.0.0.1:${PORTS[$1]}/healthz" >/dev/null && return 0; sleep 0.5; done; log "replica $1 NOT healthy"; return 1; }
kill9() { log "SIGKILL replica $1"; kill -9 "${PIDS[$1]}"; wait "${PIDS[$1]}" 2>/dev/null; }
killsessions() { log "terminating all $APP_ROLE sessions: $(admin -tAc "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE usename='$APP_ROLE'")"; }

start a; start b; wait_healthy a && wait_healthy b || exit 1
"$OUT/chaos" --write_urls=http://127.0.0.1:${PORTS[a]},http://127.0.0.1:${PORTS[b]} --log_url="$LOG_URL" \
  --public_key="$PUB" --writers=48 --duration="${WRITE_SECS}s" --settle=3m --out="$OUT/summary.json" > "$OUT/chaos.log" 2>&1 &
CHAOS=$!
log "chaos writer started"

sleep 10; kill9 a; sleep 3; start a
sleep 12; kill9 b; sleep 1; start b
sleep 12; killsessions
if [ -n "${PG_RESTART_CMD:-}" ]; then sleep 12; log "restarting PostgreSQL"; eval "$PG_RESTART_CMD"; log "PostgreSQL restarted"; fi
sleep 15; log "SIGSTOP replica a for 20s"; kill -STOP "${PIDS[a]}"; sleep 20; kill -CONT "${PIDS[a]}"; log "SIGCONT replica a"
sleep 12; log "SIGKILL both replicas"; kill -9 "${PIDS[a]}" "${PIDS[b]}"; sleep 2; start a; start b
sleep 10; killsessions
log "failure schedule done; waiting for the chaos tool to verify"
wait $CHAOS; RC=$?
cat "$OUT/summary.json" 2>/dev/null || tail -20 "$OUT/chaos.log"
log "chaos tool exit code $RC"

log "waiting 75s so that garbage collection (1m interval) runs"; sleep 75
printf '%s' "$PUB" > "$OUT/log.pub"
"$OUT/fsck" --storage_url="$LOG_URL" --public_key="$OUT/log.pub" --ui=false > "$OUT/fsck.log" 2>&1; FSCK=$?
log "fsck exit code $FSCK"
psql -X "$(dsn_for_db "$DB")" -c "SET search_path TO tessera_log" \
  -c "SELECT s.next, i.seq AS integrated, s.next - i.seq AS lag, p.size AS published, g.fromSize AS gc_from, (SELECT count(*) FROM Seq) AS seq_rows FROM SeqCoord s, IntCoord i, PubCoord p, GCCoord g" | tee -a "$OUT/events.log"
for r in a b; do
  log "replica $r: ERROR lines=$(grep -c 'level=ERROR' "$OUT/replica-$r.log") integrity-fail=$(grep -c 'integrity fail' "$OUT/replica-$r.log")"
done
exit $(( RC != 0 || FSCK != 0 ))
