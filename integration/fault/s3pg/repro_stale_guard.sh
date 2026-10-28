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

# Reproduction: FALSE-POSITIVE ErrCoordinationBehind trip on a healthy log (database never behind).
#
# Replica 0 talks to PostgreSQL through tcpproxy and to S3 through s3proxy, which delays its GETs of
# the checkpoint by CP_DELAY (before forwarding them, so they return the checkpoint as it is when
# they are forwarded). Replica 1 is direct. Both guards read the checkpoint while holding a row
# lock (integration: IntCoord, publication: PubCoord). Every KILL_EVERY seconds replica 0's
# PostgreSQL connections are reset, which aborts its transaction and releases the lock while its
# goroutine is still waiting for the checkpoint; replica 1 then integrates the same rows, commits
# and publishes a larger checkpoint. When replica 0's GET returns, it compares that newer checkpoint
# with the IntCoord.seq / tree size it read before losing the lock and trips the sticky guard:
# replica 0 then refuses all new entries and /healthz returns 503 until it is restarted, although
# the database was never behind (dbcheck -watch: PubCoord/checkpoint <= IntCoord throughout).
#
# Fixed in 3c22ab3 (confirmBehind re-reads IntCoord; a stale comparison is the transient errStaleView):
# the script now also counts errStaleView occurrences (expected: some, harmless), checks that no
# tripped marker object was written, and that replica 0 is live afterwards (/healthz 200, its entry
# accepted and published). CP_DELAY / KILL_EVERY make it more aggressive (e.g. 8s / 0.7).
# Exit 0 if no trip, no marker and live; 1 if the false positive was reproduced (or not live).
set -uo pipefail
HERE=${HERE:-$(cd "$(dirname "$0")" && pwd)}
BIN=${BIN:-/home/claude/tools/faults/bin}; PGPORT=${PGPORT:-5530}; SLOT=${SLOT:-4}; SECS=${SECS:-60}
CP_DELAY=${CP_DELAY:-4s}; KILL_EVERY=${KILL_EVERY:-1.5}; PUBLISH_INTERVAL=${PUBLISH_INTERVAL:-1s}
OUT=${OUT:-/home/claude/tools/faults/out/repro-stale-guard}; mkdir -p "$OUT" && rm -rf "${OUT:?}"/*
S3=http://127.0.0.1:9000; SUF=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n'); DB=s3pgf_$SUF; BUCKET=s3pgf-$SUF
ADMIN="postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable"; DIRECTDB="postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable"
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
BASE=$((7000 + 20*SLOT)); PORT=$((3300 + 10*SLOT))
log() { echo "[$(date -u +%T.%3N)] $*" | tee -a "$OUT/events.log"; }
cpsize() { curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 2p; }
PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done; wait 2>/dev/null
  psql -X -q "$ADMIN" -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)"; python3 "$HERE/objcheck.py" "$BUCKET" delete-bucket >/dev/null
}
trap cleanup EXIT
python3 "$HERE/objcheck.py" "$BUCKET" create-bucket >/dev/null || exit 1
psql -X -q "$ADMIN" -c "CREATE DATABASE $DB"; psql -X -q "$DIRECTDB" -c "CREATE SCHEMA tessera_log"
psql -X -q "$DIRECTDB" -c "SET search_path TO tessera_log" -f "$HERE/../../../storage/s3pg/schema.sql"
"$BIN/tcpproxy" -listen 127.0.0.1:$BASE -ctl 127.0.0.1:$((BASE+1)) -upstream 127.0.0.1:$PGPORT -faults=false -log "$OUT/pgproxy.log" & PIDS+=($!)
"$BIN/s3proxy" -listen 127.0.0.1:$((BASE+2)) -ctl 127.0.0.1:$((BASE+3)) -faults=false -latency_ops=reads -latency_prefix=checkpoint -log "$OUT/s3proxy.log" & PIDS+=($!)
sleep 0.5
start() { # $1 replica, $2 pg addr, $3 s3 endpoint
  TESSERA_PG_DSN="postgres://postgres@$2/$DB?sslmode=disable&application_name=replica-$1" TESSERA_S3_ACCESS_KEY=test TESSERA_S3_SECRET=test TESSERA_SIGNER="$KEY" \
    "$BIN/conformance-s3pg" --bucket="$BUCKET" --s3_endpoint="$3" --pg_schema=tessera_log --skip_schema_init --publish_interval="$PUBLISH_INTERVAL" \
    --listen=127.0.0.1:$((PORT+$1)) >> "$OUT/replica-$1.log" 2>&1 & PIDS+=($!); }
start 0 127.0.0.1:$BASE http://127.0.0.1:$((BASE+2)); start 1 127.0.0.1:$PGPORT $S3
for r in 0 1; do for _ in $(seq 60); do curl -sf -m 2 "http://127.0.0.1:$((PORT+r))/healthz" >/dev/null && break; sleep 0.5; done; done
"$BIN/cpwatch" --log_url="$S3/$BUCKET/" --public_key="$PUB" --out="$OUT/cpwatch.json" --events="$OUT/cpwatch.log" >/dev/null 2>&1 & CPW=$!; PIDS+=($CPW)
"$BIN/dbcheck" --dsn="$DIRECTDB&search_path=tessera_log" --checkpoint_url="$S3/$BUCKET/checkpoint" --watch=200ms --out="$OUT/dbwatch.json" --events="$OUT/dbwatch.log" >/dev/null 2>&1 & DBW=$!; PIDS+=($DBW)
# Writes go to replica 1 only (replica 0 only takes part in integration and publication).
( n=0; while [ ! -f "$OUT/stopw" ]; do curl -s -m 5 -o /dev/null -X POST --data "e-$n-$RANDOM" "http://127.0.0.1:$((PORT+1))/add"; n=$((n+1)); sleep 0.02; done ) & PIDS+=($!)
sleep 3; log "published checkpoint: $(cpsize); delaying replica 0's checkpoint GETs by $CP_DELAY and resetting its PG connections every ${KILL_EVERY}s"
curl -s "http://127.0.0.1:$((BASE+3))/ctl?latency=$CP_DELAY" > /dev/null
end=$((SECONDS + SECS)); kills=0
while [ $SECONDS -lt $end ]; do
  sleep "$KILL_EVERY"; curl -s -m 2 "http://127.0.0.1:$((BASE+1))/ctl?killall=1" >/dev/null; kills=$((kills+1))
  grep -q "coordination database is behind" "$OUT/replica-0.log" && { log "replica 0 TRIPPED after $kills resets"; break; }
done
curl -s "http://127.0.0.1:$((BASE+3))/ctl?latency=0s" > /dev/null; sleep 3
H0=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "http://127.0.0.1:$PORT/healthz"); H1=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "http://127.0.0.1:$((PORT+1))/healthz")
ADD0=$(curl -s -m 5 -w ' HTTP %{http_code}' -X POST --data "probe-$SUF" "http://127.0.0.1:$PORT/add" | cut -c1-200)
log "after faults: replica 0 /healthz=$H0 add=[$ADD0]; replica 1 /healthz=$H1; checkpoint $(cpsize)"
# Liveness: the checkpoint keeps growing (writers still running) and replica 0's entry gets published.
C1=$(cpsize); IDX0=$(grep -o '^[0-9]*' <<< "$ADD0"); LIVE=no
for _ in $(seq 1 40); do C2=$(cpsize); [ "${C2:-0}" -gt "${C1:-0}" ] 2>/dev/null && [ -n "$IDX0" ] && [ "$C2" -gt "$IDX0" ] && { LIVE=yes; break; }; sleep 0.5; done
log "liveness: checkpoint $C1 -> $(cpsize) (replica 0's entry at index ${IDX0:-none} published: $LIVE)"
touch "$OUT/stopw"; sleep 2; kill -TERM $CPW $DBW; wait $CPW; wait $DBW
MK=$(python3 "$HERE/objcheck.py" "$BUCKET" marker | cut -c1-300)
# errStaleView: a guard's comparison failed only because the replica's view was stale (no trip).
STALE=$(grep -c 'coordination state moved on' "$OUT/replica-0.log"); STALE_INT=$(grep 'coordination state moved on' "$OUT/replica-0.log" | grep -c 'msg=integrateEntries')
STALE_PUB=$(grep 'coordination state moved on' "$OUT/replica-0.log" | grep -c 'checkpoint publisher')
# A trip logs exactly one 'msg="Coordination database is behind ..."' line (guard.go, trip). A suspected
# lag whose confirming read failed (no trip, retried) is counted separately: since 6f2d29c the read uses
# the transaction's own connection, so after a reset it fails ("could not confirm a suspected
# coordination lag ...: failed to re-read the integrated size"); 3c22ab3 logged "failed to re-read
# integrated size to confirm (<ErrCoordinationBehind text>)".
CONFIRM_RE='could not confirm a suspected coordination lag|failed to re-read integrated size to confirm'
TRIPS=$(grep -c 'msg="Coordination database is behind' "$OUT/replica-0.log"); CONFIRM_FAIL=$(grep -Ec "$CONFIRM_RE" "$OUT/replica-0.log")
CONFIRM_INT=$(grep -E "$CONFIRM_RE" "$OUT/replica-0.log" | grep -c 'msg=integrateEntries'); CONFIRM_PUB=$(grep -E "$CONFIRM_RE" "$OUT/replica-0.log" | grep -c 'checkpoint publisher')
log "dbwatch (checkpoint <= PubCoord <= IntCoord): $(tr -d '\n ' < "$OUT/dbwatch.json" | cut -c1-300)"
log "cpwatch: $(tr -d '\n ' < "$OUT/cpwatch.json" | cut -c1-200)"
log "resets=$kills trips=$TRIPS errStaleView=$STALE (integration $STALE_INT, publication $STALE_PUB) confirm_failed=$CONFIRM_FAIL (integration $CONFIRM_INT, publication $CONFIRM_PUB) marker=$MK"
grep -h 'coordination state moved on' "$OUT/replica-0.log" | head -2 | sed 's/^time=[^ ]* //' | cut -c1-300 | tee -a "$OUT/events.log"
grep -h 'msg="Coordination database is behind' "$OUT"/replica-*.log | head -3 | cut -c1-400 | tee -a "$OUT/events.log"
echo "{\"resets\": $kills, \"trips\": $TRIPS, \"stale_view\": $STALE, \"stale_view_integration\": $STALE_INT, \"stale_view_publication\": $STALE_PUB, \"confirm_reread_failed\": $CONFIRM_FAIL, \"confirm_failed_integration\": $CONFIRM_INT, \"confirm_failed_publication\": $CONFIRM_PUB, \"healthz0\": \"$H0\", \"live\": \"$LIVE\", \"marker\": $(grep -q '"exists": true' <<< "$MK" && echo true || echo false)}" > "$OUT/result.json"
if [ "$TRIPS" != 0 ]; then log "REPRODUCED: false-positive ErrCoordinationBehind trip"; exit 1; fi
if grep -q '"exists": true' <<< "$MK"; then log "FAIL: tripped marker written"; exit 1; fi
if [ "$H0" != 200 ] || [ "$LIVE" != yes ]; then log "FAIL: replica 0 not live"; exit 1; fi
log "no trip observed ($kills resets), no marker, live"; exit 0
