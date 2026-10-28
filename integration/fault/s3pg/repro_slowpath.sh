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

# Integration retried after its database transaction was lost mid-way, so that the retry finds the
# bucket AHEAD of IntCoord and takes checkBucketConsistent's slow path (comparison of every
# overlapping bundle and level-0 tile with the entries being integrated). It must pass (identical
# data), not trip, and complete within the integration timeout, also for a batch spanning many
# bundles.
#
# One replica (PostgreSQL through tcpproxy, S3 through s3proxy, which adds S3_LATENCY to every
# request). An external session holds the IntCoord row lock while the chaos writer writes for
# WRITE_SECS, building a backlog that is integrated in one go once the lock is released. As soon as
# that integration starts writing (its writes are held WRITE_LATENCY by the proxy), the replica's
# PostgreSQL connections are reset: its transaction aborts while its writes are in flight, they land
# anyway, its IntCoord update fails, and the retry starts from the same IntCoord.seq with the bucket
# ahead. The chaos writer then verifies every acknowledged entry.
#
# Reports the time from the kill until IntCoord first advances and until it has caught up with the
# backlog, and the failed integration attempts in between (fixed in 3c22ab3: the slow path checks
# bundles concurrently; expected 1-2 attempts). WRITE_SECS=50 gives ~120 bundles.
# Environment: WRITE_SECS (20), WRITERS (32), S3_LATENCY (0s, every request), WRITE_LATENCY (2s,
# writes of the aborted attempt only), SLOT (4), BIN, PGPORT, OUT; NOKILL=1 skips the reset (control run:
# the same backlog integrated without an aborted attempt, i.e. without the slow path); FRESH=1
# restarts the replica just before the lock is released, so that its adaptive integration limit
# (6f2d29c) has not already been halved by the attempts that timed out waiting for the lock.
# Exit 0 if the retry passed (no trip, chaos ok, objcheck/fsck ok), 1 otherwise.
set -uo pipefail
HERE=${HERE:-$(cd "$(dirname "$0")" && pwd)}
BIN=${BIN:-/home/claude/tools/faults/bin}; PGPORT=${PGPORT:-5530}; SLOT=${SLOT:-4}
WRITE_SECS=${WRITE_SECS:-20}; WRITERS=${WRITERS:-32}; S3_LATENCY=${S3_LATENCY:-0s}; WRITE_LATENCY=${WRITE_LATENCY:-2s}
OUT=${OUT:-/home/claude/tools/faults/out/repro-slowpath}; mkdir -p "$OUT" && rm -rf "${OUT:?}"/*
S3=http://127.0.0.1:9000; SUF=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n'); DB=s3pgf_$SUF; BUCKET=s3pgf-$SUF
ADMIN="postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable"; DIRECTDB="postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable"
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
BASE=$((7000 + 20*SLOT)); PORT=$((3300 + 10*SLOT))
log() { echo "[$(date -u +%T.%3N)] $*" | tee -a "$OUT/events.log"; }
q() { psql -X -tA "$DIRECTDB" -c "$1" 2>/dev/null; }
cpsize() { curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 2p; }
nbundles() { python3 - "$BUCKET" <<'EOF'
import sys, boto3, botocore
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))
print(sum(len(p.get("Contents", [])) for p in s3.get_paginator("list_objects_v2").paginate(Bucket=sys.argv[1], Prefix="tile/entries/")))
EOF
}
PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done; wait 2>/dev/null
  psql -X -q "$ADMIN" -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)"; python3 "$HERE/objcheck.py" "$BUCKET" delete-bucket >/dev/null
}
trap cleanup EXIT
python3 "$HERE/objcheck.py" "$BUCKET" create-bucket || exit 1
psql -X -q "$ADMIN" -c "CREATE DATABASE $DB" && psql -X -q "$DIRECTDB" -c "CREATE SCHEMA tessera_log" -c "SET search_path TO tessera_log" -f "$HERE/../../../storage/s3pg/schema.sql" || exit 1
printf '%s' "$PUB" > "$OUT/log.pub"
"$BIN/tcpproxy" -listen 127.0.0.1:$BASE -ctl 127.0.0.1:$((BASE+1)) -upstream 127.0.0.1:$PGPORT -faults=false -log "$OUT/pgproxy.log" & PIDS+=($!)
"$BIN/s3proxy" -listen 127.0.0.1:$((BASE+2)) -ctl 127.0.0.1:$((BASE+3)) -faults=false -latency="$S3_LATENCY" -log "$OUT/s3proxy.log" & PIDS+=($!)
sleep 0.5
start_replica() {
  TESSERA_PG_DSN="postgres://postgres@127.0.0.1:$BASE/$DB?sslmode=disable&application_name=replica-0" TESSERA_S3_ACCESS_KEY=test TESSERA_S3_SECRET=test TESSERA_SIGNER="$KEY" \
    "$BIN/conformance-s3pg" --bucket="$BUCKET" --s3_endpoint=http://127.0.0.1:$((BASE+2)) --pg_schema=tessera_log --skip_schema_init --publish_interval=1s \
    --listen=127.0.0.1:$PORT >> "$OUT/replica-0.log" 2>&1 & RPID=$!; PIDS+=($RPID)
  for _ in $(seq 60); do curl -sf -m 2 "http://127.0.0.1:$PORT/healthz" >/dev/null && break; sleep 0.5; done
}
start_replica
"$BIN/cpwatch" --log_url="$S3/$BUCKET/" --public_key="$PUB" --out="$OUT/cpwatch.json" --events="$OUT/cpwatch.log" >/dev/null 2>&1 & CPW=$!; PIDS+=($CPW)
# Start mid-bundle: 100 entries integrated and published.
for i in $(seq 1 100); do curl -s -m 5 -o /dev/null -X POST --data "pre-$SUF-$i" "http://127.0.0.1:$PORT/add" & done; wait_pre=$!; wait $wait_pre
for _ in $(seq 60); do [ "$(cpsize)" -ge 100 ] 2>/dev/null && break; sleep 0.5; done
log "start: checkpoint $(cpsize), IntCoord $(q 'SELECT seq FROM tessera_log.IntCoord')"
# Hold the IntCoord row lock while the backlog builds up.
psql -X -q "$DIRECTDB" -c "BEGIN" -c "SET application_name = 'lockholder'" -c "SELECT seq FROM tessera_log.IntCoord WHERE id = 0 FOR UPDATE" -c "SELECT pg_sleep(3600)" > /dev/null 2>&1 & LH=$!; PIDS+=($LH)
"$BIN/chaos" --write_urls="http://127.0.0.1:$PORT" --log_url="$S3/$BUCKET/" --public_key="$PUB" --writers="$WRITERS" --duration="${WRITE_SECS}s" --settle=5m --out="$OUT/chaos.json" > "$OUT/chaos.log" 2>&1 & CH=$!; PIDS+=($CH)
sleep "$WRITE_SECS"
I0=$(q 'SELECT seq FROM tessera_log.IntCoord'); N0=$(q 'SELECT next FROM tessera_log.SeqCoord'); R0=$(q 'SELECT count(*) FROM tessera_log.Seq'); B0=$(nbundles)
log "backlog: IntCoord $I0, SeqCoord.next $N0 ($((N0 - I0)) entries in $R0 Seq rows, ~$(( (N0 - 1) / 256 - I0 / 256 + 1 )) bundles); bundle objects in bucket: $B0; releasing the IntCoord lock"
# Integration attempts which timed out waiting for the lock have already halved the replica's
# adaptive Seq-row limit (6f2d29c) several times; FRESH=1 restarts the replica first, so that the
# aborted attempt and its retries start from the full DefaultIntegrationSizeLimit, as after a lost
# commit in production.
if [ "${FRESH:-0}" = 1 ]; then
  kill -9 "$RPID"; wait "$RPID" 2>/dev/null; echo "=== restart (FRESH=1) ===" >> "$OUT/replica-0.log"; start_replica
  log "restarted the replica (fresh integration limit) before releasing the lock; lock-wait timeouts before the restart: $(grep -c 'failed to read IntCoord: timeout' "$OUT/replica-0.log")"
fi
# IntCoord over time (epoch seconds, seq), to show the size of each successful integration.
( while kill -0 $CH 2>/dev/null; do echo "$(date +%s.%N) $(q 'SELECT seq FROM tessera_log.IntCoord')"; sleep 0.2; done ) > "$OUT/intcoord.log" 2>/dev/null & PIDS+=($!)
curl -s "http://127.0.0.1:$((BASE+3))/stats" > "$OUT/stats0"
puts() { curl -s -m 2 "http://127.0.0.1:$((BASE+3))/stats" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("op_PUT_entries_ifnonematch", 0))'; }
P0=$(puts); curl -s -m 2 "http://127.0.0.1:$((BASE+3))/ctl?write_latency=$WRITE_LATENCY" >/dev/null
psql -X -q "$ADMIN" -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'lockholder'" >/dev/null
# Wait until the backlog integration starts writing bundles (held WRITE_LATENCY by the proxy), then
# reset the replica's PG connections half-way through.
T0=$(date +%s.%N)
for _ in $(seq 1 1200); do [ "$(puts)" -gt "$P0" ] && break; sleep 0.02; done
HALF=$(python3 -c "import sys; d='$WRITE_LATENCY'; print(float(d[:-2])/2000 if d.endswith('ms') else float(d[:-1])/2)")
sleep "$HALF"; [ "${NOKILL:-0}" = 1 ] || curl -s -m 2 "http://127.0.0.1:$((BASE+1))/ctl?killall=1" >/dev/null; TK=$(date +%s.%N)
curl -s -m 2 "http://127.0.0.1:$((BASE+3))/ctl?write_latency=0s" >/dev/null
log "killed the replica's PG connections $(echo "$TK - $T0" | bc)s after releasing the lock, with its writes in flight; IntCoord $(q 'SELECT seq FROM tessera_log.IntCoord')"
sleep "$HALF"; sleep 0.3; IK=$(q 'SELECT seq FROM tessera_log.IntCoord')
AHEAD=$(python3 - "$BUCKET" "$I0" <<'EOF'
import sys, boto3, botocore
b, seq = sys.argv[1], int(sys.argv[2])
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))
n = seq // 256
def name(n):
    s = "%03d" % (n % 1000); n //= 1000
    while n: s = "x%03d/" % (n % 1000) + s; n //= 1000
    return "tile/entries/" + s
keys = [o["Key"] for p in s3.get_paginator("list_objects_v2").paginate(Bucket=b, Prefix=name(n)) for o in p.get("Contents", [])]
w = max([256 if k == name(n) else int(k.rsplit("/", 1)[1]) for k in keys if k == name(n) or k.startswith(name(n) + ".p/")] or [0])
print("yes (bucket has bundle %d up to width %d > %d)" % (n, w, seq % 256) if w > seq % 256 else "no (widest %d)" % w)
EOF
)
log "after the aborted attempt's writes landed: IntCoord $IK (unchanged: $([ "$IK" = "$I0" ] && echo yes || echo NO)); bucket ahead of IntCoord $I0 (=> the retry takes the slow path): $AHEAD"
for _ in $(seq 1 1200); do [ "$(q 'SELECT seq FROM tessera_log.IntCoord')" -gt "$I0" ] 2>/dev/null && break; sleep 0.1; done
TD=$(date +%s.%N); I1=$(q 'SELECT seq FROM tessera_log.IntCoord')
log "IntCoord $I0 -> $I1, $(echo "$TD - $TK" | bc)s after the kill"
# Caught up: everything sequenced before the lock was released is integrated.
for _ in $(seq 1 1200); do [ "$(q 'SELECT seq FROM tessera_log.IntCoord')" -ge "$N0" ] 2>/dev/null && break; sleep 0.1; done
TC=$(date +%s.%N); IC=$(q 'SELECT seq FROM tessera_log.IntCoord')
if [ "${IC:-0}" -ge "$N0" ] 2>/dev/null; then log "caught up (IntCoord $IC >= $N0) $(echo "$TC - $TK" | bc)s after the kill"
else log "NOT caught up: IntCoord $IC < $N0 $(echo "$TC - $TK" | bc)s after the kill"; fi
sleep 1; log "IntCoord steps after the kill (seconds after the kill: +entries): $(awk -v tk="$TK" -v tc="$TC" 'NF == 2 && $2 != "" { if (p != "" && $2 > p && $1 >= tk && $1 <= tc + 1) printf "%.1fs: +%d; ", $1 - tk, $2 - p; p = $2 }' "$OUT/intcoord.log")"
curl -s "http://127.0.0.1:$((BASE+3))/stats" > "$OUT/stats1"
wait $CH; CHRC=$?
log "chaos rc=$CHRC: $(tr -d '\n ' < "$OUT/chaos.json" | cut -c1-260)"
sleep 3; kill -TERM $CPW; wait $CPW
OBJ=$(python3 "$HERE/objcheck.py" "$BUCKET" check); FS=$("$BIN/fsck" --storage_url="$S3/$BUCKET/" --public_key="$OUT/log.pub" --ui=false -N 4 > "$OUT/fsck.log" 2>&1; echo $?)
TRIPS=$(grep -c 'coordination database is behind' "$OUT/replica-0.log"); H=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "http://127.0.0.1:$PORT/healthz")
log "integration errors (whole run): $(grep -c 'msg=integrateEntries' "$OUT/replica-0.log") ($(grep 'msg=integrateEntries' "$OUT/replica-0.log" | sed -E 's/.*error="//' | cut -c1-90 | sort | uniq -c | sort -rn | head -4 | tr '\n' ';'))"
# Failed integration attempts from the kill until caught up (the aborted attempt included): the
# retry should succeed at the first or second attempt.
ATT=$(python3 - "$OUT/replica-0.log" "$TK" "$TC" <<'EOF'
import sys, re, datetime
tk, tc = float(sys.argv[2]), float(sys.argv[3])
errs = []
for l in open(sys.argv[1]):
    m = re.match(r"time=(\S+) .*msg=integrateEntries", l)
    if m and tk - 0.5 <= datetime.datetime.fromisoformat(m.group(1)).timestamp() <= tc + 0.5:
        errs.append(re.sub(r'.*error="', "", l.strip())[:100])
print("%d failed [%s]" % (len(errs), "; ".join(errs[:4])))
EOF
)
log "integration attempts from the kill until caught up: $ATT"
log "S3 requests while the backlog was integrated: $(python3 -c "
import json; a=json.load(open('$OUT/stats0')); b=json.load(open('$OUT/stats1'))
print({k[3:]: b[k]-a.get(k,0) for k in sorted(b) if k.startswith('op_') and b[k]-a.get(k,0)})")"
log "trips=$TRIPS healthz=$H cpwatch=$(tr -d '\n ' < "$OUT/cpwatch.json" | cut -c1-120) objcheck=$OBJ fsck rc=$FS"
[ "$TRIPS" = 0 ] && [ "$CHRC" = 0 ] && [ "$FS" = 0 ] && [ "$H" = 200 ] && grep -q '"prefix_mismatches": 0' <<< "$OBJ" && grep -q '"regressions": 0' "$OUT/cpwatch.json" && { log "PASS"; exit 0; }
log "FAIL"; exit 1
