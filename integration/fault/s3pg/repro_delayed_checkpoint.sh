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

# Minimal reproduction: a DELAYED checkpoint PUT rolls the published checkpoint back.
#
# One replica, no database faults. The S3 proxy delays exactly one checkpoint PUT: the client is
# held until its publication timeout (5s) expires, and the request is forwarded to S3 8-9s after
# that. Meanwhile the replica retries and publishes a larger checkpoint. Writes then stop, the
# delayed PUT lands and overwrites the newer checkpoint with the older one, and because
# PubCoord.size already equals IntCoord.seq, nothing republishes until the republish interval
# (10 minutes by default): the published checkpoint stays rolled back.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
BIN=${BIN:-/home/claude/tools/faults/bin}; PGPORT=${PGPORT:-5530}; SLOT=${SLOT:-4}
OUT=${OUT:-/home/claude/tools/faults/out/repro-delayed-checkpoint}; mkdir -p "$OUT" && rm -rf "${OUT:?}"/*
S3=http://127.0.0.1:9000; SUF=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n'); DB=s3pgf_$SUF; BUCKET=s3pgf-$SUF
DIRECTDB="postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable"
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
BASE=$((7000 + 20*SLOT)); PORT=$((3300 + 10*SLOT))
log() { echo "[$(date -u +%T.%3N)] $*" | tee -a "$OUT/events.log"; }
cpsize() { curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 2p; }
s3b() { python3 - "$BUCKET" "$1" <<'EOF'
import sys, json, boto3, botocore
b, op = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))
if op == "create":
    s3.create_bucket(Bucket=b)
    s3.put_bucket_policy(Bucket=b, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::%s/*" % b]}]}))
else:
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs: s3.delete_objects(Bucket=b, Delete={"Objects": objs, "Quiet": True})
    s3.delete_bucket(Bucket=b)
EOF
}
PIDS=()
cleanup() { for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done; wait 2>/dev/null; psql -X -q "postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable" -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)"; s3b delete; }
trap cleanup EXIT
s3b create; psql -X -q "postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable" -c "CREATE DATABASE $DB"; psql -X -q "$DIRECTDB" -c "CREATE SCHEMA tessera_log"
"$BIN/s3proxy" -listen 127.0.0.1:$((BASE+2)) -ctl 127.0.0.1:$((BASE+3)) -faults=false -seed 1 -only_prefix=checkpoint -w_delayed=1 -stall=30s -delay_min=8s -delay_max=9s -log "$OUT/s3proxy.log" & PIDS+=($!)
TESSERA_PG_DSN="$DIRECTDB&search_path=tessera_log" TESSERA_S3_ACCESS_KEY=test TESSERA_S3_SECRET=test TESSERA_SIGNER="$KEY" \
  "$BIN/conformance-s3pg" --bucket="$BUCKET" --s3_endpoint=http://127.0.0.1:$((BASE+2)) --listen=127.0.0.1:$PORT > "$OUT/replica.log" 2>&1 & PIDS+=($!)
"$BIN/cpwatch" --log_url="$S3/$BUCKET/" --public_key="$PUB" --out="$OUT/cpwatch.json" --events="$OUT/cpwatch.log" >/dev/null 2>&1 & CPW=$!; PIDS+=($CPW)
for _ in $(seq 60); do curl -sf -m 2 "http://127.0.0.1:$PORT/healthz" >/dev/null && break; sleep 0.5; done
( n=0; while [ ! -f "$OUT/stopw" ]; do curl -s -m 5 -o /dev/null -X POST --data "e-$n" "http://127.0.0.1:$PORT/add"; n=$((n+1)); sleep 0.05; done ) & PIDS+=($!)
sleep 5; log "published checkpoint: $(cpsize); delaying the next checkpoint PUT"
curl -s "http://127.0.0.1:$((BASE+3))/ctl?faults=on" > /dev/null
for _ in $(seq 100); do grep -q DELAYED "$OUT/s3proxy.log" && break; sleep 0.1; done
curl -s "http://127.0.0.1:$((BASE+3))/ctl?faults=off" > /dev/null; log "one checkpoint PUT is being delayed: $(grep -m1 DELAYED "$OUT/s3proxy.log")"
for _ in $(seq 150); do grep -q "Failed to execute checkpoint publisher job" "$OUT/replica.log" && break; sleep 0.1; done
log "replica saw its publication fail: $(grep -m1 'Failed to execute checkpoint publisher job' "$OUT/replica.log" | cut -c1-160)"
sleep 4.5; touch "$OUT/stopw"; sleep 1; log "writes stopped; published checkpoint: $(cpsize); IntCoord: $(psql -X -tA "$DIRECTDB" -c 'SELECT seq FROM tessera_log.IntCoord')"
sleep 10; log "$(grep 'DELAYED write forwarded' "$OUT/s3proxy.log" | cut -c1-140)"
for i in 1 2 3 4 5 6; do log "published checkpoint: $(cpsize)  IntCoord: $(psql -X -tA "$DIRECTDB" -c 'SELECT seq FROM tessera_log.IntCoord') PubCoord.size: $(psql -X -tA "$DIRECTDB" -c 'SELECT size FROM tessera_log.PubCoord')"; sleep 10; done
# Liveness: the log must keep publishing after the fault (no wedge, no guard trip).
C0=$(cpsize); for i in $(seq 1 50); do curl -s -m 5 -o /dev/null -X POST --data "after-$SUF-$i" "http://127.0.0.1:$PORT/add"; done
for _ in $(seq 30); do [ "$(cpsize)" -gt "$C0" ] && break; sleep 0.5; done; C1=$(cpsize)
H=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "http://127.0.0.1:$PORT/healthz")
log "liveness: checkpoint $C0 -> $C1 after 50 more entries, /healthz=$H; checkpoint precondition failures: $(grep -c 'changed concurrently' "$OUT/replica.log"); guard trips: $(grep -c 'msg="Coordination database is behind' "$OUT/replica.log"); 412s on checkpoint PUTs at S3: $(grep -c 'checkpoint.*upstream 412\|DELAYED write forwarded now: upstream status 412' "$OUT/s3proxy.log")"
kill -TERM $CPW; wait $CPW; log "cpwatch: $(tr -d '\n' < "$OUT/cpwatch.json")"
grep -q '"regressions": 0' "$OUT/cpwatch.json" || { log "REPRODUCED: published checkpoint rolled back"; exit 1; }
[ "$C1" -gt "$C0" ] && [ "$H" = 200 ] || { log "LIVENESS FAILURE: publication stuck or replica unhealthy after the delayed write"; exit 2; }
log "no regression reproduced, log still live"; exit 0
