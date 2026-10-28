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

# Reproduction: published-checkpoint rollback WITHOUT any late-delivered request.
#
# Replica 0's checkpoint PUTs take 2s (slow object storage; well within the 5s publication
# timeout, the client is still waiting when S3 applies them) and its PostgreSQL connections are
# reset every ~0.7s. When a reset hits while replica 0 holds the PubCoord row lock and its PUT is in
# flight, PostgreSQL aborts the transaction and releases the lock; replica 1 publishes a newer
# checkpoint, and then replica 0's (older) PUT completes and overwrites it.
set -uo pipefail
HERE=${HERE:-$(cd "$(dirname "$0")" && pwd)}
BIN=${BIN:-/home/claude/tools/faults/bin}; PGPORT=${PGPORT:-5530}; SLOT=${SLOT:-0}; SECS=${SECS:-60}
OUT=${OUT:-/home/claude/tools/faults/out/repro-lockloss-checkpoint}; mkdir -p "$OUT" && rm -rf "${OUT:?}"/*
S3=http://127.0.0.1:9000; SUF=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n'); DB=s3pgf_$SUF; BUCKET=s3pgf-$SUF
ADMIN="postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable"; DIRECTDB="postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable"
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
BASE=$((7000 + 20*SLOT)); PORT=$((3300 + 10*SLOT))
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
cleanup() { for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done; wait 2>/dev/null; psql -X -q "$ADMIN" -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)"; s3b delete; }
trap cleanup EXIT
s3b create; psql -X -q "$ADMIN" -c "CREATE DATABASE $DB"; psql -X -q "$DIRECTDB" -c "CREATE SCHEMA tessera_log"
psql -X -q "$DIRECTDB" -c "SET search_path TO tessera_log" -f "$HERE/../../../storage/s3pg/schema.sql" || exit 1
"$BIN/tcpproxy" -listen 127.0.0.1:$BASE -ctl 127.0.0.1:$((BASE+1)) -upstream 127.0.0.1:$PGPORT -faults=false -log "$OUT/pgproxy.log" & PIDS+=($!)
"$BIN/s3proxy" -listen 127.0.0.1:$((BASE+2)) -ctl 127.0.0.1:$((BASE+3)) -faults=false -only_prefix=checkpoint -latency_filtered -latency=2s -log "$OUT/s3proxy.log" & PIDS+=($!)
sleep 0.5
start() { # $1 replica, $2 pg addr, $3 s3 endpoint
  TESSERA_PG_DSN="postgres://postgres@$2/$DB?sslmode=disable&search_path=tessera_log" TESSERA_S3_ACCESS_KEY=test TESSERA_S3_SECRET=test TESSERA_SIGNER="$KEY" \
    "$BIN/conformance-s3pg" --bucket="$BUCKET" --s3_endpoint="$3" --skip_schema_init --listen=127.0.0.1:$((PORT+$1)) >> "$OUT/replica-$1.log" 2>&1 & PIDS+=($!); }
start 0 127.0.0.1:$BASE http://127.0.0.1:$((BASE+2)); start 1 127.0.0.1:$PGPORT $S3
for r in 0 1; do for _ in $(seq 60); do curl -sf -m 2 "http://127.0.0.1:$((PORT+r))/healthz" >/dev/null && break; sleep 0.5; done; done
"$BIN/cpwatch" --log_url="$S3/$BUCKET/" --public_key="$PUB" --out="$OUT/cpwatch.json" --events="$OUT/cpwatch.log" >/dev/null 2>&1 & CPW=$!; PIDS+=($CPW)
"$BIN/chaos" --write_urls=http://127.0.0.1:$PORT,http://127.0.0.1:$((PORT+1)) --log_url="$S3/$BUCKET/" --public_key="$PUB" --writers=8 --duration=${SECS}s --settle=2m --out="$OUT/chaos.json" > "$OUT/chaos.log" 2>&1 & CH=$!; PIDS+=($CH)
end=$((SECONDS + SECS)); while [ $SECONDS -lt $end ]; do sleep 0.7; curl -s -m 2 "http://127.0.0.1:$((BASE+1))/ctl?killall=1" >/dev/null; done
wait $CH; CHRC=$?; echo "chaos rc=$CHRC: $(tr -d '\n ' < "$OUT/chaos.json" | cut -c1-200)"
# Liveness after the faults: both replicas healthy, and the checkpoint keeps growing.
C0=$(curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 2p)
for i in $(seq 1 40); do curl -s -m 5 -o /dev/null -X POST --data "after-$SUF-$i" "http://127.0.0.1:$((PORT + i % 2))/add"; done
for _ in $(seq 30); do [ "$(curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 2p)" -gt "$C0" ] && break; sleep 0.5; done; C1=$(curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 2p)
H0=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "http://127.0.0.1:$PORT/healthz"); H1=$(curl -s -o /dev/null -w '%{http_code}' -m 6 "http://127.0.0.1:$((PORT+1))/healthz")
sleep 2; kill -TERM $CPW; wait $CPW
echo "cpwatch: $(tr -d '\n' < "$OUT/cpwatch.json")"
echo "replica 0 publication failures: $(grep -c 'Failed to execute checkpoint publisher job' "$OUT/replica-0.log"), checkpoint precondition failures (both): $(cat "$OUT"/replica-*.log | grep -c 'changed concurrently'), guard trips (both): $(cat "$OUT"/replica-*.log | grep -c 'msg="Coordination database is behind'), errStaleView (both): $(cat "$OUT"/replica-*.log | grep -c 'coordination state moved on'), tripped marker: $(python3 "$HERE/objcheck.py" "$BUCKET" marker | cut -c1-120)"
echo "liveness: checkpoint $C0 -> $C1, /healthz $H0 $H1"
cat "$OUT"/replica-*.log | grep -m3 'coordination database is behind' | cut -c1-300
grep -q '"regressions": 0' "$OUT/cpwatch.json" || { echo "REPRODUCED: published checkpoint rolled back"; exit 1; }
[ "$C1" -gt "$C0" ] && [ "$H0" = 200 ] && [ "$H1" = 200 ] && [ "$CHRC" = 0 ] || { echo "LIVENESS/HEALTH FAILURE after the faults"; exit 2; }
echo "no regression reproduced, log still live"; exit 0
