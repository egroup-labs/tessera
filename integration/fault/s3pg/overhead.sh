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

# Steady-state cost of integration and publication, without faults: REPLICAS conformance servers
# (binary CONF, e.g. an older build for comparison) talk to S3 through s3proxy, which only counts
# requests by operation/class (and adds S3_LATENCY to every request, to emulate a remote object
# store); PostgreSQL is direct. The chaos writer (WRITERS) writes for SECS while latprobe measures
# ack / integration / publication latency. Reports acked entries/s, integrations (IntCoord
# updates), entries per integration, and LIST/GET/PUT requests per integration and per publication
# (counted over the whole run, including the drain, so that requests and IntCoord updates match;
# the LIST counts include garbage collection, also reported separately as LIST_gc).
#
# Environment: CONF, REPLICAS (1), SECS (60), WRITERS (16), S3_LATENCY (0s), SLOT (2), PGPORT,
#   PUBLISH_INTERVAL (1s), NAME, OUT, BIN.
set -uo pipefail
HERE=${HERE:-$(cd "$(dirname "$0")" && pwd)}
BIN=${BIN:-/home/claude/tools/faults/bin}; CONF=${CONF:-$BIN/conformance-s3pg}; PGPORT=${PGPORT:-5530}
REPLICAS=${REPLICAS:-1}; SECS=${SECS:-60}; WRITERS=${WRITERS:-16}; S3_LATENCY=${S3_LATENCY:-0s}; SLOT=${SLOT:-2}
PUBLISH_INTERVAL=${PUBLISH_INTERVAL:-1s}; NAME=${NAME:-overhead}
OUT=${OUT:-/home/claude/tools/faults/out/$NAME}; mkdir -p "$OUT" && rm -rf "${OUT:?}"/*
S3=http://127.0.0.1:9000; SUF=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n'); DB=s3pgf_$SUF; BUCKET=s3pgf-$SUF
ADMIN="postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable"; DIRECTDB="postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable"
DIRECT="$DIRECTDB&search_path=tessera_log"
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
BASE=$((7000 + 20*SLOT)); CBASE=$((3300 + 10*SLOT))
PIDS=()
cleanup() {
  for p in "${PIDS[@]}"; do kill -9 "$p" 2>/dev/null; done; wait 2>/dev/null
  psql -X -q "$ADMIN" -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)"; python3 "$HERE/objcheck.py" "$BUCKET" delete-bucket >/dev/null
}
trap cleanup EXIT
python3 "$HERE/objcheck.py" "$BUCKET" create-bucket || exit 1
psql -X -q "$ADMIN" -c "CREATE DATABASE $DB" && psql -X -q "$DIRECTDB" -c "CREATE SCHEMA tessera_log" -c "SET search_path TO tessera_log" -f "$HERE/../../../storage/s3pg/schema.sql" || exit 1
URLS=""
for r in $(seq 0 $((REPLICAS-1))); do
  "$BIN/s3proxy" -listen 127.0.0.1:$((BASE+4*r+2)) -ctl 127.0.0.1:$((BASE+4*r+3)) -faults=false -latency="$S3_LATENCY" -log "$OUT/s3proxy-$r.log" & PIDS+=($!)
done
sleep 0.5
for r in $(seq 0 $((REPLICAS-1))); do
  TESSERA_PG_DSN="$DIRECT" TESSERA_S3_ACCESS_KEY=test TESSERA_S3_SECRET=test TESSERA_SIGNER="$KEY" \
    "$CONF" --bucket="$BUCKET" --s3_endpoint=http://127.0.0.1:$((BASE+4*r+2)) --skip_schema_init --publish_interval="$PUBLISH_INTERVAL" \
    --listen=127.0.0.1:$((CBASE+r)) --pg_max_conns=16 --pg_max_idle_conns=4 > "$OUT/replica-$r.log" 2>&1 & PIDS+=($!)
  URLS="$URLS,http://127.0.0.1:$((CBASE+r))"
done
for r in $(seq 0 $((REPLICAS-1))); do for _ in $(seq 60); do curl -sf -m 2 "http://127.0.0.1:$((CBASE+r))/healthz" >/dev/null && break; sleep 0.5; done; done
stats() { for r in $(seq 0 $((REPLICAS-1))); do curl -s -m 2 "http://127.0.0.1:$((BASE+4*r+3))/stats"; echo; done; }
dbstat() { psql -X -tA "$DIRECTDB" -c "SELECT pg_stat_clear_snapshot()" -c "SELECT (SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname='intcoord' AND schemaname='tessera_log')||' '||(SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname='pubcoord' AND schemaname='tessera_log')||' '||(SELECT seq FROM tessera_log.IntCoord)" | tail -1; }
sleep 2; stats > "$OUT/stats0"; D0=$(dbstat); T0=$(date +%s.%N)
"$BIN/latprobe" --add_url="http://127.0.0.1:$CBASE" --checkpoint_url="$S3/$BUCKET/checkpoint" --dsn="$DIRECT" --duration="${SECS}s" --out="$OUT/latprobe.json" > /dev/null 2>&1 & LP=$!; PIDS+=($LP)
"$BIN/chaos" --write_urls="${URLS#,}" --log_url="$S3/$BUCKET/" --public_key="$PUB" --writers="$WRITERS" --duration="${SECS}s" --settle=2m --out="$OUT/chaos.json" > "$OUT/chaos.log" 2>&1 & CH=$!; PIDS+=($CH)
sleep "$SECS"; T1=$(date +%s.%N)
wait $LP; wait $CH; CHRC=$?
# Request and IntCoord-update counts over the whole run, once everything is integrated, published and
# PostgreSQL's statistics have been flushed.
sleep 12; stats > "$OUT/stats1"; D1=$(dbstat)
python3 - "$OUT" "$NAME" "$REPLICAS" "$S3_LATENCY" "$CHRC" "$D0" "$D1" "$T0" "$T1" <<'EOF' | tee "$OUT/result.json"
import json, sys
out, name, n, lat, chrc = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4], int(sys.argv[5])
d0, d1 = [int(x) for x in sys.argv[6].split()], [int(x) for x in sys.argv[7].split()]
secs = float(sys.argv[9]) - float(sys.argv[8])
def tot(f):
    t = {}
    for l in open(out + "/" + f):
        if l.strip():
            for k, v in json.loads(l).items(): t[k] = t.get(k, 0) + v
    return t
s0, s1 = tot("stats0"), tot("stats1")
ops = {k[3:]: s1.get(k, 0) - s0.get(k, 0) for k in s1 if k.startswith("op_") and s1.get(k, 0) - s0.get(k, 0)}
integ, pubs, entries = d1[0] - d0[0], d1[1] - d0[1], d1[2] - d0[2]
def per(pred, d): return round(sum(v for k, v in ops.items() if pred(k)) / d, 2) if d else None
ch = json.load(open(out + "/chaos.json")) if chrc == 0 else {}
lp = json.load(open(out + "/latprobe.json"))
print(json.dumps({"name": name, "replicas": n, "s3_latency": lat, "secs": round(secs, 1), "chaos_rc": chrc, "acked": ch.get("acked"),
  "acked_per_s": round(ch.get("acked", 0) / secs, 1), "integrated_entries": entries,
  "integrations": integ, "entries_per_integration": round(entries / integ, 1) if integ else None, "publications": pubs,
  "per_integration": {"LIST": per(lambda k: k.startswith("LIST"), integ), "GET": per(lambda k: k.startswith("GET"), integ), "PUT": per(lambda k: k.startswith("PUT"), integ),
                      "LIST_entries": per(lambda k: k == "LIST_entries", integ), "LIST_tile": per(lambda k: k == "LIST_tile", integ),
                      "LIST_gc": per(lambda k: k.startswith("LIST") and k.endswith("_partials"), integ),
                      "GET_checkpoint": per(lambda k: k == "GET_checkpoint", integ), "GET_tile": per(lambda k: k == "GET_tile", integ), "GET_entries": per(lambda k: k == "GET_entries", integ),
                      "PUT_tile": per(lambda k: k.startswith("PUT_tile"), integ), "PUT_entries": per(lambda k: k.startswith("PUT_entries"), integ)},
  "per_publication": {"PUT_checkpoint": per(lambda k: k.startswith("PUT_checkpoint"), pubs)},
  "latency_s": lp, "ops": ops}))
EOF
