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

# Fault-injection run for the s3pg driver (see README.md). N conformance replicas share one log;
# each talks to PostgreSQL through its own tcpproxy and to S3 through its own s3proxy. The chaos
# writer (integration/chaos/s3pg) writes through the replicas while faults are injected; cpwatch
# and dbcheck -watch observe the published checkpoint and the coordination DB directly. After
# WRITE_SECS faults are switched off, and the run verifies: chaos (no lost/misplaced/duplicate
# acked entries, no fork), cpwatch (no regression/fork/inconsistency), settled DB invariants
# (dbcheck -settled, retried for up to SETTLE_SECS) and cmd/fsck.
#
# Environment (all optional):
#   SLOT        0..4: selects ports 7000+20*SLOT.. (proxies) and 3300+10*SLOT.. (replicas)
#   REPLICAS    number of replicas (default 2, max 3)
#   SEED        RNG seed for the proxies and the crash schedule (default 1)
#   WRITE_SECS  write duration (default 120); WRITERS (default 24)
#   PG_FAULTS   extra tcpproxy flags, e.g. "-reset_prob=0.2 -commit_ambig_prob=0.05 -blackhole_prob=0.02"
#   S3_FAULTS   extra s3proxy flags, e.g. "-w_ambig=0.3 -w_err=0.05"
#   CRASH=1     SIGKILL a random replica every 8-20s (supervisor restarts it)
#   PGKILL=1    kill -9 the postmaster of the cluster at PGPORT every 20-40s and restart it (needs PGDATA)
#   S3PART=1    partition (blackhole) S3 for all replicas for 5-25s every 20-40s
#   PGPART=1    partition PostgreSQL for one random replica for 5-25s every 20-40s
#   PGPORT      PostgreSQL port (default 5530, superuser postgres, trust auth); PGDATA for PGKILL
#   SCHEMA=1    start the replicas with --pg_schema=tessera_log and no search_path in the DSN
#   CP_CHURN=s  a concurrent "publisher" rewrites the checkpoint every s seconds with the same
#               size/root but different bytes (adds/removes an extra signature line, like a
#               cosignature would), conditionally on its ETag: the replicas' If-Match writes then
#               fail with a precondition failure; publication must neither wedge nor trip
#   REPLICA_FLAGS extra conformance server flags (e.g. --publish_interval=1s)
#   OUT         output directory; KEEP=1 keeps DB/bucket even if all checks pass
#
# Besides the checks above, result.json reports guard activity from the replica logs (ErrCoordinationBehind
# trips by guard, checkpoint precondition failures), /healthz polled every second per replica (any
# non-200; 503s attributed to a tripped guard), the DB-behind invariant (dbcheck -watch with the
# checkpoint), S3 requests by operation/class (s3proxy /stats) and integrations (IntCoord updates).
set -uo pipefail
HERE=${HERE:-$(cd "$(dirname "$0")" && pwd)}
BIN=${BIN:-/home/claude/tools/faults/bin}
SLOT=${SLOT:-0}; REPLICAS=${REPLICAS:-2}; SEED=${SEED:-1}; WRITE_SECS=${WRITE_SECS:-120}; WRITERS=${WRITERS:-24}
PG_FAULTS=${PG_FAULTS:-}; S3_FAULTS=${S3_FAULTS:-}; CRASH=${CRASH:-0}; PGKILL=${PGKILL:-0}; S3PART=${S3PART:-0}; PGPART=${PGPART:-0}
PGPORT=${PGPORT:-5530}; PGDATA=${PGDATA:-/home/claude/tools/faults/pgdata}; PGBIN=${PGBIN:-/usr/lib/postgresql/16/bin}
SETTLE_SECS=${SETTLE_SECS:-120}; NAME=${NAME:-run}; SCHEMA=${SCHEMA:-0}; REPLICA_FLAGS=${REPLICA_FLAGS:-}; CP_CHURN=${CP_CHURN:-0}
OUT=${OUT:-/home/claude/tools/faults/out/$NAME-seed$SEED}
mkdir -p "$OUT" && rm -rf "${OUT:?}"/*
RANDOM=$SEED
S3=http://127.0.0.1:9000
SUF=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n')
DB=s3pgf_$SUF; ROLE=s3pgf_app_$SUF; PW=pw$SUF; BUCKET=s3pgf-$SUF
ADMIN="postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable"
DIRECT="postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable&search_path=tessera_log"
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
BASE=$((7000 + 20*SLOT)); CBASE=$((3300 + 10*SLOT))
pgp() { echo $((BASE + 4*$1)); }; pgc() { echo $((BASE + 4*$1 + 1)); }
s3p() { echo $((BASE + 4*$1 + 2)); }; s3c() { echo $((BASE + 4*$1 + 3)); }
log() { echo "[$(date -u +%T.%3N)] $*" | tee -a "$OUT/events.log"; }
psqla() { psql -X -q -v ON_ERROR_STOP=1 "$@"; }
s3py() { python3 - "$BUCKET" "$1" <<'EOF'
import sys, json, boto3, botocore
b, op = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test",
                  region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))
if op == "create":
    s3.create_bucket(Bucket=b)
    s3.put_bucket_policy(Bucket=b, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::%s/*" % b]}]}))
else:
    p = s3.get_paginator("list_objects_v2")
    for page in p.paginate(Bucket=b):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        for i in range(0, len(objs), 1000):
            s3.delete_objects(Bucket=b, Delete={"Objects": objs[i:i+1000], "Quiet": True})
    s3.delete_bucket(Bucket=b)
EOF
}

declare -a BGPIDS=()
cleanup() {
  touch "$OUT/stop"
  for p in "${BGPIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
  for r in $(seq 0 $((REPLICAS-1))); do [ -f "$OUT/pid.$r" ] && kill -9 "$(cat "$OUT/pid.$r")" 2>/dev/null; done
  sleep 1
  for r in $(seq 0 $((REPLICAS-1))); do [ -f "$OUT/pid.$r" ] && kill -9 "$(cat "$OUT/pid.$r")" 2>/dev/null; done
  wait 2>/dev/null
  if [ "${KEEP:-0}" != 1 ] && [ "${FAILED:-1}" = 0 ]; then
    psqla "$ADMIN" -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" -c "DROP ROLE IF EXISTS $ROLE" 2>/dev/null
    s3py delete 2>/dev/null
  else
    log "kept DB $DB (role $ROLE) and bucket $BUCKET for analysis"
    echo "$DB $ROLE $BUCKET" > "$OUT/kept"
  fi
}
trap cleanup EXIT

log "run $NAME seed=$SEED replicas=$REPLICAS slot=$SLOT db=$DB bucket=$BUCKET pg_faults='$PG_FAULTS' s3_faults='$S3_FAULTS' crash=$CRASH pgkill=$PGKILL s3part=$S3PART pgpart=$PGPART"
s3py create || exit 1
psqla "$ADMIN" -c "CREATE DATABASE $DB" -c "CREATE ROLE $ROLE LOGIN PASSWORD '$PW'" \
  -c "ALTER ROLE $ROLE SET idle_in_transaction_session_timeout = '60s'" -c "ALTER ROLE $ROLE SET statement_timeout = '60s'" \
  -c "ALTER ROLE $ROLE SET lock_timeout = '60s'" || exit 1
psqla "postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable" -c "CREATE SCHEMA tessera_log" -c "SET search_path TO tessera_log" \
  -f "$HERE/../../../storage/s3pg/schema.sql" -c "GRANT USAGE ON SCHEMA tessera_log TO $ROLE" \
  -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA tessera_log TO $ROLE" || exit 1
printf '%s' "$PUB" > "$OUT/log.pub"

for r in $(seq 0 $((REPLICAS-1))); do
  "$BIN/tcpproxy" -listen 127.0.0.1:$(pgp $r) -ctl 127.0.0.1:$(pgc $r) -upstream 127.0.0.1:$PGPORT -seed $((SEED*10+r)) \
    -faults=false -log "$OUT/pgproxy-$r.log" $PG_FAULTS 2>>"$OUT/pgproxy-$r.log" & BGPIDS+=($!)
  "$BIN/s3proxy" -listen 127.0.0.1:$(s3p $r) -ctl 127.0.0.1:$(s3c $r) -upstream $S3 -seed $((SEED*10+5+r)) \
    -faults=false -log "$OUT/s3proxy-$r.log" $S3_FAULTS 2>>"$OUT/s3proxy-$r.log" & BGPIDS+=($!)
done
sleep 0.5

if [ "$SCHEMA" = 1 ]; then SP=""; REPLICA_FLAGS="--pg_schema=tessera_log $REPLICA_FLAGS"; else SP="&search_path=tessera_log"; fi
supervise() {
  local r=$1
  while [ ! -f "$OUT/stop" ]; do
    TESSERA_PG_DSN="postgres://$ROLE:$PW@127.0.0.1:$(pgp $r)/$DB?sslmode=disable$SP&application_name=replica-$r&connect_timeout=5" \
    TESSERA_S3_ACCESS_KEY=test TESSERA_S3_SECRET=test TESSERA_SIGNER="$KEY" \
      "$BIN/conformance-s3pg" --bucket="$BUCKET" --s3_endpoint=http://127.0.0.1:$(s3p $r) --skip_schema_init $REPLICA_FLAGS \
      --listen=127.0.0.1:$((CBASE+r)) --pg_max_conns=16 --pg_max_idle_conns=4 >> "$OUT/replica-$r.log" 2>&1 &
    local pid=$!
    echo $pid > "$OUT/pid.$r"
    echo "[$(date -u +%T.%3N)] replica $r started pid $pid" >> "$OUT/events.log"
    wait $pid; local rc=$?
    echo "[$(date -u +%T.%3N)] replica $r pid $pid exited rc=$rc" >> "$OUT/events.log"
    sleep 1
  done
}
for r in $(seq 0 $((REPLICAS-1))); do supervise $r & BGPIDS+=($!); done
for r in $(seq 0 $((REPLICAS-1))); do
  for _ in $(seq 1 120); do curl -sf -m 2 "http://127.0.0.1:$((CBASE+r))/healthz" >/dev/null && break; sleep 0.5; done
done
URLS=$(for r in $(seq 0 $((REPLICAS-1))); do printf 'http://127.0.0.1:%d,' $((CBASE+r)); done); URLS=${URLS%,}
# /healthz of every replica, once a second (time code).
for r in $(seq 0 $((REPLICAS-1))); do
  ( while [ ! -f "$OUT/stop" ]; do echo "$(date -u +%T.%1N) $(curl -s -o /dev/null -m 3 -w '%{http_code}' "http://127.0.0.1:$((CBASE+r))/healthz")" >> "$OUT/healthz-$r.log"; sleep 1; done ) & BGPIDS+=($!)
done

"$BIN/cpwatch" --log_url="$S3/$BUCKET/" --public_key="$PUB" --out="$OUT/cpwatch.json" --events="$OUT/cpwatch.log" >/dev/null 2>&1 & CPW=$!; BGPIDS+=($CPW)
"$BIN/dbcheck" --dsn="$DIRECT" --checkpoint_url="$S3/$BUCKET/checkpoint" --watch=500ms --out="$OUT/dbwatch.json" --events="$OUT/dbwatch.log" >/dev/null 2>&1 & DBW=$!; BGPIDS+=($DBW)
"$BIN/chaos" --write_urls="$URLS" --log_url="$S3/$BUCKET/" --public_key="$PUB" --writers="$WRITERS" \
  --duration="${WRITE_SECS}s" --settle=4m --out="$OUT/chaos.json" > "$OUT/chaos.log" 2>&1 & CHAOS=$!; BGPIDS+=($CHAOS)
log "chaos writer started (pid $CHAOS)"

ctl_all() { for r in $(seq 0 $((REPLICAS-1))); do curl -s -m 2 "http://127.0.0.1:$(pgc $r)/ctl?$1" >/dev/null; curl -s -m 2 "http://127.0.0.1:$(s3c $r)/ctl?$1" >/dev/null; done; }
ctl_all "faults=on"; log "faults ON"
if [ "$CP_CHURN" != 0 ]; then
  python3 - "$BUCKET" "$CP_CHURN" "$OUT" <<'EOF' > "$OUT/cpchurn.json" 2>"$OUT/cpchurn.err" & BGPIDS+=($!)
import sys, os, time, json, base64, boto3, botocore
b, every, out = sys.argv[1], float(sys.argv[2]), sys.argv[3]
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}, retries={"max_attempts": 1}))
extra = "\u2014 churn.example/cosigner " + base64.b64encode(os.urandom(68)).decode() + "\n"
n = {"writes": 0, "precondition_failed": 0, "errors": 0}
while not os.path.exists(out + "/stop") and not os.path.exists(out + "/churn.stop"):
    try:
        r = s3.get_object(Bucket=b, Key="checkpoint"); cp, etag = r["Body"].read().decode(), r["ETag"]
        cp = cp.replace(extra, "") if extra in cp else cp + extra
        s3.put_object(Bucket=b, Key="checkpoint", Body=cp.encode(), IfMatch=etag, ContentType="text/plain; charset=utf-8", CacheControl="no-cache")
        n["writes"] += 1
    except botocore.exceptions.ClientError as e:
        k = "precondition_failed" if e.response["Error"]["Code"] in ("PreconditionFailed", "ConditionalRequestConflict") else "errors"
        n[k] += 1
    except Exception:
        n["errors"] += 1
    time.sleep(every)
print(json.dumps(n))
EOF
  log "checkpoint churn every ${CP_CHURN}s started"
fi

# Failure schedule (seeded by SEED via $RANDOM).
END=$((SECONDS + WRITE_SECS)); next_crash=$((SECONDS + 8 + RANDOM % 12)); next_pgkill=$((SECONDS + 20 + RANDOM % 20))
next_s3part=$((SECONDS + 20 + RANDOM % 20)); next_pgpart=$((SECONDS + 20 + RANDOM % 20)); crashes=0; pgkills=0; s3parts=0; pgparts=0
s3part_end=0; pgpart_end=0; pgpart_r=0
while [ $SECONDS -lt $END ]; do
  sleep 1
  if [ "$CRASH" = 1 ] && [ $SECONDS -ge $next_crash ]; then
    r=$((RANDOM % REPLICAS)); pid=$(cat "$OUT/pid.$r"); log "SIGKILL replica $r (pid $pid)"; kill -9 "$pid" 2>/dev/null
    crashes=$((crashes+1)); next_crash=$((SECONDS + 8 + RANDOM % 12))
  fi
  if [ "$PGKILL" = 1 ] && [ $SECONDS -ge $next_pgkill ]; then
    pm=$(head -1 "$PGDATA/postmaster.pid"); log "kill -9 postmaster $pm"; kill -9 "$pm"; sleep $((1 + RANDOM % 3))
    # Surviving backends exit once they notice the postmaster died; retry until the restart succeeds.
    for _ in $(seq 1 20); do runuser -u postgres -- "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/../pg.log" -w start >/dev/null 2>&1 && break; sleep 1; done
    log "postgres restarted (crash recovery): $(pg_isready -h 127.0.0.1 -p $PGPORT)"
    pgkills=$((pgkills+1)); next_pgkill=$((SECONDS + 20 + RANDOM % 20))
  fi
  if [ "$S3PART" = 1 ]; then
    if [ $s3part_end -gt 0 ] && [ $SECONDS -ge $s3part_end ]; then
      for r in $(seq 0 $((REPLICAS-1))); do curl -s -m 2 "http://127.0.0.1:$(s3c $r)/ctl?partition=off" >/dev/null; done; log "S3 partition healed"; s3part_end=0
    elif [ $s3part_end = 0 ] && [ $SECONDS -ge $next_s3part ]; then
      d=$((5 + RANDOM % 21)); for r in $(seq 0 $((REPLICAS-1))); do curl -s -m 2 "http://127.0.0.1:$(s3c $r)/ctl?partition=on" >/dev/null; done
      log "S3 partitioned (all replicas) for ${d}s"; s3part_end=$((SECONDS + d)); s3parts=$((s3parts+1)); next_s3part=$((SECONDS + d + 20 + RANDOM % 20))
    fi
  fi
  if [ "$PGPART" = 1 ]; then
    if [ $pgpart_end -gt 0 ] && [ $SECONDS -ge $pgpart_end ]; then
      curl -s -m 2 "http://127.0.0.1:$(pgc $pgpart_r)/ctl?partition=off" >/dev/null; log "PG partition of replica $pgpart_r healed"; pgpart_end=0
    elif [ $pgpart_end = 0 ] && [ $SECONDS -ge $next_pgpart ]; then
      d=$((5 + RANDOM % 21)); pgpart_r=$((RANDOM % REPLICAS)); curl -s -m 2 "http://127.0.0.1:$(pgc $pgpart_r)/ctl?partition=on" >/dev/null
      log "PG partitioned for replica $pgpart_r for ${d}s"; pgpart_end=$((SECONDS + d)); pgparts=$((pgparts+1)); next_pgpart=$((SECONDS + d + 20 + RANDOM % 20))
    fi
  fi
done
ctl_all "faults=off&partition=off"; touch "$OUT/churn.stop"; log "faults OFF, partitions healed; waiting for chaos verification"
pg_isready -q -h 127.0.0.1 -p $PGPORT || { runuser -u postgres -- "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/../pg.log" -w start >/dev/null; log "postgres started"; }

wait $CHAOS; CHAOS_RC=$?
log "chaos exit code $CHAOS_RC"
# Settled DB invariants (retry until the stragglers - sequenced but unacked entries - are integrated and published).
t0=$SECONDS; SETTLED_RC=1
while [ $((SECONDS - t0)) -lt $SETTLE_SECS ]; do
  "$BIN/dbcheck" --dsn="$DIRECT" --settled --checkpoint_url="$S3/$BUCKET/checkpoint" --out="$OUT/dbsettled.json" >/dev/null 2>&1 && { SETTLED_RC=0; break; }
  sleep 2
done
SETTLE_TIME=$((SECONDS - t0))
log "dbcheck settled rc=$SETTLED_RC after ${SETTLE_TIME}s"
"$BIN/fsck" --storage_url="$S3/$BUCKET/" --public_key="$OUT/log.pub" --ui=false -N 4 > "$OUT/fsck.log" 2>&1; FSCK_RC=$?
log "fsck rc=$FSCK_RC"
kill -TERM $CPW $DBW; wait $CPW; CPW_RC=$?; wait $DBW; DBW_RC=$?
touch "$OUT/stop"
psql -X -tA "postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable" -c "SELECT n_tup_upd FROM pg_stat_user_tables WHERE relname='intcoord' AND schemaname='tessera_log'" > "$OUT/intcoord_updates" 2>"$OUT/intcoord_updates.err"
for r in $(seq 0 $((REPLICAS-1))); do
  curl -s -m 2 "http://127.0.0.1:$(pgc $r)/stats" > "$OUT/pgproxy-$r.stats"; curl -s -m 2 "http://127.0.0.1:$(s3c $r)/stats" > "$OUT/s3proxy-$r.stats"
done
python3 - "$OUT" "$NAME" "$SEED" "$REPLICAS" "$CHAOS_RC" "$SETTLED_RC" "$SETTLE_TIME" "$FSCK_RC" "$CPW_RC" "$DBW_RC" "$crashes" "$pgkills" "$s3parts" "$pgparts" <<'EOF' | tee "$OUT/result.json"
import sys, json, glob, os, re
out, name, seed, n = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
rc = dict(zip(["chaos", "settled", "settle_time", "fsck", "cpwatch", "dbwatch", "crashes", "pgkills", "s3parts", "pgparts"], map(int, sys.argv[5:])))
def load(p):
    try: return json.load(open(os.path.join(out, p)))
    except Exception: return {}
ch, cw, dw, ds = load("chaos.json"), load("cpwatch.json"), load("dbwatch.json"), load("dbsettled.json")
churn = load("cpchurn.json")
pg, s3 = {}, {}
for r in range(n):
    for d, f in ((pg, "pgproxy-%d.stats" % r), (s3, "s3proxy-%d.stats" % r)):
        for k, v in load(f).items(): d[k] = d.get(k, 0) + v
errs = {}
health = {"polls": 0, "non200": 0, "replicas_non200": [], "replicas_tripped": []}
for r in range(n):
    try: txt = open(os.path.join(out, "replica-%d.log" % r)).read()
    except Exception: txt = ""
    lines = txt.splitlines()
    # The transient "failed to re-read integrated size to confirm (<guard error>): <db error>" quotes the
    # guard's text without tripping (guard.go, confirmBehind): not counted as a guard error.
    cb = [l for l in lines if "coordination database is behind" in l and "failed to re-read integrated size" not in l]
    for k, v in (("error_lines", sum("level=ERROR" in l for l in lines)),
                 ("differs_written", sum("differs from data to-be-written" in l for l in lines)),
                 ("integrity_fail", txt.count("integrity fail")),
                 # ErrCoordinationBehind, by the guard that produced it (each line is one failed attempt
                 # or health check; the first one trips the replica's sticky flag).
                 ("coord_behind_lines", len(cb)),
                 ("trip_integration", sum("refusing to integrate" in l or "differs from the entries being integrated" in l for l in cb)),
                 ("trip_publication", sum("refusing to publish" in l or "is not consistent with published" in l for l in cb)),
                 ("trip_startup", sum("Failed to create new appender" in l for l in cb)),
                 ("trip_healthz_503", sum("healthz: coordination database is behind" in l for l in lines)),
                 # Checkpoint writes refused because the checkpoint changed since it was read (If-Match).
                 ("cp_precondition_failed", sum("changed concurrently" in l for l in lines)),
                 ("confirm_reread_failed", sum("failed to re-read integrated size" in l or "could not confirm a suspected coordination lag" in l for l in lines)),
                 ("stale_view", sum("coordination state moved on" in l for l in lines)),
                 ("trips", sum('msg="Coordination database is behind' in l for l in lines)),
                 ("startup_fail", txt.count("Failed to create new appender")), ("panic", txt.count("panic:"))):
        errs[k] = errs.get(k, 0) + v
    if cb:
        errs.setdefault("first_trip", []).append("replica %d: %s" % (r, cb[0][:400]))
    try: hl = open(os.path.join(out, "healthz-%d.log" % r)).read().split()[1::2]
    except Exception: hl = []
    bad = [c for c in hl if c != "200"]
    health["polls"] += len(hl); health["non200"] += len(bad)
    if bad: health["replicas_non200"].append(r)
    if any("healthz: coordination database is behind" in l for l in lines): health["replicas_tripped"].append(r)
ops = {k[3:]: v for k, v in s3.items() if k.startswith("op_")}
try: integrations = int(open(os.path.join(out, "intcoord_updates")).read().strip() or 0)
except Exception: integrations = 0
def per(pred): return round(sum(v for k, v in ops.items() if pred(k)) / integrations, 2) if integrations else None
s3req = {"integrations": integrations, "total": sum(ops.values()),
         "LIST_per_integration": per(lambda k: k.startswith("LIST")), "GET_per_integration": per(lambda k: k.startswith("GET")),
         "PUT_per_integration": per(lambda k: k.startswith("PUT")), "by_op": ops,
         "412": {k: v for k, v in s3.items() if k.startswith("412_")}}
s3 = {k: v for k, v in s3.items() if not k.startswith("op_") and not k.startswith("412_")}
res = {"name": name, "seed": seed, "replicas": n, "acked": ch.get("acked"), "write_errors": ch.get("write_errors"), "final_size": ch.get("final_size"),
       "lost": ch.get("lost"), "wrong_index": ch.get("wrong_index"), "dups": ch.get("duplicates"), "chaos_forks": ch.get("forks"),
       "unacked_present": ch.get("unacked_but_present"), "chaos_problems": ch.get("problems"),
       "cp_distinct": cw.get("distinct_checkpoints"), "cp_regressions": cw.get("regressions"), "cp_forks": cw.get("forks"), "cp_inconsistent": cw.get("inconsistent"),
       "db_watch_violations": dw.get("violations"), "settled_violations": ds.get("violations"), "settled_state": ds.get("last_state"),
       "pg_faults": {k: pg.get(k, 0) for k in ("conns", "resets", "ambiguous_commits", "commits", "blackholes", "partitioned_conns")},
       "s3_faults": s3, "replica_log": errs, "healthz": health, "s3_requests": s3req, **({"cp_churn": churn} if churn else {}), **rc}
res["PASS"] = rc["chaos"] == 0 and rc["settled"] == 0 and rc["fsck"] == 0 and rc["cpwatch"] == 0 and rc["dbwatch"] == 0
res["NO_TRIPS"] = errs.get("coord_behind_lines", 0) == 0
print(json.dumps(res))
EOF
grep -q '"PASS": true' "$OUT/result.json" && FAILED=0 || FAILED=1
log "result: $(grep -o '"PASS": [a-z]*' "$OUT/result.json")"
exit $FAILED
