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

# Scenario D: the coordination database loses commits while the bucket keeps the newer state
# (asynchronous-standby failover, restore from an old backup). See README.md.
#
# The 6 coordination tables are snapshotted at a chosen point (SNAP=sql: consistent copy into a
# side schema inside one REPEATABLE READ transaction; SNAP=datadir: stop the cluster, copy its data
# directory, start it) and later restored while the replicas are running (SNAP=sql: one transaction
# under ACCESS EXCLUSIVE locks; SNAP=datadir: stop, copy back, start). cpwatch verifies that the
# published checkpoint never regresses or forks; objcheck verifies object-level consistency and
# lists the objects written after the restore. Probe writers (one entry per replica every 50ms)
# and /healthz pollers record, per replica, when the guard tripped, when /healthz turned 503 and
# how many entries were still acknowledged after the restore and after the trip; the database
# timeline gives the number of indices handed out after the restore. Then all replicas are
# restarted (start-up guard, tripped marker `s3pg-coordination-tripped`) while the probes keep
# writing: acknowledgements, start-up refusals and indices handed out by the restarted processes
# are recorded separately. Finally (RECOVER=1) the runbook of storage/s3pg/README.md
# ("Recovering from a lost commit") is applied step by step: stop all writers, inspect + dump,
# fsck, quarantine and remove the objects beyond the published size, reset the coordination
# tables, verify, check that a writer still refuses to start while the tripped marker exists,
# delete the marker (last step), restart ONE writer and check that a larger checkpoint is
# published, then the other; a fresh chaos verification, dbcheck -settled, fsck, objcheck and a
# check that no marker was written again then run on top.
#
# MODE=behind     snapshot at WHEN=random|int_ahead|after_pub|mid_int, restore once the published
#                 checkpoint is beyond the snapshot's IntCoord.seq.
# MODE=seqintact  build a Seq backlog (S3 latency), stop writing, snapshot, let everything be
#                 integrated and published, restore: IntCoord < published but Seq rows intact.
# MODE=partial    publish_interval=20s; snapshot with IntCoord ahead of the published checkpoint,
#                 restore after integration advanced further but before the next publication:
#                 published < DB IntCoord < integrated in S3.
# SNAP=datadir stops the whole cluster: don't share PGPORT with other runs (use a separate cluster).
# SCHEMA=1 starts the replicas with --pg_schema (no search_path in the DSN).
set -uo pipefail
HERE=${HERE:-$(cd "$(dirname "$0")" && pwd)}
BIN=${BIN:-/home/claude/tools/faults/bin}
SLOT=${SLOT:-3}; SEED=${SEED:-1}; MODE=${MODE:-behind}; SNAP=${SNAP:-sql}; WHEN=${WHEN:-random}; WRITERS=${WRITERS:-16}
OBSERVE_SECS=${OBSERVE_SECS:-30}; RECOVER=${RECOVER:-1}
PGPORT=${PGPORT:-5530}; PGDATA=${PGDATA:-/home/claude/tools/faults/pgdata}; PGBIN=${PGBIN:-/usr/lib/postgresql/16/bin}
NAME=${NAME:-D-$MODE-$SNAP-$WHEN}; OUT=${OUT:-/home/claude/tools/faults/out/$NAME-seed$SEED}; SCHEMA=${SCHEMA:-0}
mkdir -p "$OUT" && rm -rf "${OUT:?}"/*
RANDOM=$SEED; REPLICAS=2
S3=http://127.0.0.1:9000
SUF=$(head -c4 /dev/urandom | od -An -tx1 | tr -d ' \n')
DB=s3pgf_$SUF; ROLE=s3pgf_app_$SUF; PW=pw$SUF; BUCKET=s3pgf-$SUF
ADMIN="postgres://postgres@127.0.0.1:$PGPORT/postgres?sslmode=disable"
DIRECTDB="postgres://postgres@127.0.0.1:$PGPORT/$DB?sslmode=disable"
DIRECT="$DIRECTDB&search_path=tessera_log"
KEY="PRIVATE+KEY+example.com/log/testdata+33d7b496+AeymY/SZAX0jZcJ8enZ5FY1Dz+wTML2yWSkK+9DSF3eg"
PUB="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
BASE=$((7000 + 20*SLOT)); CBASE=$((3300 + 10*SLOT))
REPLICA_FLAGS=${REPLICA_FLAGS:-}; [ "$MODE" = partial ] && REPLICA_FLAGS="--publish_interval=20s $REPLICA_FLAGS"
if [ "$SCHEMA" = 1 ]; then SP=""; REPLICA_FLAGS="--pg_schema=tessera_log $REPLICA_FLAGS"; else SP="&search_path=tessera_log"; fi
pgp() { echo $((BASE + 4*$1)); }; s3p() { echo $((BASE + 4*$1 + 2)); }; s3c() { echo $((BASE + 4*$1 + 3)); }
pgpart() { for r in 0 1; do curl -s -m 2 "http://127.0.0.1:$((BASE + 4*r + 1))/ctl?partition=$1" >/dev/null; done; }
log() { echo "[$(date -u +%T.%3N)] $*" | tee -a "$OUT/events.log"; }
q() { psql -X -q -tA "$DIRECTDB" -c "SET search_path TO tessera_log" -c "$1" 2>/dev/null; }
state() { q "SELECT 'next='||s.next||' int='||i.seq||' pub='||p.size||' gc='||g.fromSize||' seqrows='||(SELECT count(*) FROM Seq) FROM SeqCoord s, IntCoord i, PubCoord p, GCCoord g"; }
intseq() { q "SELECT seq FROM IntCoord"; }
nextidx() { q "SELECT next FROM SeqCoord"; }
cpsize() { curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 2p; }
cproot() { curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 3p | base64 -d | od -An -tx1 | tr -d ' \n'; }
waitfor() { local t=$1; shift; for _ in $(seq 1 $((t*5))); do eval "$*" && return 0; sleep 0.2; done; return 1; }

declare -a BGPIDS=()
cleanup() {
  touch "$OUT/stop"; rm -f "$OUT"/hold*
  for p in "${BGPIDS[@]}"; do kill -9 "$p" 2>/dev/null; done
  for _ in 1 2; do for r in 0 1; do [ -f "$OUT/pid.$r" ] && kill -9 "$(cat "$OUT/pid.$r")" 2>/dev/null; done; sleep 1; done
  wait 2>/dev/null
  pg_isready -q -h 127.0.0.1 -p $PGPORT || runuser -u postgres -- "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/../pg.log" -w start >/dev/null
  if [ "${KEEP:-0}" != 1 ]; then
    psql -X -q "$ADMIN" -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" -c "DROP ROLE IF EXISTS $ROLE" 2>/dev/null
    python3 - "$BUCKET" <<'EOF' 2>/dev/null
import sys, boto3, botocore
b = sys.argv[1]
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=b):
    objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    if objs: s3.delete_objects(Bucket=b, Delete={"Objects": objs, "Quiet": True})
s3.delete_bucket(Bucket=b)
EOF
  else
    log "kept DB $DB and bucket $BUCKET"
  fi
  rm -rf "$OUT/datadir-snap"
}
trap cleanup EXIT

log "lostcommit mode=$MODE snap=$SNAP when=$WHEN seed=$SEED db=$DB bucket=$BUCKET"
python3 - "$BUCKET" <<'EOF' || exit 1
import sys, json, boto3, botocore
b = sys.argv[1]
s3 = boto3.client("s3", endpoint_url="http://127.0.0.1:9000", aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1", config=botocore.config.Config(s3={"addressing_style": "path"}))
s3.create_bucket(Bucket=b)
s3.put_bucket_policy(Bucket=b, Policy=json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::%s/*" % b]}]}))
EOF
psql -X -q -v ON_ERROR_STOP=1 "$ADMIN" -c "CREATE DATABASE $DB" -c "CREATE ROLE $ROLE LOGIN PASSWORD '$PW'" \
  -c "ALTER ROLE $ROLE SET idle_in_transaction_session_timeout = '60s'" -c "ALTER ROLE $ROLE SET statement_timeout = '60s'" -c "ALTER ROLE $ROLE SET lock_timeout = '60s'" || exit 1
psql -X -q -v ON_ERROR_STOP=1 "$DIRECTDB" -c "CREATE SCHEMA tessera_log" -c "SET search_path TO tessera_log" -f "$HERE/../../../storage/s3pg/schema.sql" \
  -c "GRANT USAGE ON SCHEMA tessera_log TO $ROLE" -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA tessera_log TO $ROLE" || exit 1
printf '%s' "$PUB" > "$OUT/log.pub"
for r in 0 1; do
  "$BIN/tcpproxy" -listen 127.0.0.1:$(pgp $r) -ctl 127.0.0.1:$((BASE+4*r+1)) -upstream 127.0.0.1:$PGPORT -faults=false -log "$OUT/pgproxy-$r.log" 2>>"$OUT/pgproxy-$r.log" & BGPIDS+=($!)
  "$BIN/s3proxy" -listen 127.0.0.1:$(s3p $r) -ctl 127.0.0.1:$(s3c $r) -upstream $S3 -faults=false -log "$OUT/s3proxy-$r.log" 2>>"$OUT/s3proxy-$r.log" & BGPIDS+=($!)
done
sleep 0.5
supervise() {
  local r=$1
  while [ ! -f "$OUT/stop" ]; do
    while [ -f "$OUT/hold" ] || [ -f "$OUT/hold.$r" ]; do sleep 0.5; done
    [ -f "$OUT/stop" ] && break
    TESSERA_PG_DSN="postgres://$ROLE:$PW@127.0.0.1:$(pgp $r)/$DB?sslmode=disable$SP&application_name=replica-$r&connect_timeout=5" \
    TESSERA_S3_ACCESS_KEY=test TESSERA_S3_SECRET=test TESSERA_SIGNER="$KEY" \
      "$BIN/conformance-s3pg" --bucket="$BUCKET" --s3_endpoint=http://127.0.0.1:$(s3p $r) --skip_schema_init $REPLICA_FLAGS \
      --listen=127.0.0.1:$((CBASE+r)) --pg_max_conns=16 --pg_max_idle_conns=4 >> "$OUT/replica-$r.log" 2>&1 &
    local pid=$!; echo $pid > "$OUT/pid.$r"; echo "[$(date -u +%T.%3N)] replica $r started pid $pid" >> "$OUT/events.log"
    wait $pid; local rc=$?; echo "[$(date -u +%T.%3N)] replica $r pid $pid exited rc=$rc" >> "$OUT/events.log"
    sleep 1
  done
}
for r in 0 1; do supervise $r & BGPIDS+=($!); done
for r in 0 1; do waitfor 60 "curl -sf -m 2 http://127.0.0.1:$((CBASE+r))/healthz >/dev/null"; done
URLS=http://127.0.0.1:$CBASE,http://127.0.0.1:$((CBASE+1))
"$BIN/cpwatch" --log_url="$S3/$BUCKET/" --public_key="$PUB" --out="$OUT/cpwatch.json" --events="$OUT/cpwatch.log" >/dev/null 2>&1 & CPW=$!; BGPIDS+=($CPW)
"$BIN/dbcheck" --dsn="$DIRECT" --watch=500ms --out="$OUT/dbwatch.json" --events="$OUT/dbwatch.log" >/dev/null 2>&1 & DBW=$!; BGPIDS+=($DBW)
( while [ ! -f "$OUT/stop" ]; do echo "$(date +%s.%N) $(date -u +%T.%1N) $(state) cp=$(cpsize)" >> "$OUT/timeline.log"; sleep 0.5; done ) & BGPIDS+=($!)
# Probe writers and /healthz pollers (epoch-seconds code [index-or-error]) per replica.
for r in 0 1; do
  ( n=0; while [ ! -f "$OUT/stop" ] && [ ! -f "$OUT/probe.stop" ]; do t=$(date +%s.%N); b=$(curl -s -m 5 -w ' %{http_code}' -X POST --data "probe-$r-$n-$SUF" "http://127.0.0.1:$((CBASE+r))/add" | tr '\n' ' ' | cut -c1-160)
      echo "$t ${b##* } ${b% *}" >> "$OUT/probe-$r.log"; n=$((n+1)); sleep 0.05; done ) & BGPIDS+=($!)
  ( while [ ! -f "$OUT/stop" ]; do echo "$(date +%s.%N) $(curl -s -o /dev/null -m 3 -w '%{http_code}' "http://127.0.0.1:$((CBASE+r))/healthz")" >> "$OUT/healthz-$r.log"; sleep 0.5; done ) & BGPIDS+=($!)
done
writer() { "$BIN/chaos" --write_urls="$URLS" --log_url="$S3/$BUCKET/" --public_key="$PUB" --writers="$WRITERS" --duration="$1" --settle="$2" --out="$OUT/$3.json" > "$OUT/$3.log" 2>&1; }
"$BIN/chaos" --write_urls="$URLS" --log_url="$S3/$BUCKET/" --public_key="$PUB" --writers="$WRITERS" --duration=600s --settle=5s > "$OUT/load1.log" 2>&1 & W1=$!; BGPIDS+=($W1)

snapshot() {
  if [ "$SNAP" = sql ]; then
    psql -X -q -v ON_ERROR_STOP=1 "$DIRECTDB" -c "BEGIN ISOLATION LEVEL REPEATABLE READ" -c "DROP SCHEMA IF EXISTS snap CASCADE" -c "CREATE SCHEMA snap" \
      -c "CREATE TABLE snap.seqcoord AS SELECT * FROM tessera_log.seqcoord" -c "CREATE TABLE snap.seq AS SELECT * FROM tessera_log.seq" \
      -c "CREATE TABLE snap.intcoord AS SELECT * FROM tessera_log.intcoord" -c "CREATE TABLE snap.pubcoord AS SELECT * FROM tessera_log.pubcoord" \
      -c "CREATE TABLE snap.gccoord AS SELECT * FROM tessera_log.gccoord" -c "COMMIT"
    SNAPSTATE=$(psql -X -tA "$DIRECTDB" -c "SELECT 'next='||s.next||' int='||i.seq||' pub='||p.size||' seqrows='||(SELECT count(*) FROM snap.seq) FROM snap.seqcoord s, snap.intcoord i, snap.pubcoord p")
    SNAPINT=$(psql -X -tA "$DIRECTDB" -c "SELECT seq FROM snap.intcoord"); SNAPNEXT=$(psql -X -tA "$DIRECTDB" -c "SELECT next FROM snap.seqcoord")
  else
    # The replicas' PostgreSQL proxies are partitioned while the cluster is down and until the
    # state has been read, so that it is exactly the state in the copy.
    pgpart on; runuser -u postgres -- "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop >/dev/null
    rm -rf "$OUT/datadir-snap"; cp -a "$PGDATA" "$OUT/datadir-snap"
    runuser -u postgres -- "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/../pg.log" -w start >/dev/null
    SNAPSTATE=$(state); SNAPINT=$(intseq); SNAPNEXT=$(nextidx); pgpart off
  fi
}
restore() {
  if [ "$SNAP" = sql ]; then
    # Atomic restore; retried because the exclusive locks can deadlock with a replica's transaction.
    # The state it replaces is read inside the transaction, under the locks (exact "lost" values).
    for attempt in $(seq 1 60); do
    PRE=$(psql -X -q -tA -v ON_ERROR_STOP=1 "$DIRECTDB" -c "BEGIN" -c "SET LOCAL lock_timeout = '2s'" \
      -c "LOCK TABLE tessera_log.intcoord, tessera_log.seqcoord, tessera_log.seq, tessera_log.pubcoord, tessera_log.gccoord IN ACCESS EXCLUSIVE MODE" \
      -c "SELECT i.seq||' '||s.next FROM tessera_log.intcoord i, tessera_log.seqcoord s" \
      -c "DELETE FROM tessera_log.seq" -c "INSERT INTO tessera_log.seq SELECT * FROM snap.seq" \
      -c "DELETE FROM tessera_log.seqcoord" -c "INSERT INTO tessera_log.seqcoord SELECT * FROM snap.seqcoord" \
      -c "DELETE FROM tessera_log.intcoord" -c "INSERT INTO tessera_log.intcoord SELECT * FROM snap.intcoord" \
      -c "DELETE FROM tessera_log.pubcoord" -c "INSERT INTO tessera_log.pubcoord SELECT * FROM snap.pubcoord" \
      -c "DELETE FROM tessera_log.gccoord" -c "INSERT INTO tessera_log.gccoord SELECT * FROM snap.gccoord" -c "COMMIT" 2>>"$OUT/restore.err") && break
    sleep 0.2; done
    PRE_CP=$(cpsize); PRE_INT=${PRE% *}; PRE_NEXT=${PRE#* }; RESTORE_CP=$PRE_CP
    log "restore committed after $attempt attempt(s); replaced state: IntCoord $PRE_INT, SeqCoord.next $PRE_NEXT, published $PRE_CP"
  else
    # With the proxies partitioned nothing can commit any more: read the exact state being lost.
    pgpart on; PRE_INT=$(intseq); PRE_NEXT=$(nextidx); runuser -u postgres -- "$PGBIN/pg_ctl" -D "$PGDATA" -m immediate -w stop >/dev/null
    find "$PGDATA" -mindepth 1 -delete && cp -a "$OUT/datadir-snap/." "$PGDATA/"
    runuser -u postgres -- "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/../pg.log" -w start >/dev/null
    RESTORE_CP=$(cpsize); log "datadir restored: $(state) cp=$RESTORE_CP (lost: IntCoord $PRE_INT, SeqCoord.next $PRE_NEXT)"; pgpart off
  fi
}

case $MODE in
behind)
  sleep $((6 + RANDOM % 10))
  case $WHEN in
    int_ahead) waitfor 20 '[ "$(intseq)" -gt "$(cpsize)" ]' ;;
    after_pub) waitfor 20 'i=$(intseq); [ "$i" = "$(cpsize)" ] && [ "$i" = "$(q "SELECT size FROM PubCoord")" ]' ;;
    mid_int)   waitfor 20 '[ "$(q "SELECT count(*) FROM pg_locks l JOIN pg_class c ON l.relation=c.oid WHERE c.relname='"'intcoord'"' AND l.mode='"'RowShareLock'"' AND l.granted")" -gt 0 ]' ;;
  esac
  log "snapshot point: db $(state) cp=$(cpsize)"; snapshot; log "SNAPSHOT taken: $SNAPSTATE"
  waitfor 60 '[ "$(cpsize)" -gt "$SNAPINT" ]' || log "WARNING: published checkpoint did not pass the snapshot's IntCoord"
  sleep $((2 + RANDOM % 8))
  ;;
seqintact)
  for r in 0 1; do curl -s -m 2 "http://127.0.0.1:$(s3c $r)/ctl?faults=on&latency=1500ms" >/dev/null; done
  waitfor 40 '[ $(( $(nextidx) - $(intseq) )) -gt 1500 ]'; kill $W1; wait $W1 2>/dev/null; sleep 3
  log "snapshot point: db $(state) cp=$(cpsize)"; snapshot; log "SNAPSHOT taken: $SNAPSTATE"
  for r in 0 1; do curl -s -m 2 "http://127.0.0.1:$(s3c $r)/ctl?faults=off&latency=0s" >/dev/null; done
  # Everything in the snapshot (up to its SeqCoord.next) integrated and published (the probe writers keep adding entries).
  waitfor 90 '[ "$(cpsize)" -ge "$SNAPNEXT" ] && [ "$(intseq)" -ge "$SNAPNEXT" ]' || log "WARNING: did not settle before restore"
  ;;
partial)
  c0=$(cpsize); waitfor 40 '[ "$(cpsize)" != "$c0" ]'; sleep $((2 + RANDOM % 3))
  log "snapshot point: db $(state) cp=$(cpsize)"; snapshot; log "SNAPSHOT taken: $SNAPSTATE (published $(cpsize))"
  sleep $((4 + RANDOM % 5))
  ;;
esac
PRE_INT=$(intseq); PRE_CP=$(cpsize); PRE_NEXT=$(nextidx)
log "before restore: db $(state) cp=$PRE_CP"
restore; RESTORE_TS=$(date +%s.%N)
log "RESTORED snapshot: db $(state) cp=$(cpsize) (lost: IntCoord $PRE_INT -> $SNAPINT, SeqCoord.next $PRE_NEXT -> $SNAPNEXT)"
# The published size at the moment of the restore (a publication may land between "before restore" and the restore).
[ "$MODE" = partial ] && log "partial case: published $RESTORE_CP < DB IntCoord $SNAPINT < S3-integrated $PRE_INT ? $([ "$RESTORE_CP" -lt "$SNAPINT" ] && [ "$SNAPINT" -lt "$PRE_INT" ] && echo yes || echo NO)"
[ "$MODE" = seqintact ] && { "$BIN/chaos" --write_urls="$URLS" --log_url="$S3/$BUCKET/" --public_key="$PUB" --writers="$WRITERS" --duration=600s --settle=5s > "$OUT/load2.log" 2>&1 & W1=$!; BGPIDS+=($W1); }
sleep "$OBSERVE_SECS"
log "after ${OBSERVE_SECS}s with writers running: db $(state) cp=$(cpsize)"
OBSERVE_END_TS=$(date +%s.%N)
python3 "$HERE/objcheck.py" "$BUCKET" marker > "$OUT/marker-at-restart.json"; log "tripped marker before the restart: $(cut -c1-400 "$OUT/marker-at-restart.json")"
# Restart both replicas; the probe writers keep running, so acknowledgements by the restarted
# processes (start-up guard, tripped marker) are measured too.
for r in 0 1; do echo "=== restart (start-up guard) ===" >> "$OUT/replica-$r.log"; done
log "restarting both replicas (start-up guard, tripped marker)"; RESTART_TS=$(date +%s.%N); for r in 0 1; do kill -9 "$(cat "$OUT/pid.$r")"; done; sleep 12
RESTART_END_TS=$(date +%s.%N); touch "$OUT/probe.stop"
log "after restart: db $(state) cp=$(cpsize); replica exits since restore: $(awk -v t="$(date -u -d @${RESTORE_TS%.*} +%T)" '$1 > "["t && /exited/' "$OUT/events.log" | wc -l)"
since_restart() { sed -n '/=== restart (start-up guard) ===/,$p' "$OUT/replica-$1.log"; }
for r in 0 1; do
  log "replica $r after the restart: start-up refusals citing the marker $(since_restart $r | grep 'Failed to create new appender' | grep -c 'marker'), other start-up failures $(since_restart $r | grep 'Failed to create new appender' | grep -vc 'marker'), probe acks $(awk -v t="$RESTART_TS" '$1 >= t && $2 == 200' "$OUT/probe-$r.log" | wc -l)"
done
kill $W1 2>/dev/null; wait $W1 2>/dev/null
OBJ=$(python3 "$HERE/objcheck.py" "$BUCKET" check --since "$RESTORE_TS"); log "objcheck: $OBJ"
STALL_CP=$(cpsize)
for r in 0 1; do
  log "replica $r: coord_behind=$(grep -c 'coordination database is behind' "$OUT/replica-$r.log") precondition_failed=$(grep -c 'precondition failed' "$OUT/replica-$r.log") appender_fail=$(grep -c 'Failed to create new appender' "$OUT/replica-$r.log")"
done
grep -h 'level=ERROR' "$OUT"/replica-*.log | sed 's/time=[^ ]* //' | sed -E 's/[0-9a-f]{64}/<hash>/g; s/[0-9]+/N/g' | cut -c1-200 | sort | uniq -c | sort -rn | head -8 > "$OUT/error-kinds.txt"

RECOVERED=skipped; RECOVERY_NOTES=""
if [ "$RECOVER" = 1 ]; then
  # storage/s3pg/README.md, "Recovering from a lost commit".
  log "RECOVERY (README runbook). Inspecting: stop every writer"
  touch "$OUT/hold"; for r in 0 1; do kill -9 "$(cat "$OUT/pid.$r")" 2>/dev/null; done; sleep 2
  ACTIVE=$(psql -X -tA "$ADMIN" -c "SELECT count(*) FROM pg_stat_activity WHERE usename = '$ROLE'")
  log "sessions of the runtime role left: $ACTIVE"; [ "$ACTIVE" = 0 ] || RECOVERY_NOTES="$RECOVERY_NOTES sessions_left=$ACTIVE"
  S=$(cpsize); ROOT_B64=$(curl -s -m 3 "$S3/$BUCKET/checkpoint" | sed -n 3p)
  log "published checkpoint S=$S R=$ROOT_B64; coordination state: $(state); Seq: $(q "SELECT count(*)||' batches, min '||coalesce(min(seq)::text,'-')||', max '||coalesce(max(seq)::text,'-') FROM Seq")"
  pg_dump -n tessera_log "$DIRECTDB" > "$OUT/incident-schema-dump.sql" 2>>"$OUT/recovery.err"
  "$BIN/fsck" --storage_url="$S3/$BUCKET/" --public_key="$OUT/log.pub" --ui=false -N 4 > "$OUT/fsck-pre-recovery.log" 2>&1; FRC0=$?
  log "fsck of the published checkpoint before recovery: rc=$FRC0"
  if [ $FRC0 != 0 ]; then
    log "fsck failed: the runbook does not apply"; RECOVERED=NO
  else
    # Step 1: quarantine and remove the objects beyond S.
    log "step 1: quarantine + remove objects beyond $S: $(python3 "$HERE/objcheck.py" "$BUCKET" prune "$S" --quarantine "$BUCKET-q")"
    # Step 2 (antispam) does not apply: no antispam store. Step 3: reset the coordination tables.
    psql -X -q -v ON_ERROR_STOP=1 "$DIRECTDB" -c "BEGIN" -c "SET LOCAL search_path TO tessera_log" \
      -c "UPDATE IntCoord SET seq = $S, rootHash = decode('$ROOT_B64', 'base64') WHERE id = 0" -c "UPDATE SeqCoord SET next = $S WHERE id = 0" \
      -c "UPDATE PubCoord SET size = $S WHERE id = 0" -c "DELETE FROM Seq" -c "COMMIT" 2>>"$OUT/recovery.err" || RECOVERY_NOTES="$RECOVERY_NOTES reset_failed"
    log "step 3: coordination tables reset: $(state)"
    # Step 4: verify before restarting.
    "$BIN/fsck" --storage_url="$S3/$BUCKET/" --public_key="$OUT/log.pub" --ui=false -N 4 > "$OUT/fsck-step4.log" 2>&1; FRC4=$?
    IC=$(q "SELECT seq||' '||encode(rootHash, 'base64') FROM IntCoord")
    log "step 4: fsck rc=$FRC4; IntCoord = '$IC' (expected '$S $ROOT_B64')"; [ "$IC" = "$S $ROOT_B64" ] && [ $FRC4 = 0 ] || RECOVERY_NOTES="$RECOVERY_NOTES step4_failed"
    # Tripped marker (s3pg-coordination-tripped): while it exists, writers refuse to start even though
    # the database has been reconciled; deleting it is the LAST step, before restarting writers.
    MK0=$(python3 "$HERE/objcheck.py" "$BUCKET" marker | cut -c1-300)
    if grep -q '"exists": true' <<< "$MK0"; then
      for r in 0 1; do echo "=== marker check ===" >> "$OUT/replica-$r.log"; done
      mkref() { sed -n '/=== marker check ===/,$p' "$OUT/replica-0.log" | grep 'Failed to create new appender' | grep -c 'marker'; }
      touch "$OUT/hold.1"; rm -f "$OUT/hold"; waitfor 30 '[ "$(mkref)" -ge 1 ]'; sleep 1
      MKH=$(curl -s -o /dev/null -m 2 -w '%{http_code}' "http://127.0.0.1:$CBASE/healthz"); MKREF=$(mkref)
      touch "$OUT/hold"; sleep 1; kill -9 "$(cat "$OUT/pid.0")" 2>/dev/null; sleep 1
      log "marker check (database reconciled, marker still present): replica 0 start-up refusals citing the marker: $MKREF, /healthz $MKH"
      [ "$MKREF" -ge 1 ] && [ "$MKH" != 200 ] || RECOVERY_NOTES="$RECOVERY_NOTES marker_did_not_block"
      MKD=$(python3 "$HERE/objcheck.py" "$BUCKET" marker --delete | python3 -c 'import json,sys; d=json.load(sys.stdin); print("exists=%s deleted=%s content=%r" % (d["exists"], d.get("deleted"), d.get("content", "")[:120]))')
      log "last step: delete the tripped marker: $MKD"; grep -q 'deleted=True' <<< "$MKD" || RECOVERY_NOTES="$RECOVERY_NOTES marker_delete_failed"
    else
      log "no tripped marker present: $MK0"; MKREF=none
    fi
    # Step 5: restart ONE writer, add a few entries, check that a larger checkpoint is published,
    # without differs / coordination errors, and fsck it; then start the other writer.
    for r in 0 1; do echo "=== recovery restart ===" >> "$OUT/replica-$r.log"; done
    touch "$OUT/hold.1"; rm -f "$OUT/hold"; waitfor 60 "curl -sf -m 2 http://127.0.0.1:$CBASE/healthz >/dev/null"
    for i in $(seq 1 20); do curl -s -m 5 -o /dev/null -X POST --data "recovery-$SUF-$i" "http://127.0.0.1:$CBASE/add"; done
    waitfor 30 '[ "$(cpsize)" -gt "$S" ]'; S5=$(cpsize)
    E5=$(sed -n '/=== recovery restart ===/,$p' "$OUT/replica-0.log" | grep -c 'differs\|coordination database')
    "$BIN/fsck" --storage_url="$S3/$BUCKET/" --public_key="$OUT/log.pub" --ui=false -N 4 > "$OUT/fsck-step5.log" 2>&1; FRC5=$?
    log "step 5: one writer: checkpoint $S -> $S5, differs/coordination errors: $E5, fsck rc=$FRC5"
    [ "$S5" -gt "$S" ] && [ "$E5" = 0 ] && [ $FRC5 = 0 ] || RECOVERY_NOTES="$RECOVERY_NOTES step5_failed"
    rm -f "$OUT/hold.1"; waitfor 60 "curl -sf -m 2 http://127.0.0.1:$((CBASE+1))/healthz >/dev/null"
    writer 25s 2m verify; VRC=$?
    log "post-recovery chaos verification rc=$VRC: $(tr -d '\n ' < "$OUT/verify.json" 2>/dev/null | cut -c1-400)"
    sleep 3
    "$BIN/dbcheck" --dsn="$DIRECT" --settled --checkpoint_url="$S3/$BUCKET/checkpoint" --out="$OUT/dbsettled.json" >/dev/null 2>&1; SRC=$?
    "$BIN/fsck" --storage_url="$S3/$BUCKET/" --public_key="$OUT/log.pub" --ui=false -N 4 > "$OUT/fsck.log" 2>&1; FRC=$?
    OBJ2=$(python3 "$HERE/objcheck.py" "$BUCKET" check); log "post-recovery: dbcheck settled rc=$SRC fsck rc=$FRC objcheck $OBJ2"
    E6=$(for r in 0 1; do sed -n '/=== recovery restart ===/,$p' "$OUT/replica-$r.log"; done | grep -c 'differs\|coordination database')
    log "post-recovery differs/coordination errors (both replicas): $E6"
    MKEND=$(python3 "$HERE/objcheck.py" "$BUCKET" marker | cut -c1-300); log "tripped marker after the recovery: $MKEND"
    grep -q '"exists": false' <<< "$MKEND" || RECOVERY_NOTES="$RECOVERY_NOTES marker_after_recovery"
    RECOVERED=$([ $VRC = 0 ] && [ $SRC = 0 ] && [ $FRC = 0 ] && [ "$E6" = 0 ] && [ -z "$RECOVERY_NOTES" ] && echo yes || echo NO)
  fi
  python3 "$HERE/objcheck.py" "$BUCKET-q" delete-bucket 2>/dev/null
fi
kill -TERM $CPW $DBW; wait $CPW; wait $DBW
python3 - "$OUT" "$RESTORE_TS" "$OBSERVE_END_TS" "${SNAPNEXT:-0}" "$RESTART_TS" "$RESTART_END_TS" <<EOF | tee "$OUT/result.json"
import json, sys, re, datetime
out, restore_ts, observe_end, snapnext = sys.argv[1], float(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4] or 0)
restart_ts, restart_end = float(sys.argv[5]), float(sys.argv[6])
def load(p):
    try: return json.load(open(out + "/" + p))
    except Exception: return {}
cw, dw = load("cpwatch.json"), load("dbwatch.json")
def lines(p):
    try: return open(out + "/" + p).read().splitlines()
    except Exception: return []
# Per replica: first trip (replica log), first /healthz 503, probe entries acknowledged after the
# restore / after the trip (request START time), until the end of the observation (before restarts);
# and, separately, by the restarted processes (requests started in the 12s after the restart).
reps = {}
for r in (0, 1):
    trip = None
    for l in lines("replica-%d.log" % r):
        if "coordination database is behind" in l and "Failed to create new appender" not in l and "healthz:" not in l and "failed to re-read integrated size" not in l:
            m = re.match(r"time=(\S+)", l)
            if m:
                t = datetime.datetime.fromisoformat(m.group(1)).timestamp()
                if t >= restore_ts and trip is None: trip = t
    h503 = next((float(l.split()[0]) for l in lines("healthz-%d.log" % r) if len(l.split()) > 1 and float(l.split()[0]) >= restore_ts and l.split()[1] == "503"), None)
    probes = [l.split(None, 2) for l in lines("probe-%d.log" % r)]
    probes = [(float(p[0]), p[1], p[2] if len(p) > 2 else "") for p in probes if len(p) >= 2]
    win = [p for p in probes if restore_ts <= p[0] <= observe_end]
    ok_after_restore = [p for p in win if p[1] == "200"]
    ok_after_trip = [p for p in win if p[1] == "200" and trip and p[0] >= trip]
    err_after_trip = [p for p in win if p[1] != "200" and trip and p[0] >= trip]
    reps[r] = {"trip_after_restore_s": round(trip - restore_ts, 2) if trip else None,
               "healthz_503_after_restore_s": round(h503 - restore_ts, 2) if h503 else None,
               "probe_acked_after_restore": len(ok_after_restore), "probe_acked_after_trip": len(ok_after_trip),
               "probe_acked_after_trip_examples": [(round(p[0] - trip, 2), p[2]) for p in ok_after_trip[:5]],
               "last_probe_ack_after_restore_s": round(max(p[0] for p in ok_after_restore) - restore_ts, 2) if ok_after_restore else None,
               "probe_errors_after_trip": len(err_after_trip), "probe_error_example": err_after_trip[0][2][:160] if err_after_trip else None}
    after, seen = [], False
    for l in lines("replica-%d.log" % r):
        seen = seen or l.startswith("=== restart (start-up guard)")
        if seen and l.startswith("=== marker check"): break
        if seen: after.append(l)
    st = [l for l in after if "Failed to create new appender" in l]
    reps[r].update({"probe_acked_after_restart": sum(1 for p in probes if restart_ts <= p[0] <= restart_end and p[1] == "200"),
                    "probe_attempts_after_restart": sum(1 for p in probes if restart_ts <= p[0] <= restart_end),
                    "restart_refusals_marker": sum("marker" in l for l in st), "restart_refusals_other": sum("marker" not in l for l in st),
                    "restart_refusal_example": re.sub(r"^time=\S+ ", "", st[0])[:240] if st else None,
                    "healthz_200_after_restart": sum(1 for l in lines("healthz-%d.log" % r) if len(l.split()) > 1 and restart_ts <= float(l.split()[0]) <= restart_end and l.split()[1] == "200")})
# Indices handed out after the restore (SeqCoord.next growth from the restored value) during the
# observation, from the database timeline.
nexts = []
for l in lines("timeline.log"):
    m = re.match(r"(\S+) \S+ next=(\d+)", l)
    if m and restore_ts < float(m.group(1)) <= observe_end: nexts.append((float(m.group(1)), int(m.group(2))))
seq_after = max((n for _, n in nexts), default=snapnext) - snapnext if snapnext else None
rnexts = []
for l in lines("timeline.log"):
    m = re.match(r"(\S+) \S+ next=(\d+)", l)
    if m and restart_ts <= float(m.group(1)) <= restart_end: rnexts.append(int(m.group(2)))
seq_after_restart = rnexts[-1] - rnexts[0] if rnexts else None
last_growth = None
for i in range(1, len(nexts)):
    if nexts[i][1] > nexts[i - 1][1]: last_growth = round(nexts[i][0] - restore_ts, 2)
print(json.dumps({"name": "$NAME", "seed": $SEED, "snapshot": "$SNAPSTATE", "before_restore": {"int": "$PRE_INT", "next": "$PRE_NEXT", "cp": "$PRE_CP", "cp_at_restore": "${RESTORE_CP:-}"},
  "cp_after_observe_and_restart": "$STALL_CP", "objcheck_after_restore": json.loads('''$OBJ'''),
  "cp_regressions": cw.get("regressions"), "cp_forks": cw.get("forks"), "cp_inconsistent": cw.get("inconsistent"), "cp_max": cw.get("max_size"),
  "db_watch_violations": dw.get("violations"), "recovered": "$RECOVERED", "recovery_notes": "$RECOVERY_NOTES".strip(),
  "replicas": reps, "indices_assigned_after_restore": seq_after, "last_seqcoord_growth_after_restore_s": last_growth,
  "indices_assigned_after_restart": seq_after_restart, "marker_at_restart": load("marker-at-restart.json"),
  "marker_check_refusals": "${MKREF:-}",
  "error_kinds": open(out + "/error-kinds.txt").read().strip().splitlines()}))
EOF
