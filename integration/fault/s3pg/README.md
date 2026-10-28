# s3pg fault-injection harness

Jepsen-style fault injection for the `storage/s3pg` driver. Several conformance replicas
(`cmd/conformance/s3pg`) share one log; each talks to PostgreSQL through its own `tcpproxy` and to
S3 through its own `s3proxy`, which inject faults. Verification always reads S3 and PostgreSQL
**directly**, never through a proxy.

## Tools

| Tool | What it does |
|------|--------------|
| `tcpproxy/` | TCP proxy for PostgreSQL. Per connection (seeded RNG): reset (RST both sides) at an exponentially distributed byte offset in either direction; **ambiguous COMMIT** (parses the client stream, forwards `COMMIT` — simple or extended protocol — then resets before the reply reaches the client); blackhole (stop forwarding *and* hold closes for up to `-blackhole_max`, then resume or reset); latency/jitter per chunk; global partition switch; `killall` (reset every open connection now). |
| `s3proxy/` | HTTP proxy for path-style S3. Per request (seeded): 500/503 `SlowDown`/`ServiceUnavailable`; drop before forwarding; **ambiguous write** (forward, then reset or reply 500); stall beyond client timeouts; **delayed write** (client held and reset, request forwarded upstream `-delay_min..-delay_max` after the client gave up); latency/jitter applied *before* forwarding, optionally only to reads or writes (`-latency_ops`) and/or keys with a prefix (`-latency_prefix`), plus `-write_latency` for every write; global partition (requests hang until healed). `-only_prefix` restricts faults to some keys (e.g. `checkpoint`). `/stats` also counts every request by operation and key class (`LIST_entries`, `GET_checkpoint`, `PUT_tile_ifnonematch`, `PUT_checkpoint_ifmatch`, `DELETEOBJECTS`, ...) and upstream 412s per class (`412_PUT_checkpoint`). |
| `cpwatch/` | Polls the published checkpoint (signature verified) and flags REGRESSION (smaller than an earlier one), FORK (same size, different root) and INCONSISTENT (consistency proof from the log's own tiles fails). |
| `dbcheck/` | Coordination-DB invariants in one REPEATABLE READ snapshot: `Seq` is a contiguous chain from `IntCoord.seq` to `SeqCoord.next` (key + gob batch length = next key), `IntCoord.seq <= SeqCoord.next`, `PubCoord.size <= IntCoord.seq`; with `-checkpoint_url` the published checkpoint (read first) is not larger than `IntCoord.seq` (the database is never behind the published log); with `-settled`: `Seq` empty, checkpoint size = `IntCoord.seq` = `PubCoord.size`, checkpoint root = `IntCoord.rootHash`. `-watch` checks continuously. |
| `latprobe/` | Under load, adds one entry every `-interval` and measures how long until it is acknowledged, integrated (`IntCoord.seq`) and published (checkpoint); prints percentiles. |
| `objcheck.py` | `check`: every narrower partial tile/bundle must be a prefix of every wider one (and of the full one) — detects divergent timelines in the bucket; `--since` lists objects written after a time. `prune SIZE [--quarantine QBUCKET] [--dry-run]`: recovery helper deleting (after copying to a quarantine bucket) every tile/bundle describing indices beyond `SIZE`, per the rule in `storage/s3pg/README.md`. `create-bucket` / `delete-bucket`: test helpers. `marker [--delete]`: reports (JSON) whether the persistent tripped marker `s3pg-coordination-tripped` exists (and its content), `--delete` removes it and verifies that it is gone. |

Both proxies have a control API (`/ctl?faults=on|off&partition=on|off&<rate>=...`, `/stats`) and
log every injected fault with a timestamp. All fault rates default to 0. Latency/jitter are not
faults: `faults=off` leaves them in place.

## Scenarios

* `run.sh` — N replicas behind the proxies, `integration/chaos/s3pg` writing; optional SIGKILL of
  replicas (`CRASH=1`), `kill -9` of the postmaster + crash recovery (`PGKILL=1`, needs a
  disposable cluster), S3 partitions (`S3PART=1`), per-replica PostgreSQL partitions (`PGPART=1`),
  `--pg_schema` instead of `search_path` (`SCHEMA=1`), a concurrent checkpoint writer changing the
  checkpoint's bytes/ETag (same size and root) every `CP_CHURN` seconds. After `WRITE_SECS` faults
  stop; then chaos verification (no lost / misplaced / duplicated acked entry, no fork), cpwatch,
  `dbcheck -settled` (retried up to `SETTLE_SECS`) and `cmd/fsck`. `result.json` also reports
  guard activity from the replica logs (`ErrCoordinationBehind` lines by guard, the first trip,
  checkpoint precondition failures), `/healthz` polled every second (non-200s, replicas whose guard
  tripped), the DB-behind invariant (`dbcheck -watch -checkpoint_url`), and S3 requests by
  operation (+ per integration, from `IntCoord` updates). `PASS` covers the safety checks and
  settling; `NO_TRIPS` must also hold for runs without lost commits.
* `lostcommit.sh` — scenario D (database loses commits while the bucket keeps newer state); see
  the header for `MODE=behind|seqintact|partial`, `SNAP=sql|datadir`, `WHEN=...`. Probe writers and
  `/healthz` pollers per replica measure when each replica's guard tripped, when `/healthz` turned
  503, and how many entries were acknowledged after the restore and after the trip; the database
  timeline gives the indices handed out after the restore. Both replicas are then restarted while
  the probes keep writing: per replica, start-up refusals citing the tripped marker (or other
  start-up failures), entries acknowledged by the restarted processes and the indices handed out
  after the restart are recorded separately (`probe_acked_after_restart`,
  `restart_refusals_marker`, `indices_assigned_after_restart`, `marker_at_restart` in
  `result.json`). `RECOVER=1` (default) then applies the runbook of `storage/s3pg/README.md`
  ("Recovering from a lost commit") step by step (see below) and re-verifies the log.
* `repro_delayed_checkpoint.sh` — a delayed (late-landing) checkpoint PUT; before the conditional
  checkpoint writes this rolled the published checkpoint back. Now checks no regression *and* that
  the log keeps publishing afterwards.
* `repro_lockloss_checkpoint.sh` — the same without any late-delivered request: slow (2s)
  checkpoint PUTs on one replica plus resets of its PostgreSQL connections, which release the
  `PubCoord` row lock while its PUT is still in flight. Also checks liveness and `/healthz`.
* `repro_stale_guard.sh` — false-positive `ErrCoordinationBehind` on a healthy log: a guard
  compares the published checkpoint with database state read under a row lock which was lost (the
  replica's PostgreSQL connection reset) while its checkpoint GET was slow; meanwhile another
  replica integrated and published. Before 3c22ab3 the replica tripped its sticky guard although
  the database was never behind (exit 1 = reproduced). Now also counts, by guard, the transient
  `errStaleView` retries and the suspected lags whose confirming read failed (`confirm_failed`;
  since 6f2d29c that read uses the transaction's own connection, so after a reset it fails and
  the operation is retried instead of reaching `errStaleView`), checks that no tripped marker was written and
  that the replica is live afterwards (`/healthz` 200, its entry accepted and published);
  `CP_DELAY` / `KILL_EVERY` set the aggressiveness (e.g. `8s` / `0.7`). Writes `result.json`.
* `repro_slowpath.sh` — an integration whose transaction is lost after (some of) its objects
  landed; the retry finds the bucket ahead and runs `checkBucketConsistent`'s slow path over the
  whole batch. `WRITE_SECS`/`WRITERS` size the backlog (an external session holds the `IntCoord`
  lock meanwhile; `WRITE_SECS=50` gives ~120 bundles), `S3_LATENCY` emulates a remote object
  store, `NOKILL=1` is the control run. Reports the time from the kill until `IntCoord` first
  advances and until the backlog is caught up, the failed integration attempts in between and the
  size of each successful integration (`IntCoord` steps). The lock-wait timeouts while the backlog
  builds already halve the adaptive integration limit of 6f2d29c (5 timeouts: 20480 -> 640 Seq
  rows); `FRESH=1` restarts the replica just before the lock is released, so that the retries start
  from the full `DefaultIntegrationSizeLimit`, as after a lost commit in production.
* `overhead.sh` — steady-state cost without faults (any conformance binary, e.g. an older build):
  acked and integrated entries/s, latprobe latencies, S3 LIST/GET/PUT per integration.

Example:

```bash
NAME=B SEED=1 REPLICAS=3 WRITE_SECS=150 \
  PG_FAULTS="-reset_prob=0.7 -reset_mean_bytes=15000 -commit_ambig_prob=0.2 -blackhole_prob=0.05" \
  S3_FAULTS="-w_err=0.1 -w_drop=0.05 -w_ambig=0.3 -w_stall=0.01 -r_err=0.05 -r_drop=0.03" \
  ./integration/fault/s3pg/run.sh
```

The scripts expect a **disposable** PostgreSQL cluster (`PGPORT`, superuser `postgres` with trust
auth; `PGDATA` for `PGKILL`/`SNAP=datadir`; both stop or restore the *whole* cluster, so give
them a cluster of their own when running scenarios in parallel), S3 at `127.0.0.1:9000` (moto or
MinIO, credentials `test`/`test`) and the binaries in `$BIN` (`go build` `cmd/conformance/s3pg`,
`cmd/fsck`, `integration/chaos/s3pg` and the tools above). Create the cluster with e.g.
`runuser -u postgres -- /usr/lib/postgresql/16/bin/initdb -D $PGDATA -U postgres --auth=trust`,
then set `port`, `listen_addresses = '127.0.0.1'` and `max_connections = 400` in
`$PGDATA/postgresql.conf`. The scripts create and drop a unique database, role and bucket per run
(kept for analysis if a check fails). Bash reads scripts lazily: don't edit a script while an
instance of it is running (run a copy, with `HERE` pointing at this directory). Note that libpq
(`psql`) rejects `search_path` in a URI, unlike pgx: the scripts pass it only to the Go tools.

## Recovery runbook as automated by `lostcommit.sh` (`RECOVER=1`)

Follows `storage/s3pg/README.md`, "Recovering from a lost commit":

1. Stop every writer; check that `pg_stat_activity` shows no session of the runtime role.
2. Read the published checkpoint `(S, R)` and the coordination state, `pg_dump -n` the schema,
   `fsck` the published checkpoint (abort if it fails).
3. Step 1: `objcheck.py BUCKET prune S --quarantine BUCKET-q` (copy, verify the copies, delete).
4. Step 3: one transaction `UPDATE IntCoord SET seq=S, rootHash=decode(R,'base64')`,
   `UPDATE SeqCoord SET next=S`, `UPDATE PubCoord SET size=S`, `DELETE FROM Seq` (`GCCoord` and
   `Tessera` untouched; step 2, antispam, does not apply).
5. Step 4: `fsck` again; `IntCoord` must be exactly `(S, R)`.
6. Tripped marker: if `s3pg-coordination-tripped` exists, start one writer and require that it
   refuses to start citing the marker (although the database is now reconciled) and never serves
   `/healthz` 200; stop it again. Then, as the **last** step before restarting writers, delete
   the marker (`objcheck.py BUCKET marker --delete`) and verify that it is gone.
7. Restart (storage/s3pg/README.md, "Restarting"): start one writer, add entries, require a larger checkpoint, no `differs` /
   `coordination database` errors and a passing `fsck`; then start the other writer.
8. Re-verify: chaos writer (no lost/misplaced/duplicate entries), `dbcheck -settled`, `fsck`,
   `objcheck`, no `differs` / `coordination database` errors since the restart, and no tripped
   marker written again.

## Defects found by the re-verification (HEAD a945b03), fixed in 3c22ab3

* **False-positive `ErrCoordinationBehind` after a lost row lock** (`repro_stale_guard.sh`, 3/3
  runs, trips after 3-4 connection resets). `checkIntegrationAllowed` compares `IntCoord.seq` read
  under the `IntCoord` row lock with the checkpoint read afterwards, and `checkPublicationAllowed`
  compares the size read under the `PubCoord` lock likewise. When the replica's PostgreSQL
  connection dies in between (reset, failover, `idle_in_transaction_session_timeout`), the lock is
  released while the replica's goroutine continues; another replica integrates and publishes, and
  the stale replica sees `published > its size` and trips its sticky flag: it refuses all entries
  and `/healthz` returns 503 until restarted, although the database was never behind.
* **Integration wedged by `checkBucketConsistent`'s slow path** (`repro_slowpath.sh` with
  `S3_LATENCY=20ms`: no progress for 6 minutes; the `NOKILL=1` control integrates the same backlog
  in one attempt). After an integration's transaction is lost once its objects landed (connection
  reset, PostgreSQL restart, integration timeout), every retry compares every bundle and level-0
  tile of the batch (two sequential LIST + GET round trips per bundle and resource) inside the 10s
  integration timeout, persists nothing when it runs out of time, and the batch keeps growing while
  sequencing continues. Seen in `run.sh` B3/C3 runs (IntCoord stuck while the bucket held the whole
  backlog, never recovered after faults stopped) and as minutes-long stalls in F and in
  `lostcommit.sh SNAP=datadir` (a PostgreSQL restart during a large integration).

## Final re-verification (HEAD 6f2d29c)

Binaries built from the clean tree at 6f2d29c; own PostgreSQL 16 clusters; up to 5 scenarios in
parallel on 2 CPUs. Controls marked *3c22ab3* use the previous campaign's conformance binary.

* `repro_slowpath.sh`, the variants that stayed wedged at 3c22ab3, 2 runs each: all caught up
  without operator action at the first retry (1 failed attempt = the aborted one): ~121 bundles /
  161 Seq rows at 100 ms (`WRITERS=192 S3_LATENCY=100ms`) 10.9 s and 10.1 s after the kill,
  ~161 bundles / 107 rows at 50 ms (`WRITERS=384`) 11.6 s and 12.8 s. With `FRESH=1` (full
  integration limit, see above): 100 ms 12.9 s (1 failed) and 19.1 s (2 failed), 50 ms 20.8 s (2) and
  12.4 s (1), each time the whole backlog in one integration. These variants **no longer
  discriminate**, though: the *3c22ab3* binary also caught up at the first retry here (100 ms, 7.7 s),
  so the earlier wedge depended on the environment (that campaign ran more concurrent load). The
  adaptive limit is exercised when a full retry cannot fit into the 10 s integration timeout:
  - `FRESH=1 S3_LATENCY=200ms WRITERS=192`, 2 runs: 8 failed attempts each (20480 -> 80 rows), first
    progress after 80.6 s / 81.2 s (+15360 entries = 80 rows), caught up after 88.9 s / 90.5 s
    (+15552, limit doubled). *3c22ab3* control: 11 failed attempts, caught up after 113 s in one
    integration.
  - `FRESH=1 S3_LATENCY=150ms WRITERS=384`, 2 runs: 9 failed attempts (-> 40 rows), first progress
    after 89.7 s / 91.7 s (+15360), caught up after 98.4 s / 99.4 s (+25728). *3c22ab3* control:
    12 failed attempts, caught up after 125 s in one integration.
  - Without `FRESH` (limit already halved by the lock-wait timeouts) 200 ms/192 and 150 ms/384:
    3 failed attempts each, caught up after 33.7 s and 34.8 s.

  The *3c22ab3* controls show that a fixed-size retry eventually gets through here as well (not
  every retry exceeds the timeout), so the adaptive limit bounds the stall at ~log2(20480 / rows
  that fit) failed attempts of 10 s each rather than fixing a reproducible wedge in this
  environment. All 17 runs: 0 trips, `/healthz` 200, chaos 0 lost / misplaced / duplicated, no
  checkpoint regression, objcheck and fsck OK.
* `repro_stale_guard.sh`, 3 runs (`CP_DELAY`/`KILL_EVERY` 4s/1.5, 6s/1.0, 8s/0.7; 182 connection
  resets): 0 trips, 0 marker objects, replica 0 live in every run (`/healthz` 200, its entry
  published), 0 checkpoint regressions / forks, 0 `dbcheck -watch` violations. The guards still
  suspected a lag 10 times (5 integration, 5 publication), but now the confirming read uses the
  transaction's own (reset) connection, fails and the operation is retried
  (`could not confirm a suspected coordination lag ...`; the script now counts these as
  `confirm_failed`); 0 `errStaleView`.
* `lostcommit.sh`, 5 runs (partial sql x2, partial datadir x2, behind sql `int_ahead`): every one of
  the 10 replicas tripped at its first sequencing after the restore through the new high-water
  check (e.g. `next index 1466 < 1873`), 0.04-0.57 s after the restore; 0 entries acknowledged
  between the restore and the trip, 0 after the trip, 0 indices handed out after the restore
  (3c22ab3: 0-5 acknowledgements and 18-133 indices per run); restarted replicas acknowledged 0 of
  104-177 attempts each (12-15 start-up refusals citing the marker), 0 indices; 0 regressions /
  forks / inconsistent pairs, 0 `dbcheck -watch` violations; runbook recovery (marker refused a
  start with the database reconciled, deleted last) passed 5/5 with no marker written again.
* `run.sh`: B3 seed 2: 7,543 acknowledged / final 9,422 (12 new checkpoints in the 149 s fault
  window, reaching 3,934; 109 PostgreSQL resets, 233 ambiguous commits, 4 blackholes); C3 seed 3
  (11 replica crashes, 4 postmaster kills, 3 S3 and 3 PostgreSQL partitions): 7,022 / 7,480 (26
  checkpoints, reaching 7,321); A2 seed 1 with `--pg_schema` (`SCHEMA=1`): 10,063 / 10,546 (38
  checkpoints, reaching 9,994). All PASS: 0 lost / misplaced / duplicated / forks, 0 trips, 0
  `errStaleView`, 0 confirm failures, settled within 0 s, fsck OK.

## Final re-verification (HEAD 3c22ab3)

* `repro_stale_guard.sh`, 8 runs (`CP_DELAY`/`KILL_EVERY` 4s/1.5, 6s/1.0 x2, 8s/0.7 x2, 3s/0.5,
  4s/0.8 x2; 525 connection resets in total): 0 trips, 0 marker objects, replica 0 live in every
  run (`/healthz` 200, its entry published), 0 checkpoint regressions, 0 `dbcheck -watch`
  violations. 70 transient `errStaleView` retries (46 integration, 24 publication), and once
  `failed to re-read integrated size to confirm (...)` (the fresh read hit a reset; no trip). That
  message quotes the `ErrCoordinationBehind` text, so the scripts now count trips by the
  `msg="Coordination database is behind ...` line that `trip` logs once (and `run.sh` ignores the
  confirm failures). Before the fix: 3/3 runs tripped after 3-4 resets.
* `run.sh` B3 seed 2 and C3 seed 3 (stuck before) and the controls A2 seed 1, B2 seed 3: all PASS,
  0 lost / misplaced / duplicated / forks, 0 trips, settled within 0 s, fsck OK. New checkpoints
  during the 150 s fault window: B3s2 7, reaching 4728 (before: stuck at size 0, never settled),
  C3s3 20, reaching 6610 (before: 4, stuck at 544), A2s1 34 (34), B2s3 3 (1).
* `lostcommit.sh`, 9 runs (behind sql x2, seqintact sql, partial sql x2, partial datadir x4; one
  datadir run became a "behind" case because a publication landed just before the stop): 0
  regressions / forks / inconsistent pairs, 0 entries acknowledged after a trip, 0 acknowledgements
  and 0 indices handed out by restarted replicas (every restart refused, citing the marker;
  before: 16-104 indices per partial run), the marker check refused a start with the database
  already reconciled, and every recovery (marker deleted last) passed chaos, settled dbcheck, fsck
  and objcheck with no marker written again.
* `repro_slowpath.sh` (one aborted attempt, then the slow path): ~120 bundles at 20 ms caught up
  9.1 s after the kill, at 50 ms 10.3 s, both at the first retry (before: no progress for 6 min at
  20 ms). Still **wedged** (IntCoord unchanged 455 s after the kill, 46 failed attempts, each timing
  out after 10 s) with ~160 bundles (40960 entries, the conformance server's pushback limit) at 50 ms
  (`WRITERS=384`) and with ~120 bundles at 100 ms (`WRITERS=192 S3_LATENCY=100ms`), while the
  `NOKILL=1` controls integrate the same backlogs in one attempt (6-7 s after the release, including
  the 2 s `WRITE_LATENCY` applied to the first writes). A retry
  costs ~6x the S3 requests of a fresh attempt (per bundle and resource: the slow path's LIST + GET,
  then every If-None-Match PUT answered 412 and read back for comparison; ~2000 requests per
  attempt for 161 bundles vs ~330), all within the fixed 10 s integration timeout, and nothing is
  kept when it runs out, so every retry starts over. Reproduce with
  `S3_LATENCY=100ms WRITE_SECS=50 WRITERS=192 ./repro_slowpath.sh` (exit 1, `NOT caught up`;
  add `NOKILL=1` for the control). Neither variant stayed wedged in the 6f2d29c re-run (above); the 100 ms
  variant did not even with the 3c22ab3 binary.
* F1: `repro_delayed_checkpoint.sh` and `repro_lockloss_checkpoint.sh` 2 runs each: no
  regression, live. (`repro_lockloss_checkpoint.sh` now finds `schema.sql` via `HERE`, so it can
  run from a copy.)
