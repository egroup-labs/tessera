# Verification of `storage/s3pg`

How the S3 + PostgreSQL driver was verified, what was found, and how to repeat each check. The
driver documented in [README.md](README.md) is commit `6f2d29c`. Every number below comes from the
commit messages of the branch (`git log 262505d..6f2d29c`), the READMEs of the harnesses under
`integration/*/s3pg`, or the results of the verification campaigns run against the branch, and is
labelled with the commit it was measured on. The last full re-verification (fault injection,
lost commits, real MinIO, adversarial review) ran on `3c22ab3`; `6f2d29c` fixes what that round
found and was verified with regression tests, targeted mutants, a real standby, a focused
fault-injection re-run and the full Go suite against real MinIO ([11](#11-final-round-6f2d29c)).

Most harnesses expect PostgreSQL (13 or newer) and an S3-compatible server with path-style
addressing at `http://127.0.0.1:9000` with credentials `test`/`test` (e.g.
`moto_server -H 127.0.0.1 -p 9000`, as in CI). Use disposable clusters and buckets: several
harnesses kill, restart or roll back PostgreSQL.

| Area | Harness | Result |
| ---- | ------- | ------ |
| Go test suite | `storage/s3pg/...`, `cmd/conformance/s3pg` | regression tests for every finding below, including the `6f2d29c` fixes ([1](#1-go-test-suite)); results per PostgreSQL version in [2](#2-postgresql-versions-and-ci) |
| PostgreSQL 13–18 | `s3pg_pg_matrix.yml`, local clusters | all pass on 13.23, 14.23, 15.18, 16.13, 17.10, 18.4 ([2](#2-postgresql-versions-and-ci)) |
| Linters, vulnerabilities | CI's golangci set, `govulncheck` | clean; 0 reachable vulnerabilities ([3](#3-linters-and-vulnerabilities)) |
| Parity with `storage/aws` | `integration/differential/s3pg` | byte-identical logs, checkpoints, antispam and migration; 0 unexplained differences ([4](#4-differential-parity-with-storageaws)) |
| Real MinIO | `integration/minio/s3pg`, suite, hammer, chaos | conditional writes: exactly one winner, never `409`; suite, hammer and chaos pass ([5](#5-real-minio)); `3c22ab3`: suite, stale-view repro (0 trips), chaos incl. MinIO `SIGKILL` (0 lost, wrong, duplicated or forked), hammer (0 errors) and the tripped-marker runbook with `BucketPrefix` pass |
| PgBouncer | not in the repository | 1.22.0 and 1.26.0, transaction pooling, two logs on one role and pool: no cross-log effects ([6](#6-pgbouncer)) |
| Fault injection | `integration/fault/s3pg/run.sh` | 0 lost, wrong, duplicated or forked entries; findings F1–F5 and two later defects, all fixed ([7](#7-fault-injection)); `3c22ab3`: stale-guard repro 0 trips in 8 runs (3/3 tripped before), 4 `run.sh` runs pass, slow-path wedge left only for larger backlogs or higher S3 latency (adaptive limit in `6f2d29c`; re-run on `6f2d29c`: caught up, see [11](#11-final-round-6f2d29c)) |
| Lost commits, recovery runbook | `integration/fault/s3pg/lostcommit.sh` | runbook recovered 18/18 (`a945b03`) and 9/9 with the tripped marker (`3c22ab3`); on `3c22ab3` 0 entries acknowledged after a trip or by restarted replicas ([8](#8-lost-commits-and-the-recovery-runbook)) |
| Standby | `TestCurrentTreeRefusesStandby` with a streaming replica | `6f2d29c`: a read answered by a real standby is refused ([11](#11-final-round-6f2d29c)) |
| `synchronous_commit = off` | not in the repository | no regression of acknowledged indices in 5 crash runs; control without the driver lost 44–56 acknowledged commits ([8](#8-lost-commits-and-the-recovery-runbook)) |
| Chaos and load | `integration/chaos/s3pg`, `internal/hammer` | see MinIO ([9](#9-chaos-and-load)) |
| Mutation testing | not in the repository | rounds of 31, 86, 8 (`3c22ab3`) and 9 (`6f2d29c`) mutants; every survivor equivalent or a test gap since closed ([10](#10-mutation-testing)) |
| Adversarial review | — | `3c22ab3`: no safety bug found; all findings fixed in `6f2d29c` ([11](#11-final-round-6f2d29c)) |

## 1. Go test suite

```sh
export TESSERA_S3PG_TEST_DSN='postgres://postgres:postgres@127.0.0.1:5432/tessera_test?sslmode=disable'
export TESSERA_S3PG_TEST_S3_ENDPOINT=http://127.0.0.1:9000
go test -race -count=1 ./storage/s3pg/... -pg_test_optional=false
go test -race -count=1 ./cmd/conformance/s3pg/
```

Every test creates its own PostgreSQL schema and bucket, so tests run in parallel against one
database (allow a few hundred connections; the matrix workflow sets `max_connections = 300`). For
MinIO, also set `TESSERA_S3PG_TEST_S3_ACCESS_KEY`/`TESSERA_S3PG_TEST_S3_SECRET` (MinIO rejects
secrets shorter than 8 characters). Coverage:

* **Sequencer** (`pgsequencer_test.go`, `s3pg_test.go`): the `storage/aws` tests, ported; schema
  initialisation (fresh, concurrent, re-initialisation keeps state, `SkipSchemaInit`,
  compatibility version, incomplete schema, `CHECK` constraints); `consumeEntries` (rollback when
  the callback fails, limit, forced update, gaps); pushback boundary; large entries; indices
  beyond 2^32; multi-replica stress (separate pools assigning and consuming concurrently: no
  entry lost, duplicated, reordered or gapped); independence of schemas; concurrent publication
  and publication rules; GC progress, concurrency and delete errors; the multi-replica stress,
  concurrent initialisation, publication and GC again with `default_transaction_isolation`
  `REPEATABLE READ` and `SERIALIZABLE` (9346801).
* **End to end on S3, robustness** (`e2e_test.go`, `robustness_test.go`): append and verify, two
  appenders, restart, GC with and without `BucketPrefix`, `MigrationWriter` end to end,
  `deleteObjectsWithPrefix` (pagination, nothing to delete, per-key errors), pool ownership and
  `Close`, `MaxIdleConns`, `S3Options` without `SDKConfig`.
* **Hardening** (written independently of the code, daa39cd): schema qualification against a
  decoy `search_path`, including a schema name that needs quoting (`schema_qualified_test.go`);
  conditional checkpoint writes and the publish race (`checkpoint_cond_test.go`); the bucket
  consistency check, 31 cases plus an end-to-end retry after a failed commit
  (`bucket_consistent_test.go`); S3 writes of unknown outcome through a lossy fake S3
  (`unknown_outcome_test.go`); the sticky trip and edge cases T1–T11 (`hardening_test.go`);
  `internal/pgtx` (durable commit, isolation, qualifier); the `MigrationWriter`'s `LogReader`.
* **Guards** (`guard_test.go`, `guard_stale_test.go`, `hardening_test.go`): start-up refuses a
  stale database; a running instance whose database loses commits never changes the published
  checkpoint and writes no object except the tripped marker; publication rules (regression,
  same-size fork, inconsistent growth, unparseable checkpoint); `LogReader` partial -> full
  fallback.
* **Regression tests for 3c22ab3**: a stale view (database moved on, or exactly at the published
  size) returns `errStaleView` and neither trips nor writes the marker, while a database really
  behind trips and writes it (`TestGuardStaleViewDoesNotTrip`); the slow path over 80 bundles
  with 20ms per object store call must finish within a quarter of the integration timeout
  (`TestCheckBucketConsistentSlowPathLatency`; the commit reports ~0.45s, ~6.4s before); the
  refused start-up wrote the marker and a second start was refused citing it
  (`TestStartupChecksPublishedCheckpoint`; since `6f2d29c` a refused start-up writes nothing, see
  below); `$` in `Config.Schema` (`TestSchemaWithDollarUsesAnotherLog`,
  `TestQualifierSchemaWithDollar`); `If-Match` on a missing object
  (`TestSetObjectIfETagSemantics`); `errPreconditionFailed` survives `updateCheckpoint`
  (`TestUpdateCheckpointPreservesPreconditionFailed`). The three tests that demonstrated defects
  in daa39cd are no longer skipped.
* **Regression tests for 6f2d29c**: the integration job halves its `Seq`-row limit after each
  failed attempt until attempts succeed, and doubles it after each success
  (`TestIntegrationLimitAdapts`); with a single-connection pool (`MaxOpenConns = 1`), a database
  that is really behind is confirmed on the transaction's own connection and trips
  (`TestConfirmBehindSingleConnectionPool`); `currentTree` refuses an answer from a standby
  (`TestCurrentTreeRefusesStandby`; skipped unless `TESSERA_S3PG_TEST_STANDBY_DSN` names a
  streaming-replication standby, which CI doesn't set); `assignEntries` refuses below the
  high-water mark without changing `SeqCoord.next` (`TestAssignEntriesRefusesBelowHighWater`),
  and after the database loses commits, `Add` is refused at once, before any integration attempt,
  for indices the instance has seen published or integrated, nothing is handed out, and the
  appender trips (`TestHighWaterRefusesStaleIndices`); a refused start-up writes nothing, not
  even the marker (`TestStartupChecksPublishedCheckpoint`); while the marker exists, start-up is
  refused citing it and its contents, even against a healthy database, and succeeds once it is
  deleted (`TestTrippedMarkerBlocksStartup`). `TestGarbageCollect` now fails fast if an `Add`
  fails (e.g. on the 2s assign timeout on a heavily loaded machine) instead of hanging.
* **Migrations** (`migrations_test.go`): `migrations/` is equivalent to `schema.sql`.
* **Antispam** (`antispam/`): the upstream tests, ported; pushback; concurrent initialisation;
  `SkipSchemaInit`; compatibility version; re-initialisation; first index wins; batches beyond
  PostgreSQL's 65535 bind parameters; indices beyond 2^32; `NewAntispamWithDB`/`Close`; sharing
  the log's schema; isolation level; schema qualification; no `CREATE` privilege.

## 2. PostgreSQL versions and CI

CI (`.github/workflows`):

* `go_test.yml`, job `test-s3pg-postgres`: the suite on PostgreSQL 16 with moto 5.2.3.
* `s3pg_pg_matrix.yml`: the suite on PostgreSQL 13, 14, 15, 16, 17 and 18, and per version a
  least-privilege end-to-end run: `deployment/live/s3pg/postgres-init/10-tessera-roles.sh`,
  `migrate.sh` applied twice, the conformance server as `tessera_app` with `--skip_schema_init`,
  `--pg_schema`, `--antispam_pg_schema` and antispam, `integration/log`, `cmd/fsck`.
* `integration_test.yml`, job `s3pg-tlog-tiles-api`: the compose stack of
  [`deployment/live/s3pg`](/deployment/live/s3pg/) (PostgreSQL 16, MinIO
  `RELEASE.2025-09-06T17-38-46Z`), with and without antispam: `integration/log`, a restart of the
  server, `integration/log` again, `cmd/fsck`.

Result: the full suite passes on PostgreSQL 13.23, 14.23, 15.18, 16.13, 17.10 and 18.4 (256
tests on each version in the first run); re-run after the hardening, 270 tests pass on 13, 15, 17
and 18.

To reproduce locally, start a throwaway cluster of each version on its own port and point the
suite at it:

```sh
V=15 PORT=54315 D=/tmp/pg$V    # PostgreSQL binaries of version $V in /usr/lib/postgresql/$V/bin
runuser -u postgres -- /usr/lib/postgresql/$V/bin/initdb -D $D -U postgres --auth=trust
runuser -u postgres -- /usr/lib/postgresql/$V/bin/pg_ctl -D $D -l $D/log \
  -o "-p $PORT -c listen_addresses=127.0.0.1 -c max_connections=300" start
psql -h 127.0.0.1 -p $PORT -U postgres -c 'CREATE DATABASE tessera_test'
TESSERA_S3PG_TEST_DSN="postgres://postgres@127.0.0.1:$PORT/tessera_test?sslmode=disable" \
TESSERA_S3PG_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
  go test -race -count=1 ./storage/s3pg/... -pg_test_optional=false
```

## 3. Linters and vulnerabilities

CI runs golangci-lint v2 (`.golangci.yml`: the default linters govet, errcheck, staticcheck
including unused, ineffassign, plus sloglint with attr-only and context `all`). Equivalent
standalone run, plus `govulncheck` against a local copy of the Go vulnerability database
(`golang/vulndb`):

```sh
P="./storage/s3pg/... ./cmd/conformance/s3pg/... ./integration/chaos/s3pg/... ./integration/differential/s3pg/... ./integration/fault/s3pg/... ./integration/minio/s3pg/..."
go vet $P && staticcheck $P && errcheck $P && ineffassign $P && sloglint -attr-only -ctx-only=all $P
govulncheck -db file:///path/to/vulndb $P
```

Result: govet, staticcheck (including unused), errcheck, ineffassign and sloglint are clean on
all new code; `govulncheck` finds 0 reachable vulnerabilities.

## 4. Differential parity with `storage/aws`

[`integration/differential/s3pg`](/integration/differential/s3pg/) drives `storage/aws` (MySQL 8)
and `storage/s3pg` with the same deterministic workload and compares every object (bytes,
`Content-Type`, `Cache-Control`), the signed checkpoints and the coordination state after each
phase; then garbage collection, the antispam followers (`AntispamIDSeq` contents and lookups
through both decorators), migration (including an idempotent re-run into the same buckets) and
the retry of an integration whose database commit failed. Differences explained by the upstream
bugs fixed in s3pg are reported as `KNOWN`; anything else fails. Its README describes the
workload and each scenario.

```sh
go run ./integration/differential/s3pg                         # full workload, ~10 minutes
go run ./integration/differential/s3pg --scale=small --bucket_prefix=logs/diff --rerun_timeout=90s
go run ./integration/differential/s3pg --scale=small --pg_schema_mode=qualified
```

Results:

* Full scale: 131,599 entries in 10 phases, 11,171 object comparisons; the checkpoints (note
  text and Ed25519 signature) are byte-identical at all 10 sizes. Antispam:
  114,494 `AntispamIDSeq` rows identical, 4,080 lookups with 0 mismatches. Migration: 1,036
  objects identical.
* Re-run after the hardening, including the schema-qualified mode (`--pg_schema_mode=qualified`,
  with a decoy schema in `search_path`) and the failed-commit retry scenario: 0 unexplained
  differences.
* Known differences, all upstream bugs (fixed in s3pg): an empty `DeleteObjects` stalls garbage
  collection; `BucketPrefix` is applied twice in the idempotent re-write check; no partial ->
  full fallback in the `LogReader`; the `MigrationWriter`'s `LogReader` panics.

## 5. Real MinIO

MinIO built from source (commit `7aac2a2`) with a 4-drive erasure set.
[`integration/minio/s3pg`](/integration/minio/s3pg/) probes the S3 behaviour the driver relies
on (see the comment at the top of its `main.go` and `ifmatch.go`).

Results (before `3c22ab3`):

* `If-None-Match: *` races (`inm-race`): 200 rounds x 32 writers, exactly one winner each.
* `If-Match` (`ifmatch`): current ETag `200`, stale ETag `412`, missing object `404 NoSuchKey`
  (since `3c22ab3` the driver treats that as a failed condition). Races (`ifmatch-race`):
  32 writers x 200 rounds and 64 writers x 500 rounds, exactly one winner each, never `409`.
* Full Go test suite against MinIO: passes (`-race -count=3`).
* Hammer, 10 minutes, two replicas: 160,973 and 140,833 entries, 0 errors.
* Chaos (`integration/chaos/s3pg`), 5 runs of ~45k–54k acknowledged entries each, one of them
  with MinIO itself `SIGKILL`ed (by hand; not a step of `run.sh`): 0 lost, wrong, duplicated or
  forked entries, `fsck` OK.
* Earlier findings, reflected in the driver: MinIO answers a rejected conditional `PUT` with a
  large body by closing the connection, so the client may get no response (the driver resolves
  such writes by reading the object back, 7107823); an empty `DeleteObjects` is rejected (the
  driver skips it); MinIO refuses anonymous `PUT`s, whereas moto accepts them.

Results of the final re-verification on `3c22ab3` (`stall-repro3` and `rewrite-3x` are campaign
scripts that are not in the repository; "A / F" means A acknowledged entries, final log size F):

* Full Go test suite against MinIO, `-race -count=2`: passes (112 tests, 159 subtests, 0 skipped).
* The stale-view false positive (`stall-repro3`): 3 runs of 20 cycles at 3s and 3 runs of 25
  cycles at 500ms: 0 trips, 0 tripped markers, the log grew in every run, `fsck` OK.
* Chaos, 300s: 48,724 / 48,750; chaos with MinIO `SIGKILL`ed: 55,017 / 55,057; both with 0 lost,
  wrong, duplicated or forked entries.
* `rewrite-3x`: 3/3 OK.
* Hammer, 5 minutes, two replicas: 75,361 entries, 0 errors.
* The guard's two `LIST`s now overlap: p50 5.07ms (7.26ms before `3c22ab3`); `LIST`s take 16.2%
  of the integration's S3 time (20.9% before).
* Tripped marker on MinIO with `BucketPrefix`, including the full runbook: restarts were refused
  until the marker was deleted; then 50 more entries were added and `fsck` passed.

To reproduce (any MinIO build or release; four drives form one erasure set):

```sh
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin-secret \
  minio server --address 127.0.0.1:9100 /tmp/minio/d{1...4}
go run ./integration/minio/s3pg --endpoint=http://127.0.0.1:9100 --access_key=minioadmin --secret=minioadmin-secret
go run ./integration/minio/s3pg --endpoint=http://127.0.0.1:9100 --access_key=minioadmin --secret=minioadmin-secret \
  --probes=ifmatch-race --racers=64 --ifm_rounds=500
TESSERA_S3PG_TEST_S3_ENDPOINT=http://127.0.0.1:9100 TESSERA_S3PG_TEST_S3_ACCESS_KEY=minioadmin \
TESSERA_S3PG_TEST_S3_SECRET=minioadmin-secret TESSERA_S3PG_TEST_DSN=... \
  go test -race -count=3 ./storage/s3pg/... -pg_test_optional=false
```

For hammer and chaos, see [9](#9-chaos-and-load) with `S3_ENDPOINT`/`--s3_endpoint` pointing at
MinIO and the bucket made anonymously readable (`mc anonymous set download <alias>/<bucket>`).

## 6. PgBouncer

The suite and conformance servers were run through PgBouncer; the harness is not in the
repository. Results (reflected in [README.md](README.md#pgbouncer)):

| PgBouncer | Pool mode | Configuration | Result |
| --------- | --------- | ------------- | ------ |
| 1.22.0 | transaction | `max_prepared_statements = 200` | pass |
| 1.22.0 | transaction | DSN `default_query_exec_mode=simple_protocol` | pass |
| 1.22.0 | transaction | DSN `default_query_exec_mode=exec` | pass |
| 1.22.0 | transaction | `max_prepared_statements = 0`, pgx default mode | fails: SQLSTATE `42P05` / `26000` |
| 1.22.0, 1.26.0 | transaction | two logs (`--pg_schema`) sharing one role and pool, including DSNs carrying the other log's `search_path` | no cross-log effects |
| 1.26.0 (PostgreSQL 16) | transaction | `search_path` as a startup parameter | leaked between clients (re-checked 2026-10); schema-qualified statements are immune |
| — | session | | works, but doesn't multiplex: pool sizes must exceed the sum of all replicas' pools; when clients queue, `Add` can fail after `assignEntries`' 2s deadline |
| — | transaction | a statement that fails while being prepared | poisons that client connection (`08P01` `prepared statement name is already in use`) until `database/sql` reconnects |

To reproduce: run PgBouncer in front of the test database, e.g.

```ini
[databases]
tessera = host=127.0.0.1 port=5432 dbname=tessera
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = 6432
auth_type = scram-sha-256
auth_file = userlist.txt
pool_mode = transaction
max_prepared_statements = 200
default_pool_size = 20
```

then start two conformance servers for two logs with the same runtime role through port 6432,
each with its own `--pg_schema` (and bucket), run `integration/log` against both and `cmd/fsck`
on both; repeat with the other log's `search_path` added to each DSN (configuring PgBouncer to
accept the parameter where it would reject it, for this test only), with
`max_prepared_statements = 0` plus `default_query_exec_mode=simple_protocol` or `exec` in the
DSNs, and with `pool_mode = session`. The `search_path` leak: `default_pool_size = 1`,
transaction pooling, two pgx connections with `search_path=log_a` and `search_path=log_b` (and
`default_query_exec_mode=simple_protocol`) alternately running `SHOW search_path`: when the
parameter leaks, A's second query reports `log_b`.

## 7. Fault injection

[`integration/fault/s3pg`](/integration/fault/s3pg/) runs several conformance replicas, each
behind its own PostgreSQL TCP proxy (resets, ambiguous `COMMIT`s, blackholes, partitions) and S3
HTTP proxy (errors, drops, ambiguous and delayed writes, stalls, partitions), optionally with
`SIGKILL`ed replicas and a `kill -9`ed postmaster, and verifies: no acknowledged entry lost,
moved or duplicated, no checkpoint regression, fork or inconsistency (`cpwatch`), settled
database invariants (`dbcheck -settled`), object-level consistency (`objcheck.py`) and
`cmd/fsck`. Prerequisites (a disposable cluster on `PGPORT`, the binaries in `BIN`) are in its
README.

```sh
NAME=B SEED=1 REPLICAS=3 WRITE_SECS=150 \
  PG_FAULTS="-reset_prob=0.7 -reset_mean_bytes=15000 -commit_ambig_prob=0.2 -blackhole_prob=0.05" \
  S3_FAULTS="-w_err=0.1 -w_drop=0.05 -w_ambig=0.3 -w_stall=0.01 -r_err=0.05 -r_drop=0.03" \
  ./integration/fault/s3pg/run.sh
./integration/fault/s3pg/repro_delayed_checkpoint.sh    # F1: delayed checkpoint PUT
./integration/fault/s3pg/repro_lockloss_checkpoint.sh   # F1: lock lost while the PUT is in flight
./integration/fault/s3pg/repro_stale_guard.sh           # stale-view false positive (fixed in 3c22ab3)
S3_LATENCY=20ms ./integration/fault/s3pg/repro_slowpath.sh   # slow-path stall (fixed in 3c22ab3)
S3_LATENCY=100ms WRITE_SECS=50 WRITERS=192 ./integration/fault/s3pg/repro_slowpath.sh   # wedge left on 3c22ab3 (re-run on 6f2d29c)
```

* **First campaign** (11 runs, before `7107823`): 82,790 acknowledged writes, 1,177 ambiguous
  commits, 450 mid-stream PostgreSQL resets, 34 blackholes, 1,220 ambiguous S3 writes, 2,356
  idempotency (`412`) retries; 0 lost, wrong, duplicated or forked entries. Its findings F1–F5
  are fixed in `7107823`, whose message lists them together with the findings of the MinIO and
  PgBouncer tests, among them F1 (a checkpoint `PUT` outliving its publisher's transaction could
  roll the published checkpoint back; fixed by conditional checkpoint writes), F3 (with the
  database behind the bucket, integration could write partial objects contradicting full ones;
  fixed by the bucket consistency check) and F4 (guards now trip a sticky flag that stops
  sequencing and is reported by `CoordinationError()`).
* **Second campaign** (`a945b03`): 12 runs of scenarios A, B, C and F plus 18 lost-commit runs
  (8). The F1 and F3 fixes held: 0 checkpoint regressions, forks or inconsistent pairs. It found
  two defects, fixed in `3c22ab3` (details in the harness README):
  * a false-positive trip on a healthy log after a lost row lock (`repro_stale_guard.sh`: 3/3
    runs, tripping after 3–4 connection resets): guards now confirm with a fresh read of
    `IntCoord` (`errStaleView`);
  * integration wedged in the bucket-consistency slow path under S3 latency (`repro_slowpath.sh`
    with `S3_LATENCY=20ms`: no progress for 6 minutes): the slow path now runs concurrently.
* **Final re-verification** (`3c22ab3`; details in the harness README):
  * `repro_stale_guard.sh`, 8 runs (525 PostgreSQL connection resets in total, checkpoint `GET`s
    delayed 3–8s): 0 trips, 0 tripped markers, 70 harmless `errStaleView` retries. Before the
    fix, 3/3 runs tripped.
  * `repro_slowpath.sh`: a backlog of ~119 bundles at 20ms per S3 call caught up 9.1s after the
    integration's connection was killed, ~121 bundles at 50ms in 10.3s (before the fix: no progress
    for more than 6 minutes). Still **wedged** with ~161 bundles at 50ms and ~119 bundles at 100ms:
    a retry costs about six times the S3 requests of a first attempt, all within the fixed 10s
    integration timeout, and keeps nothing when it runs out. Addressed in `6f2d29c` by the adaptive
    integration limit; the re-run is in progress ([11](#11-final-round-6f2d29c)).
  * `run.sh`, A/B/C scenarios ("A / F": A acknowledged entries, final log size F): B3 seed 2
    (before the fix: stuck at 0) 7,397 / 9,086; C3 seed 3 (before: stuck at 544) 6,636 / 6,958;
    A2 seed 1 9,802 / 10,347; B2 seed 3 6,512 / 8,188. All with 0 lost, wrong, duplicated or
    forked entries, 0 trips, `fsck` OK.
  * F1 repros (`repro_delayed_checkpoint.sh`, `repro_lockloss_checkpoint.sh`), 2 runs each: 0
    checkpoint regressions.

## 8. Lost commits and the recovery runbook

`integration/fault/s3pg/lostcommit.sh` snapshots the coordination tables (`SNAP=sql`, or the
whole data directory with `SNAP=datadir`) and restores them while replicas run, so the database
loses commits the bucket already reflects: `MODE=behind` (published checkpoint beyond the
snapshot), `MODE=seqintact` (integration commits lost, `Seq` rows intact: the driver stays
fail-closed, by design), `MODE=partial` (published < database < bucket). It checks that the
published checkpoint never regresses or forks and that no contradicting objects are written,
restarts the replicas (start-up guard; on `3c22ab3` also the tripped marker), and with
`RECOVER=1` applies the runbook of [README.md](README.md#recovering-from-a-lost-commit) (without
antispam) and verifies the recovered log (chaos verification, `dbcheck -settled`, `cmd/fsck`,
`objcheck.py`). On `3c22ab3` it also checks that, with the database reconciled, a writer is still
refused while the marker exists, and then deletes the marker as the last step before restarting.

```sh
MODE=behind SNAP=sql WHEN=int_ahead SEED=1 ./integration/fault/s3pg/lostcommit.sh
MODE=seqintact SEED=1 ./integration/fault/s3pg/lostcommit.sh
MODE=partial SEED=1 SCHEMA=1 ./integration/fault/s3pg/lostcommit.sh
```

* Second campaign (`a945b03`, before the tripped marker existed): the runbook recovered the log
  in 18 of 18 lost-commit runs.
* Final re-verification (`3c22ab3`, with the tripped marker), 9 runs: `MODE=behind` x2,
  `MODE=seqintact` x1, `MODE=partial` with `SNAP=sql` x2 and with `SNAP=datadir` x4. 0 checkpoint
  regressions, forks or inconsistent pairs; the guards tripped 0.01–2.5s after the restore; 0
  entries acknowledged after a trip, and 0 by restarted replicas (12–21 refused restarts per
  replica; before the marker, restarted replicas had handed out 16–104 indices per partial run); the
  runbook, including deleting the marker as its last step, recovered 9/9.
* Not re-run on `6f2d29c`, which changes when the marker is written (not by start-up refusals)
  and adds the high-water mark ([11](#11-final-round-6f2d29c)).
* `synchronous_commit = off` at system, database and role level, with crashes of PostgreSQL: with
  the driver's per-transaction override (`internal/pgtx`), no acknowledged index regressed in 5
  runs; a control without the driver lost 44–56 acknowledged commits. The harness is not in the
  repository; the check with the driver can be repeated with `run.sh` (`PGKILL=1`) against a
  disposable cluster configured with `ALTER SYSTEM SET synchronous_commit = off`, whose chaos
  verification fails if an acknowledged entry is lost or moved.

## 9. Chaos and load

[`integration/chaos/s3pg`](/integration/chaos/s3pg/): two replicas share a log while a writer
records every acknowledged entry; replicas are `SIGKILL`ed and `SIGSTOP`ped, all database
sessions terminated and PostgreSQL restarted; then no acknowledged entry may be lost, moved or
duplicated, every observed checkpoint must be consistent with the final one, and `cmd/fsck`
must pass. Results on MinIO are in [5](#5-real-minio).

```sh
PG_ADMIN_DSN='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
S3_ENDPOINT=http://127.0.0.1:9000 S3_ACCESS_KEY=test S3_SECRET=test BUCKET=chaos-test \
PG_RESTART_CMD='...' ./integration/chaos/s3pg/run.sh
```

Load against two running servers of one log (e.g. started like `run.sh` does) with the upstream
hammer (`--max_runtime` ends the run with exit status 1 by design; judge it by the write errors
it reports):

```sh
go run ./internal/hammer --log_url=<read URL>/ \
  --write_log_url=http://localhost:2024 --write_log_url=http://localhost:2025 \
  --log_public_key=<key> --num_writers=256 --max_write_ops=1024 --max_runtime=10m --show_ui=false
```

## 10. Mutation testing

Single mutations of the driver (e.g. dropping a guard, the trailing slash in `key()`, pool
closing, `MaxIdleConns` semantics) are applied to a scratch copy, and the targeted tests must
fail. The scripts are not in the repository.

* First round: 31 mutants, all killed except 6 that were equivalent or showed test gaps; the
  gaps were closed since.
* Second round (daa39cd): 86 mutants, 83 killed, 3 equivalent.
* Third round: 8 targeted mutants of the `3c22ab3` fixes, all killed.
* Final round: 9 targeted mutants of the `6f2d29c` fixes, all killed. They covered, among
  others, the transaction-bound read, the high-water check, both `observe` paths, the adaptive
  limit, the start-up marker and the persistence of a sequencing trip.

## 11. Final round (6f2d29c)

The adversarial review of `3c22ab3` found no safety bug. Its findings, and the slow-path wedge
left by the fault re-run ([7](#7-fault-injection)), are fixed in `6f2d29c`:

* **Slow-path wedge**: the integration job halves its `Seq`-row limit after a failed attempt and
  doubles it after a success (upstream uses a fixed limit), so every attempt eventually fits in
  the 10s timeout.
* **Pool starvation**: with `MaxOpenConns = 1`, `confirmBehind` could not get a connection, so a
  database that was really behind never tripped. The guards' reads inside
  `consumeEntries`/`publishCheckpoint` now run on the transaction's own connection.
* **Standby reads** (e.g. through a load-balancing pooler) could make the guards false-trip:
  `currentTree` fails if `pg_is_in_recovery()`, an error rather than a trip. Verified against a
  real streaming replica (`TestCurrentTreeRefusesStandby` with `TESSERA_S3PG_TEST_STANDBY_DSN`).
* **Stale indices before the next integration**: running instances could hand out indices of the
  published log after lost commits until their next integration or publication attempt (relevant
  for static CT, whose SCTs carry the index). Sequencing now refuses, and trips, below the largest
  size the instance has seen published or integrated (in-process high-water mark).
* **Start-up marker**: a misconfigured instance refused at start-up wrote the marker and so
  blocked healthy instances. Start-up refusals no longer write it (run-time trips by integration,
  publication and sequencing do); `onBehind` (`CoordinationError()`) is set before the marker
  write, which now has a 10s deadline.
* **Confirm-failure message**: it quoted `ErrCoordinationBehind`'s text, so log-based alerts
  fired on a non-trip; it now reads `could not confirm a suspected coordination lag ...`.
* **Fast-path comment** of `checkBucketConsistent`: now documents the blind spot (see
  [Known limitations](#known-limitations-and-residual-risks)).
* Conformance server: no shutdown wait for un-integratable entries once tripped (exit status 1);
  `TestGarbageCollect` fails fast on `Add` errors instead of hanging.

Verification of `6f2d29c`: a regression test for each fix ([1](#1-go-test-suite)), 9 targeted
mutants all killed ([10](#10-mutation-testing)), the standby check against a real streaming
replica (and the control against the primary), the full Go suite against real MinIO (117
top-level tests pass, the only skip being the standby test, which needs a replica), and a focused
fault-injection re-run (`integration/fault/s3pg/README.md`, "Final re-verification (HEAD
6f2d29c)"):

* **Slow path** (`repro_slowpath.sh`, 17 runs, all with 0 trips, 0 lost/duplicated/misplaced
  entries, fsck OK): the variants that stayed wedged on `3c22ab3` (~121 bundles at 100ms, ~161 at
  50ms) caught up at the first retry in 10.1-12.8s (2 runs each). Under that run's lighter load the
  old binary also caught up, so harsher variants were added with `FRESH=1` (retries start from the
  full batch): at 150-200ms per S3 call the batch halved over 8-9 failed attempts (to 40-80 rows)
  and the backlog was integrated within 89-99s, i.e. a bounded stall rather than a wedge.
* **Stale guard** (`repro_stale_guard.sh`, 3 runs, 182 connection resets): 0 trips, 0 markers; the
  10 suspected lags were each resolved by the transaction-bound confirm read failing on the reset
  connection (a retry, not a trip).
* **Lost commits** (scenario D, partial ×4, behind ×1): every replica tripped at its first
  sequencing after the restore via the high-water check (0.04-0.57s); 0 entries acknowledged and 0
  indices handed out after the restore (on `3c22ab3`: 0-5 acknowledgements and 18-133 indices per
  run); restarted replicas acknowledged 0 of 104-177 attempts each (12-15 refused starts citing the
  marker); 0 regressions/forks/inconsistent pairs; runbook recovery 5/5.
* **`run.sh`**: B3 seed 2 (7,543 acked / 9,422 final), C3 seed 3 (7,022 / 7,480, with 11 crashes,
  4 postmaster kills and partitions), A2 seed 1 with `--pg_schema` (10,063 / 10,546): all 0
  lost/wrong/duplicated/forked, 0 trips, fsck OK.

## Known limitations and residual risks

* **Lost commits still lose entries.** The guards protect the published log, not entries:
  entries sequenced (or integrated) but not yet published when the database lost commits are
  gone, and their submitters hold indices that will be reused. Since `6f2d29c` a running instance
  never hands out an index below its in-process high-water mark (what it has seen published or
  integrated), but until it trips it still accepts entries at indices above that mark: indices
  that were only sequenced, or that other instances integrated or published since it last
  looked; a restarted instance's mark starts at the published size. Synchronous replication (and
  no restores of the database older than the bucket) is required, see
  [README.md](README.md#high-availability-and-durability).
* **Blind spot of the bucket check's fast path** (documented at `checkBucketConsistent` in
  `guard.go`): if a failed integration attempt wrote later bundles but not the one containing
  `IntCoord.seq` (they are written concurrently), and the database then also lost those `Seq`
  rows, the check passes; integrating different entries there then fails on the conditional
  write (`differs from data to-be-written`), so integration stalls, retrying, without tripping.
  A stall, never a fork; it must be noticed by monitoring (README, "Detecting").
* **Recovery is manual** and must be rehearsed: it rewrites the coordination tables and deletes
  objects. While the tripped marker exists, no appender starts, so a run-time trip (integration,
  publication or sequencing) stops the log until an operator acts. Since `6f2d29c` a refusal at
  start-up (e.g. of an instance started against the wrong database) writes no marker and blocks
  only that instance. If writing the marker fails (it is bounded by 10s), only the start-up guard
  protects restarts.
* **The driver must only talk to the primary.** `currentTree` (start-up guard, the guards'
  confirming re-read, `LogReader.IntegratedSize`) refuses an answer from a standby, as an error
  rather than a trip (verified against a real streaming replica), but other reads outside a
  transaction (the pushback check, `LogReader.NextIndex`) are not checked. Load-balancing poolers
  (e.g. Pgpool-II) and multi-host DSNs without `target_session_attrs=read-write` must not be used.
* **PgBouncer leaks a `search_path` startup parameter** between clients (1.26.0 with PostgreSQL
  16, re-checked 2026-10). This is a PgBouncer problem; the driver is immune only with
  `Config.Schema` / `AntispamOpts.Schema`.
* **Checkpoint publication requires `If-Match`** on `PutObject`: a store that ignores it is
  unsafe, and one that rejects it cannot publish. `If-Match` was verified on a MinIO source build
  (`7aac2a2`) and moto 5.2.3, not on every MinIO release: probe the release you run.
* **Guard overhead**: one checkpoint `GET` and two `LIST`s per integration; if the bucket is ahead
  (a retry after a failed commit), a `LIST` and up to two `GET`s per touched bundle and level-0
  tile, 16 at a time.
* **Timeouts are upstream's compile-time constants**: `assignEntries` 2s, integration 10s, GC
  30s. A database (or PgBouncer queue) slower than 2s, or extreme load on the server, makes `Add`
  fail on the 2s assign timeout (`TestGarbageCollect` now fails fast on such errors on a heavily
  loaded machine instead of hanging). Integration adapts its batch size to the 10s instead
  (`6f2d29c`); until the re-run in [11](#11-final-round-6f2d29c) reports, that the adaptive limit
  resolves the slow-path wedge seen on `3c22ab3` is shown only by `TestIntegrationLimitAdapts`.
* The guards trust the published `checkpoint` object (its signature is not verified) and cannot
  detect a bucket that went back in time or a replaced checkpoint. The start-up guard only
  refuses a database that is behind the published checkpoint (or at its size with another
  root), not a larger one, e.g. another, larger log's. A `MigrationWriter` has no guards.
* CI tests the S3 code paths against moto, which does not enforce access control like MinIO; the
  PgBouncer, `synchronous_commit` and mutation harnesses are not in the repository, and CI does not
  run the standby test (`TESSERA_S3PG_TEST_STANDBY_DSN` unset, so it is skipped).
