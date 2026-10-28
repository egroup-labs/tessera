# Tessera on S3-compatible storage + PostgreSQL (`s3pg`)

Operator guide for `storage/s3pg`, a port of the [`storage/aws`](../aws/) driver which
replaces Aurora MySQL with PostgreSQL and works with any S3-compatible object store that
meets the requirements below (MinIO in particular). Semantics are identical to
`storage/aws`, except for the upstream bugs fixed here, `MaxIdleConns`, optional
schema-qualified SQL (`Config.Schema`), transactions pinned to `READ COMMITTED` with durable
commits, an integration batch size that adapts to failed attempts, a refusal to read the log's
state from a standby, and the fail-closed protection of the published log: conditional checkpoint
writes and guards against a coordination database which is behind the bucket, which stop the log
(and keep it stopped across restarts) until an operator has recovered it (see
[High availability and durability](#high-availability-and-durability) and
[Keeping in sync with upstream](#keeping-in-sync-with-upstream-storageaws)). Read the upstream
[README](../aws/README.md) for the design and the life of a leaf, and
[VERIFICATION.md](VERIFICATION.md) for how this driver has been tested.

A runnable example (PostgreSQL + MinIO + the conformance server, least-privilege roles,
schema applied by a migration step) is in [`deployment/live/s3pg`](/deployment/live/s3pg/).

## Architecture

**Object storage** holds the log itself, and is what clients read (directly, or via a CDN):

| Object                          | Written by          | Notes                                        |
| ------------------------------- | ------------------- | -------------------------------------------- |
| `checkpoint`                    | publisher           | overwritten, but only conditionally: `If-Match` on the ETag read before publication (`If-None-Match: *` if there is none); `Cache-Control: no-cache` |
| `tile/<L>/<N>[.p/<W>]`          | integration         | immutable, written with `If-None-Match: *`   |
| `tile/entries/<N>[.p/<W>]`      | integration         | entry bundles; immutable, `If-None-Match: *` |
| `s3pg-coordination-tripped`     | the first guard trip of a running appender (integration, publication or sequencing; not a refusal at start-up) | never present in a healthy log; while it exists, appenders refuse to start (see [A trip is sticky and persistent](#a-trip-is-sticky-and-persistent)); deleted only by the operator |

Everything is stored under `Config.BucketPrefix`, if set. Partial (`.p/`) tiles and bundles
are deleted by the garbage collector once a published checkpoint covers the full version.

**PostgreSQL** coordinates the writers (any number of server replicas may share one log).
The tables (unquoted identifiers, so PostgreSQL stores them in lower case) are defined in
[`schema.sql`](schema.sql), which is also exported as `s3pg.SchemaSQL`:

| Table      | Contents                                                                         |
| ---------- | -------------------------------------------------------------------------------- |
| `Tessera`  | one row: `compatibilityVersion` of the stored data (`SchemaCompatibilityVersion`) |
| `SeqCoord` | one row: `next`, the next index to assign                                         |
| `Seq`      | queue of sequenced-but-not-integrated batches, keyed by first index; `v` is a gob of the batch (full entry data) |
| `IntCoord` | one row: size (`seq`) and `rootHash` of the integrated tree                       |
| `PubCoord` | one row: when (`publishedAt`, ns) and at what `size` the checkpoint was last published |
| `GCCoord`  | one row: garbage collection progress (`fromSize`)                                 |

Writers coordinate through row locks (`SELECT ... FOR UPDATE`) on the single-row tables.
Some transactions deliberately stay open while talking to object storage, which matters for
[timeouts](#timeouts):

| Transaction (lock held)             | Does inside the transaction                         | Bounded by                                    |
| ----------------------------------- | --------------------------------------------------- | --------------------------------------------- |
| sequencing (`SeqCoord`)             | check `next` against the process's [high-water mark](#fail-closed-guards), insert one `Seq` row per batch | 2s (`defaultAssignEntriesTimeout`)            |
| integration (`IntCoord`, `Seq` rows) | read up to `limit` `Seq` rows (see [Timeouts](#timeouts): adapts after failures), read the published `checkpoint`, list the current entry bundle and level-0 tile (two `LIST`s in parallel; only if the bucket is ahead, compare every bundle and level-0 tile of the batch, 16 at a time), write entry bundles + tiles, delete `Seq` rows | 10s (`defaultIntegrationTimeout`; also the forced integration at start-up) |
| publication (`PubCoord`)            | read the published `checkpoint` and its ETag (and, if growing, the tiles for a consistency proof), sign, witness/mirror, write `checkpoint` conditionally | `CheckpointPublicationTimeout` (default 5s; raised to the witness/mirror timeouts if larger) |
| garbage collection (`GCCoord`)      | list + delete obsolete partial objects               | 30s (`defaultGCTimeout`)                       |
| antispam follower (`AntispamFollowCoord`) | read entry bundles, insert into `AntispamIDSeq` | 10s (`defaultBatchTimeout`)                    |
| `MigrationWriter` integration (`IntCoord`) | read bundles, write tiles                      | the caller's context only                      |

## Requirements

* **PostgreSQL 13 or newer** (`schema.sql` uses the `autovacuum_vacuum_insert_*` storage
  parameters added in 13). Every transaction explicitly requests `READ COMMITTED`, which the
  row-locking protocol relies on, so a stricter `default_transaction_isolation` for the server,
  database or role does not break it. Every transaction also raises `synchronous_commit` to `on`
  for itself (`set_config(..., true)`) if the session's value is `off`, `local` or
  `remote_write`; `on` and `remote_apply` are left alone. A commit which is acknowledged and then
  lost lets the driver hand out indices that were already returned to clients. In crash tests with
  `synchronous_commit = off` at server, database or role level, no acknowledged index was handed
  out again with the driver, while a control without it lost 44-56 acknowledged commits (see
  [VERIFICATION.md](VERIFICATION.md)). Consequences: with synchronous replication configured
  (`synchronous_standby_names`), the driver's commits wait for the synchronous standbys to flush
  even if the role is set to `local` or `remote_write`; and the override does nothing for
  asynchronous replication, where a standby may never have received an acknowledged commit (see
  [High availability and durability](#high-availability-and-durability)). Every connection must
  go to the primary, never to a standby ([Connect only to the primary](#connect-only-to-the-primary)).
* **S3-compatible object storage** which:
  * supports conditional `PutObject`, and answers a failed condition with
    `412 PreconditionFailed` (AWS's `409 ConditionalRequestConflict` for racing conditional
    writes is treated the same):
    * `If-None-Match: *` must fail if the object exists. Tessera relies on this to never
      overwrite a tile or bundle with different content. MinIO supports it from
      `RELEASE.2024-05-07T06-41-25Z`.
    * `If-Match: <ETag>` must fail if the object's current ETag differs. Every checkpoint write
      uses it (see [High availability and durability](#high-availability-and-durability)). If the
      object doesn't exist, MinIO and moto answer `404 NoSuchKey`, which the driver also treats as
      a failed condition.

    Stores which silently ignore either header are **not safe**, and a store without `If-Match`
    support cannot publish checkpoints with this driver. Verify both for the store and release you
    run with the probes in [`integration/minio/s3pg`](/integration/minio/s3pg/) (`ifmatch`,
    `ifmatch-race`, `inm-race`; see [VERIFICATION.md](VERIFICATION.md)). They passed on MinIO built
    from source (commit `7aac2a2`, 4-drive erasure set); moto 5.2.3, used by the test suite,
    honours both headers too.
  * is strongly consistent (read-after-write, including after overwrite and delete, and
    list-after-write), since integration reads back and lists partial tiles/bundles written
    moments earlier, possibly by another replica. MinIO and AWS S3 are.
  * supports `ListObjectsV2` (integration and garbage collection) and `DeleteObjects` (garbage
    collection).
  * for MinIO, path-style addressing: set `UsePathStyle` and `BaseEndpoint` via
    `Config.S3Options`, which is honoured even when `SDKConfig` is nil (the conformance
    server's `--s3_endpoint` does this).
* Object store permissions for the writer: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`
  on the objects and `s3:ListBucket` on the bucket. Readers need only `s3:GetObject`.
* If bucket versioning is enabled, add a lifecycle rule expiring non-current versions:
  the garbage collector and checkpoint updates otherwise accumulate old versions forever.

**MinIO notes.** MinIO evaluates a conditional `PUT` before reading the request body; for large
bodies (MiB range) it rejects a failed condition without draining the body and the connection is
closed, so the client may get no response at all (`connection reset by peer`, `broken pipe`).
The driver treats every conditional write without an HTTP response as of unknown outcome and
reads the object back: if it holds exactly the data written, the write succeeded. With a
`GetObject`-only bucket policy MinIO refuses anonymous `PUT`s, whereas moto accepts them, so
don't use moto to test access control.

## One schema per log

All tables of a log live in one PostgreSQL schema. Name it with `Config.Schema`
(`--pg_schema` in the conformance server): every statement then names its tables fully
(`"mylog".SeqCoord`, ...), whatever the session's `search_path` is, which also makes it safe
behind a connection pooler ([PgBouncer](#pgbouncer)). The schema must already exist (see
[Roles and least privilege](#roles-and-least-privilege)). Its name is used exactly as given, as
a quoted identifier (any name works, `$` included): use the lower-case name PostgreSQL stores for
an unquoted one. Without `Config.Schema`, tables are resolved via the session's `search_path`, and
created in its first schema; this is only safe with a direct connection (or a runtime role per
log with `ALTER ROLE ... SET search_path`), never through a pooler.

Logs in different schemas are completely independent, so one database can host many logs. A
log's antispam tables (`AntispamMeta`, `AntispamIDSeq`, `AntispamFollowCoord`, see
[`antispam/schema.sql`](antispam/schema.sql)) are named with `AntispamOpts.Schema`
(`--antispam_pg_schema`) in the same way; they may share the log's schema (table names and
schema initialisation lock are distinct) or have their own.

Never point two logs at the same schema, or two logs' servers at the same bucket + prefix.

## Roles and least privilege

Use two roles: an **owner** that owns the schema and applies DDL (only used by your
migration tooling), and a **runtime** role for the Tessera servers with DML only:

```sql
CREATE ROLE tessera_owner LOGIN PASSWORD '...';
CREATE ROLE tessera_app   LOGIN PASSWORD '...';
REVOKE ALL ON DATABASE tessera FROM PUBLIC;
-- Before PostgreSQL 15, every role may create objects in the public schema (see below):
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE tessera TO tessera_owner, tessera_app;

CREATE SCHEMA mylog AUTHORIZATION tessera_owner;
GRANT USAGE ON SCHEMA mylog TO tessera_app;
-- Tables created by tessera_owner from now on:
ALTER DEFAULT PRIVILEGES FOR ROLE tessera_owner IN SCHEMA mylog
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO tessera_app;
-- ...and any which already exist:
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA mylog TO tessera_app;

ALTER ROLE tessera_app SET idle_in_transaction_session_timeout = '60s';
ALTER ROLE tessera_app SET statement_timeout = '60s';
ALTER ROLE tessera_app SET lock_timeout = '60s';
```

Before PostgreSQL 15, `CREATE` on schema `public` is granted to `PUBLIC` in *every* database:
run the `REVOKE CREATE ON SCHEMA public FROM PUBLIC` in each database the runtime role can
connect to (by default every role may connect to `postgres` and to any database created without
`REVOKE ALL ON DATABASE ... FROM PUBLIC`), or revoke `CONNECT` on those databases from `PUBLIC`.

The runtime role needs no sequences, functions, `TRUNCATE`, `TEMPORARY` or `CREATE`. It
**must** run with `Config.SkipSchemaInit = true` (`--skip_schema_init` in the conformance
server): otherwise start-up applies `schema.sql`, whose `CREATE TABLE IF NOT EXISTS` PostgreSQL
rejects without `CREATE` on the schema *even when the tables exist*. With `SkipSchemaInit` no DDL
is attempted. (Without it, the role needs `CREATE` on the schema, and owns the tables if it is the
one that creates them, so it could also drop them.)

Either way, start-up then verifies that `Tessera.compatibilityVersion` equals
`SchemaCompatibilityVersion`, that the initial rows of `SeqCoord`, `IntCoord`, `PubCoord` and
`GCCoord` exist, and that `Seq` is readable, and fails otherwise: a missing or partially applied
schema is reported at start-up, not by the first write. The antispam store checks
`AntispamMeta.compatibilityVersion`.

### Applying the schema with migration tools

`schema.sql` is idempotent and never resets an existing log, so it can be applied on every
deploy, as the owner, with `search_path` set to the log's schema. Connect directly, not through
PgBouncer: `PGOPTIONS` is sent as a startup parameter (see [PgBouncer](#pgbouncer)). A
`SET LOCAL search_path` inside the transaction, as in
[`deployment/live/s3pg/migrate.sh`](/deployment/live/s3pg/migrate.sh), needs no startup
parameter.

```sh
PGOPTIONS='-c search_path=mylog' psql "postgres://tessera_owner@db:5432/tessera" \
  -v ON_ERROR_STOP=1 --single-transaction -f storage/s3pg/schema.sql
```

Go programs can apply `s3pg.SchemaSQL` (and `antispam.SchemaSQL`) themselves; with
`Config.Schema` set and `SkipSchemaInit` unset, the driver applies it in that schema (with a
transaction-local `search_path`). For versioned tools, [`migrations/`](migrations/) holds the
same schema as ordered plain-SQL files named `NNNNNN_description.up.sql` (golang-migrate's
convention; usable by any tool that applies ordered SQL files). `000001_initial.up.sql` is a
byte-for-byte copy of `schema.sql` for `SchemaCompatibilityVersion` 1, and `migrations_test.go`
fails if applying the migrations in order stops being equivalent to applying `schema.sql`. Make
sure the tool's own bookkeeping table ends up where you want it (by default, in the first schema
of its `search_path`).

Only the log schema has a `migrations/` copy: apply the antispam schema from
[`antispam/schema.sql`](antispam/schema.sql) (also idempotent) in the same way.

## Connection pooling

Each `Appender` (and each `MigrationWriter`) opens its own pool from `Config.DSN`; the
antispam store opens another. Concurrency per server is low (a few sequencing transactions,
one integration, one publication, one GC, plus reads), so small pools are enough:

* Set `MaxOpenConns` (e.g. 8-16; zero means unlimited) and make sure
  `replicas x (log pool + antispam pool)` stays well below `max_connections`. The guards' reads
  during integration and publication use that transaction's own connection, so they never need a
  second one and even `MaxOpenConns = 1` is safe (before `6f2d29c`, such a pool kept a database
  that was really behind from ever tripping). A pool that small serialises everything, though:
  while integration holds the connection, sequencing waits and can exceed its 2s timeout.
* `MaxIdleConns`: `0` keeps the `database/sql` default (2 idle connections), `> 0` keeps that
  many, `< 0` keeps none, so every query opens a new connection (expensive with PostgreSQL).
  This intentionally differs from `storage/aws`, where `0` disables idle connections.
  `AntispamOpts.MaxIdleConns` behaves the same, so `--pg_max_idle_conns` means the same for
  both pools. Setting it equal to `MaxOpenConns` is reasonable.
* For anything else (`ConnMaxLifetime`, a custom pgx tracer, `pgxpool` via
  `stdlib.OpenDBFromPool`), open the `*sql.DB` yourself and pass it as `Config.DB` (or to
  `antispam.NewAntispamWithDB`); `DSN`, `MaxOpenConns` and `MaxIdleConns` are then ignored and
  Tessera never closes it. The handle **must** use pgx's `database/sql` driver
  (`github.com/jackc/pgx/v5/stdlib`: `sql.Open("pgx", ...)`, `stdlib.OpenDB` or
  `stdlib.OpenDBFromPool`); other drivers are unsupported, as queries pass Go slices as
  PostgreSQL arrays (`= ANY($2)` with `[]int64`, `unnest($1::bytea[], ...)`).
* The DSN is parsed by [pgx](https://pkg.go.dev/github.com/jackc/pgx/v5#ParseConfig), so the
  standard `PG*` environment variables (`PGPASSWORD`, `PGPASSFILE`, `PGSSLROOTCERT`, ...)
  work and keep secrets out of the DSN. Use `sslmode=verify-full` across networks.

`(*s3pg.Storage).Close()` closes the pools that the `Storage` opened from `Config.DSN` (never a
`Config.DB` handle), and `(*antispam.AntispamStorage).Close()` likewise closes only a pool
opened by `NewAntispam`. `s3pg.New` returns a `tessera.Driver`, so type-assert it, and call
`Close` only once the appender (or migration writer) and its `LogReader` are no longer used,
including by their background work:

```go
driver, err := s3pg.New(ctx, cfg)
appCtx, cancel := context.WithCancel(ctx) // scopes integration, publication, GC, followers
appender, shutdown, reader, err := tessera.NewAppender(appCtx, driver, opts)
// ... serve ...
err = shutdown(ctx) // wait for added entries to be integrated and published
cancel()
err = driver.(*s3pg.Storage).Close()
```

If `Appender()` fails after opening its pool, it closes that pool itself.

### PgBouncer

Verified with PgBouncer 1.22.0 and 1.26.0 in transaction pooling mode, with two logs sharing
one runtime role and one pool, including DSNs deliberately carrying the *other* log's
`search_path`: no cross-log effects (see [VERIFICATION.md](VERIFICATION.md)). Configure:

1. **Always select the schemas with `Config.Schema` / `AntispamOpts.Schema`** (`--pg_schema` /
   `--antispam_pg_schema`). Never put `search_path` or `options` in the DSNs, and never use
   PgBouncer's `track_extra_parameters` or `ignore_startup_parameters` for `search_path`. PgBouncer
   does not handle a `search_path` startup parameter safely: depending on version and settings it
   rejects the connection (PgBouncer 1.22 by default: `unsupported startup parameter:
   search_path`), silently drops the parameter (statements then run in the server's default
   `search_path`), or **leaks** it to other clients that share a server connection, so that one
   client's statements run in another client's schema. PgBouncer 1.26.0 with PostgreSQL 16 still
   leaks it (re-checked 2026-10). Schema-qualified statements are immune to all of these.
2. **Transaction pooling** (`pool_mode = transaction`, recommended) needs either PgBouncer's
   prepared statement support, `max_prepared_statements` > 0 (e.g. 200, the default in
   PgBouncer 1.26), or `default_query_exec_mode=simple_protocol` (or `exec`) in the DSNs: pgx
   caches named prepared statements by default, and with `max_prepared_statements = 0` they fail
   with SQLSTATE `42P05` (prepared statement already exists) or `26000` (does not exist). With
   prepared statements, a statement that fails while being prepared (e.g. because a table is
   missing) poisons that client connection: further statements on it fail with `08P01`
   (`prepared statement name is already in use`) until `database/sql` discards it and
   reconnects. A server connection is held for a whole transaction, including integration (up to
   10s) and garbage collection (up to 30s) while they wait for object storage, so size the pool
   for every replica's concurrent transactions.
3. **Session pooling** (`pool_mode = session`) also works, with or without prepared statements,
   but doesn't multiplex: each client connection holds a server connection. Size
   `default_pool_size` (and `max_db_connections` / `max_user_connections`, if set) above the sum
   of all replicas' pools (`replicas x (log MaxOpenConns + antispam MaxOpenConns)`). Otherwise
   clients queue in PgBouncer, and a sequencing transaction that has to wait longer than the 2s
   of `assignEntries` (`defaultAssignEntriesTimeout`, as upstream) fails, so `Add` returns an
   error.

Don't use statement pooling. The only session state Tessera sets is transaction-scoped
(`pg_advisory_xact_lock`, a `search_path` while applying the schema, and `synchronous_commit` via
`set_config(..., true)`).

## Timeouts

Integration keeps its transaction open while writing tiles and bundles to object storage, for
up to ~10s, and garbage collection for up to ~30s (see the table under
[Architecture](#architecture)). During that time the session is *idle in transaction*, and
other replicas block on the same row lock. Therefore, for the runtime role:

* `idle_in_transaction_session_timeout`, `statement_timeout`, `lock_timeout` and (PostgreSQL 17+)
  `transaction_timeout` must be comfortably larger than 30s, and larger than any
  publication/witness/mirror timeout you configure. **60s** is a good value: it never fires on a
  healthy server, but cleans up after a hung client.
* Don't set them below ~35s: PostgreSQL would then abort healthy GC/integration transactions,
  which shows up as recurring `integrateEntries`/garbage collector errors in the logs.
* The client-side timeouts are compile-time constants in `s3pg.go`; if object storage is slow
  enough to hit them, integration fails and is retried within a second, and pushback starts once
  `next - IntCoord.seq` exceeds `PushbackMaxOutstanding`.
* Unlike upstream, which always integrates up to `DefaultIntegrationSizeLimit` (20480) `Seq`
  rows per attempt, the integration job halves that limit after every failed attempt (down to
  one row) and doubles it after every successful one (up to `DefaultIntegrationSizeLimit`). A
  failed attempt keeps nothing, and a retry of a large batch whose objects were already written
  (after a lost commit, a connection reset or a timeout) costs about six times the S3 requests of
  the first attempt (the bucket consistency check, then every `If-None-Match` write answered `412`
  and read back), so with a fixed limit such a retry could time out forever under high S3 latency
  (seen in fault injection, see [VERIFICATION.md](VERIFICATION.md)); with the adaptive limit the
  attempts shrink until they fit within the 10s again.

## High availability and durability

PostgreSQL is the source of truth for which entry has which index, while the bucket is always
**at or ahead of** the integration/publication state committed in the database: integration
writes tiles and bundles *before* committing `IntCoord`, and the checkpoint is written before
`PubCoord` is committed. Being behind by such an in-flight step is harmless: the `Seq` rows are
still there, re-doing the work writes identical bytes, and `If-None-Match` makes that idempotent.

Losing commits that were already acted upon is not harmless. That is what happens when
PostgreSQL comes back without its most recent commits: a failover to an asynchronously
replicated standby, a restore (or point-in-time recovery) to a moment older than the bucket,
`fsync = off`, `UNLOGGED` tables. The database then describes an older log than the bucket. (A
`synchronous_commit` of `off`, `local` or `remote_write` no longer has this effect on a single
server, since the driver raises it to `on` for its own transactions; see
[Requirements](#requirements).)

### Fail-closed guards

Where `storage/aws` would publish a smaller checkpoint over the published one and then integrate
different entries at already-published indices (a rollback and a fork, which witnesses and
monitors reject), this driver fails closed ([`guard.go`](guard.go), error
`s3pg.ErrCoordinationBehind`):

* **Start-up**: the appender refuses to start while the
  [tripped marker](#a-trip-is-sticky-and-persistent) exists, or if `IntCoord.seq` is smaller
  than the published checkpoint's size, or equal to it with a different root hash. A refusal at
  start-up only stops the starting process: it writes nothing, not even the marker (start-up
  re-checks every time, and a misconfigured instance must not block correctly configured ones).
* **Sequencing** refuses to hand out an index below the largest log size the process has seen
  published or integrated (an in-process *high-water mark*, raised whenever the process reads the
  published checkpoint, i.e. at start-up and in each integration or publication it attempts, and
  whenever it integrates entries), and trips. In a healthy log `SeqCoord.next` is never below
  that, so this only fires after the database lost commits, typically at the first `Add` after
  the failover, before any integration or publication attempt could notice. It matters most for
  static CT, where the index goes into the SCT returned to the submitter: an index handed out
  twice is a signed promise that will be broken. The mark lives in the process and starts from the
  published checkpoint at start-up, so it does not cover indices the process hasn't seen (see
  [Entries are still lost](#entries-are-still-lost)).
* **Integration** refuses to write any bundle or tile while the published checkpoint is larger
  than `IntCoord.seq`. It also checks the bucket itself, with one listing each (in parallel) for
  the entry bundle and the level-0 tile containing index `IntCoord.seq`. Only if the bucket
  already holds a wider version of either (data beyond the database's integrated size) are all
  bundles and level-0 tiles touched by the batch listed, read and compared with the entries about
  to be integrated, up to 16 at a time. Identical data is normal (a retry after a failed
  database commit). Different data means the database lost commits whose objects had already
  been written: integrating other entries there would leave partial and full versions of a
  bundle or tile which contradict each other. The cost is one `GET` of the checkpoint and two
  `LIST` requests per integration.
* **Publication** only replaces the checkpoint with an append-only extension of it: never a
  smaller one, at the same size only with the same root, and a larger one only if a consistency
  proof, built from the log's own tiles, verifies against the published root.
* **The checkpoint write is conditional** on the checkpoint the publication guard checked:
  `If-Match` with its ETag, or `If-None-Match: *` if there was none. The `PubCoord` row lock
  serialises publishers only while their transaction lives: without the condition, a `PUT` still
  in flight when its publisher's transaction had ended (e.g. delayed in the network, or slow while
  the publisher lost its database connection and with it the lock) could land on top of a newer
  checkpoint and roll the published log back. If the condition fails (`precondition failed:
  "checkpoint" changed concurrently`, or `... was deleted concurrently` for a `404` answer to
  `If-Match`), `PubCoord` is not updated and publication is retried at the next interval. A write
  without an HTTP response is resolved by reading the checkpoint back.
* **A stale view does not trip.** Before tripping because the published checkpoint is larger than
  the size it is working with, the integration and publication guards re-read `IntCoord.seq` with
  a new statement on their transaction's own connection (`READ COMMITTED`: a fresh snapshot; no
  second connection needed). If that has caught up with the published size, only this instance's
  view was stale (its row lock was lost while another instance integrated and published), and the
  attempt fails with the transient error `coordination state moved on while checking (lost
  connection or lock?); retrying`, without tripping. If the re-read itself fails (typically
  because the connection, and with it the row lock, is gone; or because a standby answered, see
  [Connect only to the primary](#connect-only-to-the-primary)), the attempt fails with `could not
  confirm a suspected coordination lag (published size N), will retry: failed to re-read the
  integrated size: ...`, also without tripping. Neither message contains the text of
  `ErrCoordinationBehind` (`coordination database is behind ...`), so log-based alerting on that
  text raises no false alarms. A different root at the same size, a failed consistency proof and
  differing bucket contents trip immediately.
* If reading the `checkpoint` fails for any reason other than not-found, or it cannot be
  parsed, start-up, integration and publication fail as well (`failed to read ... checkpoint`,
  `failed to parse published checkpoint`) instead of proceeding unchecked; these errors (like a
  failure to read the tripped marker at start-up, or to re-read `IntCoord`) do not trip the
  guards, and integration and publication retry them.
* **Blind spot of the bucket check.** Its fast path only looks at the bundle and level-0 tile
  containing `IntCoord.seq`. If an earlier, failed integration attempt wrote later bundles but not
  that first one (they are written concurrently) and the database then also lost those `Seq`
  rows, the bucket check passes; writing different data to those later objects then fails on the
  conditional write (`precondition failed: resource content for "<key>" differs from data
  to-be-written`), so integration stalls, retrying, without tripping. Never a fork, but the
  stall must be noticed (see [Detecting](#detecting)).

The guards and the marker protect appenders. A `MigrationWriter`, which copies an existing log
into a new one, has no guards (as upstream).

### A trip is sticky and persistent

Once a check returns `ErrCoordinationBehind` (the guard *trips*), the process in which it tripped:

* refuses new entries at once (`Add` returns that error, and the conformance server answers
  `POST /add` with 500; duplicates answered by an antispam layer, in memory or persistent, still
  get their earlier index), and stops integrating and publishing. Garbage collection keeps
  running; it only removes partial objects superseded within the published checkpoint;
* reports the error from `(*s3pg.Storage).CoordinationError()`: use it in health checks (the
  conformance server's `GET /healthz` then answers 503). The flag is never cleared, so the
  process must be restarted;
* writes the object `s3pg-coordination-tripped` (below `BucketPrefix`, if set) with the time and
  the error, on its first trip only, and only for a trip at run time (by integration,
  publication or sequencing; never for a refusal at start-up). `CoordinationError()` is set
  first; the write is bounded by 10s. **While this marker exists, every appender refuses to
  start** (`ErrCoordinationBehind`, citing the marker), so restarted instances cannot accept
  entries against a database that has not been reconciled. Only the operator deletes it,
  as the last step of the [recovery procedure](#recovery-procedure). If writing it fails, that is
  logged (`Failed to write tripped marker`), and restarts are then protected only by the start-up
  guard.

Other running instances don't read the marker. Each trips on its own, since they check the same
database and bucket: at its next sequencing of a batch if `SeqCoord.next` is below its
high-water mark (the first `Add` after the loss, if the instance had seen the lost part of the log
published or integrated), at its next integration attempt (integration runs as soon as entries are
waiting, so the first batch such an instance accepts triggers one), or at its next publication
attempt (when one is due: the tree has grown, or the republish interval has passed). Until then
they may still accept entries (see below).

An instance started against the wrong database (an empty one, or another log's that is smaller)
is refused at start-up (`database tree (size N, ...) vs published checkpoint ...`) but writes
nothing, so it does not block correctly configured instances; fix its configuration. The start-up
guard only detects a database that is *behind* the published checkpoint: one that is larger
(e.g. a larger log's) is not refused at start-up, so keep each log's DSN and schema distinct
([One schema per log](#one-schema-per-log)). The guards compare against the published checkpoint
only (its signature is not checked), so they cannot help if the `checkpoint` object itself is
lost or replaced.

### Entries are still lost

The guards prevent damage to the published log, not loss of entries:

* Entries whose sequencing commits were lost and which were not yet published are gone: those
  already integrated survive only as unreferenced objects beyond the published size, the others
  nowhere. Their submitters were told indices that will be assigned to other entries.
* Instances that keep running through the failover (they reconnect to the new primary) never
  hand out an index below their high-water mark, i.e. one they have seen published or integrated:
  sequencing trips instead. Until their guard trips, though, they may hand out indices from the
  stale `SeqCoord.next` that are above their mark: indices of entries that were only sequenced,
  or that other instances integrated or published since this one last looked. These entries are
  never integrated or published, and the recovery below discards them: they are lost too, and
  their indices are reused.
* If the database still covers the published checkpoint (only unpublished commits were lost),
  the start-up and publication guards do not fire. Integration does, through the bucket check,
  as soon as the entries it integrates differ from objects the lost history wrote, and so does
  sequencing in every running instance whose high-water mark is above the restored
  `SeqCoord.next`. Entries that were only sequenced (never written to the bucket, and not seen
  integrated by a running instance) vanish silently, and their indices are reused.

Hence:

* **Use synchronous replication** for HA (`synchronous_standby_names` with at least one
  standby, `synchronous_commit` = `on` or `remote_apply`), so a failover never loses an
  acknowledged commit. With asynchronous replication, a failover that loses the last
  `SeqCoord`/`IntCoord` commits leaves the database behind state that is already in the
  bucket (and possibly already published). Fail over only to a standby that was synchronous.
  The driver's per-transaction `synchronous_commit` cannot help here.
* Keep `fsync` and `full_page_writes` on.
* **Never make the tables `UNLOGGED`** (nor create them in an unlogged way): unlogged tables
  are emptied after a crash and are not replicated.
* **Never let either half go back in time.** Point-in-time recovery of PostgreSQL must replay
  all archived WAL up to the moment of the failure. The bucket matters even more, because it
  holds what clients, witnesses and monitors have already seen: protect it with versioning or
  replication rather than restoring old snapshots of it. A database older than the bucket is
  caught by the guards and can be repaired as described below, losing unpublished entries. A
  bucket older than the database cannot: its checkpoint has gone backwards (a rollback visible
  to everyone who saw a newer one), and integration and publication fail because tiles and
  bundles the database refers to are missing.
* Make sure the servers only ever talk to the primary, also after a failover (next section).

### Connect only to the primary

Every connection of the driver (log and antispam) must go to the current primary. Writes fail on
a standby anyway (read-only transactions), but a read answered by a lagging standby would show the
guards an integrated size smaller than the published checkpoint, which is exactly what lost
commits look like. Therefore:

* For multi-host setups, list all nodes in the DSN with **`target_session_attrs=read-write`**
  (e.g. `host=pg1,pg2,pg3 ... target_session_attrs=read-write`), so that pgx only uses a server
  that accepts writes and the servers reconnect to the new primary after a failover. Without it
  (pgx's default is `any`), pgx uses the first host that answers, which may be a standby.
* Don't put a load-balancing pooler between the driver and a replicated cluster, e.g. Pgpool-II
  with `load_balance_mode = on`, which sends read-only `SELECT`s to standbys; nor a PgBouncer,
  HAProxy or DNS name that can point at a replica. Point the driver at the primary only.
* As a safety net, the read of the integrated tree (`currentTree`: used by the start-up guard, by
  the guards' confirming re-read and by `LogReader.IntegratedSize`) also selects
  `pg_is_in_recovery()` and fails if a standby answered: `failed to read IntCoord: connected to a
  standby server; the coordination database connection must go to the primary`. This is an error,
  never a trip: start-up fails, and a confirming re-read answered by a standby fails that
  integration or publication attempt (`could not confirm a suspected coordination lag ...`), which
  is retried. Fix the routing if you see it. Other reads outside a
  transaction (the pushback check before sequencing, `LogReader.NextIndex`) are not checked; a
  lagging answer there only causes extra pushback or a stale value. Verified against a real
  streaming replica (see [VERIFICATION.md](VERIFICATION.md)).

## Recovering from a lost commit

> [!WARNING]
> This procedure rewrites the coordination state and deletes objects from the bucket. A mistake
> can destroy the log, e.g. by deleting objects the published checkpoint needs, or by letting a
> forked tree be built. **Rehearse it on a copy first**: restore a dump of the log's schema
> (`pg_dump -n mylog`) into a scratch database and copy the bucket (or prefix) to a scratch
> bucket, run every step there, and check the result with `fsck` before touching the real log.
> Keep the dump, the quarantined objects and the logs until the recovered log has been verified.

The procedure was rehearsed with the lost-commit scenarios of
[`integration/fault/s3pg`](/integration/fault/s3pg/) (`lostcommit.sh`, `RECOVER=1`), which apply
these steps (without antispam) and then verify the log; see [VERIFICATION.md](VERIFICATION.md).

### Detecting

Any of these means the database may have lost commits:

* An appender fails to start with an error containing `coordination database is behind or
  inconsistent with the published checkpoint`. The conformance server exits with status 1 and
  logs e.g.
  `msg="Failed to create new appender" error="failed to init appender lifecycle: failed to initialise log storage: s3pg: coordination database is behind or inconsistent with the published checkpoint: database tree (size 300, root fc50b1…) vs published checkpoint (size 733, root 84d46b…)"`,
  or, once a running instance has tripped, `...: marker "s3pg-coordination-tripped" exists
  (written when an instance's guard tripped: "<time>\n<error>"); complete the recovery
  procedure, then delete the marker`. Match the message text: `tessera.NewAppender` wraps the
  error with `%v`, so `errors.Is(err, s3pg.ErrCoordinationBehind)` does not see it. A refusal by
  the start-up guard (`database tree ... vs published checkpoint ...`) writes no marker and stops
  only that instance: either the log's database has lost commits, or that instance was pointed at
  the wrong database or schema (an empty one, another log's). Compare the log's own database with
  the published checkpoint ([Monitoring](#monitoring)): if it is not behind, fix the instance's
  configuration; nothing else needs repair.
* A running process trips ([A trip is sticky and persistent](#a-trip-is-sticky-and-persistent)).
  It logs `Coordination database is behind or inconsistent with the log in object storage:
  refusing to integrate, publish or sequence until recovered`, with the error that tripped it,
  once; `CoordinationError()` is non-nil from then on and the conformance server's `GET /healthz`
  answers 503 (logging `healthz: coordination database is behind the published log; manual
  recovery required`); it writes the `s3pg-coordination-tripped` marker, and refuses `Add` from
  then on. The error that tripped it comes
  * from sequencing: `s3pg: coordination database is behind ...: next index 300 < 733 (already
    integrated or published); refusing to sequence`. It is only in that log line and in the
    failed `Add` (the conformance server answers `POST /add` with 500 and the error).
  * from integration, also logged as `msg=integrateEntries error="s3pg: coordination database is
    behind ...: database integrated size 300 < published checkpoint size 733; refusing to
    integrate"`, or `...: entry bundle 4 in the bucket (tile/entries/004, 256 items) differs from
    the entries being integrated from index 1062` (or `level 0 tile`). Integration only runs once
    entries are waiting.
  * from publication, also logged as `msg="Failed to execute checkpoint publisher job"` with
    `refusing to publish checkpoint of size ... over published size ...`, `refusing to publish root
    ... for size ...` or `new checkpoint (size ...) is not consistent with published checkpoint
    (size ...)`. If the tree hasn't grown, this may take until the republish interval.

  The other running instances trip in the same way at their next sequencing, integration or
  publication attempt, and restarted instances are refused while the marker exists.
* The marker object `s3pg-coordination-tripped` exists (see [Monitoring](#monitoring)).
* Integration fails repeatedly with `precondition failed: resource content for "<key>" differs from
  data to-be-written`, preceded by a `Resource non-idempotent writen` error for the same key (the
  [blind spot](#fail-closed-guards) of the bucket check: a stall without a trip).
* The published checkpoint's size is larger than `IntCoord.seq` (see [Monitoring](#monitoring)).

Not signs of lost commits (none of them trips; the attempt is retried, and `/add` and `/healthz`
are unaffected):

* `coordination state moved on while checking (lost connection or lock?); retrying (fresh
  integrated size ... >= published size ...)`, logged by integration or publication when an
  instance's own view was stale (see [Fail-closed guards](#fail-closed-guards)).
* `could not confirm a suspected coordination lag (published size N), will retry: failed to
  re-read the integrated size: ...`: the confirming re-read failed, typically because the
  connection was lost. If it says `connected to a standby server`, fix the routing
  ([Connect only to the primary](#connect-only-to-the-primary)). Since `6f2d29c` this message no
  longer contains the text `coordination database is behind`, so alerts matching that text don't
  fire on it.

If they recur, look for connection problems (resets, failovers,
`idle_in_transaction_session_timeout`) or a pooler routing to a standby.

### Inspecting

First **stop every writer** of the log (every process running an appender against this schema
and bucket, including any that kept running through the failover), and check that
`pg_stat_activity` shows no sessions of the runtime role left. Writers stay stopped until the
procedure is complete ([Restarting](#restarting)). (A tripped conformance server stops promptly on
`SIGTERM`: it doesn't wait for its outstanding entries, which can never be integrated, and exits
with status 1.)

Then read the published checkpoint (its second line is the size `S`, the third the root hash `R`,
base64), the tripped marker if there is one (the time of the first trip and its error), and the
coordination state, and save them, the server logs and a schema dump (`pg_dump -n mylog`) for the
incident record:

```sh
curl -s https://<log URL>/checkpoint
curl -s https://<log URL>/s3pg-coordination-tripped   # or read it with your S3 tooling
```

```sql
SET search_path TO mylog;
SELECT seq AS integrated_size, encode(rootHash, 'base64') AS integrated_root FROM IntCoord;
SELECT next AS next_index FROM SeqCoord;
SELECT size AS published_size FROM PubCoord;
SELECT fromSize AS gc_from_size FROM GCCoord;
SELECT count(*) AS batches, min(seq), max(seq) FROM Seq;
-- If the antispam store is used, in its schema:
SELECT nextIdx FROM AntispamFollowCoord;
```

* `integrated_size < S`, or `integrated_size = S` with `integrated_root` different from `R`: the
  database is behind the published log (what the start-up guard reports). This includes the case
  where the `Seq` rows from `integrated_size` on are still intact (the database lost integration
  commits but not the sequencing ones): integration would rebuild the published tree from them,
  but the driver deliberately stays fail-closed whenever the published checkpoint is larger than
  `IntCoord.seq`. Recover with the same procedure.
* `integrated_size > S`, or `= S` with root `R`: the published log is still covered by the
  database, but later commits may have been lost; a bucket mismatch (`differs`) comes from
  objects the lost history wrote beyond what the database knows, and a sequencing trip (`next
  index N < M`) from an instance that had seen the log integrated up to `M` before the database
  lost the commits.
* `published_size` and `gc_from_size` never exceed `S`: they are only advanced to sizes already
  published in the bucket.

A start-up refusal alone, by an instance that was pointed at the wrong database or schema, needs
no repair (see [Detecting](#detecting)): it writes nothing, not even the marker. If the marker
exists, a running instance has tripped against this log's database and bucket (integration,
publication or sequencing): run the whole procedure.

Finally verify the published checkpoint and everything it references with
[`cmd/fsck`](/cmd/fsck/): `go run ./cmd/fsck --storage_url=https://<log URL>/ --public_key=<file> --ui=false`
checks the checkpoint's signature and re-derives its root hash from the bucket's bundles and
tiles. If it fails, stop: this procedure does not apply.

### Recovery procedure

The target is the state of the published checkpoint `(S, R)`: it is what clients have seen, and
`fsck` has just shown that the bucket holds all of it. Everything beyond `S` is discarded:
entries that were sequenced or integrated but not published, including those the database still
has (in `Seq`, or integrated beyond `S`), are lost and must be resubmitted by their submitters,
and their indices will be reused.

Run the procedure whenever the database may have lost commits (e.g. after a failover to a standby
which was not synchronous), even if no error has shown up yet: objects written by the lost
history make integration trip as soon as new entries reach their keys.

1. **Quarantine and remove the objects beyond `S`**, i.e. those written for a tree larger than
   `S`. They are not referenced by the published checkpoint, and they make integration from `S`
   trip wherever the new entries differ. Classify every object by its key, using
   `n = S` for entry bundles (`tile/entries/<N>[.p/<W>]`, or the personality's entries path, e.g.
   `tile/data/...` with the CT layout) and `n = S >> (8*L)` (that is, `floor(S / 256^L)`) for
   hash tiles at level `L` (`tile/<L>/<N>[.p/<W>]`), all below `BucketPrefix` if set. `<N>` uses
   the tlog-tiles encoding (`x001/x234/067` is 1234067; `layout.ParseTileIndexPartial` parses
   it), and `.p/<W>` marks a partial object of width `W`. With `F = floor(n / 256)` and
   `w = n mod 256`, the checkpoint references the full objects with index `< F` and, if `w > 0`,
   `<F>.p/<w>`. An object is **beyond `S`** if and only if
   * its index is greater than `F`, or
   * its index is `F` and it is the full object or a partial with width greater than `w`.

   Leave every other object, in particular the `checkpoint`, the `s3pg-coordination-tripped`
   marker (step 5), narrower partials with index `F` and partials with smaller indices (older
   versions which garbage collection removes later).
   For example, for `S = 766`: bundles and level-0 tiles have `F = 2, w = 254`, so `.../002`,
   `.../002.p/255` and everything from index 3 up is beyond; level 1 has `n = 2`, so `tile/1/000`,
   `tile/1/000.p/<W>` with `W > 2`, and every higher index is beyond; at level 2 (`n = 0`) every
   object is beyond.

   Copy each of these objects to a separate, non-public quarantine bucket, then delete them.
   `integration/fault/s3pg/objcheck.py BUCKET prune S --quarantine QBUCKET` implements this
   classification and was used in the rehearsals; it is a test helper (fixed endpoint and
   credentials, default layout without `BucketPrefix`), so adapt and dry-run it (`--dry-run`)
   before using it on a real bucket. If a CDN or caching proxy serves the bucket, purge these keys
   there as well: tiles and bundles are served with `Cache-Control: max-age=604800,immutable`, so
   stale copies would otherwise be served in place of the new objects written under the same keys.
2. **Antispam** (only if the antispam store is used): mappings for indices `>= S` point at
   entries that are no longer in the log. Remove them, in the antispam schema:
   `DELETE FROM AntispamIDSeq WHERE idx >= S; UPDATE AntispamFollowCoord SET nextIdx = LEAST(nextIdx, S) WHERE id = 0;`
   Otherwise a resubmitted lost entry is answered as a duplicate, with an index that will hold a
   different entry, and the follower stops with `followFrom X > size Y` until the log has grown
   past `X`, and then never indexes the entries from `S` to `X`. If in doubt, rebuild the store
   instead: empty `AntispamIDSeq` and set `nextIdx = 0`; the follower then re-reads the whole log,
   with antispam pushback until it has caught up. (This step follows from the follower's code; the
   rehearsals ran without antispam.)
3. **Reset the coordination tables** to `(S, R)`, in one transaction, as a role with DML on the
   schema; `<R>` is the checkpoint's base64 root hash:

   ```sql
   BEGIN;
   SET LOCAL search_path TO mylog;
   UPDATE IntCoord SET seq = <S>, rootHash = decode('<R>', 'base64') WHERE id = 0;
   UPDATE SeqCoord SET next = <S> WHERE id = 0;
   DELETE FROM Seq;
   UPDATE PubCoord SET size = <S> WHERE id = 0;
   UPDATE GCCoord SET fromSize = LEAST(fromSize, <S>) WHERE id = 0;
   COMMIT;
   ```

   Leave `Tessera` alone. (`GCCoord.fromSize` should already be at most `S`; `LEAST` makes sure.)
4. **Verify**: `fsck` must still succeed for the (unchanged) checkpoint, and
   `SELECT seq, encode(rootHash, 'base64') FROM IntCoord` must return exactly `S` and `R` (the
   start-up guard refuses a smaller size, or a different root at the same size).
5. **Last step: delete the tripped marker** `s3pg-coordination-tripped` (below `BucketPrefix`, if
   set), after saving its contents with the incident record, e.g.
   `aws s3 rm s3://<bucket>/<prefix>/s3pg-coordination-tripped` or
   `mc rm <alias>/<bucket>/<prefix>/s3pg-coordination-tripped`. **Never delete it before steps
   1-4 are done and verified**: it is what keeps restarted instances from accepting entries
   against an unreconciled database. If there is no marker (nothing had tripped), skip this step.

### Restarting

Restart **one writer**, add a few entries, and check that a larger checkpoint is published (every
publication is checked against a consistency proof from the published one), that `GET /healthz`
(or `CoordinationError()`) reports healthy, that no marker has been written again, that no
`differs` or `coordination database is behind` errors are logged, and that `fsck` succeeds for
the new checkpoint. Then start the other writers.

Every writer must be a new process: a tripped guard is never reset, and a process that ran
before the recovery may hold a high-water mark above `S` (from entries it integrated beyond `S`),
so its sequencing would trip again and write a new marker. While the marker exists, every start
is refused.

Entries acknowledged with indices `>= S` are lost: tell their submitters to resubmit them. If
writers were started before all steps were done, stop them and start again from
[Inspecting](#inspecting), with the then-published checkpoint. If something goes wrong, restore
the quarantined objects and the schema dump; never edit or replace the `checkpoint` object.

## Autovacuum

`Seq` is a queue: every batch is inserted and, once integrated, deleted; the coordination tables
are single rows updated several times per second. `schema.sql` already sets per-table storage
parameters (`autovacuum_vacuum_*` / `autovacuum_vacuum_insert_*` thresholds of 1000 with zero
scale factors on `Seq`, `fillfactor = 50` on the single-row tables for HOT updates), so keep
autovacuum enabled and don't override these. What hurts is anything holding back the xmin
horizon — long-running transactions, abandoned replication slots, `hot_standby_feedback` with
long standby queries — because dead row versions then can't be removed: reads of the hot
single-row tables walk ever longer version chains, and `Seq` bloats. Monitor `n_dead_tup`
(below) and the oldest transaction age.

## Monitoring

Run in the log's schema (e.g. `SET search_path TO mylog`):

```sql
-- Integration lag: indices handed out but not yet integrated. Pushback starts when this exceeds
-- PushbackMaxOutstanding (default 4096; 40960 in the conformance server).
SELECT s.next - i.seq AS integration_lag, s.next AS next_index, i.seq AS integrated_size
  FROM SeqCoord s, IntCoord i WHERE s.id = 0 AND i.id = 0;

-- Checkpoint: published size and age. Age should stay around the checkpoint interval while the
-- log grows, and below the republish interval (default 10m) when idle.
SELECT p.size AS published_size, i.seq - p.size AS unpublished,
       now() - to_timestamp(p.publishedAt / 1e9) AS checkpoint_age
  FROM PubCoord p, IntCoord i WHERE p.id = 0 AND i.id = 0;

-- Garbage collection progress (should follow published_size).
SELECT fromSize AS gc_from_size FROM GCCoord WHERE id = 0;

-- Queue size and bloat.
SELECT count(*) AS pending_batches FROM Seq;
SELECT relname, n_live_tup, n_dead_tup, last_autovacuum, autovacuum_count
  FROM pg_stat_user_tables WHERE schemaname = current_schema() ORDER BY relname;

-- Long transactions (anything much older than ~30s is suspicious) and replication.
SELECT pid, usename, application_name, state, now() - xact_start AS xact_age, query
  FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start LIMIT 10;
SELECT application_name, state, sync_state, replay_lag FROM pg_stat_replication;
```

Whether the database is behind the published log can't be seen from the database alone. Compare
the bucket's checkpoint with `IntCoord`, reading the checkpoint first: checkpoints are only
published for sizes `IntCoord` has already reached, and `IntCoord` only grows, so in this order a
healthy log always passes.

```sh
pub=$(curl -sf https://<log URL>/checkpoint | sed -n 2p)
int=$(psql -XtAc 'SELECT seq FROM mylog.IntCoord WHERE id = 0')
[ "$pub" -le "$int" ] || echo "ALERT: coordination database is behind the published checkpoint"
```

Alert if the tripped marker exists: it is never present in a healthy log, and while it exists no
appender can start. With a publicly readable bucket:

```sh
curl -sf -o /dev/null https://<log URL>/s3pg-coordination-tripped && echo "ALERT: s3pg guard tripped"
```

In the servers, expose `(*s3pg.Storage).CoordinationError()` in a health check (as the
conformance server's `/healthz` does) and alert on it. Also alert on the servers' logs/metrics:
repeated `integrateEntries` or checkpoint publication errors, any `coordination database is
behind` or `differs from data to-be-written` message (see [Detecting](#detecting); since
`6f2d29c` no transient message contains `coordination database is behind`), any `connected to a
standby server` message (a routing error, see [Connect only to the primary](#connect-only-to-the-primary)),
and the OpenTelemetry metrics emitted by Tessera. Occasional `coordination state moved on while
checking` or `could not confirm a suspected coordination lag` messages are benign; frequent ones
point at unstable database connections.

## Multiple logs

Give each log its own schema (`Config.Schema`, and ideally its own runtime role), its own signing
key/origin, and its own bucket or `Config.BucketPrefix` (`--bucket_prefix`) within a shared
bucket; its public URL is then `<bucket URL>/<prefix>/`. Logs share nothing else, so they can be
created, migrated and dropped (`DROP SCHEMA ... CASCADE` plus deleting its objects)
independently. Budget connections per log.

## Keeping in sync with upstream `storage/aws`

`s3pg.go` is a copy of `storage/aws/aws.go` without the MySQL sequencer, and `pgsequencer.go`
is a port of that sequencer (`mySQLSequencer`); likewise `antispam/` mirrors
`storage/aws/antispam`, and [`cmd/conformance/s3pg`](/cmd/conformance/s3pg/) mirrors
`cmd/conformance/aws` (its README lists the differences). `guard.go` and `internal/pgtx` have no
upstream counterpart. Every difference below is intentional: don't revert any of them as noise
when porting upstream changes.

Upstream bugs fixed here (marked *bug* below), worth reporting upstream: an empty
`DeleteObjects` request stalls garbage collection; `BucketPrefix` is applied twice in the
idempotent re-write check; the `LogReader`'s partial -> full fallback never happens (`NoSuchKey`
not mapped); the `MigrationWriter`'s `LogReader` panics in `IntegratedSize`/`NextIndex`; a
delayed or retried checkpoint `PUT` can roll the published checkpoint back (most likely affects
upstream too, which writes the checkpoint unconditionally); a database behind the bucket makes
upstream publish a rollback or fork (no guard); a missing `IntCoord` row silently drops batches
(`assignEntries`) or stalls integration (`consumeEntries`).

**`s3pg.go`** (`diff -u storage/aws/aws.go storage/s3pg/s3pg.go` should show only these):

* package name and doc comments; span names `tessera.storage.s3pg.*`.
* `Config`: `DSN` is a pgx DSN; new `DB`, `Schema` and `SkipSchemaInit`; `MaxIdleConns == 0`
  keeps the `database/sql` default (see [Connection pooling](#connection-pooling)).
* `New` honours a caller's `S3Options` even when `SDKConfig` is nil (upstream overwrites it with
  a no-op), and prints no "Here be dragons" warning for a non-AWS configuration.
* `Storage` tracks the sequencers it opens and has `Close()`, and `CoordinationError()` (set by
  its appenders through `Appender.onBehind`); `Appender` closes its pool if appender
  initialisation fails. `newPGSequencer(ctx, cfg, ...)` replaces `newMySQLSequencer`.
* Guards ([`guard.go`](guard.go)): `Appender.init` starts with `checkTrippedMarker` and ends
  with `checkStartup`, `integrateEntries` starts with `checkIntegrationAllowed` (published size,
  then the bucket consistency check) followed by `observe(fromSeq + len(entries))`, and
  `updateCheckpoint` reads the checkpoint with its ETag (`readCheckpointWithETag`), runs
  `checkPublicationAllowed`, and writes with `setCheckpointIfETag` instead of upstream's
  unconditional `setCheckpoint`, wrapping its error with `%w` (upstream: `%v`) so that
  `errPreconditionFailed` stays visible. *Bug*: without them, upstream can publish a rollback or
  fork.
* Sticky trip and high-water mark: `Appender.behind`/`onBehind`/`highWater`; `newAppender` hands
  the mark to the sequencer (`pgSequencer.minNext = r.highWater.Load`); the sequencing queue's
  flush function refuses batches once tripped, and passes `assignEntries` errors through `trip`
  (so a refusal below the high-water mark trips and writes the marker; other errors pass
  unchanged).
* `integrateEntriesJob` adapts the `Seq`-row limit it passes to `consumeEntries`: halved after a
  failed attempt (minimum 1), doubled after a successful one (maximum
  `DefaultIntegrationSizeLimit`); upstream always passes `DefaultIntegrationSizeLimit` (see
  [Timeouts](#timeouts)). A deliberate deviation, not a bug fix. The forced integration in
  `Appender.init` still uses the fixed limit.
* `MigrationWriter` sets the `LogReader`'s `integratedSize`/`nextIndex` from the sequencer.
  *Bug*: upstream leaves them nil, so `IntegratedSize`/`NextIndex` panic.
* `MigrationStorage.buildTree` uses `$n` placeholders, `pgtx.Begin`/`pgtx.Rollback` and
  `m.q.SQL(...)`.
* `logResourceStore.get` maps S3's `NoSuchKey` to `os.ErrNotExist`. *Bug*: upstream returns the
  S3 error as is, so the partial -> full fallback of `ReadTile`/`ReadEntryBundle`
  (`fetcher.PartialOrFullResource`) never happens: reading a partial tile or bundle which garbage
  collection has already replaced by the full one fails.
* `objStore` has three more methods: `getObjectWithETag`, `setObjectIfETag` (returns
  `errPreconditionFailed`) and `listObjects`, implemented by `s3Storage` and by `memObjStore` in
  `s3pg_test.go`.
* `s3Storage.key()`/`getKey()` apply `BucketPrefix` in one place. *Bug*: upstream applies the
  prefix twice in `setObjectIfNoneMatch`'s idempotency check, so with a prefix every idempotent
  re-write failed. Keys use `path.Join` instead of `filepath.Join` and keep a trailing slash, so
  GC prefixes such as `x.p/` are not widened to `x.p`.
* `setObjectIfNoneMatch` (and `setObjectIfETag`) also treat `409 ConditionalRequestConflict` as a
  failed condition, and resolve a write without HTTP response by reading the object back
  (MinIO; see [MinIO notes](#requirements)). `setObjectIfETag` also treats `404 NoSuchKey` in
  answer to `If-Match` (MinIO, moto) as a failed condition. The non-idempotent write error logs
  sizes and SHA-256 hashes instead of upstream's `cmp.Diff` of the (entry) data.
* `deleteObjectsWithPrefix` paginates the listing, skips empty `DeleteObjects` requests, sets
  `Quiet` and returns per-key delete errors. *Bug*: S3/MinIO reject an empty request with
  `MalformedXML`, which stalled garbage collection upstream.

**`pgsequencer.go`**, compared with `mySQLSequencer`:

* pgx `database/sql` driver; `$n` placeholders; `INSERT IGNORE` becomes `ON CONFLICT DO NOTHING`;
  `PingContext` instead of `Ping`; span `tessera.storage.s3pg.assignEntries` (upstream:
  `tessera.storage.gcp.assignEntries`).
* The DDL and initial rows live in `schema.sql` (`SchemaSQL`), applied in one transaction under
  `pg_advisory_xact_lock(hashtext('tessera/s3pg/init'))` (after `q.SetSearchPath` if
  `Config.Schema` is set), or not at all with `SkipSchemaInit`. Upstream's `ALTER TABLE PubCoord
  ADD COLUMN size` is part of the initial schema. `schema.sql` uses `BIGINT` with
  `CHECK (... >= 0)` on the `next`, `seq` and `size` columns in place of MySQL's `UNSIGNED` (and
  also on `publishedAt` and `fromSize`, which are signed upstream), `BYTEA` in place of the BLOB
  types, and sets autovacuum/fillfactor storage parameters.
* Start-up schema check (compatibility version, initial rows, `Seq` readable; see
  [Roles and least privilege](#roles-and-least-privilege)).
* The pool is `Config.DB` or opened from `DSN` (closed again if construction fails);
  `MaxIdleConns` semantics as above.
* Every SQL statement is wrapped in `s.q.SQL(...)` (schema qualification, `internal/pgtx`).
* `assignEntries` and `consumeEntries` return an error if the `IntCoord` row is missing.
  *Bug*: upstream returns nil from `assignEntries`, silently dropping the batch, and
  `(false, nil)` from `consumeEntries`, so nothing is ever integrated.
* `assignEntries` returns `ErrCoordinationBehind` without writing anything if `SeqCoord.next`
  (read under its row lock) is below `minNext()`, the appender's high-water mark.
* `consumeEntries` and `publishCheckpoint` pass their transaction to the callback in its context
  (`withTx`), and `currentTree` reads on that transaction if the context carries one (otherwise
  through the pool), so the guards' confirming re-read never needs a second connection.
  `currentTree` also selects `pg_is_in_recovery()` and fails if a standby answered.
* Consumed `Seq` rows are deleted with `seq = ANY($2)` (upstream builds an `IN (?, ...)` list),
  and `RowsAffected` is verified.
* `consumeEntries` closes the `Seq` result set (and checks the error) before running further
  statements in the transaction.
* Every transaction is started with `pgtx.Begin` (`READ COMMITTED`, `synchronous_commit` at least
  `on`) and rolled back with `pgtx.Rollback`, which ignores `sql.ErrTxDone` and logs other
  failures at WARN. Upstream uses `BeginTx(ctx, nil)` (and `Begin()`, which ignores the context,
  in `publishCheckpoint` and `garbageCollect`); its `assignEntries`/`consumeEntries` log every
  rollback failure as an ERROR, including `sql.ErrTxDone` for an already finished transaction,
  and the other functions ignore them.

**`internal/pgtx`**: `Begin` (isolation level and durable commit), `Rollback`, and `Qualifier`,
which rewrites the table names given to `NewQualifier` (`coordTableNames` in `pgsequencer.go`,
`antispamTableNames` in `antispam/`) to `"schema".Table` (the schema quoted with
`pgx.Identifier.Sanitize`, and inserted literally, never as a regular-expression template, so
`$` in a schema name is safe). Names are matched case-sensitively as whole words, exactly as
written in the SQL. When porting upstream SQL, wrap every statement in
`q.SQL(...)`, keep the table names' spelling, add new tables to those lists, and start every
transaction with `pgtx.Begin`.

**`antispam/antispam.go`**: pgx and `$n`; schema in `antispam/schema.sql` (`SchemaSQL`, own
advisory lock, `SkipSchemaInit`); `NewAntispamWithDB` and `Close()`; `AntispamOpts.Schema` and
`q.SQL(...)` on every statement; the batch is inserted with
`unnest($1::bytea[], $2::bigint[]) ... ON CONFLICT (h) DO NOTHING` instead of a multi-row
`INSERT IGNORE`; `MaxIdleConns` semantics as the log; every transaction uses
`pgtx.Begin`/`pgtx.Rollback`.

**`guard.go`**: when porting upstream changes, keep the guard calls listed above. Every code path
that writes the checkpoint must go through `updateCheckpoint` (ETag read, `checkPublicationAllowed`
and the conditional write, while holding the `PubCoord` lock), and every appender code path that
writes tiles or bundles must go through `checkIntegrationAllowed` (while holding the `IntCoord`
lock). Only errors wrapping `ErrCoordinationBehind` trip the appender: `trip` (run-time trips
from integration, publication and sequencing) sets the flag, calls `onBehind`, then writes the
`trippedMarkerPath` object with a 10s deadline (`markerWriteTimeout`), on the first trip only;
`checkStartup` uses `tripWith(..., false)`, which writes no marker; `checkTrippedMarker`
refuses start-up while the marker exists. A "published size > ours" comparison must go through
`confirmBehind`, which re-reads `IntCoord` through `currentTree` with the transaction-carrying
context (never a separate pool read: a small pool would starve it) and returns the non-tripping
`errStaleView` if the fresh size has caught up, or a non-tripping error whose text must not
contain `ErrCoordinationBehind`'s if the re-read fails; other mismatches trip directly. Every read
of the published checkpoint by the guards (`publishedCheckpoint`, `checkPublicationAllowed`) and
every integration call `observe`, which raises the high-water mark that `assignEntries` checks.
The guards parse checkpoints with `parse.CheckpointUnsafe`, build consistency proofs with
`client.NewProofBuilder` from the `LogReader`'s tiles, and list bundles and level-0 tiles by the
names `entriesPath` and `layout.TilePath` produce (`widest`); follow upstream changes to those. If
upstream adds its own protection against a stale database, compare it with `guard.go` before
dropping either.

When pulling upstream changes:

1. Apply upstream changes to the shared code as-is, keeping the differences above; port changes
   to `mySQLSequencer` into `pgsequencer.go`.
2. Schema changes in upstream's `initDB` go into `schema.sql` *and* a new
   `migrations/NNNNNN_*.up.sql`. Never edit a released migration: `000001_initial.up.sql` only
   mirrors `schema.sql` while it is the only migration.
3. If upstream bumps `SchemaCompatibilityVersion`, bump it here too, update the value seeded in
   `schema.sql`, and add a migration which converts existing data and updates
   `Tessera.compatibilityVersion` (servers refuse to start against a different version). The
   same applies to `antispam.SchemaCompatibilityVersion` and `antispam/schema.sql`, which has no
   `migrations/` yet: create `antispam/migrations/`, starting with a copy of the current
   `antispam/schema.sql`.
4. Run `go test -race ./storage/s3pg/... -pg_test_optional=false` against a real PostgreSQL
   (`TESSERA_S3PG_TEST_DSN`) and S3 (`TESSERA_S3PG_TEST_S3_ENDPOINT`, e.g. `http://127.0.0.1:9000`;
   the credentials default to `test`/`test` and can be set with `TESSERA_S3PG_TEST_S3_ACCESS_KEY`
   and `TESSERA_S3PG_TEST_S3_SECRET`, e.g. for MinIO, which rejects secrets shorter than 8
   characters), the [compose integration test](/deployment/live/s3pg/), and the harnesses listed
   in [VERIFICATION.md](VERIFICATION.md). `TestCurrentTreeRefusesStandby` runs only if
   `TESSERA_S3PG_TEST_STANDBY_DSN` names a streaming-replication standby whose `search_path`
   holds an s3pg schema.
