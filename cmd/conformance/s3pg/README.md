# Conformance testing binary for S3-compatible storage + PostgreSQL

This binary is the [`storage/s3pg`](/storage/s3pg/) counterpart of the
[AWS conformance binary](../aws/): a personality which accepts `POST` requests to `/add`,
appends the request body to the log, and returns the assigned index. The log is read directly
from the bucket (e.g. `http://localhost:9000/<bucket>/` for a public MinIO bucket).

The quickest way to run it is the Docker Compose stack in
[`deployment/live/s3pg`](/deployment/live/s3pg/), which also runs PostgreSQL and MinIO,
and which is used by the integration test in CI.

## Flags and environment variables

Secrets can be passed through environment variables instead of flags, so they don't appear in
process listings. A non-empty flag takes precedence over its environment variable, except for
`TESSERA_ADDITIONAL_SIGNERS`, whose keys are appended to any `--additional_signer` flags.

| Flag                   | Environment variable         | Default     | Description |
| ---------------------- | ---------------------------- | ----------- | ----------- |
| `--bucket`             |                              |             | Bucket holding the log (required). |
| `--bucket_prefix`      |                              |             | Prefix within the bucket for all log objects, e.g. to store several logs in one bucket. |
| `--pg_dsn`             | `TESSERA_PG_DSN`             |             | PostgreSQL DSN (URL or keyword/value) of the log's coordination DB (required). |
| `--pg_schema`          |                              |             | PostgreSQL schema holding the log's tables (`Config.Schema`): every statement names its tables fully. Recommended; required behind PgBouncer. If unset, the session's `search_path` selects the schema. |
| `--pg_max_conns`       |                              | `0`         | Maximum open connections per pool (0 = unlimited; set it). |
| `--pg_max_idle_conns`  |                              | `2`         | Maximum idle connections per pool; `0` keeps the `database/sql` default (2), a negative value keeps none. |
| `--skip_schema_init`   |                              | `false`     | Don't apply the schemas at start-up (log and antispam); they must have been applied already, otherwise start-up fails. Required when the DB role has no DDL privileges. |
| `--s3_endpoint`        |                              |             | Endpoint of a non-AWS S3 service, e.g. `http://minio:9000`. Enables path-style addressing and static credentials. If unset, the AWS SDK's default configuration and credentials are used. |
| `--s3_region`          |                              | `us-east-1` | Region used with `--s3_endpoint`. |
| `--s3_access_key`      | `TESSERA_S3_ACCESS_KEY`      |             | Access key ID for `--s3_endpoint` (required with it). |
| `--s3_secret`          | `TESSERA_S3_SECRET`          |             | Secret access key for `--s3_endpoint` (required with it). |
| `--signer`             | `TESSERA_SIGNER`             |             | Note signer (private key) used to sign checkpoints (required). |
| `--additional_signer`  | `TESSERA_ADDITIONAL_SIGNERS` |             | Additional checkpoint signer; may be repeated. The variable holds a list of signer keys separated by newlines and/or commas (whitespace is trimmed, empty entries are ignored). |
| `--publish_interval`   |                              | `3s`        | How frequently to publish updated checkpoints. |
| `--antispam`           |                              | `false`     | EXPERIMENTAL: enable persistent antispam (deduplication). |
| `--antispam_pg_dsn`    | `TESSERA_ANTISPAM_PG_DSN`    |             | PostgreSQL DSN for the antispam tables (required with `--antispam`). |
| `--antispam_pg_schema` |                              |             | PostgreSQL schema holding the antispam tables (`AntispamOpts.Schema`), as `--pg_schema`. May be the same schema as the log's. |
| `--listen`             |                              | `:2024`     | Address to listen on. |
| `--trace_fraction`     |                              | `0`         | Fraction of OpenTelemetry traces to sample. |
| `--slog_level`         |                              | `0`         | Log level (`-4` debug, `0` info, ...). |
| `--log_format`         |                              | `text`      | `text` or `json`. |

The DSNs are parsed by [pgx](https://pkg.go.dev/github.com/jackc/pgx/v5#ParseConfig), so the
standard `PG*` variables also work; e.g. leave the password out of the DSN and set `PGPASSWORD`.
A DSN which can't be parsed stops the server (the DSN itself is never logged). If a schema flag
is unset and its DSN sets no `search_path`, a warning is logged and the role's default
`search_path` applies. Always use the schema flags and keep `search_path` (and `options`) out of
the DSNs: through PgBouncer, a `search_path` startup parameter may be rejected, dropped, or even
leak to other clients. Behind PgBouncer in transaction mode, also enable its prepared statement
support (`max_prepared_statements` > 0) or add `default_query_exec_mode=simple_protocol` to the
DSNs (see [PgBouncer](/storage/s3pg/README.md#pgbouncer)). The DSNs must only ever reach the
primary: list multiple hosts with `target_session_attrs=read-write`, and don't connect through a
pooler that load-balances reads to standbys (see
[Connect only to the primary](/storage/s3pg/README.md#connect-only-to-the-primary)).
`--pg_max_conns`/`--pg_max_idle_conns` apply to the log pool and the antispam pool separately.

Differences from the AWS binary, besides the database flags:

* `--pg_max_idle_conns=0` keeps the `database/sql` default of 2 idle connections, whereas
  `--db_max_idle_conns=0` disables them (reconnecting for every query); use a negative value for that.
* `GET /healthz` (used as the container health check) returns 200 once the server is serving and
  can read its checkpoint from the bucket. It returns 503 if reading the checkpoint fails, and
  permanently once the appender has found the coordination database behind or inconsistent with
  the published log (`(*s3pg.Storage).CoordinationError()`, see below).
* `SIGINT`/`SIGTERM` trigger a graceful shutdown: stop accepting requests, then wait (up to 60s)
  for the entries already sequenced to be integrated and published, then close the database pools
  (`(*s3pg.Storage).Close`, `(*antispam.AntispamStorage).Close`). If the appender has tripped
  (see below), the server doesn't wait, since those entries can never be integrated: it logs
  `Not waiting for outstanding entries: the coordination database is behind the published log`,
  closes the pools and exits with status 1.
* OpenTelemetry export is vendor-neutral (OTLP/gRPC, configured with the standard `OTEL_*`
  environment variables) and only enabled when `OTEL_EXPORTER_OTLP_ENDPOINT` (or the
  `_TRACES_`/`_METRICS_` variant) is set, e.g. `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317`.

## Coordination database behind the bucket

The driver refuses to publish a rollback or fork of the checkpoint in the bucket
([High availability and durability](/storage/s3pg/README.md#high-availability-and-durability)):

* At start-up the server exits with status 1 and logs
  `msg="Failed to create new appender" error="... s3pg: coordination database is behind or inconsistent with the published checkpoint: database tree (size N, root ...) vs published checkpoint (size M, root ...)"`:
  the database has lost commits, or is not this log's database (e.g. an empty one). Such a
  refusal writes nothing, not even the marker below, so a misconfigured server doesn't block the
  others: fix its configuration, or, if the log's own database is behind, follow the recovery
  procedure.
* While running, the server trips when sequencing, integration or publication finds the database
  behind. It logs `Coordination database is behind or inconsistent with the log in object
  storage: refusing to integrate, publish or sequence until recovered` once, with the error
  (integration and publication also log it as `msg=integrateEntries` or `msg="Failed to execute
  checkpoint publisher job"`; sequencing refuses with `next index N < M (already integrated or
  published); refusing to sequence` as soon as the database's next index is below the largest
  log size the server has seen published or integrated). From then on that server answers `POST /add` of a new
  entry with 500 and that error, stops integrating and publishing, and `GET /healthz` returns 503
  (logging `healthz: coordination database is behind the published log; manual recovery
  required`) until the process is restarted; on `SIGTERM` it exits with status 1 without waiting
  for outstanding entries. Other servers of the same log do the same at their next sequencing,
  integration or publication attempt.
* The first such trip of a running server writes the object `s3pg-coordination-tripped` into the
  bucket (below `--bucket_prefix`). While it exists, **every** server exits at start-up with
  `... marker "s3pg-coordination-tripped" exists (written when an instance's guard tripped: ...);
  complete the recovery procedure, then delete the marker`, so restarting does not bring the log
  back.

Follow [Recovering from a lost commit](/storage/s3pg/README.md#recovering-from-a-lost-commit):
stop every server, reconcile and verify the database, delete the marker as the last step, then
restart one server and check it before starting the others. Never delete the marker before
that.

These errors from integration or publication are not failures of this kind: they don't trip, don't
affect `/add` or `/healthz`, and are retried:

* `coordination state moved on while checking (lost connection or lock?); retrying`;
* `could not confirm a suspected coordination lag (published size N), will retry: ...` (the
  confirming re-read failed, e.g. on a lost connection). Its text doesn't contain `coordination
  database is behind`, so log alerts on that text only fire on real trips.

`failed to read IntCoord: connected to a standby server; the coordination database connection
must go to the primary` (at start-up, or in the error above) means `--pg_dsn` reaches a standby,
e.g. through a load-balancing pooler or a multi-host DSN without
`target_session_attrs=read-write`; see
[Connect only to the primary](/storage/s3pg/README.md#connect-only-to-the-primary).

A different start-up error also points at a database and a bucket which don't belong together:
`msg="Failed to create new appender" error="... forced integrate: integrate: storage.Integrate: failed to create range covering existing log: missing node: ..."`
(or a missing entry bundle) means the bucket has no `checkpoint`, but the database describes a
non-empty log, e.g. an empty bucket with an existing database. Point the server at the right
bucket and prefix.

## Running it by hand

Given a PostgreSQL schema prepared as in the [operator guide](/storage/s3pg/README.md#roles-and-least-privilege)
and a bucket in MinIO:

```sh
export TESSERA_PG_DSN='postgres://tessera_app@localhost:5432/tessera?sslmode=disable'
export PGPASSWORD=...           # the runtime role's password
export TESSERA_S3_ACCESS_KEY=... TESSERA_S3_SECRET=...
export TESSERA_SIGNER="$(cat log.sec)"
go run ./cmd/conformance/s3pg \
  --bucket=tessera-log \
  --s3_endpoint=http://localhost:9000 \
  --pg_schema=tessera_log \
  --skip_schema_init \
  --pg_max_conns=16 --pg_max_idle_conns=4
```

Keys can be generated with
`go run github.com/transparency-dev/serverless-log/cmd/generate_keys@80334bc9dc573e8f6c5b3694efad6358da50abd4 --key_name=<origin> --out_priv=log.sec --out_pub=log.pub`.
Then follow the [codelab](/cmd/conformance/README.md#codelab) with `WRITE_URL=http://localhost:2024/`
and `READ_URL=http://localhost:9000/tessera-log/`.
