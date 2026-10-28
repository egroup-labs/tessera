# Tessera s3pg conformance stack (Docker Compose)

A local, self-contained deployment of a Tessera log on [`storage/s3pg`](/storage/s3pg/):
PostgreSQL for coordination, MinIO for the log's objects, and the
[s3pg conformance server](/cmd/conformance/s3pg/). It is used by the `s3pg-tlog-tiles-api`
job in [`integration_test.yml`](/.github/workflows/integration_test.yml), and shows the
recommended production layout in miniature:

* a dedicated PostgreSQL schema per log (`tessera_log`, plus `tessera_antispam`), selected with
  `--pg_schema` / `--antispam_pg_schema` (every statement names its tables fully) rather than
  `search_path` in the DSN, so that schema selection also works through PgBouncer;
* an owner role (`tessera_owner`) which applies `storage/s3pg/schema.sql` like a migration tool
  would (the `migrate` service), and a runtime role (`tessera_app`) with DML privileges only,
  used by the server with `--skip_schema_init` (the init script also revokes `CREATE` on schema
  `public` from `PUBLIC` in the `tessera` database, which PostgreSQL grants by default before
  version 15; with PostgreSQL older than 15 in production, do that in every database the runtime
  role can connect to, see [Roles and least privilege](/storage/s3pg/README.md#roles-and-least-privilege));
* role-level `idle_in_transaction_session_timeout`/`statement_timeout`/`lock_timeout` of 60s;
* a MinIO bucket which is publicly readable (GetObject only), and a server access key which can
  only read, write, list and delete within that bucket;
* secrets passed to the server through environment variables rather than flags.

| Service                    | What it does |
| -------------------------- | ------------ |
| `postgres`                 | PostgreSQL 16. On first start, [`postgres-init/10-tessera-roles.sh`](postgres-init/10-tessera-roles.sh) creates the roles, schemas and grants. |
| `migrate`                  | One-shot: [`migrate.sh`](migrate.sh) applies `storage/s3pg/schema.sql` and `storage/s3pg/antispam/schema.sql` as `tessera_owner`. Idempotent; runs on every `up`. |
| `minio`                    | MinIO, published on `127.0.0.1:9000`. |
| `minio-init`               | One-shot: [`minio-init.sh`](minio-init.sh) creates the `tessera-log` bucket, its public-read policy, and the server's access key. Idempotent. |
| `tessera-conformance-s3pg` | The server, built from [`cmd/conformance/s3pg/Dockerfile`](/cmd/conformance/s3pg/Dockerfile), on `127.0.0.1:2024`. Healthy while `GET /healthz` succeeds: once the server can read the checkpoint, and until it finds the database behind the published log. |

> [!CAUTION]
> This is for testing. The default passwords are well known, and the log is signed with the
> example key from [`cmd/conformance/posix/docker/compose.yaml`](/cmd/conformance/posix/docker/compose.yaml)
> (`example.com/log/testdata`), whose private key is public. Never use it for a real log.

## Running

From the repository root:

```sh
docker compose -f deployment/live/s3pg/compose.yaml up --build --detach
curl -s http://localhost:2024/healthz          # "ok" once ready
curl -s http://localhost:9000/tessera-log/checkpoint
```

Then follow the [codelab](/cmd/conformance/README.md#codelab) with:

```sh
export WRITE_URL=http://localhost:2024/
export READ_URL=http://localhost:9000/tessera-log/
export LOG_PUBLIC_KEY=example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx
```

Run the log integration test against it (as CI does):

```sh
go test -v -race ./integration/log --run_integration_test=true \
  --log_url="http://localhost:9000/tessera-log/" \
  --write_log_url="http://localhost:2024" \
  --log_public_key="example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx"
```

and check the whole log with fsck:

```sh
printf '%s' "example.com/log/testdata+33d7b496+AeHTu4Q3hEIMHNqc6fASMsq3rKNx280NI+oO5xCFkkSx" > /tmp/log.pub
go run ./cmd/fsck --storage_url=http://localhost:9000/tessera-log/ --public_key=/tmp/log.pub --ui=false
```

Inspect the coordination state (see the [monitoring queries](/storage/s3pg/README.md#monitoring)):

```sh
docker compose -f deployment/live/s3pg/compose.yaml exec postgres \
  psql -U postgres -d tessera -c 'SET search_path TO tessera_log' \
  -c 'SELECT s.next - i.seq AS integration_lag, s.next, i.seq FROM SeqCoord s, IntCoord i'
```

Tear down, **removing both volumes together**:

```sh
docker compose -f deployment/live/s3pg/compose.yaml down --volumes
```

The log's state lives in both the `pgdata` and `miniodata` volumes. Deleting (or restoring) only
one of them leaves PostgreSQL and the bucket inconsistent, and the server then refuses to start
(compose keeps restarting it; see `docker compose logs tessera-conformance-s3pg`):

* a fresh database, or one older than the bucket's checkpoint, fails at start-up with
  `coordination database is behind or inconsistent with the published checkpoint`, rather than
  starting again at index 0 and publishing a smaller checkpoint over the existing one. A refusal
  at start-up writes nothing, so the server starts again once the right database is back;
* a server which is already running when the database goes back in time trips at its next
  sequencing (if the database's next index is below what it has seen published or integrated),
  integration or publication attempt: from then on it refuses new entries and `/healthz` fails
  (compose reports it unhealthy, but doesn't restart it; `docker compose stop` makes it exit with
  status 1 promptly, without waiting for outstanding entries). Such a trip writes the object
  `s3pg-coordination-tripped` into the bucket
  (`curl -s http://localhost:9000/tessera-log/s3pg-coordination-tripped` shows when and why).
  While it exists, every start fails with `marker "s3pg-coordination-tripped" exists`, even once
  the right database is back;
* a fresh bucket with a database that already holds entries fails with `forced integrate: ...`
  (e.g. `missing node`), since the tiles and bundles the database refers to are gone.

For this test stack, `down --volumes` (which also removes the marker) and start again. For a
real log, see
[High availability and durability](/storage/s3pg/README.md#high-availability-and-durability) and
[Recovering from a lost commit](/storage/s3pg/README.md#recovering-from-a-lost-commit), which
deletes the marker only as its last step.

## Configuration

Environment variables read by `compose.yaml` (e.g. from your shell or an `.env` file next to it):

| Variable                 | Default                         | Used for |
| ------------------------ | ------------------------------- | -------- |
| `TESSERA_ANTISPAM`       | `false`                         | `--antispam` (the antispam schema is always created) |
| `TESSERA_SLOG_LEVEL`     | `0`                             | `--slog_level` (`-4` for debug) |
| `TESSERA_SIGNER`         | example key                     | checkpoint signer; the public key must match what you verify with |
| `POSTGRES_PASSWORD`      | `postgres-dev-only`             | PostgreSQL superuser |
| `TESSERA_OWNER_PASSWORD` | `tessera-owner-dev-only`        | `tessera_owner` (set on first start only) |
| `TESSERA_APP_PASSWORD`   | `tessera-app-dev-only`          | `tessera_app` (set on first start only) |
| `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` | `minioadmin`/`minioadmin-dev-only` | MinIO root credentials |
| `TESSERA_S3_ACCESS_KEY`/`TESSERA_S3_SECRET` | `tessera-log-writer`/`tessera-log-writer-dev-only` | the server's MinIO access key |

The image versions are pinned in `compose.yaml`. Pin MinIO to the same release you run in
production. It must support conditional writes with both `If-None-Match: *` (MinIO
`RELEASE.2024-05-07T06-41-25Z` or newer) and `If-Match` on `PutObject` (used for every
checkpoint write); check the release you run with the probes in
[`integration/minio/s3pg`](/integration/minio/s3pg/) (`ifmatch`, `ifmatch-race`, `inm-race`; see
[`storage/s3pg/VERIFICATION.md`](/storage/s3pg/VERIFICATION.md), where they were run against
MinIO built from source, not against the image pinned here).

To host several logs, add a schema (and runtime role) per log to the init script and the
`migrate` command, and run one server per log with its own `--pg_schema`, signer, and
`--bucket_prefix` (or bucket); see [Multiple logs](/storage/s3pg/README.md#multiple-logs).
