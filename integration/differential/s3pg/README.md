# Differential test: `storage/s3pg` vs upstream `storage/aws`

`storage/s3pg` (S3-compatible object storage + PostgreSQL) is derived from `storage/aws`
(S3 + MySQL). This command drives **both** drivers with the same deterministic workload and
checks that they produce byte-identical logs. It is a `main` package, so it never runs as part of
`go test ./...`.

## What it does

1. **log**: grows a log in phases. Each phase starts a fresh Appender (a personality restart) with
   `WithBatching(N, 1h)`, adds exactly `N` entries per step from one goroutine (so the sequencer
   queue flushes exactly once, as one `Seq` row), and waits for that step to be integrated before
   the next one. Integration points, and therefore the set of partial tiles/bundles written, are
   then identical for both drivers. The full workload (~131.6k entries) uses batch sizes 1, 7, 24,
   256, 300, 50 (~60KB and 65535-byte entries), 62288 (lands exactly on 256*256), 1, 66050 (crosses
   2*256*256) and 5, and contains empty entries and duplicates (no antispam, so each one gets a new
   index; the harness asserts this). After each phase it compares:
   * every object in both buckets: key set, bytes, `Content-Type`, `Cache-Control`
     (tiles, full and partial entry bundles),
   * the signed checkpoints (note text and Ed25519 signature; same key, so both must be
     identical), and verifies each one,
   * the coordination state (`Tessera`, `SeqCoord.next`, `IntCoord` size/root, number of `Seq`
     rows, `PubCoord.size`, `GCCoord.fromSize`) in MySQL vs PostgreSQL. `PubCoord.publishedAt`
     is a timestamp and is not compared.
2. **gc**: runs both drivers with a 250ms garbage collection interval, waits for GC to finish
   (or stall), and compares the remaining objects with each other and with an independent model
   of upstream's GC semantics.
3. **antispam**: runs the `storage/aws/antispam` (MySQL) and `storage/s3pg/antispam` (PostgreSQL)
   followers over the same log (the upstream one, via its `LogReader`), for each of
   `--antispam_batches`, then compares the complete `AntispamIDSeq` tables with each other and
   with a model ("first index of each identity wins"), and looks up duplicates, a random sample
   and never-added entries through both `Decorator`s.
4. **migrate**: `tessera.NewMigrationTarget` + `Migrate` from the upstream log into a fresh
   upstream target and a fresh s3pg target; compares the target buckets and coordination state.
   Then migrates again into the same buckets with fresh databases (every write becomes an
   idempotent re-write of an existing object) and checks that nothing changed. Also compares
   what the `MigrationWriter`'s `LogReader.IntegratedSize` returns.
5. **retry**: an integration whose coordination transaction fails *after* its tiles and entry
   bundles were written must be retried, and both drivers must converge to identical buckets.
   A new pair of logs gets triggers on `IntCoord` which make the integration's
   `UPDATE IntCoord` fail (MySQL: `SIGNAL`; PostgreSQL: `RAISE`), or, in PostgreSQL, its
   `COMMIT` (a deferred constraint trigger). Failures are counted outside the failing
   transaction (a MyISAM table, a sequence), so that a trigger can fail exactly once. Phases,
   each compared like the log scenario:
   * `fail-once` (250 -> 253): the UPDATE fails exactly once; the retry integrates the same entries.
   * `fail-grow` (-> 267, crossing 256): attempts fail (s3pg: at COMMIT) until a second batch has
     been sequenced; the successful retry integrates both batches. The failed attempts' partial
     bundle and tile at size 260 must be left behind in both buckets.
   * `fail-restart` (-> 567, crossing 512): attempts fail, the appender is killed (no shutdown),
     the trigger is disarmed and a new appender performs the retry.

   For s3pg this exercises `checkBucketConsistent` (the bucket holds wider resources than
   `IntCoord`), and every phase checks that no coordination guard has tripped.

Differences which are explained by intentional deviations of s3pg from upstream are reported as
`KNOWN` (see below); anything else is reported as `DIFF` and makes the command exit non-zero.

## Running

Needs MySQL 8 (`root`/`root`), PostgreSQL and an S3-compatible server supporting
`If-None-Match: *` (e.g. moto) with path-style addressing and credentials `test`/`test`:

```sh
go run ./integration/differential/s3pg                        # full workload, ~10 minutes
go run ./integration/differential/s3pg --scale=small          # ~1.6k entries, ~2 minutes
go run ./integration/differential/s3pg --scale=small --bucket_prefix=logs/diff --rerun_timeout=90s
go run ./integration/differential/s3pg --scale=small --pg_schema_mode=qualified
```

Flags: `--mysql_dsn` (without database name), `--pg_dsn`, `--s3_endpoint`, `--scenarios`
(default `log,gc,antispam,migrate,retry`), `--antispam_batches`, `--rerun_timeout` (how long
the migration re-run may take; upstream never finishes it with `--bucket_prefix`), `--keep`
(keep the per-run buckets, MySQL databases and PostgreSQL schemas, all named
`diff[-_]<run id>[-_]...`; otherwise they are removed at exit).

`--pg_schema_mode=qualified` configures every s3pg store with `Config.Schema` /
`AntispamOpts.Schema` (schemas named `Diff-<run id>-<tag>`, which need quoting), while the
search_path of their DSNs selects a *decoy* schema holding initialised copies of all log and
antispam tables. A statement which is not schema-qualified would silently read or write the
decoy: reads show up as differences from upstream, and the decoy's contents are checked to be
unchanged at the end. (Negative control: with `Config.Schema` left empty the first phase fails
and the decoy check reports the decoy's `SeqCoord`, `IntCoord`, ... as changed.)

## Known, explained differences

* **GC on S3-compatible stores**: upstream `deleteObjectsWithPrefix` sends `DeleteObjects` with
  an empty object list when a prefix has no partial objects; S3/moto/MinIO reject that with
  `MalformedXML`, so the upstream GC run fails, never advances `GCCoord.fromSize`, and keeps
  re-collecting the same first 101 bundles. s3pg skips empty prefixes and completes. The harness
  checks that upstream's remaining objects match its model at the stalled position, and that
  s3pg's match the model of a complete GC.
* **BucketPrefix + re-writes**: upstream `setObjectIfNoneMatch` re-applies the bucket prefix when
  reading back an existing object after `PreconditionFailed`, so idempotent re-writes fail when
  `BucketPrefix` is set (visible in the migration re-run with `--bucket_prefix`). Fixed in s3pg.
  For the same reason upstream can never retry an integration whose DB transaction failed when
  `BucketPrefix` is set: with `--bucket_prefix`, the retry scenario first shows this on a small
  separate log pair (upstream stuck after exactly one injected failure, s3pg retries), then
  compares s3pg *with* the prefix against upstream *without* it (keys are compared relative to
  the prefix).
* **LogReader partial -> full fallback**: upstream's `LogReader` does not map S3 `NoSuchKey` to
  `os.ErrNotExist`, so after GC removed a partial bundle, `ReadEntryBundle(0, p)` fails instead
  of falling back to the full bundle. s3pg falls back.
* **MigrationWriter's LogReader**: upstream leaves its `integratedSize`/`nextIndex` funcs nil, so
  `IntegratedSize` (called by the antispam follower of `MigrationOptions.WithAntispam`) panics;
  s3pg wires them to the sequencer and returns the migrated size.
