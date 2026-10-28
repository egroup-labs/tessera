# s3pg chaos test

A failure-injection test for the `storage/s3pg` driver. Two conformance servers (replicas) share
one log — the same PostgreSQL schema and bucket — while `chaos` writes unique entries to both as
fast as it can, and `run.sh` injects failures:

1. `SIGKILL` replica a, restart it; then the same for replica b.
2. Terminate every database session of the runtime role (`pg_terminate_backend`).
3. Restart PostgreSQL (only if `PG_RESTART_CMD` is set).
4. `SIGSTOP` replica a for 20s (it may be holding coordination row locks), then `SIGCONT`.
5. `SIGKILL` both replicas at once, restart both.
6. Terminate all database sessions again.

The `chaos` tool then waits for every acknowledged write to be integrated and checks that:

- no acknowledged entry is lost, stored at a different index, or stored twice;
- no fork occurs (two checkpoints with the same size but different roots), and every
  checkpoint observed during the run is consistent with the final one (consistency proofs);
- inclusion proofs verify for a sample of entries.

Afterwards, `run.sh` runs `cmd/fsck` over the whole log (after garbage collection has had time
to run) and prints the coordination state from PostgreSQL. A healthy run shows lag 0, `seq_rows`
0, `published` equal to the checkpoint size, and `gc_from` close to the log size.

## Running

Use a **disposable** PostgreSQL cluster and an empty bucket. The script creates (and drops on
exit) its own database and a uniquely named runtime role, so it doesn't touch existing roles.
It does terminate that role's sessions and, if asked, restarts the server.

```bash
PG_ADMIN_DSN='postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable' \
S3_ENDPOINT=http://127.0.0.1:9000 S3_ACCESS_KEY=minioadmin S3_SECRET=minioadmin \
BUCKET=chaos-test PG_RESTART_CMD='sudo systemctl restart postgresql' \
  ./integration/chaos/s3pg/run.sh
```

The bucket must be anonymously readable at `$LOG_URL`, which defaults to
`$S3_ENDPOINT/$BUCKET/`. For MinIO, run `mc anonymous set download local/chaos-test`.
The script needs `go`, `psql`, `python3` and `curl`. The exit code is non-zero if any check fails.
Output goes to `./chaos-out` (`summary.json`, `events.log`, replica logs, `fsck.log`).

Write errors and "unacked but present" entries in the summary are expected. They are requests
that failed or timed out while a replica or the database was down; some were sequenced before
the failure. They are not invariant violations.
