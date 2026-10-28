# egroup-labs/tessera

A fork of [transparency-dev/tessera](https://github.com/transparency-dev/tessera)
that adds `storage/s3pg`: the `aws` driver's S3 object store with PostgreSQL
instead of MySQL for sequencing. aibox's tlog service uses it
([EGL-801](https://linear.app/e-group/issue/EGL-801)). The driver, its
differences from `storage/aws`, and its verification are in
[`storage/s3pg/README.md`](storage/s3pg/README.md).

## Branches

- `s3pg` (default): upstream main plus the s3pg commits. aibox pins commits on
  it.
- `main`: an untouched mirror of upstream main.
- `upstream-sync`: written by the sync workflow; never commit to it.

## Rules

1. **Only add files.** Never edit an upstream file, except `go.mod`/`go.sum`
   for the pgx requirement. An upstream merge then cannot conflict anywhere
   else, so the sync stays automatic. New CI goes in `.github/workflows/s3pg_*.yml`.
2. **Merge upstream; never rebase or squash `s3pg`.** aibox pins commits on
   `s3pg` by pseudo-version, and a rewritten commit breaks that pin.
   Merge the sync PR with a merge commit (the only merge method this repo allows).

## Taking upstream changes

`s3pg upstream sync` runs every Monday (or on demand: Actions → Run workflow).
It merges upstream main into `upstream-sync`, runs the s3pg tests on the result,
and opens a PR listing upstream's changes to `storage/aws`, `storage/internal`,
`internal/` and the appender API.

It needs the repository secret `SYNC_TOKEN`, a fine-grained token for this
repository only with Contents, Workflows and Pull requests: read and write.
The workflow token cannot push upstream commits that change
`.github/workflows`; without the secret, a sync that brings such a commit
fails with that message, and the others still go through. The job also
fast-forwards `main` to upstream main.

`storage/s3pg/s3pg.go` is a derived copy of `storage/aws/aws.go`, so a fix
upstream makes there does not reach s3pg by merging: port it by hand onto
the `upstream-sync` branch, following "Porting upstream changes" in the
driver README, then merge the PR.

## Updating aibox

In aibox, `services/tlog/go.mod` replaces `github.com/transparency-dev/tessera`
with this fork. To take the head of `s3pg`:

```sh
cd services/tlog
go mod edit -replace github.com/transparency-dev/tessera=github.com/egroup-labs/tessera@s3pg
go mod tidy
```

Dependency CVEs (pgx, the AWS SDK, …) need no fork change: bump them in tlog's
`go.mod`, which wins over this fork's minimums. Upstream's `govulncheck`
workflow also runs here, on the fork's own code.

## Upstreaming

`git diff main s3pg` is the whole change. Every part of it that upstream
accepts is code this fork no longer carries.
