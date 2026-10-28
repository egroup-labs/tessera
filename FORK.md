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
- `upstream-sync`: the open sync, from the sync workflow. Push hand ports to
  it; later runs merge into it and keep them. It is deleted when its PR merges.

`main` is not maintained; compare with upstream directly
(`git fetch https://github.com/transparency-dev/tessera.git main`).

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

If the merged code does not build, the PR still opens, with the build log in
its description, and the tests are skipped until the port makes it build.
A merge conflict outside `go.mod`/`go.sum`, or a fork `go.mod` that differs
from upstream's beyond pgx, stops the run with an error: merge by hand.

### One-time setup

1. **Environment `upstream-sync`** (Settings → Environments): deployment
   branches limited to `s3pg`. Upstream's own workflows and PR branches then
   cannot read its secret.
2. **Secret `SYNC_TOKEN`** in that environment (not a repository secret): a
   fine-grained token for this repository only, with Contents, Workflows and
   Pull requests: read and write. It is required; the run fails without it.
   The workflow token cannot push upstream commits that change
   `.github/workflows`, and PRs it opens run no checks.
3. **Package access**: the e2e job pulls aibox's private MinIO mirror. In the
   `ghcr.io/egroup-labs/aibox/minio` package settings, grant this repository
   Read under "Manage Actions access".
4. **Disable upstream workflows** that need upstream's cloud accounts or
   services (AWS Conformance, Benchmark Go, Scorecard): Actions → workflow →
   Disable. They stay in the tree, so merges stay clean.
5. **Branch ruleset on `s3pg`**: require a PR, require the `s3pg tests`
   checks, block force pushes and deletion.

`storage/s3pg/s3pg.go` is a derived copy of `storage/aws/aws.go`, so a fix
upstream makes there does not reach s3pg by merging: port it by hand onto
the `upstream-sync` branch, following "Porting upstream changes" in the
driver README, then merge the PR.

## Updating aibox

In aibox, `services/tlog/go.mod` replaces `github.com/transparency-dev/tessera`
with this fork. To take the head of `s3pg`:

```sh
cd services/tlog
go mod edit -replace github.com/transparency-dev/tessera=github.com/egroup-labs/tessera@<sha>
go mod tidy
```

Pin a commit whose `s3pg tests` run is green, not the branch name.

Dependency CVEs (pgx, the AWS SDK, …) need no fork change: bump them in tlog's
`go.mod`, which wins over this fork's minimums.

A CVE in Tessera itself is published against `github.com/transparency-dev/tessera`,
but trivy and govulncheck look a replaced module up by its replacement path,
which no advisory names: aibox's scans never report one. `s3pg advisories`
covers it instead. It runs daily, queries OSV for Tessera, and fails when a
fix's release tag is not merged into `s3pg`. On a failure, run the sync
(Actions → s3pg upstream sync → Run workflow), merge its PR, then re-pin aibox.
A fix tagged only on an upstream release branch also fails the check; if main
carries the same fix, say so in the sync PR.

## Upstreaming

`git diff <upstream main> s3pg` is the whole change. Every part of it that upstream
accepts is code this fork no longer carries.
