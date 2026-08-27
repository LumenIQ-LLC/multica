# Rolling back the deployed multica

## The procedure

```bash
# 1. Find the last-good digest in the table below, then pin the Cloud Run job to it:
gcloud run jobs update mc-multica-runtime --region us-east1 \
  --image us-east1-docker.pkg.dev/lumeniq-saas-factory/mc-images/mc-multica-runtime@<DIGEST>
```

That is the whole rollback. The deploy is already by **immutable digest**, so a
rollback target has always existed — it was simply not findable at 2am, which is
the only problem this file solves.

## Released versions

The fork publishes its own tags. Upstream owns the `v0.4.x` series, so fork
releases use an `lq-` prefix and cannot collide with an upstream tag.

| Fork tag | Commit | Runtime image digest | Notes |
| --- | --- | --- | --- |
| `lq-v0.1.0` | `7b45a13e463d` | `sha256:43a8b5717bcb83218f628d545097476c455d366a6601f69bcffef666ee8f1c50` | First fork-owned release. Provider-neutral turn control (Codex), B1 evidence contract, idle-ordering fix. |

**Add a row on every deploy.** A digest with no commit next to it is the state
this file exists to prevent.

## Why a tag is needed at all

The runtime consumes multica as a Go module, pinned by pseudo-version. A
pseudo-version names a commit, not a release, so "which multica is in
production?" had no answer that survived a `git log`. The tag gives the digest a
human-readable name; the table gives the name a commit.

## Verifying what is actually deployed

Do not trust the table alone — confirm against the running job:

```bash
gcloud run jobs describe mc-multica-runtime --region us-east1 \
  --format='value(template.template.containers[0].image)'
```

If that digest is not in the table, the table is stale: find the
`deploy-multica-runtime` workflow run that produced it, take its head SHA, and
add the row.

## Cutting the next release

```bash
git tag -a lq-v0.1.1 <commit> -m "lq-v0.1.1: <one line>"
git push origin lq-v0.1.1
```

Then deploy, capture the digest the workflow prints, and add the row. Tag first,
deploy second — a digest whose commit was never tagged is how the last one
became unfindable.
