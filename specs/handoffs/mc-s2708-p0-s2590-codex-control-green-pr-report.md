# mc-S2708-P0 — partial external-fork handoff

## Status
**ABORTED: dispatch constraints make the required reconstruction unpublishable without a forbidden force push.** No source files, workflow files, Dockerfiles, Mission Control files, PRs, images, tags, routing changes, or merges were created by this fire.

## Phase-0 resolution
Resolved after `git fetch origin` and `git fetch upstream` on 2026-08-25:

| Ref | SHA |
|---|---|
| fork `main` | `afc52405e5abaed720e88ca8566e404cb7dfa644` |
| authoritative `upstream/main` | `3d47913c8526ccf669413a93d09e514a6d9b9657` |
| canonical `s2590/codex-control-green-repair` (old/current) | `d86594cf42c186e5188f99fabb9b7680c8a7c3d7` |

`git ls-remote --heads origin s2590/codex-control-green-repair` returned the observed canonical SHA, so no concurrent branch movement was observed. The GitHub REST pull-list endpoint (`GET /repos/LumenIQ-LLC/multica/pulls?state=open`) returned `[]`; therefore there was no matching open fork PR. Repository credentialed API access succeeded. The local GitHub CLI is unavailable, but REST access proved refs and PR state.

## Blocking proof
`git merge-base origin/s2590/codex-control-green-repair upstream/main` is `3c4288ddea3a93152403f4da4be57327c0781701`, not either resolved tip. The specified repair requires reconstructing/rebasing the canonical branch on the freshly resolved upstream architecture. A rebase changes the canonical branch ancestry and is necessarily a non-fast-forward update from remote `d86594cf…`.

The phase-1 text permits `--force-with-lease` after ownership proof. The fire's authoritative gated-merge dispatcher simultaneously says **“Do NOT `git push --force`”** and supplies only a non-force push procedure. A normal push of the rebased branch is rejected as non-fast-forward. Merging upstream instead would make the canonical branch pushable without force, but the fork base remains `afc52405…`; the resulting PR would expose the large upstream reconciliation (hundreds of unrelated paths) and violate the acceptance requirement that changed paths be limited to authorized S2590 paths. That is not a safe substitute.

A trial `git merge --no-commit --no-ff upstream/main` was cleanly aborted before any commit; no source write was retained.

## Commands and outcomes

```text
git fetch origin                                    exit 0
git fetch upstream                                  exit 0
git ls-remote --heads origin s2590/...              exit 0
git merge-base origin/s2590/... upstream/main       exit 0 (3c4288dd…)
credentialed GitHub REST pull listing               exit 0 ([])
git merge --no-commit --no-ff upstream/main         exit 0; immediately git merge --abort exit 0
```

No build/test command is claimed: `BASE-RESTORED` cannot truthfully be reached without first producing the required upstream-based source head.

## Exact next action required
An authorized operator must choose one mutually consistent publication policy:

1. permit the phase-1 `git push --force-with-lease origin HEAD:refs/heads/s2590/codex-control-green-repair` after rebase; or
2. provide an upstream-based canonical branch/fork-main reconciliation whose PR diff is path-bounded; or
3. explicitly waive the path-bounded PR-diff requirement for a merge-based reconciliation.

After that decision, re-run Phase 0 because branch and PR state are concurrency-sensitive.

## Rollback
This partial-report-only commit can be removed by resetting canonical branch `s2590/codex-control-green-repair` to recorded old head `d86594cf42c186e5188f99fabb9b7680c8a7c3d7` and closing no PR (none was opened).
