---
name: rapid-analysis-ghcr-cleanup
description: Inspect, preview, or trigger cleanup of the rapid analysis GHCR container package (`ghcr.io/lsst-so/rubintv_production`). Use this skill whenever the user asks about how much GHCR storage we're using, what the next scheduled cleanup will delete, draining the initial backlog of stale images, or testing changes to `.github/workflows/cleanup_ghcr.yaml` before merge. Also apply when the user mentions "old images", "buildcache layers piling up", "GHCR getting full", or anything that suggests they want to know the state of the container registry without waiting for the Sunday 03:00 UTC cron. The two helper scripts let you preview the deletion plan and put a byte-size number on it from a laptop, without merging a workflow change first.
---

# Rapid Analysis: GHCR Cleanup

`build_and_push.yaml` pushes one branch image and one paired
`buildcache-<branch>` registry-cache layer per PR, so the GHCR package
[`ghcr.io/lsst-so/rubintv_production`](https://github.com/orgs/lsst-so/packages/container/package/rubintv_production)
accumulates stale entries forever unless something prunes them. That
something is [`.github/workflows/cleanup_ghcr.yaml`](../../../.github/workflows/cleanup_ghcr.yaml),
which runs Sunday 03:00 UTC and on demand.

This skill covers two things you'll actually want to do day-to-day:

1. **Preview** the deletion plan without merging or waiting.
2. **Trigger** the workflow from the CLI (dry-run or for-real).

## The keep rules in one paragraph

The workflow keeps `main`, `buildcache-main`, `latest`, anything
tagged like a semver release (`1.2.3`, `v1.2.3`, `2026.04.28`),
anything tagged like an LSST stack weekly (`w_2026_13`,
`w_2025_41_ts_ofc`), and anything pushed in the last 14 days. It
also keeps untagged manifests pushed in the last 30 days. Everything
else gets pruned. **Tag anything you want kept long-term** -- branch
existence on origin is not a keep signal.

## Helper scripts

| Script | What it does |
|---|---|
| [`cleanup_ghcr_dryrun.sh`](../../../.github/scripts/cleanup_ghcr_dryrun.sh) | Mirrors the workflow's planning logic; lists every keep / delete decision and writes the same markdown summary to `/tmp/cleanup_summary.md`. Never deletes. |
| [`cleanup_ghcr_sizes.py`](../../../.github/scripts/cleanup_ghcr_sizes.py) | Fetches manifest sizes for every version (parallel, ~2 minutes for ~1300 versions) and prints total / would-keep / would-delete in human units. Reads `/tmp/to-delete.txt` from the dry-run script, so run that first. |

## One-time setup

```bash
# Add packages scope to your gh token (interactive browser flow).
gh auth refresh -h github.com -s read:packages

# GNU date (the scripts use `gdate`/`-d` syntax; macOS BSD date won't work).
brew install coreutils
```

## Common tasks

### "What would the next scheduled cleanup do?"

```bash
bash .github/scripts/cleanup_ghcr_dryrun.sh
```

Scroll for per-version decisions; the markdown summary at the end
gives a counts table. No deletions happen.

### "How much storage are we using? How much would the cleanup reclaim?"

```bash
bash .github/scripts/cleanup_ghcr_dryrun.sh   # writes /tmp/to-delete.txt
python3 .github/scripts/cleanup_ghcr_sizes.py
```

Prints something like:

```
Versions:                1284
Total apparent size:         4.57 TiB   (5,029,997,320,902 B)
  Would delete:              3.91 TiB   (4,304,585,480,085 B, 1103 versions, 85.6%)
  Would keep:              675.59 GiB   (725,411,840,817 B)
```

The byte total is the **sum of compressed layer sizes referenced by
each manifest**, so it overcounts shared storage (a buildcache image
and its source share most layers, multi-arch indices share base
layers). Treat the absolute number as upper-bound; the
delete-vs-keep ratio is what's actionable.

### "Trigger the real cleanup workflow now"

```bash
# Dry run -- writes the plan to the run's Summary tab, deletes nothing.
gh workflow run cleanup_ghcr.yaml -f dry_run=true

# For real.
gh workflow run cleanup_ghcr.yaml -f dry_run=false

# Watch it.
gh run watch
```

The workflow file must exist on the default branch for
`workflow_dispatch` to work -- if you get
`workflow ... not found on the default branch`, the cleanup
workflow hasn't merged yet.

### "Drain the initial (huge) backlog"

The workflow has a 200-version safety ceiling on **scheduled** runs.
The first real cleanup will likely want to delete thousands. To
override:

```bash
gh workflow run cleanup_ghcr.yaml -f dry_run=false
```

`workflow_dispatch` runs aren't capped, so this will go through.
After the backlog is gone, weekly steady-state will be tens of
versions and the cap won't matter.

### "Test a change to the workflow before merging"

The workflow's planning logic IS the dry-run script (modulo writing
to `$GITHUB_STEP_SUMMARY`). Edit the same logic in both, then run
the dry-run script locally to validate. If it looks right, the CI
run on the merged PR will look right too.

## Don't

- **Don't `gh workflow run` from a feature branch and expect it to
  use that branch's workflow file.** GitHub resolves the workflow
  file from the default branch. Use the local dry-run script for
  branch-iteration testing.
- **Don't lower the 200-version ceiling without thinking.** It's
  the only thing standing between a logic bug and a mass wipe of
  the package.
- **Don't add tag patterns to the keep regex without checking they
  don't accidentally match branch names.** `w_2026_13` should
  match; `tickets-DM-50534-w48` should not. The current LSST
  weekly regex anchors on `^w_` at the start of the tag for this
  reason.
