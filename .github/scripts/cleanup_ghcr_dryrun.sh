#!/usr/bin/env bash
# Local dry-run companion to .github/workflows/cleanup_ghcr.yaml.
#
# Mirrors the workflow's planning logic exactly, except:
#   * DRY_RUN is forced to true -- no DELETE calls are issued.
#   * GITHUB_STEP_SUMMARY output is redirected to /tmp/cleanup_summary.md.
#
# Use this to preview what the next scheduled cleanup would do
# without waiting for Sunday and without merging a workflow change
# first. See .claude/skills/rapid-analysis-ghcr-cleanup/SKILL.md for
# the end-to-end workflow.
#
# Requires: gh (authenticated, with read:packages scope), jq, GNU date
# (gdate on macOS, install via ``brew install coreutils``).

set -euo pipefail

OWNER="${OWNER:-lsst-so}"
PACKAGE="${PACKAGE:-rubintv_production}"
RECENT_DAYS="${RECENT_DAYS:-14}"
UNTAGGED_DAYS="${UNTAGGED_DAYS:-30}"
DRY_RUN=true

# macOS BSD date doesn't speak ``-d``; coreutils ships ``gdate``.
if command -v gdate >/dev/null 2>&1; then DATE=gdate; else DATE=date; fi

API="/orgs/${OWNER}/packages/container/${PACKAGE}/versions"

tagged_cutoff=$($DATE -u -d "${RECENT_DAYS} days ago" +%s)
untagged_cutoff=$($DATE -u -d "${UNTAGGED_DAYS} days ago" +%s)
echo "Tagged cutoff:   $($DATE -u -d @${tagged_cutoff} -Iseconds) (${RECENT_DAYS}d)"
echo "Untagged cutoff: $($DATE -u -d @${untagged_cutoff} -Iseconds) (${UNTAGGED_DAYS}d)"

gh api "${API}" --paginate \
  --jq '.[] | {id, created_at, tags: (.metadata.container.tags // [])}' \
  > /tmp/versions.jsonl

total=$(wc -l < /tmp/versions.jsonl | tr -d ' ')
echo "Found ${total} package versions."

: > /tmp/to-delete.txt
: > /tmp/cat-keep-protected.txt
: > /tmp/cat-keep-release.txt
: > /tmp/cat-keep-recent.txt
: > /tmp/cat-keep-untagged-recent.txt
: > /tmp/cat-del-stale.txt
: > /tmp/cat-del-untagged.txt

while IFS= read -r line; do
  id=$(jq -r '.id' <<<"$line")
  created=$(jq -r '.created_at' <<<"$line")
  created_epoch=$($DATE -u -d "$created" +%s)
  # mapfile is bash 4+, macOS ships 3.2 -- use a portable read loop.
  tags=()
  while IFS= read -r t; do tags+=("$t"); done < <(jq -r '.tags[]?' <<<"$line")

  if [ "${#tags[@]}" -eq 0 ]; then
    if [ "$created_epoch" -lt "$untagged_cutoff" ]; then
      echo "DELETE untagged  ${id}  (created ${created})"
      echo "$id" >> /tmp/to-delete.txt
      echo "$id ${created}" >> /tmp/cat-del-untagged.txt
    else
      echo "KEEP   untagged  ${id}  (recent: ${created})"
      echo "$id ${created}" >> /tmp/cat-keep-untagged-recent.txt
    fi
    continue
  fi

  keep=0
  keep_reason=""
  keep_bucket=""
  for t in "${tags[@]}"; do
    case "$t" in
      main|buildcache-main|latest)
        keep=1; keep_reason="protected '$t'"; keep_bucket="protected"; break;;
    esac
    if [[ "$t" =~ ^v?[0-9]+(\.[0-9]+){1,2}([.+-][A-Za-z0-9.+-]+)?$ ]]; then
      keep=1; keep_reason="release tag '$t'"; keep_bucket="release"; break
    fi
    if [[ "$t" =~ ^w_[0-9]{4}_[0-9]{2}(_.+)?$ ]]; then
      keep=1; keep_reason="weekly tag '$t'"; keep_bucket="release"; break
    fi
  done

  if [ "$keep" -eq 1 ]; then
    echo "KEEP   tagged    ${id}  [${tags[*]}]  -- ${keep_reason}"
    echo "$id ${created} ${tags[*]}" >> "/tmp/cat-keep-${keep_bucket}.txt"
    continue
  fi

  if [ "$created_epoch" -lt "$tagged_cutoff" ]; then
    echo "DELETE stale     ${id}  [${tags[*]}]  (created ${created})"
    echo "$id" >> /tmp/to-delete.txt
    echo "$id ${created} ${tags[*]}" >> /tmp/cat-del-stale.txt
  else
    echo "KEEP   recent    ${id}  [${tags[*]}]  (created ${created})"
    echo "$id ${created} ${tags[*]}" >> /tmp/cat-keep-recent.txt
  fi
done < /tmp/versions.jsonl

# --- Summary, mirroring the workflow's "Write summary" step ---
summary=/tmp/cleanup_summary.md
{
  k_prot=$(wc -l < /tmp/cat-keep-protected.txt | tr -d ' ')
  k_rel=$(wc -l < /tmp/cat-keep-release.txt | tr -d ' ')
  k_rec=$(wc -l < /tmp/cat-keep-recent.txt | tr -d ' ')
  k_unt=$(wc -l < /tmp/cat-keep-untagged-recent.txt | tr -d ' ')
  d_st=$(wc -l < /tmp/cat-del-stale.txt | tr -d ' ')
  d_un=$(wc -l < /tmp/cat-del-untagged.txt | tr -d ' ')
  d_total=$(wc -l < /tmp/to-delete.txt | tr -d ' ')

  echo "# GHCR cleanup summary (LOCAL DRY RUN)"
  echo
  echo "**Package:** ghcr.io/${OWNER}/${PACKAGE}"
  echo "**Total versions before run:** ${total}"
  echo
  echo "| Action | Category | Count |"
  echo "|---|---|---:|"
  echo "| Keep | Protected | ${k_prot} |"
  echo "| Keep | Release tag | ${k_rel} |"
  echo "| Keep | Tagged & recent (< ${RECENT_DAYS}d) | ${k_rec} |"
  echo "| Keep | Untagged & recent (< ${UNTAGGED_DAYS}d) | ${k_unt} |"
  echo "| Delete | Tagged & stale | ${d_st} |"
  echo "| Delete | Untagged & old | ${d_un} |"
  echo "| **Delete total** | | **${d_total}** |"
  echo
  if [ "${d_total}" -gt 0 ]; then
    echo "## Oldest 20 planned deletions"
    echo '```'
    cat /tmp/cat-del-stale.txt /tmp/cat-del-untagged.txt | sort -k2 | head -20
    echo '```'
  fi
} > "$summary"

echo
echo "=========================================="
cat "$summary"
