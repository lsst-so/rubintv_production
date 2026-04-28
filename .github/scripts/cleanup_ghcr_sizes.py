#!/usr/bin/env python3
"""Fetch on-registry sizes for every version of a GHCR container package.

Companion to ``cleanup_ghcr_dryrun.sh``: reads the dry-run's
``/tmp/to-delete.txt`` and reports total / would-keep / would-delete
byte sums in human units. Use this to put a number on the cleanup
before merging the workflow or running it for real.

Sizes are summed from manifest layer sizes (recursing through
multi-arch indices). This is an UPPER BOUND on actual storage --
shared layers are counted once per referencing manifest, not once
per storage object. The keep-vs-delete ratio is the actionable bit;
the absolute number is order-of-magnitude.

See .claude/skills/rapid-analysis-ghcr-cleanup/SKILL.md for the
end-to-end workflow.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError
from urllib.request import Request, urlopen

OWNER = os.environ.get("OWNER", "lsst-so")
PACKAGE = os.environ.get("PACKAGE", "rubintv_production")
WORKERS = int(os.environ.get("WORKERS", "32"))

ACCEPT = ",".join(
    [
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    ]
)


def get_token() -> str:
    # Public package: anonymous pull token works.
    req = Request(f"https://ghcr.io/token?scope=repository:{OWNER}/{PACKAGE}:pull")
    with urlopen(req) as r:
        return json.load(r)["token"]


def get_versions() -> list[dict]:
    out = subprocess.check_output(
        [
            "gh",
            "api",
            f"/orgs/{OWNER}/packages/container/{PACKAGE}/versions",
            "--paginate",
            "--jq",
            ".[] | {id, name, created_at, tags: (.metadata.container.tags // [])}",
        ],
        text=True,
    )
    return [json.loads(line) for line in out.strip().splitlines() if line.strip()]


def fetch_manifest(token: str, digest: str) -> dict:
    req = Request(
        f"https://ghcr.io/v2/{OWNER}/{PACKAGE}/manifests/{digest}",
        headers={"Authorization": f"Bearer {token}", "Accept": ACCEPT},
    )
    with urlopen(req) as r:
        return json.load(r)


def manifest_size(token: str, digest: str, depth: int = 0) -> int:
    if depth > 3:
        return 0
    try:
        m = fetch_manifest(token, digest)
    except HTTPError as e:
        print(f"  WARN: HTTP {e.code} on {digest[:20]}", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"  WARN: {e} on {digest[:20]}", file=sys.stderr)
        return 0
    mt = m.get("mediaType", "")
    if "manifest.list" in mt or "image.index" in mt:
        return sum(manifest_size(token, c["digest"], depth + 1) for c in m.get("manifests", []))
    layers = m.get("layers", [])
    config = m.get("config", {}) or {}
    return sum(layer.get("size", 0) for layer in layers) + config.get("size", 0)


def human(n: float) -> str:
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PiB"


def main() -> int:
    token = get_token()
    versions = get_versions()
    print(
        f"Fetching manifest sizes for {len(versions)} versions ({WORKERS} workers)...",
        file=sys.stderr,
    )

    sizes: dict[int, int] = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(manifest_size, token, v["name"]): v for v in versions}
        for i, fut in enumerate(as_completed(futs), 1):
            v = futs[fut]
            sizes[v["id"]] = fut.result()
            if i % 100 == 0 or i == len(versions):
                print(f"  {i}/{len(versions)}", file=sys.stderr)

    to_delete: set[int] = set()
    try:
        with open("/tmp/to-delete.txt") as f:
            to_delete = {int(line.strip()) for line in f if line.strip()}
    except FileNotFoundError:
        print(
            "WARN: /tmp/to-delete.txt not found -- run cleanup_ghcr_dryrun.sh first",
            file=sys.stderr,
        )

    total = sum(sizes.values())
    delete_total = sum(s for vid, s in sizes.items() if vid in to_delete)
    keep_total = total - delete_total

    pct = (delete_total / total * 100) if total else 0
    print()
    print(f"Versions:                {len(versions)}")
    print(f"Total apparent size:     {human(total):>12}   ({total:>15,} B)")
    print(
        f"  Would delete:          {human(delete_total):>12}   "
        f"({delete_total:>15,} B, {len(to_delete)} versions, {pct:.1f}%)"
    )
    print(f"  Would keep:            {human(keep_total):>12}   ({keep_total:>15,} B)")
    print()
    print("NOTE: This is the sum of compressed layer sizes referenced by each")
    print("manifest. It overcounts shared storage -- a buildcache image and")
    print("its source share most layers but each contributes the full size to")
    print("this total. Treat the absolute number as an upper bound; the")
    print("delete-vs-keep ratio is what's actionable.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
