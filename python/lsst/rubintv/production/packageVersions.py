# This file is part of rubintv_production.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Recording the git versions of the science packages used for processing.

For each image it dispatches, the head node records the exact git version of
the handful of packages that determine the AOS results. "Version" here means
the annotated/lightweight tag if the package is checked out exactly on a tag
(the normal case for a release), otherwise the bare commit SHA. ``rubintv_-
production`` itself is usually a SHA rather than a tag, which is expected and
fine.

These versions cannot change for the lifetime of a pod, so they are computed
once and cached on the head node (see ``HeadProcessController``).

As a sanity check, the versions git actually reports can be cross-checked
against the refs pinned in the ``Dockerfile``. Git is the source of truth; the
Dockerfile merely says what *should* have been checked out. That cross-check is
purely advisory — it only ever emits a warning and must never raise, because
the Dockerfile format can drift independently of this code. A unit test pins
the Dockerfile format so that a drift which breaks the parser is caught in CI
rather than silently turning the runtime check into a no-op.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from dataclasses import dataclass

__all__ = [
    "TRACKED_PACKAGES",
    "UNKNOWN_VERSION",
    "PackageVersions",
    "envVarForPackage",
    "getGitVersion",
    "getPackageVersion",
    "findDockerfile",
    "parseDockerfileRefs",
    "versionsMatch",
    "checkVersionsAgainstDockerfile",
]


_log = logging.getLogger(__name__)


# The packages whose git versions are recorded for every dispatched image. The
# set is deliberately small: just the things that change the AOS results. It is
# kept as a plain list so it is trivial to extend.
TRACKED_PACKAGES = ["ts_wep", "donut_viz", "rubintv_production"]

# Sentinel used when a package's version genuinely can't be determined (its
# ``*_DIR`` env var isn't set, the directory isn't a git checkout, etc.). We
# never raise for this; recording "unknown" is better than taking down the
# head node over a provenance nicety.
UNKNOWN_VERSION = "unknown"

# The minimum length a Dockerfile ref must have before we treat it as an
# abbreviated SHA that may prefix-match a full git SHA. Below this, a short
# string prefix-matching a SHA is far more likely to be a coincidence than a
# real abbreviation.
_MIN_ABBREV_SHA_LEN = 7

# Matches ``ARG <something>_ref="<ref>"`` (or single/no quotes) in the
# Dockerfile, capturing the package name and the ref it is pinned to. e.g.
# ``ARG ts_wep_ref="v17.6.1-alpha"`` -> ("ts_wep", "v17.6.1-alpha"). The
# ``STACK_TAG`` arg doesn't end in ``_ref`` so it is correctly ignored.
_DOCKERFILE_REF_RE = re.compile(r"""^\s*ARG\s+([A-Za-z0-9_]+)_ref\s*=\s*["']?([^"'\s]+)["']?""")


def envVarForPackage(packageName: str) -> str:
    """Return the env var naming a setup package's directory.

    EUPS exports ``<PACKAGE>_DIR`` (upper-cased) for every package it sets up,
    e.g. ``ts_wep`` -> ``TS_WEP_DIR``.

    Parameters
    ----------
    packageName : `str`
        The package name, e.g. ``"ts_wep"``.

    Returns
    -------
    envVar : `str`
        The name of the environment variable holding the package's directory.
    """
    return f"{packageName.upper()}_DIR"


def getGitVersion(packageDir: str) -> str:
    """Get the git version of the checkout in ``packageDir``.

    Returns the tag if ``HEAD`` is sitting exactly on one (the normal case for
    a released package), otherwise the full commit SHA.

    Parameters
    ----------
    packageDir : `str`
        The path to the package's git checkout.

    Returns
    -------
    version : `str`
        The exact tag at ``HEAD``, or the full commit SHA if ``HEAD`` is not on
        a tag.

    Raises
    ------
    subprocess.SubprocessError
        Raised if the ``rev-parse`` fallback fails, i.e. ``packageDir`` is not
        a git checkout at all. Callers that must not fail should use
        `getPackageVersion`, which swallows this.
    """
    try:
        tag = subprocess.run(
            ["git", "-C", packageDir, "describe", "--tags", "--exact-match", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if tag:
            return tag
    except subprocess.CalledProcessError:
        pass  # HEAD isn't sitting on a tag, so fall through to reporting the SHA

    return subprocess.run(
        ["git", "-C", packageDir, "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def getPackageVersion(packageName: str) -> str:
    """Get the git version of a setup package by name.

    Locates the package via its ``<PACKAGE>_DIR`` env var and reports its git
    version. Never raises: if the version can't be determined the sentinel
    `UNKNOWN_VERSION` is returned and a warning is logged.

    Parameters
    ----------
    packageName : `str`
        The package name, e.g. ``"ts_wep"``.

    Returns
    -------
    version : `str`
        The package's git version, or `UNKNOWN_VERSION` if it can't be found.
    """
    envVar = envVarForPackage(packageName)
    packageDir = os.environ.get(envVar)
    if not packageDir:
        _log.warning("%s is not set; cannot record a version for %s", envVar, packageName)
        return UNKNOWN_VERSION

    try:
        return getGitVersion(packageDir)
    except (subprocess.SubprocessError, OSError) as e:
        _log.warning("Failed to get the git version for %s in %s: %s", packageName, packageDir, e)
        return UNKNOWN_VERSION


@dataclass
class PackageVersions:
    """The git versions of the tracked science packages at processing time.

    The versions are held in a dict keyed by package name rather than as named
    fields so that the set of recorded packages can be grown just by editing
    `TRACKED_PACKAGES` — no change to the wire format written to the AOS
    metadata page is required.

    Parameters
    ----------
    versions : `dict` [`str`, `str`]
        Mapping of package name to git version (tag or SHA).
    """

    versions: dict[str, str]

    @classmethod
    def fromPackages(cls, packageNames: list[str] | None = None) -> PackageVersions:
        """Build by reading the git version of each named package.

        Parameters
        ----------
        packageNames : `list` [`str`], optional
            The packages to record. Defaults to `TRACKED_PACKAGES`.

        Returns
        -------
        packageVersions : `PackageVersions`
            The collected versions.
        """
        if packageNames is None:
            packageNames = TRACKED_PACKAGES
        return cls(versions={name: getPackageVersion(name) for name in packageNames})

    def toShardDict(self) -> dict[str, str]:
        """Render as a metadata-shard cell.

        The returned dict is the cell value written to the AOS metadata
        page. It carries the standard ``DISPLAY_VALUE`` "📖" marker so the
        frontend shows a single book glyph that expands to the per-package
        versions, keeping the table tidy as the tracked set grows.

        Returns
        -------
        shardDict : `dict` [`str`, `str`]
            The package versions plus the ``DISPLAY_VALUE`` book marker.
        """
        shardDict = dict(self.versions)
        shardDict["DISPLAY_VALUE"] = "📖"
        return shardDict


def findDockerfile() -> str | None:
    """Locate the Dockerfile shipped alongside the rubintv_production checkout.

    The Dockerfile is copied into the image at the root of the
    ``rubintv_production`` checkout, so it sits next to ``$RUBINTV_PRODUCTION_-
    DIR``.

    Returns
    -------
    dockerfilePath : `str` or `None`
        The path to the Dockerfile, or `None` if it can't be located.
    """
    packageDir = os.environ.get(envVarForPackage("rubintv_production"))
    if not packageDir:
        return None
    dockerfilePath = os.path.join(packageDir, "Dockerfile")
    return dockerfilePath if os.path.isfile(dockerfilePath) else None


def parseDockerfileRefs(dockerfilePath: str) -> dict[str, str]:
    """Parse the ``ARG <package>_ref=...`` pins out of the Dockerfile.

    Parameters
    ----------
    dockerfilePath : `str`
        The path to the Dockerfile.

    Returns
    -------
    refs : `dict` [`str`, `str`]
        Mapping of package name to the ref it is pinned to, e.g.
        ``{"ts_wep": "v17.6.1-alpha", ...}``. Packages without a ``_ref`` arg
        (e.g. ``rubintv_production`` itself, which is COPYed in rather than
        checked out) are simply absent.
    """
    refs: dict[str, str] = {}
    with open(dockerfilePath) as f:
        for line in f:
            match = _DOCKERFILE_REF_RE.match(line)
            if match:
                refs[match.group(1)] = match.group(2)
    return refs


def versionsMatch(gitVersion: str, dockerfileRef: str) -> bool:
    """Check whether a git version agrees with a Dockerfile ref.

    A tag pin matches exactly. A SHA pin is often abbreviated in the
    Dockerfile, so an abbreviated ref that prefixes the full git SHA counts
    as a match.

    Parameters
    ----------
    gitVersion : `str`
        The version git reports (an exact tag or a full SHA).
    dockerfileRef : `str`
        The ref the Dockerfile pins the package to.

    Returns
    -------
    match : `bool`
        Whether the two refer to the same checkout.
    """
    if gitVersion == dockerfileRef:
        return True
    # An abbreviated SHA in the Dockerfile should prefix-match the full SHA.
    if len(dockerfileRef) >= _MIN_ABBREV_SHA_LEN and gitVersion.startswith(dockerfileRef):
        return True
    return False


def checkVersionsAgainstDockerfile(
    packageVersions: PackageVersions,
    dockerfilePath: str,
    log: logging.Logger | None = None,
) -> None:
    """Warn if the recorded git versions disagree with the Dockerfile pins.

    Git is the source of truth; this is purely a sanity check that the running
    checkouts are what the Dockerfile asked for. It is advisory only: it never
    raises, and any problem (the Dockerfile being unreadable, the format having
    drifted so nothing parses, etc.) results in at most a warning. A package
    with no ``_ref`` pin in the Dockerfile (e.g. ``rubintv_production``) is
    skipped silently.

    Parameters
    ----------
    packageVersions : `PackageVersions`
        The versions git reported for the running checkouts.
    dockerfilePath : `str`
        The path to the Dockerfile to cross-check against.
    log : `logging.Logger`, optional
        The logger to warn on. Defaults to this module's logger.
    """
    if log is None:
        log = _log

    try:
        refs = parseDockerfileRefs(dockerfilePath)
    except OSError as e:
        log.warning("Could not read the Dockerfile at %s for version cross-check: %s", dockerfilePath, e)
        return
    except Exception as e:  # noqa: BLE001 — advisory check must never take down the head node
        log.warning("Unexpected error parsing the Dockerfile at %s: %s", dockerfilePath, e)
        return

    for name, gitVersion in packageVersions.versions.items():
        expected = refs.get(name)
        if expected is None:
            continue  # no pin in the Dockerfile for this package, nothing to check
        if gitVersion == UNKNOWN_VERSION:
            continue  # couldn't determine the git version; already warned about elsewhere
        if not versionsMatch(gitVersion, expected):
            log.warning(
                "Package %s is at git version %r but the Dockerfile pins it to %r",
                name,
                gitVersion,
                expected,
            )
