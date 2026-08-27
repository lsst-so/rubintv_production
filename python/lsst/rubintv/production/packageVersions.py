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

"""Scraping the git versions of the science packages used for processing.

For each image it dispatches, the head node records the exact git version of
the handful of packages that determine the AOS results. "Version" here means
the annotated/lightweight tag if the package is checked out exactly on a tag
(the normal case for a release), otherwise the bare commit SHA. ``rubintv_-
production`` itself is usually a SHA rather than a tag, which is expected and
fine.

These versions cannot change for the lifetime of a pod, so they are computed
once and cached on the head node (see ``HeadProcessController``).

This module owns the *scraping* side only: reading versions out of the running
environment, and the advisory cross-check against the ``Dockerfile``. The
``PackageVersions`` data model itself — with its content-hash identity and its
ConsDB round-trip — lives in ``lsst.summit.utils.packageVersions``, so that
users without access to this package can still read the recorded data back.

As a sanity check, the versions git actually reports can be cross-checked
against the refs pinned in the ``Dockerfile``. Git is the source of truth; the
Dockerfile merely says what *should* have been checked out. That cross-check is
purely advisory — it only ever emits a warning and must never raise, because
the Dockerfile format can drift independently of this code. A unit test pins
the Dockerfile format so that a drift which breaks the parser is caught in CI
rather than silently turning the runtime check into a no-op.
"""

from __future__ import annotations

import importlib.metadata
import logging
import os
import re
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass

from lsst.summit.utils.packageVersions import UNKNOWN_VERSION, PackageVersions

__all__ = [
    "TRACKED_PACKAGES",
    "TRACKED_INSTALLED_PACKAGES",
    "PACKAGE_VERSIONS_SHARD_KEY",
    "VersionComparison",
    "envVarForPackage",
    "getGitVersion",
    "getPackageVersion",
    "getInstalledPackageVersion",
    "getCurrentPackageVersions",
    "makePackageVersionShardDict",
    "packageVersionsFromShardDict",
    "missingPackageDirEnvVars",
    "findDockerfile",
    "parseDockerfileRefs",
    "versionsMatch",
    "compareVersionsToDockerfile",
    "checkVersionsAgainstDockerfile",
]


_log = logging.getLogger(__name__)


# The packages whose git versions are recorded for every dispatched image. The
# set is deliberately small: just the things that change the AOS results. These
# are git checkouts located via their EUPS ``<PACKAGE>_DIR`` env var. Kept as a
# plain list so it is trivial to extend.
TRACKED_PACKAGES = ["ts_wep", "donut_viz", "rubintv_production", "tarts"]

# Tracked packages that are installed into the environment (conda/pip) rather
# than being git checkouts. These have no ``*_DIR`` env var, so their version
# comes from the installed distribution metadata instead. In the Dockerfile
# they are pinned with a conda ``name=version`` spec rather than an ``ARG
# <name>_ref``.
TRACKED_INSTALLED_PACKAGES = ["danish"]

# The AOS metadata-shard cell the package versions are written under. Shared by
# the writer (the head node) and the reader (the ConsDB backfill) so the two
# can never drift apart.
PACKAGE_VERSIONS_SHARD_KEY = "Package versions"

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


def getInstalledPackageVersion(packageName: str) -> str:
    """Get the version of an installed (conda/pip) distribution.

    Used for tracked packages that are installed into the environment rather
    than being git checkouts (e.g. ``danish``), so they have no ``*_DIR`` env
    var. The version is read from the installed distribution metadata — no need
    to import the package itself. Never raises: returns `UNKNOWN_VERSION` and
    logs a warning if the distribution can't be found.

    Parameters
    ----------
    packageName : `str`
        The distribution name, e.g. ``"danish"``.

    Returns
    -------
    version : `str`
        The installed version, or `UNKNOWN_VERSION` if it can't be found.
    """
    try:
        return importlib.metadata.version(packageName)
    except importlib.metadata.PackageNotFoundError:
        _log.warning("Installed package %s not found; cannot record its version", packageName)
        return UNKNOWN_VERSION
    except Exception as e:  # noqa: BLE001 — version recording must never raise
        _log.warning("Failed to get the version for installed package %s: %s", packageName, e)
        return UNKNOWN_VERSION


def getCurrentPackageVersions(
    packageNames: list[str] | None = None,
    installedPackageNames: list[str] | None = None,
) -> PackageVersions:
    """Collect the versions of the tracked packages in this environment.

    Parameters
    ----------
    packageNames : `list` [`str`], optional
        Git-checkout packages to record (version from the checkout at their
        ``*_DIR`` env var). Defaults to `TRACKED_PACKAGES`.
    installedPackageNames : `list` [`str`], optional
        Installed packages to record (version from the installed distribution
        metadata). Defaults to `TRACKED_INSTALLED_PACKAGES`.

    Returns
    -------
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The collected versions.
    """
    if packageNames is None:
        packageNames = TRACKED_PACKAGES
    if installedPackageNames is None:
        installedPackageNames = TRACKED_INSTALLED_PACKAGES
    versions = {name: getPackageVersion(name) for name in packageNames}
    for name in installedPackageNames:
        versions[name] = getInstalledPackageVersion(name)
    return PackageVersions(versions=versions)


def makePackageVersionShardDict(packageVersions: PackageVersions) -> dict[str, str]:
    """Render package versions as a metadata-shard cell.

    The returned dict is the cell value written to the AOS metadata page. Apart
    from the ``DISPLAY_VALUE`` "📖" marker (which makes the frontend show a
    single book glyph that expands to the per-package versions), it is exactly
    the ``{package: version}`` mapping, so the backfill tooling can recover the
    versions from the merged metadata by dropping the marker (see
    `packageVersionsFromShardDict`).

    Parameters
    ----------
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The versions to render.

    Returns
    -------
    shardDict : `dict` [`str`, `str`]
        The package versions plus the ``DISPLAY_VALUE`` book marker.
    """
    shardDict: dict[str, str] = dict(packageVersions.versions)
    shardDict["DISPLAY_VALUE"] = "📖"
    return shardDict


def packageVersionsFromShardDict(shardDict: Mapping[str, str]) -> PackageVersions:
    """Recover the package versions from a metadata-shard cell.

    The inverse of `makePackageVersionShardDict`: strips the ``DISPLAY_VALUE``
    marker and treats every remaining key as a package version. Used by the
    ConsDB backfill, whose source is the merged AOS metadata written at
    processing time.

    Parameters
    ----------
    shardDict : `Mapping` [`str`, `str`]
        The cell value as written by `makePackageVersionShardDict` and merged
        into the metadata sidecar.

    Returns
    -------
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The recovered versions.
    """
    versions = {key: value for key, value in shardDict.items() if key != "DISPLAY_VALUE"}
    return PackageVersions(versions=versions)


def missingPackageDirEnvVars(packageNames: list[str] | None = None) -> list[str]:
    """Return the tracked packages whose ``*_DIR`` env var is not set.

    EUPS sets these when it sets up a package, so a non-empty result means the
    environment is broken — the package isn't actually set up and its code
    can't run. The head node tolerates this for version recording (logging
    "unknown"), but a validation context such as the CI suite should treat it
    as a hard failure.

    Parameters
    ----------
    packageNames : `list` [`str`], optional
        The packages to check. Defaults to `TRACKED_PACKAGES`.

    Returns
    -------
    missing : `list` [`str`]
        The package names whose directory env var is unset or empty, in the
        order given.
    """
    if packageNames is None:
        packageNames = TRACKED_PACKAGES
    return [name for name in packageNames if not os.environ.get(envVarForPackage(name))]


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


def _findCondaPin(lines: list[str], packageName: str) -> str | None:
    """Find a ``name=version`` conda/pip pin for ``packageName`` in the lines.

    Matches e.g. ``    danish=1.1.1 \\`` -> ``"1.1.1"``. Scoped to the exact
    package name so it can't pick up unrelated ``foo=bar`` lines.
    """
    pattern = re.compile(rf"^\s*{re.escape(packageName)}==?([^\s\\]+)")
    for line in lines:
        match = pattern.match(line)
        if match:
            return match.group(1)
    return None


def parseDockerfileRefs(dockerfilePath: str, condaPackages: list[str] | None = None) -> dict[str, str]:
    """Parse the package version pins out of the Dockerfile.

    Picks up two pin styles: ``ARG <package>_ref=...`` (git checkouts) for any
    package, and conda/pip ``<package>=<version>`` specs for the names given in
    ``condaPackages`` (installed packages such as ``danish``).

    Parameters
    ----------
    dockerfilePath : `str`
        The path to the Dockerfile.
    condaPackages : `list` [`str`], optional
        Installed-package names to additionally look for as ``name=version``
        conda pins. Defaults to none.

    Returns
    -------
    refs : `dict` [`str`, `str`]
        Mapping of package name to the ref/version it is pinned to, e.g.
        ``{"ts_wep": "v17.6.1-alpha", "danish": "1.1.1", ...}``. Packages with
        no recognised pin (e.g. ``rubintv_production``, which is COPYed in
        rather than checked out) are simply absent.
    """
    refs: dict[str, str] = {}
    with open(dockerfilePath) as f:
        lines = f.readlines()
    for line in lines:
        match = _DOCKERFILE_REF_RE.match(line)
        if match:
            refs[match.group(1)] = match.group(2)
    for name in condaPackages or []:
        pin = _findCondaPin(lines, name)
        if pin is not None:
            refs[name] = pin
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


@dataclass
class VersionComparison:
    """The result of comparing one package's git version to the Dockerfile.

    Parameters
    ----------
    package : `str`
        The package name.
    gitVersion : `str`
        The version git reported for the running checkout.
    dockerfileRef : `str` or `None`
        The ref the Dockerfile pins the package to, or `None` if there is no
        pin (e.g. ``rubintv_production``, which is COPYed in, not checked out).
    matches : `bool` or `None`
        Whether the git version agrees with the Dockerfile pin. `None` when
        the comparison is not meaningful — there is no pin, or the git version
        could not be determined.
    """

    package: str
    gitVersion: str
    dockerfileRef: str | None
    matches: bool | None


def compareVersionsToDockerfile(
    packageVersions: PackageVersions,
    dockerfilePath: str,
    log: logging.Logger | None = None,
) -> list[VersionComparison]:
    """Compare each recorded version to its Dockerfile pin.

    Never raises: if the Dockerfile can't be read or parsed, a warning is
    logged and every package is reported as not-comparable (``matches=None``).

    Parameters
    ----------
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The versions recorded for the running packages.
    dockerfilePath : `str`
        The path to the Dockerfile to cross-check against.
    log : `logging.Logger`, optional
        The logger to warn on if the Dockerfile can't be read. Defaults to this
        module's logger.

    Returns
    -------
    comparisons : `list` [`VersionComparison`]
        One entry per tracked package, in the order they are recorded.
    """
    if log is None:
        log = _log

    try:
        refs = parseDockerfileRefs(dockerfilePath, condaPackages=TRACKED_INSTALLED_PACKAGES)
    except OSError as e:
        log.warning("Could not read the Dockerfile at %s for version cross-check: %s", dockerfilePath, e)
        refs = {}
    except Exception as e:  # noqa: BLE001 — advisory check must never take down the head node
        log.warning("Unexpected error parsing the Dockerfile at %s: %s", dockerfilePath, e)
        refs = {}

    comparisons: list[VersionComparison] = []
    for name, gitVersion in packageVersions.versions.items():
        ref = refs.get(name)
        if ref is None or gitVersion == UNKNOWN_VERSION:
            matches = None  # no pin to compare against, or no git version to compare
        else:
            matches = versionsMatch(gitVersion, ref)
        comparisons.append(
            VersionComparison(package=name, gitVersion=gitVersion, dockerfileRef=ref, matches=matches)
        )
    return comparisons


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
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The versions git reported for the running checkouts.
    dockerfilePath : `str`
        The path to the Dockerfile to cross-check against.
    log : `logging.Logger`, optional
        The logger to warn on. Defaults to this module's logger.
    """
    if log is None:
        log = _log

    for comparison in compareVersionsToDockerfile(packageVersions, dockerfilePath, log=log):
        if comparison.matches is False:
            log.warning(
                "Package %s is at git version %r but the Dockerfile pins it to %r",
                comparison.package,
                comparison.gitVersion,
                comparison.dockerfileRef,
            )
