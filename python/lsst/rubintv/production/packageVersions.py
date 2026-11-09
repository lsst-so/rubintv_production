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

"""Scraping the versions of the science packages used for processing.

For each image it dispatches, the head node records the exact version of the
handful of packages that determine the AOS results. "Version" here means the
annotated/lightweight tag if the package is checked out exactly on a tag (the
normal case for a release), otherwise the bare commit SHA. ``rubintv_-
production`` itself is usually a SHA rather than a tag. The stack is recorded
too, as the release tags of the set-up ``lsst_distrib``.

These versions cannot change for the lifetime of a pod, so they are computed
once and cached on the head node (see ``HeadProcessController``). If people are
pulling in new worker pod images without restarting the fleet this is
considered illegal.

This module owns the *scraping* side only: reading versions out of the running
environment, and the cross-check against the ``Dockerfile``. The
``PackageVersions`` data model itself, with its content-hash identity and its
ConsDB round-trip, lives in ``lsst.summit.utils.packageVersions``, so that
users without access to this package can still read the recorded data back.

The Dockerfile is the source of truth for what runs on BTS and the summit, so
the versions git actually reports are cross-checked against the refs pinned
there, and the set-up stack against its ``STACK_TAG``. A git package matches
its pin if both name the same commit in its checkout, because one commit can
carry several tags: an unchanged package picks up each new weekly tag, and
``git describe`` reports the newest one. A mismatch raises
`PackageVersionMismatchError` (or, in CI, logs a warning). A Dockerfile that
can't be read or parsed only produces a warning. A unit test pins the
Dockerfile format so that a drift which breaks the parser is caught in CI
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
    "STACK_PRODUCT",
    "PACKAGE_VERSIONS_METADATA_KEY",
    "PackageVersionMismatchError",
    "VersionComparison",
    "envVarForPackage",
    "getGitVersion",
    "getPackageVersion",
    "getInstalledPackageVersion",
    "getCurrentPackageVersions",
    "getStackVersion",
    "packageVersionsToDisplayDict",
    "packageVersionsFromDisplayDict",
    "findDockerfile",
    "parseDockerfileRefs",
    "versionsMatch",
    "compareVersionsToDockerfile",
    "checkVersionsAgainstDockerfile",
]


_log = logging.getLogger(__name__)


# The packages whose git versions are recorded for every dispatched image. The
# set is deliberately small: just the things that change the AOS results. These
# are git checkouts located via their EUPS ``<PACKAGE>_DIR`` env var.
TRACKED_PACKAGES = ["ts_wep", "donut_viz", "rubintv_production", "tarts", "summit_utils", "ts_aos_ai"]

# Tracked packages that are installed into the environment (conda/pip) rather
# than being git checkouts. These have no ``*_DIR`` env var, so their version
# comes from the installed distribution metadata instead. In the Dockerfile
# they are pinned with a conda ``name=version`` spec rather than an ``ARG
# <name>_ref``.
TRACKED_INSTALLED_PACKAGES = ["danish"]

# The stack product, recorded with the tracked packages as its release tags
# and checked against the Dockerfile's ``STACK_TAG``, the tag of the scipipe
# image the pods are built from.
STACK_PRODUCT = "lsst_distrib"

# The key (i.e. the column name on RubinTV) the package versions are written
# under in the AOS metadata. Shared by the writer (the head node) and the
# reader (the ConsDB backfill).
PACKAGE_VERSIONS_METADATA_KEY = "Package versions"

# The minimum length a Dockerfile ref must have before we treat it as an
# abbreviated SHA that may prefix-match a full git SHA. Below this, a short
# string prefix-matching a SHA is far more likely to be a coincidence than a
# real abbreviation.
_MIN_ABBREV_SHA_LEN = 7

# Matches ``ARG <something>_ref="<ref>"`` (or single/no quotes) in the
# Dockerfile, capturing the package name and the ref it is pinned to. e.g.
# ``ARG ts_wep_ref="v17.6.1-alpha"`` -> ("ts_wep", "v17.6.1-alpha"). The
# ``STACK_TAG`` arg doesn't end in ``_ref`` so it is not matched here.
_DOCKERFILE_REF_RE = re.compile(r"""^\s*ARG\s+([A-Za-z0-9_]+)_ref\s*=\s*["']?([^"'\s]+)["']?""")

# Matches ``ARG STACK_TAG="w_2026_41"`` (or single/no quotes), capturing the
# tag.
_DOCKERFILE_STACK_TAG_RE = re.compile(r"""^\s*ARG\s+STACK_TAG\s*=\s*["']?([^"'\s]+)["']?""")

# The eups tags that name one stack release: weekly, daily or numbered. Tags
# such as ``current`` and ``w_latest`` move between releases.
_STACK_RELEASE_TAG_RE = re.compile(r"^(w_\d{4}_\d{2}|d_\d{4}_\d{2}_\d{2}|v\d+(_\d+)*(_rc\d+)?)$")


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
        The tag at ``HEAD``, or the full commit SHA if ``HEAD`` is not on a
        tag.

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
        _log.warning(f"{envVar} is not set; cannot record a version for {packageName}")
        return UNKNOWN_VERSION

    try:
        return getGitVersion(packageDir)
    except (subprocess.SubprocessError, OSError) as e:
        _log.warning(f"Failed to get the git version for {packageName} in {packageDir}: {e}")
        return UNKNOWN_VERSION


def getInstalledPackageVersion(packageName: str) -> str:
    """Get the version of an installed (conda/pip) distribution.

    Used for tracked packages that are installed into the environment rather
    than being git checkouts (e.g. ``danish``), so they have no ``*_DIR`` env
    var. The version is read from the installed distribution metadata - no need
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
        _log.warning(f"Installed package {packageName} not found; cannot record its version")
        return UNKNOWN_VERSION
    except Exception as e:  # noqa: BLE001 - version recording must never raise
        _log.warning(f"Failed to get the version for installed package {packageName}: {e}")
        return UNKNOWN_VERSION


def getCurrentPackageVersions(
    packageNames: list[str] | None = None,
    installedPackageNames: list[str] | None = None,
) -> PackageVersions:
    """Collect the versions of the tracked packages and the stack in this
    environment.

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
        The collected versions, with the stack's (see `getStackVersion`)
        under `STACK_PRODUCT`.
    """
    if packageNames is None:
        packageNames = TRACKED_PACKAGES
    if installedPackageNames is None:
        installedPackageNames = TRACKED_INSTALLED_PACKAGES
    versions = {name: getPackageVersion(name) for name in packageNames}
    for name in installedPackageNames:
        versions[name] = getInstalledPackageVersion(name)
    versions[STACK_PRODUCT] = getStackVersion()
    return PackageVersions(versions=versions)


def getStackVersion() -> str:
    """Get the release tags of the set-up stack.

    Never raises: returns `UNKNOWN_VERSION` and logs a warning if
    `STACK_PRODUCT` isn't set up or has no release tag.

    Returns
    -------
    version : `str`
        The space-separated release tags of the set-up `STACK_PRODUCT`, e.g.
        ``"w_2026_41"``. One build can carry several, e.g. a weekly and a
        daily.
    """
    try:
        import eups

        product = eups.Eups().findSetupProduct(STACK_PRODUCT)
    except Exception as e:  # noqa: BLE001 - version recording must never raise
        _log.warning(f"Failed to look up the set-up {STACK_PRODUCT}: {e}")
        return UNKNOWN_VERSION
    if product is None:
        _log.warning(f"{STACK_PRODUCT} is not set up; cannot determine the stack version")
        return UNKNOWN_VERSION
    tags = sorted(tag for tag in (product.tags or []) if _STACK_RELEASE_TAG_RE.match(tag))
    if not tags:
        _log.warning(f"{STACK_PRODUCT} {product.version} has no release tag, only {product.tags}")
        return UNKNOWN_VERSION
    return " ".join(tags)


def packageVersionsToDisplayDict(packageVersions: PackageVersions) -> dict[str, str]:
    """Render package versions as the dict written to the AOS metadata.

    Parameters
    ----------
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The versions to render.

    Returns
    -------
    displayDict : `dict` [`str`, `str`]
        The ``{package: version}`` mapping plus a ``"DISPLAY_VALUE": "📖"``
        entry. ``DISPLAY_VALUE`` is the RubinTV convention for a dict-valued
        cell: the frontend shows the glyph and expands it to the rest of the
        dict on click.
    """
    displayDict: dict[str, str] = dict(packageVersions.versions)
    displayDict["DISPLAY_VALUE"] = "📖"
    return displayDict


def packageVersionsFromDisplayDict(displayDict: Mapping[str, str]) -> PackageVersions:
    """Recover the package versions from the dict in the AOS metadata.

    The inverse of `packageVersionsToDisplayDict`: drops the ``DISPLAY_VALUE``
    entry, and everything else is a ``{package: version}`` pair.

    Parameters
    ----------
    displayDict : `Mapping` [`str`, `str`]
        The dict as written by `packageVersionsToDisplayDict`.

    Returns
    -------
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The recovered versions.
    """
    versions = {key: value for key, value in displayDict.items() if key != "DISPLAY_VALUE"}
    return PackageVersions(versions=versions)


def findDockerfile() -> str | None:
    """Locate the Dockerfile shipped alongside the rubintv_production checkout.

    The Dockerfile is copied into the image at the root of the
    ``rubintv_production`` checkout, so it sits next to ``$RUBINTV_PRODUCTION_-
    DIR``.

    Returns
    -------
    dockerfilePath : `str` or `None`
        The path to the Dockerfile, or `None` if it can't be located. The
        reason is logged as a warning so a `None` is never silent.
    """
    envVar = envVarForPackage("rubintv_production")
    packageDir = os.environ.get(envVar)
    if not packageDir:
        _log.warning(f"{envVar} is not set; cannot locate the Dockerfile")
        return None
    dockerfilePath = os.path.join(packageDir, "Dockerfile")
    if not os.path.isfile(dockerfilePath):
        _log.warning(f"No Dockerfile found at {dockerfilePath}")
        return None
    return dockerfilePath


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

    Picks up ``ARG <package>_ref=...`` (git checkouts) for any package,
    conda/pip ``<package>=<version>`` specs for the names given in
    ``condaPackages`` (installed packages such as ``danish``), and ``ARG
    STACK_TAG=...`` as the pin for `STACK_PRODUCT`.

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
        ``{"ts_wep": "v17.6.1-alpha", "danish": "1.1.1", "lsst_distrib":
        "w_2026_41", ...}``. Packages with no recognised pin (e.g.
        ``rubintv_production``, which is COPYed in rather than checked out)
        are simply absent.
    """
    refs: dict[str, str] = {}
    with open(dockerfilePath) as f:
        lines = f.readlines()
    for line in lines:
        match = _DOCKERFILE_REF_RE.match(line)
        if match:
            refs[match.group(1)] = match.group(2)
        match = _DOCKERFILE_STACK_TAG_RE.match(line)
        if match:
            refs[STACK_PRODUCT] = match.group(1)
    for name in condaPackages or []:
        pin = _findCondaPin(lines, name)
        if pin is not None:
            refs[name] = pin
    return refs


def _resolveCommit(packageDir: str, ref: str) -> str | None:
    """Resolve a ref to the full SHA of the commit it names in a checkout.

    Parameters
    ----------
    packageDir : `str`
        The path to the package's git checkout.
    ref : `str`
        A tag, branch or (possibly abbreviated) commit SHA.

    Returns
    -------
    sha : `str` or `None`
        The full SHA of the commit, or `None` if ``ref`` names no commit in the
        checkout or git can't be run there.
    """
    try:
        result = subprocess.run(
            ["git", "-C", packageDir, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def versionsMatch(gitVersion: str, dockerfileRef: str, packageDir: str | None = None) -> bool:
    """Check whether a git version agrees with a Dockerfile ref.

    Given the package's checkout, they match if they name the same commit
    there, e.g. two weekly tags on an unchanged commit. Otherwise, or if
    either doesn't resolve in the checkout, they must be equal, except that a
    SHA in the Dockerfile may be abbreviated, so a ref of at least
    ``_MIN_ABBREV_SHA_LEN`` characters which is a prefix of the full git SHA
    also counts as a match.

    Parameters
    ----------
    gitVersion : `str`
        The version git reports (a tag or a full SHA).
    dockerfileRef : `str`
        The ref the Dockerfile pins the package to.
    packageDir : `str`, optional
        The path to the package's git checkout, to resolve both refs in.

    Returns
    -------
    match : `bool`
        Whether the two refer to the same checkout.
    """
    if gitVersion == dockerfileRef:
        return True
    if packageDir is not None:
        pinnedCommit = _resolveCommit(packageDir, dockerfileRef)
        runningCommit = _resolveCommit(packageDir, gitVersion)
        if pinnedCommit is not None and runningCommit is not None:
            return pinnedCommit == runningCommit
    # An abbreviated SHA in the Dockerfile should prefix-match the full SHA.
    if len(dockerfileRef) >= _MIN_ABBREV_SHA_LEN and gitVersion.startswith(dockerfileRef):
        return True
    return False


class PackageVersionMismatchError(RuntimeError):
    """Raised when a running package's version disagrees with its Dockerfile
    pin.
    """


@dataclass
class VersionComparison:
    """The result of comparing one package's git version to the Dockerfile.

    Parameters
    ----------
    package : `str`
        The package name.
    gitVersion : `str`
        The running version: the git version of a checkout, the installed
        version of a conda/pip package, or the release tags of the stack.
    dockerfileRef : `str` or `None`
        The ref the Dockerfile pins the package to, or `None` if there is no
        pin (e.g. ``rubintv_production``, which is COPYed in, not checked out).
    matches : `bool` or `None`
        Whether the git version agrees with the Dockerfile pin. `None` when
        the comparison is not meaningful - there is no pin, or the git version
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

    A git package is compared in its checkout, found via its ``<PACKAGE>_DIR``
    env var, so that a pin naming the same commit matches (see
    `versionsMatch`). The stack matches if ``STACK_TAG`` is any of its release
    tags, as one build can carry several.

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
        log.warning(f"Could not read the Dockerfile at {dockerfilePath} for version cross-check: {e}")
        refs = {}
    except Exception as e:  # noqa: BLE001 - a parse failure must never take down the head node
        log.warning(f"Unexpected error parsing the Dockerfile at {dockerfilePath}: {e}")
        refs = {}

    comparisons: list[VersionComparison] = []
    for name, gitVersion in packageVersions.versions.items():
        ref = refs.get(name)
        if ref is None or gitVersion == UNKNOWN_VERSION:
            matches = None  # no pin to compare against, or no git version to compare
        elif name == STACK_PRODUCT:
            matches = ref in gitVersion.split()
        else:
            packageDir = os.environ.get(envVarForPackage(name)) or None
            matches = versionsMatch(gitVersion, ref, packageDir=packageDir)
        comparisons.append(
            VersionComparison(package=name, gitVersion=gitVersion, dockerfileRef=ref, matches=matches)
        )
    return comparisons


def checkVersionsAgainstDockerfile(
    packageVersions: PackageVersions,
    dockerfilePath: str,
    log: logging.Logger | None = None,
    raiseOnMismatch: bool = True,
) -> None:
    """Check the recorded versions against the Dockerfile pins.

    The Dockerfile is the source of truth for what runs on BTS and the summit,
    so any package whose version differs from its pin is an error there. A
    package with no pin in the Dockerfile (e.g. ``rubintv_production``) or
    whose version could not be determined is skipped, and an unreadable
    Dockerfile only produces a warning.

    Parameters
    ----------
    packageVersions : `lsst.summit.utils.packageVersions.PackageVersions`
        The versions git reported for the running checkouts.
    dockerfilePath : `str`
        The path to the Dockerfile to cross-check against.
    log : `logging.Logger`, optional
        The logger to warn on. Defaults to this module's logger.
    raiseOnMismatch : `bool`, optional
        Raise on a mismatch if `True`, otherwise log a warning that this
        combination of code cannot run on BTS or the summit.

    Raises
    ------
    PackageVersionMismatchError
        Raised if ``raiseOnMismatch`` is set and any package's version
        disagrees with its Dockerfile pin. All the mismatching packages are
        listed in the message.
    """
    if log is None:
        log = _log

    comparisons = compareVersionsToDockerfile(packageVersions, dockerfilePath, log=log)
    mismatches = [c for c in comparisons if c.matches is False]
    if not mismatches:
        return

    details = "; ".join(
        f"{c.package} is at {c.gitVersion!r} but pinned to {c.dockerfileRef!r}" for c in mismatches
    )
    message = f"Running package versions do not match the Dockerfile at {dockerfilePath}: {details}"
    if raiseOnMismatch:
        raise PackageVersionMismatchError(message)
    log.warning(f"{message}. This combination of code cannot run on BTS or the summit as-is.")
