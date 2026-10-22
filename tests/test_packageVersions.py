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

"""Test cases for packageVersions.

Covers the git version helpers, the version scraping into the
summit_utils-owned ``PackageVersions`` dataclass, its rendering for RubinTV,
and the Dockerfile cross-check, including the set-up stack's eups tags against
``STACK_TAG``. Git pins are compared by commit, so the cross-check must not
fail when a new weekly tag lands on a pinned, unchanged commit and ``git
describe`` starts reporting it instead. The ``PackageVersions`` data model
itself - its JSON round-trip and content-hash identity - is tested in
summit_utils alongside the code. The ``test_realDockerfileFormatIsParseable``
test deliberately reads the actual ``Dockerfile`` so that any drift in the
``ARG <package>_ref="..."`` format, which would silently turn the runtime
cross-check into a no-op, fails loudly here instead.
"""

import os
import subprocess
import tempfile
import unittest
import unittest.mock
from types import SimpleNamespace

import lsst.utils.tests
from lsst.rubintv.production.packageVersions import (
    _STACK_RELEASE_TAG_RE,
    STACK_PRODUCT,
    TRACKED_INSTALLED_PACKAGES,
    TRACKED_PACKAGES,
    PackageVersionMismatchError,
    checkVersionsAgainstDockerfile,
    compareVersionsToDockerfile,
    envVarForPackage,
    findDockerfile,
    getCurrentPackageVersions,
    getGitVersion,
    getInstalledPackageVersion,
    getPackageVersion,
    getStackVersion,
    packageVersionsFromDisplayDict,
    packageVersionsToDisplayDict,
    parseDockerfileRefs,
    versionsMatch,
)
from lsst.rubintv.production.parsers import sanitizeNans
from lsst.summit.utils.packageVersions import UNKNOWN_VERSION, PackageVersions

# The real Dockerfile, located relative to this test file rather than via an
# env var so the format-pinning test works regardless of how tests are run.
REPO_DOCKERFILE = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "Dockerfile"))


def makeGitRepo(path: str, tag: str | None = None) -> str:
    """Create a one-commit git repo at ``path``, optionally tagged.

    Parameters
    ----------
    path : `str`
        The directory to initialise the repo in.
    tag : `str`, optional
        If given, a tag of this name is created on the commit.

    Returns
    -------
    sha : `str`
        The full commit SHA of the single commit.
    """
    subprocess.run(["git", "-C", path, "init", "-q"], check=True)
    sha = addCommit(path, "hello\n")
    if tag is not None:
        tagHead(path, tag)
    return sha


def tagHead(path: str, tag: str) -> None:
    """Tag ``HEAD`` of the git repo at ``path``.

    Parameters
    ----------
    path : `str`
        The repo to tag.
    tag : `str`
        The name of the tag to create.
    """
    subprocess.run(["git", "-C", path, "tag", tag], check=True)


def addCommit(path: str, contents: str) -> str:
    """Commit a new README to the git repo at ``path``.

    Parameters
    ----------
    path : `str`
        The repo to commit to.
    contents : `str`
        The README contents, which must differ from the current ones.

    Returns
    -------
    sha : `str`
        The full commit SHA of the new commit.
    """
    with open(os.path.join(path, "README"), "w") as f:
        f.write(contents)
    subprocess.run(["git", "-C", path, "add", "-A"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            path,
            "-c",
            "user.name=test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "-q",
            "-m",
            "commit",
        ],
        check=True,
    )
    return subprocess.run(
        ["git", "-C", path, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


class GitVersionTestCase(lsst.utils.tests.TestCase):
    def test_returnsTagWhenHeadIsOnTag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp, tag="v1.2.3-alpha")
            self.assertEqual(getGitVersion(tmp), "v1.2.3-alpha")

    def test_returnsShaWhenHeadIsNotOnTag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            sha = makeGitRepo(tmp)  # no tag
            version = getGitVersion(tmp)
            self.assertEqual(version, sha)
            self.assertEqual(len(version), 40)  # full SHA

    def test_returnsShaWhenHeadMovedPastTag(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp, tag="v1.0.0")
            # advance HEAD past the tag with a second, untagged commit
            sha = addCommit(tmp, "more\n")
            self.assertEqual(getGitVersion(tmp), sha)

    def test_raisesOnNonGitDir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(subprocess.SubprocessError):
                getGitVersion(tmp)


class EnvVarTestCase(lsst.utils.tests.TestCase):
    def test_envVarForPackage(self) -> None:
        self.assertEqual(envVarForPackage("ts_wep"), "TS_WEP_DIR")
        self.assertEqual(envVarForPackage("donut_viz"), "DONUT_VIZ_DIR")
        self.assertEqual(envVarForPackage("rubintv_production"), "RUBINTV_PRODUCTION_DIR")
        self.assertEqual(envVarForPackage("tarts"), "TARTS_DIR")

    def test_getPackageVersionUnknownWhenUnset(self) -> None:
        # ensure the env var is genuinely absent
        envVar = envVarForPackage("a_package_that_does_not_exist")
        self.assertNotIn(envVar, os.environ)
        self.assertEqual(getPackageVersion("a_package_that_does_not_exist"), UNKNOWN_VERSION)

    def test_getPackageVersionUsesEnvVar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp, tag="v9.9.9")
            envVar = envVarForPackage("ts_wep")
            with unittest.mock.patch.dict(os.environ, {envVar: tmp}):
                self.assertEqual(getPackageVersion("ts_wep"), "v9.9.9")

    def test_getPackageVersionUnknownWhenDirNotARepo(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:  # exists, but isn't a git repo
            envVar = envVarForPackage("ts_wep")
            with unittest.mock.patch.dict(os.environ, {envVar: tmp}):
                self.assertEqual(getPackageVersion("ts_wep"), UNKNOWN_VERSION)


class InstalledPackageVersionTestCase(lsst.utils.tests.TestCase):
    def test_knownInstalledPackage(self) -> None:
        # numpy is a guaranteed-present distribution in the stack; its version
        # comes from the installed metadata, not a git checkout or env var
        version = getInstalledPackageVersion("numpy")
        self.assertNotEqual(version, UNKNOWN_VERSION)
        self.assertTrue(version)

    def test_unknownInstalledPackage(self) -> None:
        self.assertEqual(
            getInstalledPackageVersion("a_distribution_that_does_not_exist_xyz"), UNKNOWN_VERSION
        )


class StackVersionTestCase(lsst.utils.tests.TestCase):
    """Tests for `getStackVersion`.

    The stack version is the release tags of the set-up lsst_distrib, and
    anything that doesn't name one release must come back as unknown, which
    skips the STACK_TAG check, rather than as a version that can never match
    it and so fails the head node at startup.
    """

    @staticmethod
    def _makeEups(product: SimpleNamespace | None) -> unittest.mock.MagicMock:
        eupsInstance = unittest.mock.MagicMock()
        eupsInstance.findSetupProduct.return_value = product
        return eupsInstance

    def test_returnsOnlyReleaseTags(self) -> None:
        # current, setup and w_latest move between releases, and one build
        # can carry both a daily and a weekly tag
        product = SimpleNamespace(
            version="g00e868bf88+e0f0dea02a",
            tags=["current", "w_2026_41", "w_latest", "d_2026_10_07", "setup"],
        )
        with unittest.mock.patch("eups.Eups", return_value=self._makeEups(product)):
            self.assertEqual(getStackVersion(), "d_2026_10_07 w_2026_41")

    def test_unknownWhenNotSetUp(self) -> None:
        with unittest.mock.patch("eups.Eups", return_value=self._makeEups(None)):
            with self.assertLogs(level="WARNING") as cm:
                self.assertEqual(getStackVersion(), UNKNOWN_VERSION)
        self.assertIn("not set up", cm.output[0])

    def test_unknownWhenNoReleaseTag(self) -> None:
        # eups distrib install can declare no tag at all if the server's tag
        # list is stale, leaving only the floating ones
        product = SimpleNamespace(version="g00e868bf88+e0f0dea02a", tags=["current", "w_latest"])
        with unittest.mock.patch("eups.Eups", return_value=self._makeEups(product)):
            with self.assertLogs(level="WARNING") as cm:
                self.assertEqual(getStackVersion(), UNKNOWN_VERSION)
        self.assertIn("no release tag", cm.output[0])

    def test_neverRaisesWhenEupsFails(self) -> None:
        with unittest.mock.patch("eups.Eups", side_effect=RuntimeError("no eups database")):
            with self.assertLogs(level="WARNING"):
                self.assertEqual(getStackVersion(), UNKNOWN_VERSION)


class PackageVersionsTestCase(lsst.utils.tests.TestCase):
    def test_displayDictHasBookMarker(self) -> None:
        versions = {"ts_wep": "v1", "donut_viz": "v2", "rubintv_production": "abc123"}
        pv = PackageVersions(versions=dict(versions))
        displayDict = packageVersionsToDisplayDict(pv)
        self.assertEqual(displayDict["DISPLAY_VALUE"], "📖")
        for name, version in versions.items():
            self.assertEqual(displayDict[name], version)
        # rendering must not mutate the underlying versions dict
        self.assertNotIn("DISPLAY_VALUE", pv.versions)

    def test_displayDictRoundTrips(self) -> None:
        # the metadata dict is the backfill's source, so the inverse must
        # recover exactly the versions that were rendered
        pv = PackageVersions(versions={"ts_wep": "v1", "donut_viz": "v2", "danish": "1.1.1"})
        recovered = packageVersionsFromDisplayDict(packageVersionsToDisplayDict(pv))
        self.assertEqual(recovered, pv)
        self.assertEqual(recovered.versionHash(), pv.versionHash())

    def test_displayDictSurvivesMetadataMergeSanitization(self) -> None:
        # Regression test: the metadata merge (mergeShardsAndUpload) passes
        # every value through sanitizeNans, which coerces numeric-looking
        # strings to floats. That used to recurse into the versions dict, so a
        # version like danish "1.1" reached the backfill as the float 1.1: a
        # non-string in ConsDB, and a recomputed hash that no longer matched
        # the one recorded at dispatch time.
        pv = PackageVersions(versions={"danish": "1.1", "ts_wep": "20250624", "donut_viz": "2.0"})
        merged = sanitizeNans(packageVersionsToDisplayDict(pv))
        recovered = packageVersionsFromDisplayDict(merged)
        self.assertEqual(recovered, pv)
        self.assertEqual(recovered.versionHash(), pv.versionHash())

    def test_getCurrentPackageVersions(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp, tag="v5.0.0")
            with unittest.mock.patch.dict(os.environ, {envVarForPackage("ts_wep"): tmp}):
                pv = getCurrentPackageVersions(["ts_wep", "not_a_real_package"], installedPackageNames=[])
            self.assertEqual(pv.versions["ts_wep"], "v5.0.0")
            self.assertEqual(pv.versions["not_a_real_package"], UNKNOWN_VERSION)

    def test_getCurrentPackageVersionsIncludesInstalledPackages(self) -> None:
        # numpy is a guaranteed-present installed distribution in the stack
        pv = getCurrentPackageVersions(packageNames=[], installedPackageNames=["numpy"])
        self.assertIn("numpy", pv.versions)
        self.assertNotEqual(pv.versions["numpy"], UNKNOWN_VERSION)

    def test_getCurrentPackageVersionsDefaultsToTrackedPackages(self) -> None:
        pv = getCurrentPackageVersions()
        self.assertEqual(
            list(pv.versions.keys()), TRACKED_PACKAGES + TRACKED_INSTALLED_PACKAGES + [STACK_PRODUCT]
        )

    def test_getCurrentPackageVersionsRecordsTheStack(self) -> None:
        # the stack is part of the recorded version set, so it reaches ConsDB
        # and changes the version hash like any tracked package
        with unittest.mock.patch(
            "lsst.rubintv.production.packageVersions.getStackVersion", return_value="w_2026_41"
        ):
            pv = getCurrentPackageVersions(packageNames=[], installedPackageNames=[])
        self.assertEqual(pv.versions, {STACK_PRODUCT: "w_2026_41"})


class VersionsMatchTestCase(lsst.utils.tests.TestCase):
    def test_exactMatch(self) -> None:
        self.assertTrue(versionsMatch("v17.6.1-alpha", "v17.6.1-alpha"))

    def test_mismatch(self) -> None:
        self.assertFalse(versionsMatch("v17.6.1-alpha", "v17.6.0-alpha"))

    def test_abbreviatedShaPrefixMatches(self) -> None:
        fullSha = "1485dcbc10ad7637a845ee7e9a5a29d0f1da7680"
        self.assertTrue(versionsMatch(fullSha, fullSha[:12]))
        self.assertTrue(versionsMatch(fullSha, fullSha))

    def test_shortPrefixDoesNotMatch(self) -> None:
        # too short to be treated as an abbreviated SHA, so a coincidental
        # prefix must not count as a match
        self.assertFalse(versionsMatch("v1abcdef", "v1"))

    def test_tagsOnTheSameCommitMatch(self) -> None:
        # Regression: the BTS head node refused to start with summit_utils
        # "at 'w.2026.42' but pinned to 'w.2026.41'". summit_utils hadn't
        # changed, so both weeklies tagged the same commit and git describe
        # reported the newer one: every unchanged package pinned to a weekly
        # tag mismatched as soon as the next weekly was cut.
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp, tag="w.2026.41")
            tagHead(tmp, "w.2026.42")
            self.assertTrue(versionsMatch("w.2026.42", "w.2026.41", packageDir=tmp))

    def test_tagsOnDifferentCommitsDoNotMatch(self) -> None:
        # resolving refs in the checkout must still catch a real change
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp, tag="w.2026.41")
            addCommit(tmp, "changed\n")
            tagHead(tmp, "w.2026.42")
            self.assertFalse(versionsMatch("w.2026.42", "w.2026.41", packageDir=tmp))

    def test_shaPinMatchesTagOnThatCommit(self) -> None:
        # A SHA pin whose commit also carries a tag is recorded as the tag,
        # so a string comparison can never match it. Covers the full SHA and
        # an abbreviation too short for the prefix-match fallback.
        with tempfile.TemporaryDirectory() as tmp:
            sha = makeGitRepo(tmp, tag="v1.0.0")
            self.assertTrue(versionsMatch("v1.0.0", sha, packageDir=tmp))
            self.assertTrue(versionsMatch("v1.0.0", sha[:5], packageDir=tmp))

    def test_unresolvableRefFallsBackToStringComparison(self) -> None:
        # a ref the checkout doesn't know is compared as a string, so an
        # abbreviated SHA still prefix-matches and different names don't
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp)
            fullSha = "1485dcbc10ad7637a845ee7e9a5a29d0f1da7680"
            self.assertTrue(versionsMatch(fullSha, fullSha[:12], packageDir=tmp))
            self.assertFalse(versionsMatch("v1.0.0", "v2.0.0", packageDir=tmp))


class DockerfileParsingTestCase(lsst.utils.tests.TestCase):
    def test_parsesVariousQuotingStyles(self) -> None:
        contents = (
            'ARG STACK_TAG="w_2026_22"\n'
            'ARG ts_wep_ref="v17.6.1-alpha"\n'
            "ARG donut_viz_ref='v4.4.0-alpha'\n"
            "ARG summit_utils_ref=1485dcbc10ad7637a845ee7e9a5a29d0f1da7680\n"
            "  ARG drp_pipe_ref = w.2026.22 \n"
            "# ARG commented_ref=should_be_ignored\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "Dockerfile")
            with open(path, "w") as f:
                f.write(contents)
            refs = parseDockerfileRefs(path)
        self.assertEqual(refs["ts_wep"], "v17.6.1-alpha")
        self.assertEqual(refs["donut_viz"], "v4.4.0-alpha")
        self.assertEqual(refs["summit_utils"], "1485dcbc10ad7637a845ee7e9a5a29d0f1da7680")
        self.assertEqual(refs["drp_pipe"], "w.2026.22")
        # STACK_TAG is parsed as the stack's pin, not as a package, and the
        # commented line is ignored
        self.assertEqual(refs[STACK_PRODUCT], "w_2026_22")
        self.assertNotIn("STACK_TAG", refs)
        self.assertNotIn("commented", refs)

    def test_parsesCondaPins(self) -> None:
        contents = (
            "    conda install -y \\\n"
            "    batoid \\\n"
            "    danish=1.1.1 \\\n"
            "    lsst-efd-client=1.0.0 \\\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "Dockerfile")
            with open(path, "w") as f:
                f.write(contents)
            refs = parseDockerfileRefs(path, condaPackages=["danish"])
        self.assertEqual(refs["danish"], "1.1.1")
        # only the requested conda package is extracted, not other pins
        self.assertNotIn("lsst-efd-client", refs)
        # and without asking for conda packages, nothing conda is parsed
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "Dockerfile")
            with open(path, "w") as f:
                f.write(contents)
            self.assertNotIn("danish", parseDockerfileRefs(path))

    def test_realDockerfileFormatIsParseable(self) -> None:
        # Pins the Dockerfile ARG + conda pin formats the runtime cross-check
        # depends on. If this fails, the format drifted and parseDockerfileRefs
        # needs updating (the runtime check only warns, so without this test
        # the drift is silent).
        self.assertTrue(os.path.isfile(REPO_DOCKERFILE), f"Dockerfile not found at {REPO_DOCKERFILE}")
        refs = parseDockerfileRefs(REPO_DOCKERFILE, condaPackages=TRACKED_INSTALLED_PACKAGES)
        for package in ("ts_wep", "donut_viz", "tarts"):
            self.assertIn(package, refs, f"{package}_ref no longer parseable from the Dockerfile")
            self.assertTrue(refs[package], f"{package}_ref parsed as empty")
        for package in TRACKED_INSTALLED_PACKAGES:
            self.assertIn(package, refs, f"{package} conda pin no longer parseable from the Dockerfile")
            self.assertTrue(refs[package], f"{package} conda pin parsed as empty")
        # rubintv_production is COPYed in, not checked out via a _ref arg
        self.assertNotIn("rubintv_production", refs)
        # STACK_TAG must be a tag that getStackVersion can report, or the
        # stack check can never match and the head node always raises
        self.assertIn(STACK_PRODUCT, refs, "STACK_TAG no longer parseable from the Dockerfile")
        self.assertRegex(refs[STACK_PRODUCT], _STACK_RELEASE_TAG_RE)


class FindDockerfileTestCase(lsst.utils.tests.TestCase):
    def test_findsDockerfileViaEnvVar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "Dockerfile")
            with open(path, "w") as f:
                f.write("FROM scratch\n")
            with unittest.mock.patch.dict(os.environ, {envVarForPackage("rubintv_production"): tmp}):
                self.assertEqual(findDockerfile(), path)

    def test_noneWhenEnvVarUnset(self) -> None:
        # a None must never be silent: the reason is logged so a broken
        # environment (package not set up at all) is visible in the pod logs
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            with self.assertLogs(level="WARNING") as cm:
                self.assertIsNone(findDockerfile())
        self.assertIn("is not set", cm.output[0])

    def test_noneWhenDockerfileMissing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:  # dir exists but has no Dockerfile
            with unittest.mock.patch.dict(os.environ, {envVarForPackage("rubintv_production"): tmp}):
                with self.assertLogs(level="WARNING") as cm:
                    self.assertIsNone(findDockerfile())
        self.assertIn("No Dockerfile found", cm.output[0])


class DockerfileComparisonTestCase(lsst.utils.tests.TestCase):
    """Base class for comparing versions to a temporary Dockerfile.

    Git packages are compared in the checkout their ``*_DIR`` env var points
    at, so the tracked packages' env vars are cleared: made-up versions must
    not be resolved in whatever real checkouts the stack has set up. A test
    that needs a checkout points the env var at a temporary repo.
    """

    def setUp(self) -> None:
        patcher = unittest.mock.patch.dict(os.environ)
        patcher.start()
        self.addCleanup(patcher.stop)
        for name in TRACKED_PACKAGES:
            os.environ.pop(envVarForPackage(name), None)

    def _writeDockerfile(self, tmp: str, body: str) -> str:
        path = os.path.join(tmp, "Dockerfile")
        with open(path, "w") as f:
            f.write(body)
        return path


class CheckVersionsAgainstDockerfileTestCase(DockerfileComparisonTestCase):
    def test_raisesOnMismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG ts_wep_ref="v2.0.0"\n')
            pv = PackageVersions(versions={"ts_wep": "v1.0.0"})
            with self.assertRaisesRegex(PackageVersionMismatchError, "ts_wep.*v1.0.0.*v2.0.0"):
                checkVersionsAgainstDockerfile(pv, path)

    def test_warnsInsteadOfRaisingWhenAsked(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG ts_wep_ref="v2.0.0"\n')
            pv = PackageVersions(versions={"ts_wep": "v1.0.0"})
            with self.assertLogs(level="WARNING") as cm:
                checkVersionsAgainstDockerfile(pv, path, raiseOnMismatch=False)
            self.assertTrue(any("ts_wep" in msg and "cannot run on BTS" in msg for msg in cm.output))

    def test_raiseListsEveryMismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(
                tmp,
                'ARG ts_wep_ref="v2.0.0"\nARG summit_utils_ref="2c3f3d3e322c"\nARG donut_viz_ref="v3.0.0"\n',
            )
            pv = PackageVersions(
                versions={
                    "ts_wep": "v1.0.0",
                    "summit_utils": "5dc810c80e38e9569f91ed4fed1ae28338d78c94",
                    "donut_viz": "v3.0.0",
                }
            )
            with self.assertRaises(PackageVersionMismatchError) as cm:
                checkVersionsAgainstDockerfile(pv, path)
            message = str(cm.exception)
            self.assertIn("ts_wep", message)
            self.assertIn("summit_utils", message)
            self.assertNotIn("donut_viz", message)

    def test_silentWhenMatching(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG ts_wep_ref="v1.0.0"\n')
            pv = PackageVersions(versions={"ts_wep": "v1.0.0"})
            with self.assertNoLogs(level="WARNING"):
                checkVersionsAgainstDockerfile(pv, path)

    def test_skipsUnpinnedAndUnknownPackages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG ts_wep_ref="v1.0.0"\n')
            # rubintv_production has no ref pin; ts_wep unknown -> both skipped
            pv = PackageVersions(versions={"rubintv_production": "abc123", "ts_wep": UNKNOWN_VERSION})
            with self.assertNoLogs(level="WARNING"):
                checkVersionsAgainstDockerfile(pv, path)

    def test_raisesOnStackMismatch(self) -> None:
        # Regression: an image built FROM w_2026_40 couldn't load pipelines
        # that worked on w_2026_41, and nothing reported the stack drift
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG STACK_TAG="w_2026_40"\nARG ts_wep_ref="v1.0.0"\n')
            pv = PackageVersions(versions={"ts_wep": "v1.0.0", STACK_PRODUCT: "w_2026_41"})
            with self.assertRaisesRegex(PackageVersionMismatchError, "lsst_distrib.*w_2026_41.*w_2026_40"):
                checkVersionsAgainstDockerfile(pv, path)

    def test_neverRaisesOnMissingFile(self) -> None:
        pv = PackageVersions(versions={"ts_wep": "v1.0.0"})
        missing = os.path.join("/nonexistent", "Dockerfile")
        with self.assertLogs(level="WARNING"):
            checkVersionsAgainstDockerfile(pv, missing)  # must not raise

    def test_newTagOnPinnedCommitDoesNotRaise(self) -> None:
        # Regression, end to end: the BTS head node raised with summit_utils
        # pinned to w.2026.41 but recorded as w.2026.42, the next weekly's tag
        # on the same unchanged commit. The checkout is found via
        # SUMMIT_UTILS_DIR, as on the pods.
        with tempfile.TemporaryDirectory() as repo, tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(repo, tag="w.2026.41")
            tagHead(repo, "w.2026.42")
            path = self._writeDockerfile(tmp, 'ARG summit_utils_ref="w.2026.41"\n')
            pv = PackageVersions(versions={"summit_utils": "w.2026.42"})
            with unittest.mock.patch.dict(os.environ, {envVarForPackage("summit_utils"): repo}):
                with self.assertNoLogs(level="WARNING"):
                    checkVersionsAgainstDockerfile(pv, path)


class CompareVersionsToDockerfileTestCase(DockerfileComparisonTestCase):
    def test_classifiesMatchMismatchAndUnpinned(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG ts_wep_ref="v1.0.0"\nARG donut_viz_ref="v2.0.0"\n')
            pv = PackageVersions(
                versions={
                    "ts_wep": "v1.0.0",  # matches
                    "donut_viz": "v9.9.9",  # mismatch
                    "rubintv_production": "abc123",  # no pin -> not comparable
                }
            )
            byName = {c.package: c for c in compareVersionsToDockerfile(pv, path)}
            self.assertTrue(byName["ts_wep"].matches)
            self.assertFalse(byName["donut_viz"].matches)
            self.assertIsNone(byName["rubintv_production"].matches)
            self.assertIsNone(byName["rubintv_production"].dockerfileRef)
            self.assertEqual(byName["donut_viz"].dockerfileRef, "v2.0.0")

    def test_unknownGitVersionNotComparable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG ts_wep_ref="v1.0.0"\n')
            pv = PackageVersions(versions={"ts_wep": UNKNOWN_VERSION})
            (comparison,) = compareVersionsToDockerfile(pv, path)
            self.assertIsNone(comparison.matches)

    def test_unreadableDockerfileMakesAllNotComparable(self) -> None:
        pv = PackageVersions(versions={"ts_wep": "v1.0.0", "donut_viz": "v2.0.0"})
        with self.assertLogs(level="WARNING"):
            comparisons = compareVersionsToDockerfile(pv, "/nonexistent/Dockerfile")
        self.assertEqual(len(comparisons), 2)
        self.assertTrue(all(c.matches is None for c in comparisons))

    def test_stackMatchesWhenStackTagIsOneOfItsTags(self) -> None:
        # a build tagged as both a daily and a weekly matches either, where
        # a git version would need the whole string to match
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG STACK_TAG="w_2026_41"\n')
            pv = PackageVersions(versions={STACK_PRODUCT: "d_2026_10_07 w_2026_41"})
            (stack,) = compareVersionsToDockerfile(pv, path)
        self.assertEqual(stack.dockerfileRef, "w_2026_41")
        self.assertTrue(stack.matches)

    def test_stackMismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG STACK_TAG="w_2026_40"\n')
            pv = PackageVersions(versions={STACK_PRODUCT: "w_2026_41"})
            (stack,) = compareVersionsToDockerfile(pv, path)
        self.assertFalse(stack.matches)

    def test_unknownStackNotComparable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG STACK_TAG="w_2026_41"\n')
            pv = PackageVersions(versions={STACK_PRODUCT: UNKNOWN_VERSION})
            (stack,) = compareVersionsToDockerfile(pv, path)
        self.assertIsNone(stack.matches)

    def test_crossChecksCondaPinnedInstalledPackage(self) -> None:
        # danish is pinned via a conda spec, not an ARG _ref, but is still
        # cross-checked (it is in TRACKED_INSTALLED_PACKAGES)
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, "    danish=1.1.1 \\\n")
            byName = {
                c.package: c
                for c in compareVersionsToDockerfile(PackageVersions(versions={"danish": "1.1.1"}), path)
            }
            self.assertTrue(byName["danish"].matches)
            self.assertEqual(byName["danish"].dockerfileRef, "1.1.1")

            byName = {
                c.package: c
                for c in compareVersionsToDockerfile(PackageVersions(versions={"danish": "1.0.0"}), path)
            }
            self.assertFalse(byName["danish"].matches)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
