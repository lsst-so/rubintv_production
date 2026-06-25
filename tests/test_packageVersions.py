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

Covers the git version helpers, the ``PackageVersions`` dataclass and its
metadata-shard rendering, and the advisory Dockerfile cross-check. The
``test_realDockerfileFormatIsParseable`` test deliberately reads the actual
``Dockerfile`` so that any drift in the ``ARG <package>_ref="..."`` format —
which would silently turn the runtime cross-check into a no-op — fails loudly
here instead.
"""

import json
import os
import subprocess
import tempfile
import unittest
import unittest.mock

import lsst.utils.tests
from lsst.rubintv.production.packageVersions import (
    TRACKED_PACKAGES,
    UNKNOWN_VERSION,
    UNKNOWN_VERSION_NUMBER,
    PackageVersions,
    checkVersionsAgainstDockerfile,
    envVarForPackage,
    findDockerfile,
    getGitVersion,
    getPackageVersion,
    getRegistryPath,
    parseDockerfileRefs,
    resolveVersionNumber,
    versionsMatch,
)

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
    with open(os.path.join(path, "README"), "w") as f:
        f.write("hello\n")
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
            "initial",
        ],
        check=True,
    )
    if tag is not None:
        subprocess.run(["git", "-C", path, "tag", tag], check=True)
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
            with open(os.path.join(tmp, "README"), "w") as f:
                f.write("more\n")
            subprocess.run(["git", "-C", tmp, "add", "-A"], check=True)
            subprocess.run(
                [
                    "git",
                    "-C",
                    tmp,
                    "-c",
                    "user.name=test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "-q",
                    "-m",
                    "second",
                ],
                check=True,
            )
            sha = subprocess.run(
                ["git", "-C", tmp, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
            ).stdout.strip()
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


class PackageVersionsTestCase(lsst.utils.tests.TestCase):
    def test_toShardDictHasBookMarker(self) -> None:
        versions = {"ts_wep": "v1", "donut_viz": "v2", "rubintv_production": "abc123"}
        pv = PackageVersions(versions=dict(versions))
        shardDict = pv.toShardDict()
        self.assertEqual(shardDict["DISPLAY_VALUE"], "📖")
        for name, version in versions.items():
            self.assertEqual(shardDict[name], version)
        # rendering must not mutate the underlying versions dict
        self.assertNotIn("DISPLAY_VALUE", pv.versions)

    def test_fromPackages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            makeGitRepo(tmp, tag="v5.0.0")
            with unittest.mock.patch.dict(os.environ, {envVarForPackage("ts_wep"): tmp}):
                pv = PackageVersions.fromPackages(["ts_wep", "not_a_real_package"])
            self.assertEqual(pv.versions["ts_wep"], "v5.0.0")
            self.assertEqual(pv.versions["not_a_real_package"], UNKNOWN_VERSION)

    def test_fromPackagesDefaultsToTrackedPackages(self) -> None:
        pv = PackageVersions.fromPackages()
        self.assertEqual(list(pv.versions.keys()), TRACKED_PACKAGES)


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
        # STACK_TAG doesn't end in _ref, and the commented line is ignored
        self.assertNotIn("STACK_TAG", refs)
        self.assertNotIn("commented", refs)

    def test_realDockerfileFormatIsParseable(self) -> None:
        # Pins the Dockerfile ARG format the runtime cross-check depends on. If
        # this fails, the format drifted and parseDockerfileRefs needs updating
        # (the runtime check only warns, so without this test the drift is
        # silent).
        self.assertTrue(os.path.isfile(REPO_DOCKERFILE), f"Dockerfile not found at {REPO_DOCKERFILE}")
        refs = parseDockerfileRefs(REPO_DOCKERFILE)
        for package in ("ts_wep", "donut_viz"):
            self.assertIn(package, refs, f"{package}_ref no longer parseable from the Dockerfile")
            self.assertTrue(refs[package], f"{package}_ref parsed as empty")
        # rubintv_production is COPYed in, not checked out via a _ref arg
        self.assertNotIn("rubintv_production", refs)


class FindDockerfileTestCase(lsst.utils.tests.TestCase):
    def test_findsDockerfileViaEnvVar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "Dockerfile")
            with open(path, "w") as f:
                f.write("FROM scratch\n")
            with unittest.mock.patch.dict(os.environ, {envVarForPackage("rubintv_production"): tmp}):
                self.assertEqual(findDockerfile(), path)

    def test_noneWhenEnvVarUnset(self) -> None:
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(findDockerfile())

    def test_noneWhenDockerfileMissing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:  # dir exists but has no Dockerfile
            with unittest.mock.patch.dict(os.environ, {envVarForPackage("rubintv_production"): tmp}):
                self.assertIsNone(findDockerfile())


class CheckVersionsAgainstDockerfileTestCase(lsst.utils.tests.TestCase):
    def _writeDockerfile(self, tmp: str, body: str) -> str:
        path = os.path.join(tmp, "Dockerfile")
        with open(path, "w") as f:
            f.write(body)
        return path

    def test_warnsOnMismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = self._writeDockerfile(tmp, 'ARG ts_wep_ref="v2.0.0"\n')
            pv = PackageVersions(versions={"ts_wep": "v1.0.0"})
            with self.assertLogs(level="WARNING") as cm:
                checkVersionsAgainstDockerfile(pv, path)
            self.assertTrue(any("ts_wep" in msg for msg in cm.output))

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

    def test_neverRaisesOnMissingFile(self) -> None:
        pv = PackageVersions(versions={"ts_wep": "v1.0.0"})
        missing = os.path.join("/nonexistent", "Dockerfile")
        with self.assertLogs(level="WARNING"):
            checkVersionsAgainstDockerfile(pv, missing)  # must not raise


class VersionHashTestCase(lsst.utils.tests.TestCase):
    def test_stableAndOrderIndependent(self) -> None:
        a = PackageVersions(versions={"ts_wep": "v1", "donut_viz": "v2"})
        b = PackageVersions(versions={"donut_viz": "v2", "ts_wep": "v1"})  # different insertion order
        self.assertEqual(a.versionHash(), b.versionHash())

    def test_changesWhenAVersionChanges(self) -> None:
        a = PackageVersions(versions={"ts_wep": "v1", "donut_viz": "v2"})
        c = PackageVersions(versions={"ts_wep": "v1", "donut_viz": "v3"})
        self.assertNotEqual(a.versionHash(), c.versionHash())


class GetRegistryPathTestCase(lsst.utils.tests.TestCase):
    def test_perInstrumentFilename(self) -> None:
        path = getRegistryPath("/some/dir", "LSSTCam")
        self.assertEqual(path, "/some/dir/packageVersionRegistry-LSSTCam.json")
        # different instruments get distinct files
        self.assertNotEqual(path, getRegistryPath("/some/dir", "LSSTComCam"))


class ResolveVersionNumberTestCase(lsst.utils.tests.TestCase):
    def test_firstSetGetsOne(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = getRegistryPath(tmp, "LSSTCam")
            pv = PackageVersions(versions={"ts_wep": "v1"})
            self.assertEqual(resolveVersionNumber(path, pv, timestamp="t0"), 1)

    def test_sameSetReturnsSameNumber(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = getRegistryPath(tmp, "LSSTCam")
            pv = PackageVersions(versions={"ts_wep": "v1"})
            first = resolveVersionNumber(path, pv, timestamp="t0")
            second = resolveVersionNumber(path, pv, timestamp="t1")  # later call, same versions
            self.assertEqual(first, second)

    def test_newSetIncrements(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = getRegistryPath(tmp, "LSSTCam")
            pv1 = PackageVersions(versions={"ts_wep": "v1"})
            pv2 = PackageVersions(versions={"ts_wep": "v2"})
            pv3 = PackageVersions(versions={"ts_wep": "v3"})
            self.assertEqual(resolveVersionNumber(path, pv1, timestamp="t0"), 1)
            self.assertEqual(resolveVersionNumber(path, pv2, timestamp="t1"), 2)
            self.assertEqual(resolveVersionNumber(path, pv3, timestamp="t2"), 3)
            # and the old set still maps to its original number
            self.assertEqual(resolveVersionNumber(path, pv1, timestamp="t3"), 1)

    def test_persistsAcrossInstances(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = getRegistryPath(tmp, "LSSTCam")
            pv1 = PackageVersions(versions={"ts_wep": "v1"})
            pv2 = PackageVersions(versions={"ts_wep": "v2"})
            resolveVersionNumber(path, pv1, timestamp="t0")
            resolveVersionNumber(path, pv2, timestamp="t1")
            # read the file back and check its recorded contents
            with open(path) as f:
                entries = json.load(f)["entries"]
            self.assertEqual(len(entries), 2)
            byNumber = {e["number"]: e for e in entries}
            self.assertEqual(byNumber[1]["versions"], {"ts_wep": "v1"})
            self.assertEqual(byNumber[2]["versions"], {"ts_wep": "v2"})
            self.assertEqual(byNumber[1]["hash"], pv1.versionHash())
            self.assertEqual(byNumber[1]["firstSeen"], "t0")

    def test_createsMissingDirectory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            # registry dir does not exist yet
            path = getRegistryPath(os.path.join(tmp, "newdir"), "LSSTCam")
            pv = PackageVersions(versions={"ts_wep": "v1"})
            self.assertEqual(resolveVersionNumber(path, pv, timestamp="t0"), 1)
            self.assertTrue(os.path.isfile(path))

    def test_robustToUnwritablePath(self) -> None:
        # a path under a regular file (not a dir) can't be created -> sentinel
        with tempfile.TemporaryDirectory() as tmp:
            notADir = os.path.join(tmp, "afile")
            with open(notADir, "w") as f:
                f.write("x")
            path = os.path.join(notADir, "packageVersionRegistry-LSSTCam.json")
            pv = PackageVersions(versions={"ts_wep": "v1"})
            with self.assertLogs(level="WARNING"):
                self.assertEqual(resolveVersionNumber(path, pv, timestamp="t0"), UNKNOWN_VERSION_NUMBER)

    def test_robustToMalformedRegistry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = getRegistryPath(tmp, "LSSTCam")
            os.makedirs(tmp, exist_ok=True)
            with open(path, "w") as f:
                f.write("{ this is not valid json")
            pv = PackageVersions(versions={"ts_wep": "v1"})
            with self.assertLogs(level="WARNING"):
                self.assertEqual(resolveVersionNumber(path, pv, timestamp="t0"), UNKNOWN_VERSION_NUMBER)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
