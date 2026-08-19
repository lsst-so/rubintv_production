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

Covers the git version helpers, the version scraping into the (now
summit_utils-owned) ``PackageVersions`` dataclass, its metadata-shard
rendering, and the advisory Dockerfile cross-check. The ``PackageVersions``
data model itself — its JSON round-trip and content-hash identity — is
tested in summit_utils alongside the code. The
``test_realDockerfileFormatIsParseable`` test deliberately reads the actual
``Dockerfile`` so that any drift in the ``ARG <package>_ref="..."`` format —
which would silently turn the runtime cross-check into a no-op — fails loudly
here instead.
"""

import os
import subprocess
import tempfile
import unittest
import unittest.mock

import lsst.utils.tests
from lsst.rubintv.production.packageVersions import (
    TRACKED_INSTALLED_PACKAGES,
    TRACKED_PACKAGES,
    checkVersionsAgainstDockerfile,
    compareVersionsToDockerfile,
    envVarForPackage,
    findDockerfile,
    getCurrentPackageVersions,
    getGitVersion,
    getInstalledPackageVersion,
    getPackageVersion,
    makePackageVersionShardDict,
    missingPackageDirEnvVars,
    packageVersionsFromShardDict,
    parseDockerfileRefs,
    versionsMatch,
)
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

    def test_missingPackageDirEnvVarsFlagsUnset(self) -> None:
        # all three unset -> all three reported as missing
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(missingPackageDirEnvVars(["ts_wep", "donut_viz"]), ["ts_wep", "donut_viz"])

    def test_missingPackageDirEnvVarsHonoursSetVars(self) -> None:
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            os.environ[envVarForPackage("ts_wep")] = "/some/dir"
            os.environ[envVarForPackage("donut_viz")] = ""  # empty counts as unset
            missing = missingPackageDirEnvVars(["ts_wep", "donut_viz"])
            self.assertNotIn("ts_wep", missing)
            self.assertIn("donut_viz", missing)

    def test_missingPackageDirEnvVarsDefaultsToTracked(self) -> None:
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(missingPackageDirEnvVars(), TRACKED_PACKAGES)


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


class PackageVersionsTestCase(lsst.utils.tests.TestCase):
    def test_shardDictHasBookMarker(self) -> None:
        versions = {"ts_wep": "v1", "donut_viz": "v2", "rubintv_production": "abc123"}
        pv = PackageVersions(versions=dict(versions))
        shardDict = makePackageVersionShardDict(pv)
        self.assertEqual(shardDict["DISPLAY_VALUE"], "📖")
        for name, version in versions.items():
            self.assertEqual(shardDict[name], version)
        # rendering must not mutate the underlying versions dict
        self.assertNotIn("DISPLAY_VALUE", pv.versions)

    def test_shardDictRoundTripsThroughFromShardDict(self) -> None:
        # the shard cell is the backfill's source, so the marker-stripping
        # inverse must recover exactly the versions that were rendered
        pv = PackageVersions(versions={"ts_wep": "v1", "donut_viz": "v2", "danish": "1.1.1"})
        recovered = packageVersionsFromShardDict(makePackageVersionShardDict(pv))
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
        self.assertEqual(list(pv.versions.keys()), TRACKED_PACKAGES + TRACKED_INSTALLED_PACKAGES)


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


class CompareVersionsToDockerfileTestCase(lsst.utils.tests.TestCase):
    def _writeDockerfile(self, tmp: str, body: str) -> str:
        path = os.path.join(tmp, "Dockerfile")
        with open(path, "w") as f:
            f.write(body)
        return path

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
