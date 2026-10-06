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

"""Tests for LocationConfig YAML-backed accessors and the eager-fail contract.

LocationConfig is the central frozen-dataclass that every pod consults
for filesystem paths, butler repos, bucket names and pipeline files.
Each accessor is a `cached_property` that pulls a key from the loaded
YAML and runs `_checkDir` / `_checkFile` against it.

LocationConfig must also fail fast: `__post_init__` touches every
accessor so that a path which cannot be created or read makes
construction raise, rather than letting some unrelated pod blow up
later when it happens to read the offending property. The tests here
pin both halves of that contract.

Two complementary styles are used:

- `LocationConfigTestCase` and friends monkey-patch `_loadConfigFile`
  to return a fixture dict keyed on `tmp_path`-rooted directories, so
  every accessor can be walked without touching the on-disk per-site
  YAMLs in `config/`.
- `LocationConfigInitTestCase` and `RealYamlSanityTestCase` exercise the
  real `config_usdf_testing.yaml` with the CI env vars redirected to a
  tmpdir, checking the eager-validation and `${VAR}`-expansion machinery
  against the genuine config the integration suite relies on.

Walking each accessor against a fixture dict catches three kinds of
regression that are otherwise only seen at pod startup:

- An accessor key gets renamed in code but not in the YAML (or the
  other way around) — the accessor raises ``KeyError`` instead of
  returning the configured path.
- A new directory accessor is added without ``_checkDir`` validation,
  so a misconfigured path silently passes through instead of failing
  loudly at startup.
- The ``_checkDir`` / ``_checkFile`` validation rules drift away from
  what the per-pod startup scripts depend on (e.g. a non-creating dir
  silently becomes creating, masking a missing mount).
- A site config's ``aosDataDir`` relies on an environment variable other
  than the documented CI ones, which only shows up as a failed pod start
  at that site.

The explicit key lists below back the second category: a new accessor
that bypasses validation surfaces as a missing entry in the test
lists rather than silently passing.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

import lsst.utils.tests
from lsst.rubintv.production import locationConfig as locationConfigModule
from lsst.rubintv.production.locationConfig import (
    LocationConfig,
    _expandEnvVars,
    findMissingConfigKeys,
    getAutomaticLocationConfig,
)
from lsst.utils import getPackageDir

# This directory accessor calls _checkDir(createIfMissing=False) and so
# the test must precreate it. Every other dir accessor will create on
# demand under tmp_path.
_NON_CREATING_DIR_KEYS = ("astrometryNetRefCatPath",)

# The batoid data directories, relative to aosDataDir, that batoidFeaDir
# and batoidBendDir check with _checkDir(createIfMissing=False). Like the
# _NON_CREATING_DIR_KEYS they must exist before construction.
_BATOID_SUBDIRS = (("batoid_data", "fea_legacy"), ("batoid_data", "bend"))

# Directory keys that are created on demand. Listed explicitly so the
# test fails when a new accessor is added without being classified.
_CREATED_DIR_KEYS = (
    "auxTelMetadataPath",
    "auxTelMetadataShardPath",
    "plotPath",
    "starTrackerDataPath",
    "starTrackerMetadataPath",
    "starTrackerMetadataShardPath",
    "starTrackerOutputPath",
    "moviePngPath",
    "allSkyRootDataPath",
    "allSkyOutputPath",
    "nightReportPath",
    "comCamMetadataPath",
    "comCamMetadataShardPath",
    "comCamSimMetadataPath",
    "comCamSimMetadataShardPath",
    "comCamSimAosMetadataPath",
    "comCamSimAosMetadataShardPath",
    "comCamAosMetadataPath",
    "comCamAosMetadataShardPath",
    "lsstCamAosMetadataPath",
    "lsstCamAosMetadataShardPath",
    "raPerformanceDirectory",
    "raPerformanceShardsDirectory",
    "guiderDirectory",
    "guiderShardsDirectory",
    "lsstCamMetadataPath",
    "lsstCamMetadataShardPath",
    "tmaMetadataPath",
    "tmaMetadataShardPath",
)


# Pure passthrough accessors for the AOS pipeline files. Every accessor is
# touched at construction, so the fixture must carry every one of these.
_AOS_PIPELINE_FILE_KEYS = (
    "aosLSSTCamPipelineFileDanish",
    "aosLSSTCamPipelineFileTie",
    "aosLSSTCamFullArrayModePipelineFileDanish",
    "aosLSSTCamFullArrayModePipelineFileTie",
    "aosLSSTCamRefitWcsPipelineFile",
    "aosLSSTCamAiDonutBinned2PipelineFile",
    "aosLSSTCamAiDonutUnbinnedPipelineFile",
    "aosLSSTCamTartsPipelineFile",
    "aosLSSTCamUnpairedDanishPipelineFile",
    "aosLSSTCamWcsDanishBin1PipelineFile",
    "aosLSSTCamWcsDanishBin2PipelineFile",
    "aosLATISSPipelineFile",
)


def _preCreateBatoidDirs(aosDataDir: str) -> None:
    """Create the batoid data directories under ``aosDataDir``.

    They are checked but never created by ``LocationConfig``, so they must
    exist before construction.
    """
    for subdir in _BATOID_SUBDIRS:
        os.makedirs(os.path.join(aosDataDir, *subdir), exist_ok=True)


def _buildFixtureConfig(rootDir: str) -> dict:
    """Build a fixture config dict pointing at dirs under ``rootDir``."""
    config: dict = {}

    # Directory accessors — value of each key is just a subdir under rootDir.
    for key in _CREATED_DIR_KEYS + _NON_CREATING_DIR_KEYS:
        config[key] = os.path.join(rootDir, key)
    for key in _NON_CREATING_DIR_KEYS:
        os.makedirs(config[key], exist_ok=True)

    # Pure string passthroughs (no _checkDir / _checkFile validation).
    config["dimensionUniverseFile"] = os.path.join(rootDir, "dimensionUniverse.json")
    config["scratchPath"] = "fixture/scratch"
    config["auxtelButlerPath"] = "FIXTURE_LATISS"
    config["comCamButlerPath"] = "/repo/fixtureComCam.yaml"
    config["lsstCamButlerPath"] = "FIXTURE_LSSTCam"
    config["bucketName"] = "fixture-bucket"
    config["binning"] = 8
    config["consDBURL"] = "http://fixture-consdb"

    # AOS pipeline files — accessor just returns the string, no checks.
    config["aosDataDir"] = os.path.join(rootDir, "aos_data")
    _preCreateBatoidDirs(config["aosDataDir"])
    for k in _AOS_PIPELINE_FILE_KEYS:
        config[k] = f"/fixture/{k}.yaml"

    config["sfmPipelineFile"] = {
        "LATISS": "/fixture/latissSfm.yaml",
        "LSSTComCam": "/fixture/comCamSfm.yaml",
        "LSSTComCamSim": "/fixture/comCamSimSfm.yaml",
        "LSSTCam": "/fixture/lsstCamSfm.yaml",
    }
    config["outputChains"] = {
        "LATISS": "LATISS/runs/quickLook",
        "LSSTComCam": "LSSTComCam/runs/quickLook",
        "LSSTComCamSim": "LSSTComCamSim/runs/quickLook",
        "LSSTCam": "LSSTCam/runs/quickLook",
    }
    return config


def _ciEnvForTmpdir(tmpdir: str) -> dict[str, str]:
    """Map the CI env vars in ``config_usdf_testing.yaml`` to ``tmpdir``."""
    return {
        "RA_CI_DATA_ROOT": os.path.join(tmpdir, "data_root"),
        "RA_CI_STAR_TRACKER_DATA_PATH": os.path.join(tmpdir, "star_tracker"),
        "RA_CI_ASTROMETRY_NET_REF_CAT_PATH": os.path.join(tmpdir, "astrometry"),
    }


def _preCreateNonAutoCreatedDirs(env: dict[str, str]) -> None:
    """Pre-create the directories whose ``_checkDir(createIfMissing=False)``
    calls in ``LocationConfig`` require them to exist before construction.
    """
    os.makedirs(env["RA_CI_ASTROMETRY_NET_REF_CAT_PATH"], exist_ok=True)
    # aosDataDir is ${RA_CI_DATA_ROOT}/aos_data in config_usdf_testing.yaml,
    # as RealYamlSanityTestCase pins
    _preCreateBatoidDirs(os.path.join(env["RA_CI_DATA_ROOT"], "aos_data"))


class LocationConfigTestCase(lsst.utils.tests.TestCase):
    # Cleanups are registered with addCleanup rather than tearDown so they
    # still run if construction in setUp raises: a leaked _loadConfigFile
    # patch otherwise feeds the fixture dict to every later test in the
    # process, turning one failure into a cascade of misleading ones.
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmpRoot = self._tmp.name
        self.config = _buildFixtureConfig(self.tmpRoot)
        patcher = patch.object(locationConfigModule, "_loadConfigFile", return_value=self.config)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.locationConfig = LocationConfig("fixture")

    def test_passthroughStringsRoundTrip(self) -> None:
        # Accessors that just hand the YAML value back unchanged. Pinning
        # them here means a rename of either the accessor or its YAML key
        # surfaces at test time instead of at pod startup.
        self.assertEqual(self.locationConfig.scratchPath, "fixture/scratch")
        self.assertEqual(self.locationConfig.auxtelButlerPath, "FIXTURE_LATISS")
        self.assertEqual(self.locationConfig.comCamButlerPath, "/repo/fixtureComCam.yaml")
        self.assertEqual(self.locationConfig.lsstCamButlerPath, "FIXTURE_LSSTCam")
        self.assertEqual(self.locationConfig.bucketName, "fixture-bucket")
        self.assertEqual(self.locationConfig.binning, 8)
        self.assertEqual(self.locationConfig.consDBURL, "http://fixture-consdb")
        self.assertEqual(self.locationConfig.dimensionUniverseFile, self.config["dimensionUniverseFile"])

    def test_createdDirAccessorsCreateMissingDirs(self) -> None:
        # Walking each accessor should both return the configured path
        # and ensure the directory exists.
        for key in _CREATED_DIR_KEYS:
            with self.subTest(key=key):
                value = getattr(self.locationConfig, key)
                self.assertEqual(value, self.config[key])
                self.assertTrue(os.path.isdir(value), f"{key} should have been created")

    def test_nonCreatingDirAccessors(self) -> None:
        for key in _NON_CREATING_DIR_KEYS:
            with self.subTest(key=key):
                value = getattr(self.locationConfig, key)
                self.assertEqual(value, self.config[key])
                self.assertTrue(os.path.isdir(value))

    def test_nonCreatingDirRaisesIfMissing(self) -> None:
        # A non-creating dir accessor must fail loudly when its directory
        # is absent. Because __post_init__ now touches every accessor, this
        # surfaces at construction time rather than on first access.
        # Uses astrometryNetRefCatPath as the exemplar since it is the only
        # remaining accessor that calls _checkDir(createIfMissing=False).
        with tempfile.TemporaryDirectory() as tmp:
            cfgDict = _buildFixtureConfig(tmp)
            os.rmdir(cfgDict["astrometryNetRefCatPath"])
            with patch.object(locationConfigModule, "_loadConfigFile", return_value=cfgDict):
                with self.assertRaises(RuntimeError):
                    LocationConfig("fixture")

    def test_emptyBucketNameRaises(self) -> None:
        # Production guard: an empty bucketName has previously meant the
        # YAML key was added but never set for this site. The accessor
        # must raise rather than hand back "", which would later show up
        # as silently-failing S3 uploads. Eager validation makes this
        # surface at construction time.
        with tempfile.TemporaryDirectory() as tmp:
            cfgDict = _buildFixtureConfig(tmp)
            cfgDict["bucketName"] = ""
            with patch.object(locationConfigModule, "_loadConfigFile", return_value=cfgDict):
                with self.assertRaises(RuntimeError):
                    LocationConfig("fixture")

    def test_getOutputChainDispatch(self) -> None:
        # Pure dict-lookup; pin all four supported instruments.
        for instrument in ("LATISS", "LSSTComCam", "LSSTComCamSim", "LSSTCam"):
            with self.subTest(instrument=instrument):
                self.assertEqual(
                    self.locationConfig.getOutputChain(instrument),
                    self.config["outputChains"][instrument],
                )

    def test_getOutputChainUnknownInstrumentRaises(self) -> None:
        with self.assertRaises(KeyError):
            self.locationConfig.getOutputChain("NotARealCamera")

    def test_getSfmPipelineFileDispatch(self) -> None:
        for instrument in ("LATISS", "LSSTComCam", "LSSTComCamSim", "LSSTCam"):
            with self.subTest(instrument=instrument):
                self.assertEqual(
                    self.locationConfig.getSfmPipelineFile(instrument),
                    self.config["sfmPipelineFile"][instrument],
                )

    def test_aosPipelineFileAccessors(self) -> None:
        # Pin every aos* passthrough accessor so a rename of the
        # underlying YAML key (or of the accessor itself) surfaces
        # immediately. These keys are tightly coupled to the AOS
        # pipelines referenced from per-pod scripts at startup.
        for key in _AOS_PIPELINE_FILE_KEYS + ("aosDataDir",):
            with self.subTest(key=key):
                self.assertEqual(getattr(self.locationConfig, key), self.config[key])

    def test_batoidDirsResolveUnderAosDataDir(self) -> None:
        # The batoid directories are derived from aosDataDir rather than
        # configured directly; pin the layout LSSTBuilder is handed.
        aosDataDir = self.config["aosDataDir"]
        self.assertEqual(
            self.locationConfig.batoidFeaDir, os.path.join(aosDataDir, "batoid_data", "fea_legacy")
        )
        self.assertEqual(self.locationConfig.batoidBendDir, os.path.join(aosDataDir, "batoid_data", "bend"))

    def test_batoidDirsRaiseWhenNotFound(self) -> None:
        # batoid_rubin's ensure_data_dir() downloads "fea_legacy" and "bend"
        # from Zenodo into a missing directory, so a wrong aosDataDir must
        # raise rather than become a network fetch in a pod. Because
        # __post_init__ touches every accessor, a missing batoid directory
        # surfaces at construction time, for either directory on its own.
        for subdir in _BATOID_SUBDIRS:
            with self.subTest(subdir=os.path.join(*subdir)):
                with tempfile.TemporaryDirectory() as tmp:
                    cfgDict = _buildFixtureConfig(tmp)
                    os.rmdir(os.path.join(cfgDict["aosDataDir"], *subdir))
                    with patch.object(locationConfigModule, "_loadConfigFile", return_value=cfgDict):
                        with self.assertRaises(RuntimeError):
                            LocationConfig("fixture")

    def test_postInitTouchesPlotPath(self) -> None:
        # __post_init__ touches plotPath, which is a _checkDir-creating
        # accessor. After construction the directory must exist already
        # (without the test having to call .plotPath itself).
        self.assertTrue(os.path.isdir(self.config["plotPath"]))


class GetAutomaticLocationConfigTestCase(lsst.utils.tests.TestCase):
    """Cover the env-var / argv resolution logic in
    ``getAutomaticLocationConfig``.

    The precedence pinned here is the contract the per-instrument pod
    entry-point scripts depend on: argv[1] wins, the
    ``RAPID_ANALYSIS_LOCATION`` env var is the fallback, and missing
    both raises ``RuntimeError`` (rather than silently defaulting to
    some location, which has previously caused pods to come up against
    the wrong site's config).
    """

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        patcher = patch.object(
            locationConfigModule,
            "_loadConfigFile",
            return_value=_buildFixtureConfig(self._tmp.name),
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self._savedLoc = os.environ.pop("RAPID_ANALYSIS_LOCATION", None)
        self.addCleanup(self._restoreLocationEnvVar)

    def _restoreLocationEnvVar(self) -> None:
        if self._savedLoc is not None:
            os.environ["RAPID_ANALYSIS_LOCATION"] = self._savedLoc
        elif "RAPID_ANALYSIS_LOCATION" in os.environ:
            del os.environ["RAPID_ANALYSIS_LOCATION"]

    def test_argvOverridesEnvVar(self) -> None:
        os.environ["RAPID_ANALYSIS_LOCATION"] = "ignored"
        with patch.object(locationConfigModule.sys, "argv", ["scriptname", "fixture"]):
            cfg = getAutomaticLocationConfig()
        self.assertEqual(cfg.location, "fixture")

    def test_envVarUsedWhenNoArgv(self) -> None:
        os.environ["RAPID_ANALYSIS_LOCATION"] = "FIXTURE"
        with patch.object(locationConfigModule.sys, "argv", ["scriptname"]):
            cfg = getAutomaticLocationConfig()
        # The function lower-cases the env-var value.
        self.assertEqual(cfg.location, "fixture")

    def test_raisesWhenNeitherSet(self) -> None:
        with patch.object(locationConfigModule.sys, "argv", ["scriptname"]):
            with self.assertRaises(RuntimeError):
                getAutomaticLocationConfig()


def _getSiteConfigFiles() -> list[str]:
    """Get the on-disk per-site config files shipped with the package.

    Returns
    -------
    yamlFiles : `list` [`str`]
        Absolute paths to the ``config_<site>.yaml`` files, sorted.
    """
    configDir = Path(getPackageDir("rubintv_production")) / "config"
    return sorted(str(path) for path in configDir.glob("config_*.yaml"))


class ConfigYamlKeyConsistencyTestCase(lsst.utils.tests.TestCase):
    """The on-disk per-site config files must share the same top-level keys.

    `LocationConfig` accessors assume every key they read is present in
    every site's YAML — adding a key to one config and forgetting another
    fails only at runtime in that location's pod. This is the same check
    the CI suite runs at startup, lifted into a unit test so it fails at
    development time instead.

    It also checks that ``aosDataDir`` resolves to a literal absolute path
    once a site config is loaded, as it is joined straight onto the batoid
    subdirectories.
    """

    def test_allConfigFilesHaveIdenticalTopLevelKeys(self) -> None:
        packageDir = getPackageDir("rubintv_production")
        yamlFiles = _getSiteConfigFiles()
        self.assertTrue(yamlFiles, "no config_*.yaml files found")

        missing = findMissingConfigKeys(yamlFiles)
        if missing:
            lines = ["config files have inconsistent top-level keys:"]
            for filename, keys in sorted(missing.items()):
                rel = os.path.relpath(filename, packageDir)
                lines.append(f"  {rel} is missing: {sorted(keys)}")
            self.fail("\n".join(lines))

    def test_aosDataDirIsALiteralPath(self) -> None:
        # aosDataDir is joined straight onto the batoid subdirectories, so
        # once a site config is loaded it must be a literal absolute path.
        # _loadConfigFile expands ${VAR} references, but only the documented
        # CI variables are provided here: a site relying on any other one
        # keeps the reference verbatim (expandvars leaves unset variables
        # alone) and fails this check, instead of failing at pod start.
        yamlFiles = _getSiteConfigFiles()
        self.assertTrue(yamlFiles, "no config_*.yaml files found")
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, _ciEnvForTmpdir(tmp)):
                for filename in yamlFiles:
                    site = Path(filename).stem.removeprefix("config_")
                    with self.subTest(config=Path(filename).name):
                        aosDataDir = locationConfigModule._loadConfigFile(site)["aosDataDir"]
                        self.assertNotIn("$", aosDataDir)
                        self.assertTrue(Path(aosDataDir).is_absolute(), f"{aosDataDir} is not absolute")


class LocationConfigInitTestCase(unittest.TestCase):
    """Verify LocationConfig validates every YAML-declared path at __init__."""

    def test_initSucceedsAgainstRealConfigWithEnvVarsRedirected(self) -> None:
        """A real config with the CI env vars redirected to a tmpdir
        should construct cleanly and create the auto-created dirs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _ciEnvForTmpdir(tmpdir)
            _preCreateNonAutoCreatedDirs(env)

            with patch.dict(os.environ, env):
                cfg = LocationConfig("usdf_testing")

            self.assertTrue(os.path.isdir(cfg.plotPath))
            self.assertTrue(cfg.plotPath.startswith(env["RA_CI_DATA_ROOT"]))

    def test_initFailsEagerlyOnUnreachablePath(self) -> None:
        """If any path in the YAML cannot be created, init must raise.

        Regression test for the previous behaviour where only
        ``self._config`` and ``self.plotPath`` were touched in
        ``__post_init__``: an unreachable directory could be missed at
        init and only blow up much later when some unrelated pod first
        accessed the property.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _ciEnvForTmpdir(tmpdir)
            _preCreateNonAutoCreatedDirs(env)
            # /dev/null is a char device, so makedirs under it raises.
            env["RA_CI_DATA_ROOT"] = "/dev/null/cannot_create_under_this"

            with patch.dict(os.environ, env):
                with self.assertRaises((RuntimeError, OSError)):
                    LocationConfig("usdf_testing")

    def test_initFailsWhenAYamlKeyIsMissing(self) -> None:
        """All configs share the same key set (enforced by the CI yaml-check),
        so a missing key is a real bug and must surface as a KeyError at
        init, not be silently swallowed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # A minimal config that's missing nearly every required key.
            sparseConfig = {"plotPath": os.path.join(tmpdir, "plots")}
            with patch.object(locationConfigModule, "_loadConfigFile", return_value=sparseConfig):
                with self.assertRaises(KeyError):
                    LocationConfig("sparse")


class ExpandEnvVarsTestCase(unittest.TestCase):
    """Verify ``${VAR}`` refs in YAML strings get expanded at load time."""

    def test_expandsTopLevelStringValues(self) -> None:
        with patch.dict(os.environ, {"_RA_TEST_ROOT": "/tmp/some/where"}):
            self.assertEqual(
                _expandEnvVars({"plotPath": "${_RA_TEST_ROOT}/plots"}),
                {"plotPath": "/tmp/some/where/plots"},
            )

    def test_recursesIntoNestedDictsAndLists(self) -> None:
        with patch.dict(os.environ, {"_RA_TEST_ROOT": "/tmp/x"}):
            node = {
                "outer": "${_RA_TEST_ROOT}/a",
                "nested": {"inner": "${_RA_TEST_ROOT}/b"},
                "listy": ["${_RA_TEST_ROOT}/c", "${_RA_TEST_ROOT}/d"],
            }
            self.assertEqual(
                _expandEnvVars(node),
                {
                    "outer": "/tmp/x/a",
                    "nested": {"inner": "/tmp/x/b"},
                    "listy": ["/tmp/x/c", "/tmp/x/d"],
                },
            )

    def test_leavesNonStringValuesAlone(self) -> None:
        node = {"port": 6111, "enabled": True, "ratio": 0.5, "missing": None}
        self.assertEqual(_expandEnvVars(node), node)


class RealYamlSanityTestCase(unittest.TestCase):
    """Sanity-check that the on-disk ``config_usdf_testing.yaml`` parses
    and contains the env-var placeholders we depend on for redirection."""

    def test_configUsdfTestingHasRedirectableEnvVars(self) -> None:
        cfgPath = os.path.join(os.path.dirname(__file__), "..", "config", "config_usdf_testing.yaml")
        with open(cfgPath) as f:
            raw = yaml.safe_load(f)
        # plotPath should be expressed in terms of RA_CI_DATA_ROOT so the
        # env-var-redirection trick used by the init tests is valid.
        self.assertIn("${RA_CI_DATA_ROOT}", raw["plotPath"])
        # _preCreateNonAutoCreatedDirs pre-creates the batoid directories
        # under this exact path, so the two must stay in step.
        self.assertEqual(raw["aosDataDir"], "${RA_CI_DATA_ROOT}/aos_data")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
