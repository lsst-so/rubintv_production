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

"""Tests for LocationConfig YAML-backed accessors and dispatch helpers.

LocationConfig is the central frozen-dataclass that every pod consults
for filesystem paths, butler repos, bucket names and pipeline files.
Each accessor is a `cached_property` that pulls a key from the loaded
YAML and runs `_checkDir` / `_checkFile` against it.

The tests here monkey-patch `_loadConfigFile` to return a fixture dict
keyed on `tmp_path`-rooted directories, so every accessor can be
walked without touching the on-disk per-site YAMLs in `config/`.

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

The explicit key lists below back the second category: a new accessor
that bypasses validation surfaces as a missing entry in the test
lists rather than silently passing.
"""

from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import patch

import lsst.utils.tests
from lsst.rubintv.production import locationConfig as locationConfigModule
from lsst.rubintv.production.locationConfig import (
    LocationConfig,
    getAutomaticLocationConfig,
)

# These three directory accessors call _checkDir(createIfMissing=False)
# and so the test must precreate them. Every other dir accessor will
# create on demand under tmp_path.
_NON_CREATING_DIR_KEYS = (
    "starTrackerDataPath",
    "astrometryNetRefCatPath",
    "allSkyRootDataPath",
)

# These two accessors run _checkFile, not _checkDir, so the test must
# create real files at those paths.
_FILE_KEYS = ("ts8ButlerPath", "botButlerPath")

# Directory keys that are created on demand. Listed explicitly so the
# test fails when a new accessor is added without being classified.
_CREATED_DIR_KEYS = (
    "metadataPath",
    "auxTelMetadataPath",
    "auxTelMetadataShardPath",
    "ts8MetadataPath",
    "ts8MetadataShardPath",
    "plotPath",
    "starTrackerMetadataPath",
    "starTrackerMetadataShardPath",
    "starTrackerOutputPath",
    "moviePngPath",
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
    "botMetadataPath",
    "botMetadataShardPath",
    "lsstCamMetadataPath",
    "lsstCamMetadataShardPath",
    "tmaMetadataPath",
    "tmaMetadataShardPath",
)


def _buildFixtureConfig(rootDir: str) -> dict:
    """Build a fixture config dict pointing at dirs under ``rootDir``."""
    config: dict = {}

    # Directory accessors — value of each key is just a subdir under rootDir.
    for key in _CREATED_DIR_KEYS + _NON_CREATING_DIR_KEYS:
        config[key] = os.path.join(rootDir, key)
    for key in _NON_CREATING_DIR_KEYS:
        os.makedirs(config[key], exist_ok=True)

    # File accessors — create a real empty file for each.
    for key in _FILE_KEYS:
        path = os.path.join(rootDir, f"{key}.yaml")
        with open(path, "w") as f:
            f.write("")
        config[key] = path

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
    for k in (
        "aosLSSTCamPipelineFileDanish",
        "aosLSSTCamPipelineFileTie",
        "aosLSSTCamFullArrayModePipelineFileDanish",
        "aosLSSTCamFullArrayModePipelineFileTie",
        "aosLSSTCamAiDonutPipelineFile",
        "aosLSSTCamTartsPipelineFile",
        "aosLSSTCamUnpairedDanishPipelineFile",
        "aosLSSTCamWcsDanishBin1PipelineFile",
        "aosLSSTCamWcsDanishBin2PipelineFile",
    ):
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


class LocationConfigTestCase(lsst.utils.tests.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmpRoot = self._tmp.name
        self.config = _buildFixtureConfig(self.tmpRoot)
        self._patcher = patch.object(locationConfigModule, "_loadConfigFile", return_value=self.config)
        self._patcher.start()
        self.locationConfig = LocationConfig("fixture")

    def tearDown(self) -> None:
        self._patcher.stop()
        self._tmp.cleanup()

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
        # If the precondition isn't met (the dir doesn't exist), the
        # accessor must raise rather than silently returning a bad path.
        # Build a fresh LocationConfig without precreating starTrackerDataPath.
        with tempfile.TemporaryDirectory() as tmp:
            cfgDict = _buildFixtureConfig(tmp)
            os.rmdir(cfgDict["starTrackerDataPath"])
            with patch.object(locationConfigModule, "_loadConfigFile", return_value=cfgDict):
                cfg = LocationConfig("fixture")
                with self.assertRaises(RuntimeError):
                    cfg.starTrackerDataPath

    def test_fileAccessorsValidate(self) -> None:
        for key in _FILE_KEYS:
            with self.subTest(key=key):
                value = getattr(self.locationConfig, key)
                self.assertEqual(value, self.config[key])

    def test_fileAccessorRaisesIfFileMissing(self) -> None:
        # _checkFile must fail loudly rather than silently returning a
        # path that doesn't resolve — pods rely on construction-time
        # detection of missing butler files.
        with tempfile.TemporaryDirectory() as tmp:
            cfgDict = _buildFixtureConfig(tmp)
            os.remove(cfgDict["ts8ButlerPath"])
            with patch.object(locationConfigModule, "_loadConfigFile", return_value=cfgDict):
                cfg = LocationConfig("fixture")
                with self.assertRaises(RuntimeError):
                    cfg.ts8ButlerPath

    def test_emptyBucketNameRaises(self) -> None:
        # Production guard: an empty bucketName has previously meant the
        # YAML key was added but never set for this site. The accessor
        # must raise rather than hand back "", which would later show up
        # as silently-failing S3 uploads.
        with tempfile.TemporaryDirectory() as tmp:
            cfgDict = _buildFixtureConfig(tmp)
            cfgDict["bucketName"] = ""
            with patch.object(locationConfigModule, "_loadConfigFile", return_value=cfgDict):
                cfg = LocationConfig("fixture")
                with self.assertRaises(RuntimeError):
                    cfg.bucketName

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
        for key in (
            "aosLSSTCamPipelineFileDanish",
            "aosLSSTCamPipelineFileTie",
            "aosLSSTCamFullArrayModePipelineFileDanish",
            "aosLSSTCamFullArrayModePipelineFileTie",
            "aosLSSTCamAiDonutPipelineFile",
            "aosLSSTCamTartsPipelineFile",
            "aosLSSTCamUnpairedDanishPipelineFile",
            "aosLSSTCamWcsDanishBin1PipelineFile",
            "aosLSSTCamWcsDanishBin2PipelineFile",
            "aosDataDir",
        ):
            with self.subTest(key=key):
                self.assertEqual(getattr(self.locationConfig, key), self.config[key])

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
        self._patcher = patch.object(
            locationConfigModule,
            "_loadConfigFile",
            return_value=_buildFixtureConfig(self._tmp.name),
        )
        self._patcher.start()
        self._savedLoc = os.environ.pop("RAPID_ANALYSIS_LOCATION", None)

    def tearDown(self) -> None:
        self._patcher.stop()
        self._tmp.cleanup()
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


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
