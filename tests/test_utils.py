# This file is part of summit_utils.
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

"""Test cases for utils."""

import logging
import os
import unittest
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import lsst.utils.tests
from lsst.daf.butler import DimensionRecord
from lsst.rubintv.production.predicates import (
    getDoRaise,
    hasRaDec,
    isCalibration,
    isDayObsContiguous,
    isWepImage,
    raiseIf,
    runningCI,
    runningPyTest,
    runningScons,
)
from lsst.rubintv.production.utils import (
    AOS_CCDS,
    AOS_WORKER_MAPPING,
    getFilterColorName,
    getRubinTvInstrumentName,
    mapAosWorkerNumber,
    sanitizeNans,
)
from lsst.summit.utils.utils import getSite


def _fakeRecord(**kwargs: Any) -> DimensionRecord:
    """Build a stand-in for a `DimensionRecord` exposing the given attributes.

    The predicate helpers under test only access a small handful of attributes
    on the record, so a `SimpleNamespace` is sufficient and avoids needing a
    full butler dimension universe.
    """
    return cast(DimensionRecord, SimpleNamespace(**kwargs))


class RubinTVUtilsTestCase(lsst.utils.tests.TestCase):
    """A test case RubinTV utility functions."""

    def test_isDayObsContiguous(self) -> None:
        dayObs = 20220930
        nextDay = 20221001  # next day in a different month
        differentDay = 20221005
        self.assertTrue(isDayObsContiguous(dayObs, nextDay))
        self.assertTrue(isDayObsContiguous(nextDay, dayObs))
        self.assertFalse(isDayObsContiguous(nextDay, differentDay))
        self.assertFalse(isDayObsContiguous(dayObs, dayObs))  # same day

    def test_sanitizeNans(self) -> None:
        self.assertEqual(sanitizeNans({"a": 1.0, "b": float("nan")}), {"a": 1.0, "b": None})
        self.assertEqual(sanitizeNans([1.0, float("nan")]), [1.0, None])
        self.assertIsNone(sanitizeNans(float("nan")))

        # test that a nested dictionary with nan values is sanitized
        nestedDict = {"a": 1.0, "b": {"c": float("nan"), "d": 2.0}}
        result = sanitizeNans(nestedDict)
        self.assertEqual(result["a"], 1.0)
        self.assertEqual(result["b"], {"c": None, "d": 2.0})

        noneKeyedDict = {None: 1.0, "b": {"c": float("nan"), "d": 2.0}}
        self.assertEqual(sanitizeNans(noneKeyedDict), {None: 1.0, "b": {"c": None, "d": 2.0}})

    def test_getSite(self) -> None:
        site = getSite()
        self.assertNotEqual(site.lower(), "unknown")
        self.assertIn(
            site.lower(), ["tucson", "summit", "base", "staff-rsp", "rubin-devl", "jenkins", "usdf-k8s"]
        )


class IsCalibrationTestCase(lsst.utils.tests.TestCase):
    """Tests for `isCalibration`."""

    def test_calibrationTypes(self) -> None:
        for obsType in ("bias", "dark", "flat"):
            self.assertTrue(isCalibration(_fakeRecord(observation_type=obsType)))

    def test_nonCalibrationTypes(self) -> None:
        for obsType in ("science", "engtest", "acq", "cwfs", "focus", "stuttered"):
            self.assertFalse(isCalibration(_fakeRecord(observation_type=obsType)))

    def test_caseSensitive(self) -> None:
        # The current implementation is case-sensitive: pin that behaviour so a
        # future change has to be deliberate.
        self.assertFalse(isCalibration(_fakeRecord(observation_type="BIAS")))


class IsWepImageTestCase(lsst.utils.tests.TestCase):
    """Tests for `isWepImage`."""

    def test_cwfs(self) -> None:
        self.assertTrue(isWepImage(_fakeRecord(observation_type="cwfs")))

    def test_cwfsMixedCase(self) -> None:
        # `isWepImage` is documented as `cwfs` but normalises case, unlike
        # `isCalibration`.
        self.assertTrue(isWepImage(_fakeRecord(observation_type="CWFS")))
        self.assertTrue(isWepImage(_fakeRecord(observation_type="Cwfs")))

    def test_notCwfs(self) -> None:
        for obsType in ("science", "bias", "dark", "flat", "engtest", "focus"):
            self.assertFalse(isWepImage(_fakeRecord(observation_type=obsType)))


class HasRaDecTestCase(lsst.utils.tests.TestCase):
    """Tests for `hasRaDec`."""

    def test_validRaDec(self) -> None:
        self.assertTrue(hasRaDec(_fakeRecord(tracking_ra=180.0, tracking_dec=-30.0)))
        self.assertTrue(hasRaDec(_fakeRecord(tracking_ra=0.0, tracking_dec=0.0)))

    def test_noneValues(self) -> None:
        self.assertFalse(hasRaDec(_fakeRecord(tracking_ra=None, tracking_dec=None)))
        self.assertFalse(hasRaDec(_fakeRecord(tracking_ra=180.0, tracking_dec=None)))
        self.assertFalse(hasRaDec(_fakeRecord(tracking_ra=None, tracking_dec=-30.0)))

    def test_nonFiniteValues(self) -> None:
        self.assertFalse(hasRaDec(_fakeRecord(tracking_ra=float("nan"), tracking_dec=-30.0)))
        self.assertFalse(hasRaDec(_fakeRecord(tracking_ra=180.0, tracking_dec=float("nan"))))
        self.assertFalse(hasRaDec(_fakeRecord(tracking_ra=float("inf"), tracking_dec=0.0)))
        self.assertFalse(hasRaDec(_fakeRecord(tracking_ra=0.0, tracking_dec=float("-inf"))))

    def test_missingAttributes(self) -> None:
        # A record that does not even carry the attributes (e.g. a calib
        # record) must report False rather than raising.
        self.assertFalse(hasRaDec(_fakeRecord()))


class GetRubinTvInstrumentNameTestCase(lsst.utils.tests.TestCase):
    """Tests for `getRubinTvInstrumentName`."""

    def test_knownInstruments(self) -> None:
        self.assertEqual(getRubinTvInstrumentName("LATISS"), "auxtel")
        self.assertEqual(getRubinTvInstrumentName("LSSTCam"), "lsstcam")
        self.assertEqual(getRubinTvInstrumentName("LSSTComCam"), "comcam")
        self.assertEqual(getRubinTvInstrumentName("LSSTComCamSim"), "comcam_sim")

    def test_unknownInstrumentRaises(self) -> None:
        with self.assertRaises(ValueError):
            getRubinTvInstrumentName("LSST-TS8")
        with self.assertRaises(ValueError):
            getRubinTvInstrumentName("")
        with self.assertRaises(ValueError):
            getRubinTvInstrumentName("lsstcam")  # case-sensitive on the input


class GetFilterColorNameTestCase(lsst.utils.tests.TestCase):
    """Tests for `getFilterColorName`."""

    def test_comCamFilters(self) -> None:
        self.assertEqual(getFilterColorName("u_02"), "u_color")
        self.assertEqual(getFilterColorName("g_01"), "g_color")
        self.assertEqual(getFilterColorName("r_03"), "r_color")
        self.assertEqual(getFilterColorName("i_06"), "i_color")
        self.assertEqual(getFilterColorName("z_03"), "z_color")
        self.assertEqual(getFilterColorName("y_04"), "y_color")

    def test_lsstCamFilters(self) -> None:
        self.assertEqual(getFilterColorName("u_24"), "u_color")
        self.assertEqual(getFilterColorName("g_6"), "g_color")
        self.assertEqual(getFilterColorName("r_57"), "r_color")
        self.assertEqual(getFilterColorName("i_39"), "i_color")
        self.assertEqual(getFilterColorName("z_20"), "z_color")
        self.assertEqual(getFilterColorName("y_10"), "y_color")

    def test_specialFilters(self) -> None:
        # Pinhole and "empty" filters both map to white.
        self.assertEqual(getFilterColorName("ph_5"), "white_color")
        self.assertEqual(getFilterColorName("ef_43"), "white_color")

    def test_unknownFilterReturnsNone(self) -> None:
        # Unmapped filters return None rather than raising; the caller is
        # responsible for the fallback.
        self.assertIsNone(getFilterColorName("totally_made_up"))
        self.assertIsNone(getFilterColorName(""))


class MapAosWorkerNumberTestCase(lsst.utils.tests.TestCase):
    """Tests for `mapAosWorkerNumber`.

    The mapping is built from `itertools.product(range(9), AOS_CCDS)` so that
    a flat worker index becomes a `(depth, ccd)` tuple. These tests pin the
    layout so that any change to the order has to be made deliberately.
    """

    def test_firstSlot(self) -> None:
        depth, ccd = mapAosWorkerNumber(0)
        self.assertEqual(depth, 0)
        self.assertEqual(ccd, AOS_CCDS[0])

    def test_secondSlotSameDepth(self) -> None:
        # The second slot moves to the next CCD at the same depth, since the
        # outer loop is depth and the inner loop is ccd.
        depth, ccd = mapAosWorkerNumber(1)
        self.assertEqual(depth, 0)
        self.assertEqual(ccd, AOS_CCDS[1])

    def test_depthRollover(self) -> None:
        # After 8 CCDs at depth 0 we should advance to depth 1, ccd 0.
        depth, ccd = mapAosWorkerNumber(len(AOS_CCDS))
        self.assertEqual(depth, 1)
        self.assertEqual(ccd, AOS_CCDS[0])

    def test_lastSlot(self) -> None:
        # 9 depths * 8 CCDs = 72 slots; index 71 is depth 8, last CCD.
        depth, ccd = mapAosWorkerNumber(71)
        self.assertEqual(depth, 8)
        self.assertEqual(ccd, AOS_CCDS[-1])

    def test_outOfRangeRaises(self) -> None:
        with self.assertRaises(KeyError):
            mapAosWorkerNumber(72)
        with self.assertRaises(KeyError):
            mapAosWorkerNumber(-1)

    def test_mappingHasNoGaps(self) -> None:
        self.assertEqual(set(AOS_WORKER_MAPPING.keys()), set(range(9 * len(AOS_CCDS))))


class GetDoRaiseTestCase(lsst.utils.tests.TestCase):
    """Tests for `getDoRaise`."""

    def test_unsetDefaultsToFalse(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RAPID_ANALYSIS_DO_RAISE", None)
            self.assertFalse(getDoRaise())

    def test_truthyValues(self) -> None:
        for value in ("true", "True", "TRUE", "1", "yes", "YES", "  yes  "):
            with patch.dict(os.environ, {"RAPID_ANALYSIS_DO_RAISE": value}):
                self.assertTrue(getDoRaise(), f"value {value!r} should be truthy")

    def test_falsyValues(self) -> None:
        for value in ("false", "False", "0", "no", "", "maybe", "2"):
            with patch.dict(os.environ, {"RAPID_ANALYSIS_DO_RAISE": value}):
                self.assertFalse(getDoRaise(), f"value {value!r} should be falsy")


class RunningEnvFlagsTestCase(lsst.utils.tests.TestCase):
    """Tests for `runningCI`, `runningScons`, `runningPyTest`."""

    def test_runningCITrueWhenSet(self) -> None:
        for value in ("true", "True", "TRUE"):
            with patch.dict(os.environ, {"RAPID_ANALYSIS_CI": value}):
                self.assertTrue(runningCI())

    def test_runningCIFalseWhenAbsentOrFalse(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RAPID_ANALYSIS_CI", None)
            self.assertFalse(runningCI())
        with patch.dict(os.environ, {"RAPID_ANALYSIS_CI": "false"}):
            self.assertFalse(runningCI())
        with patch.dict(os.environ, {"RAPID_ANALYSIS_CI": "1"}):
            # Note: only "true" (case-insensitive) counts; "1" does not.
            self.assertFalse(runningCI())

    def test_runningSconsTrueWhenSet(self) -> None:
        with patch.dict(os.environ, {"SCONS_BUILDING": "true"}):
            self.assertTrue(runningScons())

    def test_runningSconsFalseWhenAbsent(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("SCONS_BUILDING", None)
            self.assertFalse(runningScons())

    def test_runningPyTestTrueInsidePytest(self) -> None:
        # We are running under pytest right now, so this should be True.
        self.assertTrue(runningPyTest())

    def test_runningPyTestFalseWhenAbsent(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            self.assertFalse(runningPyTest())


class RaiseIfTestCase(lsst.utils.tests.TestCase):
    """Tests for `raiseIf`."""

    def test_raisesWhenFlagTrue(self) -> None:
        logger = logging.getLogger("test_raiseIf.raises")
        error = ValueError("boom")
        with self.assertLogs(logger, level="ERROR"):
            with self.assertRaises(ValueError):
                raiseIf(True, error, logger)

    def test_swallowsWhenFlagFalse(self) -> None:
        logger = logging.getLogger("test_raiseIf.swallows")
        error = RuntimeError("boom")
        # Should not raise, but should log the exception.
        with self.assertLogs(logger, level="ERROR") as cm:
            raiseIf(False, error, logger)
        self.assertTrue(any("boom" in line for line in cm.output))

    def test_useGivenMessage(self) -> None:
        logger = logging.getLogger("test_raiseIf.message")
        error = RuntimeError("real error")
        with self.assertLogs(logger, level="ERROR") as cm:
            raiseIf(False, error, logger, msg="custom prefix")
        self.assertTrue(any("custom prefix" in line for line in cm.output))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
