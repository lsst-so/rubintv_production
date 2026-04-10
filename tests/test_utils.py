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

import unittest
from types import SimpleNamespace
from typing import Any, cast

import lsst.utils.tests
from lsst.daf.butler import DimensionRecord
from lsst.rubintv.production.utils import (
    hasRaDec,
    isCalibration,
    isDayObsContiguous,
    isWepImage,
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


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
