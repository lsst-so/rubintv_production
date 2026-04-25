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

"""Test cases for consdbUtils."""

import unittest

import numpy as np

import lsst.utils.tests
from lsst.afw.image import ExposureSummaryStats
from lsst.rubintv.production.consdbUtils import (
    CCD_VISIT_MAPPING,
    VISIT_MIN_MED_MAX_MAPPING,
    VISIT_MIN_MED_MAX_TOTAL_MAPPING,
    _removeNans,
    changeType,
)


class MappingShapeTestCase(lsst.utils.tests.TestCase):
    """Schema-shape tests for the consDB column mappings.

    These mappings describe how Python attribute names on ExposureSummaryStats
    are translated to consDB column names. The shape tests catch the most
    common ways for them to drift:

    - a key that is not actually present on `ExposureSummaryStats` (e.g. after
      a rename in afw)
    - a non-string key or value
    - duplicated source keys or duplicated destination columns
    - a typo where the same camelCase column is mapped to two different
      snake_case names in two different mappings
    """

    def test_ccdVisitMappingKeysExistOnExposureSummaryStats(self) -> None:
        stats = ExposureSummaryStats()
        for camelKey in CCD_VISIT_MAPPING:
            self.assertTrue(
                hasattr(stats, camelKey),
                f"CCD_VISIT_MAPPING key {camelKey!r} is not an attribute on "
                f"ExposureSummaryStats — has the field been renamed?",
            )

    def test_visitMinMedMaxMappingKeysExistOnExposureSummaryStats(self) -> None:
        stats = ExposureSummaryStats()
        for camelKey in VISIT_MIN_MED_MAX_MAPPING:
            self.assertTrue(
                hasattr(stats, camelKey),
                f"VISIT_MIN_MED_MAX_MAPPING key {camelKey!r} is not an "
                f"attribute on ExposureSummaryStats — has the field been "
                f"renamed?",
            )

    def test_visitMinMedMaxTotalMappingKeysExistOnExposureSummaryStats(self) -> None:
        stats = ExposureSummaryStats()
        for camelKey in VISIT_MIN_MED_MAX_TOTAL_MAPPING:
            self.assertTrue(
                hasattr(stats, camelKey),
                f"VISIT_MIN_MED_MAX_TOTAL_MAPPING key {camelKey!r} is not an "
                f"attribute on ExposureSummaryStats — has the field been "
                f"renamed?",
            )

    def test_allKeysAndValuesAreStrings(self) -> None:
        for name, mapping in (
            ("CCD_VISIT_MAPPING", CCD_VISIT_MAPPING),
            ("VISIT_MIN_MED_MAX_MAPPING", VISIT_MIN_MED_MAX_MAPPING),
            ("VISIT_MIN_MED_MAX_TOTAL_MAPPING", VISIT_MIN_MED_MAX_TOTAL_MAPPING),
        ):
            for k, v in mapping.items():
                self.assertIsInstance(k, str, f"{name}: key {k!r} is not a str")
                self.assertIsInstance(v, str, f"{name}: value {v!r} is not a str")

    def test_destinationColumnsUnique(self) -> None:
        # If the same consDB column shows up twice as a destination in a
        # single mapping, two different summary-stats values would clobber
        # each other on insert. Within a single mapping all destination
        # columns must be unique.
        for name, mapping in (
            ("CCD_VISIT_MAPPING", CCD_VISIT_MAPPING),
            ("VISIT_MIN_MED_MAX_MAPPING", VISIT_MIN_MED_MAX_MAPPING),
            ("VISIT_MIN_MED_MAX_TOTAL_MAPPING", VISIT_MIN_MED_MAX_TOTAL_MAPPING),
        ):
            values = list(mapping.values())
            self.assertEqual(
                len(values),
                len(set(values)),
                f"{name} has duplicate destination columns: {values}",
            )

    def test_overlappingKeysAgreeOnDestination(self) -> None:
        # The CCD_VISIT_MAPPING and VISIT_MIN_MED_MAX_MAPPING share most of
        # their source keys (one is per-detector, the other is per-visit
        # rolled up). Where a key appears in both, it must map to the same
        # consDB column name — otherwise we are silently producing two
        # column names for the same logical quantity.
        sharedKeys = set(CCD_VISIT_MAPPING) & set(VISIT_MIN_MED_MAX_MAPPING)
        self.assertTrue(sharedKeys, "Expected overlap between the two mappings")
        for key in sharedKeys:
            self.assertEqual(
                CCD_VISIT_MAPPING[key],
                VISIT_MIN_MED_MAX_MAPPING[key],
                f"Key {key!r} maps to different consDB columns in the two "
                f"mappings: CCD={CCD_VISIT_MAPPING[key]!r} vs "
                f"VISIT={VISIT_MIN_MED_MAX_MAPPING[key]!r}",
            )

    def test_visitMinMedMaxTotalIsSubsetOfMinMedMax(self) -> None:
        # The TOTAL mapping is for columns that *also* want a total computed
        # alongside the min/median/max — every key in it must therefore also
        # be present in the regular min/med/max mapping.
        for key in VISIT_MIN_MED_MAX_TOTAL_MAPPING:
            self.assertIn(
                key,
                VISIT_MIN_MED_MAX_MAPPING,
                f"VISIT_MIN_MED_MAX_TOTAL_MAPPING key {key!r} is not in " f"VISIT_MIN_MED_MAX_MAPPING",
            )
            self.assertEqual(
                VISIT_MIN_MED_MAX_TOTAL_MAPPING[key],
                VISIT_MIN_MED_MAX_MAPPING[key],
                f"Key {key!r} maps to different columns in the TOTAL mapping "
                f"vs the regular min/med/max mapping",
            )


class RemoveNansTestCase(lsst.utils.tests.TestCase):
    """Tests for the private `_removeNans` helper."""

    def test_dropsPlainFloatNan(self) -> None:
        out = _removeNans({"a": 1.0, "b": float("nan"), "c": 2.0})
        self.assertEqual(out, {"a": 1.0, "c": 2.0})

    def test_dropsNumpyFloatNan(self) -> None:
        out = _removeNans({"a": 1.0, "b": np.float64("nan"), "c": np.float32("nan")})
        self.assertEqual(out, {"a": 1.0})

    def test_keepsZeroAndNegative(self) -> None:
        # 0.0 and negative values are valid numbers, not NaN — they must
        # not be silently dropped along with the NaNs.
        out = _removeNans({"a": 0.0, "b": -1.5, "c": float("nan")})
        self.assertEqual(out, {"a": 0.0, "b": -1.5})

    def test_keepsInfinity(self) -> None:
        # Infinity is not NaN; the helper only filters out NaN.
        out = _removeNans({"a": float("inf"), "b": float("-inf"), "c": float("nan")})
        self.assertEqual(out, {"a": float("inf"), "b": float("-inf")})

    def test_keepsIntsAndStrings(self) -> None:
        out = _removeNans({"i": 42, "s": "hello", "f": float("nan")})
        self.assertEqual(out, {"i": 42, "s": "hello"})

    def test_emptyMapping(self) -> None:
        self.assertEqual(_removeNans({}), {})

    def test_returnsNewDict(self) -> None:
        # The helper builds a new dict rather than mutating its input.
        original = {"a": 1.0, "b": float("nan")}
        result = _removeNans(original)
        self.assertIsNot(result, original)
        self.assertIn("b", original)  # original untouched


class ChangeTypeTestCase(lsst.utils.tests.TestCase):
    """Tests for `changeType`."""

    def test_bigintReturnsInt(self) -> None:
        typeFunc = changeType("foo", {"foo": "BIGINT"})
        self.assertIs(typeFunc, int)
        self.assertEqual(typeFunc(3.7), 3)

    def test_integerReturnsInt(self) -> None:
        typeFunc = changeType("bar", {"bar": "INTEGER"})
        self.assertIs(typeFunc, int)

    def test_doublePrecisionReturnsFloat(self) -> None:
        typeFunc = changeType("baz", {"baz": "DOUBLE PRECISION"})
        self.assertIs(typeFunc, float)
        self.assertEqual(typeFunc(3), 3.0)

    def test_unknownTypeRaises(self) -> None:
        with self.assertRaises(ValueError):
            changeType("qux", {"qux": "TEXT"})
        with self.assertRaises(ValueError):
            changeType("qux", {"qux": "VARCHAR(64)"})
        with self.assertRaises(ValueError):
            changeType("qux", {"qux": ""})

    def test_missingKeyRaisesKeyError(self) -> None:
        # `changeType` does not catch the KeyError from looking up a missing
        # column in the type mapping; the caller is expected to know which
        # columns exist.
        with self.assertRaises(KeyError):
            changeType("missing", {"foo": "BIGINT"})


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
