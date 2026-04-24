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

"""Test cases for aosUtils."""

import unittest

import numpy as np

import lsst.utils.tests
from lsst.rubintv.production.aosUtils import parseDofStr

EXPECTED_GROUPS = {
    "m2HexPos": 5,  # M2 hexapod   (DOF indices 0–4)
    "camHexPos": 5,  # Camera hexapod (DOF indices 5–9)
    "M1M3Bend": 20,  # M1M3 bending modes (DOF indices 10–29)
    "M2Bend": 20,  # M2 bending modes   (DOF indices 30–49)
}


class ParseDofStrTestCase(lsst.utils.tests.TestCase):
    """Tests for `parseDofStr`.

    `parseDofStr` parses a comma-separated DOF spec like "0-4,10-14" into
    a dict of four boolean numpy arrays, one per AOS DOF group, with True
    where that DOF is active. Tests pin both the per-group shapes and the
    boundary indices between groups.
    """

    def assertDofShape(self, result: dict[str, np.ndarray]) -> None:
        """All four expected groups present, all boolean, all correct shape."""
        self.assertEqual(set(result.keys()), set(EXPECTED_GROUPS.keys()))
        for group, length in EXPECTED_GROUPS.items():
            arr = result[group]
            self.assertEqual(arr.dtype, np.bool_, f"{group} dtype is {arr.dtype}, expected bool")
            self.assertEqual(arr.shape, (length,), f"{group} shape is {arr.shape}, expected ({length},)")

    def test_singleM2HexIndex(self) -> None:
        result = parseDofStr("0")
        self.assertDofShape(result)
        self.assertTrue(result["m2HexPos"][0])
        self.assertEqual(result["m2HexPos"].sum(), 1)
        self.assertEqual(result["camHexPos"].sum(), 0)
        self.assertEqual(result["M1M3Bend"].sum(), 0)
        self.assertEqual(result["M2Bend"].sum(), 0)

    def test_singleCamHexIndex(self) -> None:
        # Index 5 maps to camHexPos[0] (the camera hexapod's first DOF).
        result = parseDofStr("5")
        self.assertDofShape(result)
        self.assertEqual(result["m2HexPos"].sum(), 0)
        self.assertTrue(result["camHexPos"][0])
        self.assertEqual(result["camHexPos"].sum(), 1)

    def test_singleM1M3BendIndex(self) -> None:
        # Index 10 maps to M1M3Bend[0].
        result = parseDofStr("10")
        self.assertTrue(result["M1M3Bend"][0])
        self.assertEqual(result["M1M3Bend"].sum(), 1)

    def test_singleM2BendIndex(self) -> None:
        # Index 30 maps to M2Bend[0].
        result = parseDofStr("30")
        self.assertTrue(result["M2Bend"][0])
        self.assertEqual(result["M2Bend"].sum(), 1)

    def test_lastIndexInEachGroup(self) -> None:
        # Index 4 is the last m2HexPos slot, 9 the last camHexPos, 29 the
        # last M1M3Bend, 49 the last M2Bend. Pinning these guards the
        # boundary arithmetic against off-by-one regressions.
        result = parseDofStr("4,9,29,49")
        self.assertTrue(result["m2HexPos"][4])
        self.assertTrue(result["camHexPos"][4])
        self.assertTrue(result["M1M3Bend"][19])
        self.assertTrue(result["M2Bend"][19])

    def test_groupBoundaries(self) -> None:
        # Pin the exact group boundaries: 5 transitions m2HexPos→camHexPos,
        # 10 transitions camHexPos→M1M3Bend, 30 transitions M1M3Bend→M2Bend.
        boundaryResult = parseDofStr("5,10,30")
        self.assertEqual(boundaryResult["m2HexPos"].sum(), 0)
        self.assertTrue(boundaryResult["camHexPos"][0])
        self.assertTrue(boundaryResult["M1M3Bend"][0])
        self.assertTrue(boundaryResult["M2Bend"][0])

    def test_simpleRange(self) -> None:
        # "0-4" means inclusive 0,1,2,3,4 — fills the whole m2HexPos array.
        result = parseDofStr("0-4")
        self.assertTrue(result["m2HexPos"].all())
        self.assertEqual(result["camHexPos"].sum(), 0)

    def test_multipleRanges(self) -> None:
        result = parseDofStr("0-4,10-14")
        self.assertTrue(result["m2HexPos"].all())
        # Indices 10-14 → M1M3Bend[0:5]
        self.assertTrue(result["M1M3Bend"][:5].all())
        self.assertEqual(result["M1M3Bend"][5:].sum(), 0)

    def test_mixedRangesAndSingles(self) -> None:
        result = parseDofStr("3,7,9-11")
        self.assertTrue(result["m2HexPos"][3])
        self.assertEqual(result["m2HexPos"].sum(), 1)
        self.assertTrue(result["camHexPos"][2])  # idof=7 → camHexPos[2]
        self.assertTrue(result["camHexPos"][4])  # idof=9 → camHexPos[4]
        self.assertEqual(result["camHexPos"].sum(), 2)
        self.assertTrue(result["M1M3Bend"][0])  # idof=10
        self.assertTrue(result["M1M3Bend"][1])  # idof=11
        self.assertEqual(result["M1M3Bend"].sum(), 2)

    def test_duplicatesAreDeduped(self) -> None:
        # The function sorts and dedupes the parsed indices before applying
        # them — duplicates should be a no-op rather than e.g. flipping the
        # bit twice.
        result = parseDofStr("3,3,3,2-4")
        self.assertEqual(result["m2HexPos"].sum(), 3)
        self.assertTrue(result["m2HexPos"][2])
        self.assertTrue(result["m2HexPos"][3])
        self.assertTrue(result["m2HexPos"][4])

    def test_whitespaceTolerated(self) -> None:
        # The function strips leading/trailing whitespace from the whole
        # string, and `int()` handles per-part whitespace too.
        result = parseDofStr("  0,  5,  10  ")
        self.assertTrue(result["m2HexPos"][0])
        self.assertTrue(result["camHexPos"][0])
        self.assertTrue(result["M1M3Bend"][0])

    def test_outOfRangeIndicesAreSilentlyIgnored(self) -> None:
        # Pin the current behaviour: indices >= 50 fall through every elif
        # branch with no else clause and are silently ignored. If this is
        # ever tightened to raise, this test should be updated rather than
        # the behaviour silently changing under callers.
        result = parseDofStr("50,99,0")
        self.assertTrue(result["m2HexPos"][0])
        self.assertEqual(result["m2HexPos"].sum(), 1)
        self.assertEqual(result["camHexPos"].sum(), 0)
        self.assertEqual(result["M1M3Bend"].sum(), 0)
        self.assertEqual(result["M2Bend"].sum(), 0)

    def test_reversedRangeIsEmpty(self) -> None:
        # "10-5" means range(10, 6), which is empty — nothing gets set.
        result = parseDofStr("10-5")
        self.assertEqual(result["m2HexPos"].sum(), 0)
        self.assertEqual(result["camHexPos"].sum(), 0)
        self.assertEqual(result["M1M3Bend"].sum(), 0)
        self.assertEqual(result["M2Bend"].sum(), 0)

    def test_malformedStringRaises(self) -> None:
        with self.assertRaises(ValueError):
            parseDofStr("not_a_number")
        with self.assertRaises(ValueError):
            parseDofStr("1,abc,3")
        with self.assertRaises(ValueError):
            parseDofStr("")  # int('') raises ValueError
        with self.assertRaises(ValueError):
            parseDofStr("1-2-3")  # too many dashes for tuple unpack


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
