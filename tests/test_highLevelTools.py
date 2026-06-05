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

"""Test cases for highLevelTools."""

import unittest
from typing import cast
from unittest.mock import patch

from astropy.table import Table

import lsst.utils.tests
from lsst.rubintv.production.highLevelTools import (
    checkCcdVisitQuicklookTable,
    checkVisitQuicklookTable,
)
from lsst.summit.utils.consdbClient import ConsDbClient

# Module path used when patching the ConsDB query helpers that the table-check
# functions call, so the tests never need a live ConsDbClient.
_MODULE = "lsst.rubintv.production.highLevelTools"

# The query helpers are patched out in every test, so the client is never
# touched; a cast dummy keeps the call sites type-correct without a real one.
_FAKE_CLIENT = cast(ConsDbClient, object())


def makeEmptyTable(columns: list[str]) -> Table:
    """Build a table as ``ConsDbClient.query`` does for an empty result.

    The no-rows branch constructs ``Table(names=columns)``, which yields
    all-``float64`` columns and, crucially, an empty ``seq_num`` column.
    This is the shape ConsDB returns on a night with no on-sky images.

    Parameters
    ----------
    columns : `list` [`str`]
        The column names to create.

    Returns
    -------
    table : `astropy.table.Table`
        An empty table with the requested columns.
    """
    return Table(names=columns)


class QuicklookTableCheckTestCase(lsst.utils.tests.TestCase):
    """Tests for the ConsDB quicklook table-check helpers, focusing on the
    empty-table path that has no defined min/max seq_num."""

    def test_ccdVisitEmptyTableDoesNotRaise(self) -> None:
        # No on-sky images -> ccdvisit1_quicklook is empty. min()/max() over an
        # empty seq_num set used to raise ValueError; they must yield None now.
        table = makeEmptyTable(["seq_num", "detector"])
        with patch(f"{_MODULE}.getCcdVisitTableForDay", return_value=table):
            results = checkCcdVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums=set(), lastSeqNum=7)

        self.assertEqual(results.nEntries, 0)
        self.assertIsNone(results.minSeqNum)
        self.assertIsNone(results.maxSeqNum)
        self.assertFalse(results.exceedsButler)
        self.assertEqual(results.missingSeqNums, [])
        self.assertEqual(results.missingOnSkyInputs, [])

    def test_visitEmptyTableDoesNotRaise(self) -> None:
        # The wide visit table holds all images, not just on-sky ones, so it is
        # rarely empty, but a day with no ConsDB entries at all must not crash.
        table = makeEmptyTable(["seq_num", "can_see_sky", "n_inputs"])
        with patch(f"{_MODULE}.getWideQuicklookTableForDay", return_value=table):
            results = checkVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums=set(), lastSeqNum=7)

        self.assertEqual(results.nEntries, 0)
        self.assertIsNone(results.minSeqNum)
        self.assertIsNone(results.maxSeqNum)
        self.assertFalse(results.exceedsButler)

    def test_ccdVisitPopulatedTable(self) -> None:
        table = Table(rows=[[1, 0], [2, 0], [3, 0]], names=["seq_num", "detector"])
        with patch(f"{_MODULE}.getCcdVisitTableForDay", return_value=table):
            results = checkCcdVisitQuicklookTable(
                _FAKE_CLIENT, 20260601, onSkySeqNums={1, 2, 3, 4}, lastSeqNum=4
            )

        self.assertEqual(results.nEntries, 3)
        self.assertEqual(results.minSeqNum, 1)
        self.assertEqual(results.maxSeqNum, 3)
        self.assertFalse(results.exceedsButler)
        # seq_num 4 is on-sky per the butler but absent from the table.
        self.assertEqual(results.missingSeqNums, [4])
        self.assertEqual(results.missingOnSkyInputs, [4])

    def test_visitPopulatedTableFlagsMissingInputs(self) -> None:
        # seq_num 2 is on-sky and present but has zero inputs.
        table = Table(
            rows=[[1, True, 5], [2, True, 0], [3, True, 5]],
            names=["seq_num", "can_see_sky", "n_inputs"],
        )
        with patch(f"{_MODULE}.getWideQuicklookTableForDay", return_value=table):
            results = checkVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums={1, 2, 3}, lastSeqNum=3)

        self.assertEqual(results.minSeqNum, 1)
        self.assertEqual(results.maxSeqNum, 3)
        self.assertEqual(results.missingSeqNums, [])
        self.assertEqual(results.missingOnSkyInputs, [2])
        self.assertFalse(results.exceedsButler)

    def test_exceedsButlerWhenMaxSeqNumBeyondButler(self) -> None:
        table = Table(rows=[[5, 0]], names=["seq_num", "detector"])
        with patch(f"{_MODULE}.getCcdVisitTableForDay", return_value=table):
            results = checkCcdVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums={5}, lastSeqNum=3)

        self.assertEqual(results.maxSeqNum, 5)
        self.assertTrue(results.exceedsButler)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
