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

"""Test cases for aosRecipes."""

import unittest

import numpy as np
from astropy.table import Table

import lsst.utils.tests
from lsst.rubintv.production.aosRecipes import computeRotatedZernikesForConsDB


def makeZkTable(averageNm: dict[int, float]) -> Table:
    """Make a minimal ``zernikes`` table with just an ``"average"`` row.

    Parameters
    ----------
    averageNm : `dict` [`int`, `float`]
        The average row's value, in nm, for each Noll index.

    Returns
    -------
    zkTable : `astropy.table.Table`
        The table, with the metadata the recipe reads.
    """
    cols = [f"Z{j}" for j in averageNm]
    table = Table({"label": ["average"], **{col: [value] for col, value in zip(cols, averageNm.values())}})
    table.meta = {"opd_columns": cols, "noll_indices": list(averageNm)}
    return table


class ComputeRotatedZernikesForConsDBTestCase(lsst.utils.tests.TestCase):
    """Tests for `computeRotatedZernikesForConsDB`.

    Since ts_wep c33a2d67 a detector with no donuts gives a zernikes table
    whose ``"average"`` row is all NaN, rather than one with no rows at all,
    so the recipe must turn that into an empty dict for callers to skip.
    """

    def test_allNanAverageGivesEmptyDict(self) -> None:
        zkTable = makeZkTable({j: np.nan for j in range(4, 29)})
        self.assertEqual(computeRotatedZernikesForConsDB(zkTable, physicalRotation=12.3), {})

    def test_finiteValuesConvertedToMicronsAndZerosDropped(self) -> None:
        # zero rotation is the identity, so only the nm -> um scaling applies
        zkTable = makeZkTable({4: 1000.0, 5: 0.0, 6: -250.0})
        out = computeRotatedZernikesForConsDB(zkTable, physicalRotation=0.0)
        self.assertEqual(out.keys(), {"z4", "z6"})
        self.assertAlmostEqual(out["z4"], 1.0)
        self.assertAlmostEqual(out["z6"], -0.25)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
