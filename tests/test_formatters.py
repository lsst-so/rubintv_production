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

"""Test cases for the plot filename formatters.

The on-disk layout of per-seqNum plots is load-bearing in two directions:
the plotting code writes through ``makePlotFile``, and the integration
suite independently derives the same paths (via ``getPlotRelativePath``)
to check that the plots exist at the end of a run. These tests pin the
layout so that an accidental format change fails loudly here instead of
surfacing as the integration suite looking for files that were written
somewhere else.
"""

import os
import tempfile
import unittest
from types import SimpleNamespace

import lsst.utils.tests
from lsst.rubintv.production.formatters import getPlotRelativePath, makePlotFile


class GetPlotRelativePathTestCase(lsst.utils.tests.TestCase):
    """Pin the <instrument>/<dayObs>/<filename> plot layout."""

    def test_layout(self) -> None:
        # The exact current layout, including the 6-digit zero-padding of
        # the seqNum. The integration suite's expected-plot derivation and
        # everything written via makePlotFile both rely on this shape.
        path = getPlotRelativePath("LSSTCam", 20251115, 226, "calexp_mosaic", "jpg")
        self.assertEqual(path, "LSSTCam/20251115/LSSTCam_calexp_mosaic_dayObs_20251115_seqNum_000226.jpg")

    def test_seqNumPadding(self) -> None:
        # seqNums wider than the 6-digit padding must not be truncated
        path = getPlotRelativePath("LATISS", 20240813, 1234567, "mount", "png")
        self.assertTrue(path.endswith("LATISS_mount_dayObs_20240813_seqNum_1234567.png"))

    def test_makePlotFileResolvesRelativePath(self) -> None:
        # makePlotFile must resolve exactly getPlotRelativePath against
        # locationConfig.plotPath (creating the parent directory as a side
        # effect): if the two ever diverged, plots would be written where
        # the integration suite's checks aren't looking.
        with tempfile.TemporaryDirectory() as tmpDir:
            locationConfig = SimpleNamespace(plotPath=tmpDir)
            filename = makePlotFile(
                locationConfig,  # type: ignore[arg-type]
                "LSSTCam",
                20251115,
                226,
                "mount",
                "png",
            )
            relativePath = getPlotRelativePath("LSSTCam", 20251115, 226, "mount", "png")
            self.assertEqual(filename, os.path.join(tmpDir, relativePath))
            self.assertTrue(os.path.isdir(os.path.dirname(filename)))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
