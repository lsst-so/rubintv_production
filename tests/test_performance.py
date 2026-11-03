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

"""Tests for the AOS timing plot legend in performance.py.

These tests catch the legend advertising ingest markers that were never
drawn. The Butler ingest and S3 upload times come from the raws, which
are removed from the embargo repo after a few days, so plots of older
exposures have no such markers and their legend entries must go too.
"""

import unittest

from matplotlib.figure import Figure

import lsst.utils.tests
from lsst.rubintv.production.performance import createLegendBoxes

INGEST_LABELS = ("OODS ingest", "Butler ingest", "S3 upload")


def legendLabels(fig: Figure) -> list[str]:
    ax = fig.axes[0]
    legend = ax.get_legend()
    assert legend is not None
    return [text.get_text() for text in legend.get_texts()]


class CreateLegendBoxesTestCase(lsst.utils.tests.TestCase):
    """Each ingest marker appears in the legend only when asked for, and
    the task colour entries are unaffected either way.
    """

    def test_allMarkersByDefault(self) -> None:
        # Live exposures still have their raws, so the default must keep
        # all three markers in the legend.
        fig = Figure()
        ax = fig.add_subplot()
        createLegendBoxes(ax, {"isr": "red"})
        self.assertEqual(legendLabels(fig), [*INGEST_LABELS, "isr"])

    def test_rawDerivedMarkersDropped(self) -> None:
        # The case for exposures whose raws have gone: OODS times come
        # from the EFD and survive, the Butler and S3 ones do not.
        fig = Figure()
        ax = fig.add_subplot()
        createLegendBoxes(ax, {"isr": "red"}, showButler=False, showS3=False)
        self.assertEqual(legendLabels(fig), ["OODS ingest", "isr"])

    def test_noMarkers(self) -> None:
        fig = Figure()
        ax = fig.add_subplot()
        createLegendBoxes(ax, {"isr": "red"}, showOods=False, showButler=False, showS3=False)
        self.assertEqual(legendLabels(fig), ["isr"])


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
