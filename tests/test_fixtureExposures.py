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

"""Test cases for the shared fixture exposure definitions."""

import os
import tempfile
import unittest
from types import SimpleNamespace
from typing import cast

from fixtureExposures import (
    CALIB_EXPOSURES,
    LATISS_ON_SKY,
    LSSTCAM_BIAS,
    LSSTCAM_EXPOSURES,
    LSSTCAM_IN_FOCUS,
    FixtureExposure,
)

import lsst.utils.tests
from lsst.rubintv.production.formatters import makePlotFile
from lsst.rubintv.production.locationConfig import LocationConfig
from lsst.rubintv.production.processingControl import CALIBRATION_PIPELINE_LABELS


class FixtureExposureTestCase(lsst.utils.tests.TestCase):
    """Tests for `FixtureExposure` and the fixture definitions.

    These pin the id-to-dayObs/seqNum encoding, the plot path convention
    shared with the package's formatter, and the consistency of the fixture
    set with the pipelines the head node knows about.
    """

    def test_dayObsAndSeqNum(self) -> None:
        self.assertEqual(LSSTCAM_BIAS.dayObs, 20260702)
        self.assertEqual(LSSTCAM_BIAS.seqNum, 203)
        self.assertEqual(LSSTCAM_IN_FOCUS.dayObs, 20251115)
        self.assertEqual(LSSTCAM_IN_FOCUS.seqNum, 226)
        self.assertEqual(LATISS_ON_SKY.dayObs, 20240813)
        self.assertEqual(LATISS_ON_SKY.seqNum, 632)
        # seqNums below 100000 are the whole encoding, so a small one works too
        self.assertEqual(FixtureExposure("LSSTCam", 2025010100001, "test").seqNum, 1)

    def test_plotPathMatchesFormatter(self) -> None:
        # makePlotFile is what rapid analysis writes plots with, so the
        # relative path the fixtures predict must agree with it exactly,
        # zero-padding and directory layout included, or the CI's expected
        # plot list is silently wrong
        with tempfile.TemporaryDirectory() as tmp:  # makePlotFile makes the directories as a side effect
            locationConfig = cast(LocationConfig, SimpleNamespace(plotPath=tmp))
            for exposure in (*LSSTCAM_EXPOSURES, LATISS_ON_SKY):
                for plotType, suffix in (("focal_plane_mosaic", "jpg"), ("event_timeline", "png")):
                    fullPath = makePlotFile(
                        locationConfig,
                        exposure.instrument,
                        exposure.dayObs,
                        exposure.seqNum,
                        plotType,
                        suffix,
                    )
                    self.assertEqual(os.path.relpath(fullPath, tmp), exposure.plotPath(plotType, suffix))

    def test_calibExposuresMatchPipelines(self) -> None:
        # one fixture per calibration pipeline, and the CI feeds all of them
        self.assertEqual(set(CALIB_EXPOSURES), set(CALIBRATION_PIPELINE_LABELS))
        for pipelineName, exposure in CALIB_EXPOSURES.items():
            self.assertEqual(exposure.role, pipelineName.lower())
            self.assertIn(exposure, LSSTCAM_EXPOSURES)

    def test_fixturesAreDistinctAndConsistent(self) -> None:
        allExposures = (*LSSTCAM_EXPOSURES, LATISS_ON_SKY)
        ids = [exposure.id for exposure in allExposures]
        self.assertEqual(len(ids), len(set(ids)))
        for exposure in LSSTCAM_EXPOSURES:
            self.assertEqual(exposure.instrument, "LSSTCam")
        self.assertEqual(LATISS_ON_SKY.instrument, "LATISS")
        self.assertIn("bias", str(LSSTCAM_BIAS))  # the role is in the display form, for log messages


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
