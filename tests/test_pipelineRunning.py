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

"""Test cases for the parts of pipelineRunning that need no Butler."""

import unittest
from typing import cast
from unittest.mock import MagicMock

import lsst.utils.tests
from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionUniverse, LimitedButler
from lsst.rubintv.production.pipelineRunning import (
    METRIC_BUNDLE_STORAGE_CLASS,
    MetricTolerantCachingLimitedButler,
)


class MetricTolerantCachingLimitedButlerTestCase(lsst.utils.tests.TestCase):
    """Tests for `MetricTolerantCachingLimitedButler`.

    The wrapped butler is a mock whose ``put`` raises, standing in for a
    Sasquatch datastore failure escaping the butler put, so that we can check
    the failure is swallowed for metric bundles and only for metric bundles.
    """

    def setUp(self) -> None:
        universe = DimensionUniverse()
        instrumentOnly = universe.conform(["instrument"])
        dataId = DataCoordinate.standardize(instrument="LSSTCam", universe=universe)
        metricsType = DatasetType("cpBiasCore_metrics", instrumentOnly, METRIC_BUNDLE_STORAGE_CLASS)
        resultsType = DatasetType("verifyBiasResults", instrumentOnly, "ArrowAstropy")
        self.metricRef = DatasetRef(metricsType, dataId, run="run")
        self.otherRef = DatasetRef(resultsType, dataId, run="run")

        # not spec'd on LimitedButler because CachingLimitedButler reads
        # instance attributes (storageClasses) off the wrapped butler at init
        self.wrapped = MagicMock()
        self.wrapped.put.side_effect = RuntimeError("Sasquatch exploded")
        self.butler = MetricTolerantCachingLimitedButler(
            cast(LimitedButler, self.wrapped), set(), set(), set()
        )

    def test_metricBundlePutFailureIsSwallowed(self) -> None:
        with self.assertLogs("lsst.rubintv.production.pipelineRunning", level="ERROR") as cm:
            returned = self.butler.put(object(), self.metricRef)
        self.assertEqual(returned, self.metricRef)  # the caller gets its ref back as if all was well
        self.assertTrue(any("best-effort" in line for line in cm.output))
        self.wrapped.put.assert_called_once()

    def test_otherPutFailuresStillRaise(self) -> None:
        with self.assertRaises(RuntimeError):
            self.butler.put(object(), self.otherRef)

    def test_successfulPutPassesThrough(self) -> None:
        self.wrapped.put.side_effect = None
        self.wrapped.put.return_value = self.metricRef
        self.assertEqual(self.butler.put(object(), self.metricRef), self.metricRef)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
