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
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock

import lsst.utils.tests
from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionUniverse, LimitedButler
from lsst.obs.lsst import LsstCam
from lsst.pipe.base.pipeline_graph import TaskNode
from lsst.rubintv.production.pipelineRunning import (
    METRIC_BUNDLE_STORAGE_CLASS,
    MetricTolerantCachingLimitedButler,
    shouldSkipQuantum,
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


class ShouldSkipQuantumTestCase(lsst.utils.tests.TestCase):
    """Tests for `shouldSkipQuantum`.

    cp_verify's per-detector quanta are skipped on the guiders and wavefront
    sensors, and nothing else is: not ISR on those detectors, not cp_verify
    on science detectors, and not the exposure-level cp_verify merges.
    """

    def setUp(self) -> None:
        self.camera = LsstCam.getCamera()
        self.universe = DimensionUniverse()
        # the decision only reads the class name off the task node
        self.cpVerifyDetTask = self._fakeTask("lsst.cp.verify.verifyBias.CpVerifyBiasTask")
        self.cpVerifyMergeTask = self._fakeTask("lsst.cp.verify.mergeResults.CpVerifyExpMergeTask")
        self.isrTask = self._fakeTask("lsst.ip.isr.isrTaskLSST.IsrTaskLSST")

    @staticmethod
    def _fakeTask(taskClassName: str) -> TaskNode:
        return cast(TaskNode, SimpleNamespace(task_class_name=taskClassName))

    def _dataId(self, detector: int | None = None) -> DataCoordinate:
        dataId: dict[str, int | str] = {"instrument": "LSSTCam", "exposure": 2026070200203}
        if detector is not None:
            dataId["detector"] = detector
        return DataCoordinate.standardize(dataId, universe=self.universe)

    def test_cpVerifySkippedOnGuidersAndWavefrontSensors(self) -> None:
        for detector in (189, 190, 191, 192, 204):
            self.assertTrue(
                shouldSkipQuantum(self.camera, self.cpVerifyDetTask, self._dataId(detector)), detector
            )

    def test_cpVerifyRunsOnScienceDetectors(self) -> None:
        for detector in (0, 94, 188):
            self.assertFalse(
                shouldSkipQuantum(self.camera, self.cpVerifyDetTask, self._dataId(detector)), detector
            )

    def test_isrNeverSkipped(self) -> None:
        for detector in (94, 189, 191):
            self.assertFalse(shouldSkipQuantum(self.camera, self.isrTask, self._dataId(detector)), detector)

    def test_exposureLevelCpVerifyNeverSkipped(self) -> None:
        # the step1b merges have no detector, so there is nothing to judge by
        self.assertFalse(shouldSkipQuantum(self.camera, self.cpVerifyMergeTask, self._dataId()))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
