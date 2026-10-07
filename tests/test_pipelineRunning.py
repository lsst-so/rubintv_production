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

from astropy.units import adu
from fixtureExposures import LSSTCAM_BIAS, LSSTCAM_IN_FOCUS

import lsst.utils.tests
from lsst.analysis.tools.interfaces import MetricMeasurementBundle
from lsst.analysis.tools.interfaces.datastore import SasquatchDatastore, SasquatchDispatcher
from lsst.daf.butler import DataCoordinate, DatasetRef, DatasetType, DimensionUniverse, LimitedButler
from lsst.daf.butler.datastore import Datastore
from lsst.daf.butler.datastores.chainedDatastore import ChainedDatastore
from lsst.obs.lsst import LsstCam
from lsst.pipe.base.pipeline_graph import TaskNode
from lsst.rubintv.production.pipelineRunning import (
    METRIC_BUNDLE_STORAGE_CLASS,
    MetricTolerantCachingLimitedButler,
    addSasquatchFields,
    getSasquatchDatastores,
    getSasquatchFields,
    shouldSkipQuantum,
)
from lsst.verify import Measurement


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
        self.cpVerifyDetTask = self._makeFakeTask("lsst.cp.verify.verifyBias.CpVerifyBiasTask")
        self.cpVerifyMergeTask = self._makeFakeTask("lsst.cp.verify.mergeResults.CpVerifyExpMergeTask")
        self.isrTask = self._makeFakeTask("lsst.ip.isr.isrTaskLSST.IsrTaskLSST")

    @staticmethod
    def _makeFakeTask(taskClassName: str) -> TaskNode:
        return cast(TaskNode, SimpleNamespace(task_class_name=taskClassName))

    def _makeDataId(self, detector: int | None = None) -> DataCoordinate:
        dataId: dict[str, int | str] = {"instrument": "LSSTCam", "exposure": 2026070200203}
        if detector is not None:
            dataId["detector"] = detector
        return DataCoordinate.standardize(dataId, universe=self.universe)

    def test_cpVerifySkippedOnGuidersAndWavefrontSensors(self) -> None:
        for detector in (189, 190, 191, 192, 204):
            self.assertTrue(
                shouldSkipQuantum(self.camera, self.cpVerifyDetTask, self._makeDataId(detector)), detector
            )

    def test_cpVerifyRunsOnScienceDetectors(self) -> None:
        for detector in (0, 94, 188):
            self.assertFalse(
                shouldSkipQuantum(self.camera, self.cpVerifyDetTask, self._makeDataId(detector)), detector
            )

    def test_isrNeverSkipped(self) -> None:
        for detector in (94, 189, 191):
            self.assertFalse(
                shouldSkipQuantum(self.camera, self.isrTask, self._makeDataId(detector)), detector
            )

    def test_exposureLevelCpVerifyNeverSkipped(self) -> None:
        # the step1b merges have no detector, so there is nothing to judge by
        self.assertFalse(shouldSkipQuantum(self.camera, self.cpVerifyMergeTask, self._makeDataId()))


class SasquatchFieldsTestCase(lsst.utils.tests.TestCase):
    """Tests for adding the payload's exposure and day_obs to the Sasquatch
    records of the metric bundles written while processing it.

    The calibration metric bundles are instrument-level, so without these
    fields nothing in their records tells one exposure's metrics from the
    next except the dispatch time. These catch the fields going missing,
    being sent with a different type or under new names (either of which
    changes the topic's schema, so Sasquatch rejects the records), and one
    payload's fields leaking into the next payload's records.
    """

    def setUp(self) -> None:
        self.universe = DimensionUniverse()
        # full, as payloads' data ids are expanded on arrival
        self.exposureDataId = DataCoordinate.standardize(
            instrument="LSSTCam",
            exposure=LSSTCAM_BIAS.id,
            day_obs=LSSTCAM_BIAS.dayObs,
            group="group",
            physical_filter="empty",
            band="white",
            universe=self.universe,
        )
        self.fields = {"exposure": str(LSSTCAM_BIAS.id), "day_obs": str(LSSTCAM_BIAS.dayObs)}

    def test_getSasquatchFields(self) -> None:
        self.assertEqual(getSasquatchFields(self.exposureDataId), self.fields)

    def test_getSasquatchFieldsOnlyUsesWhatTheDataIdHas(self) -> None:
        # a visit-level payload (e.g. SFM step1b) has a day_obs but no exposure
        visitDataId = DataCoordinate.standardize(
            instrument="LSSTCam",
            visit=LSSTCAM_IN_FOCUS.id,
            day_obs=LSSTCAM_IN_FOCUS.dayObs,
            physical_filter="r_57",
            band="r",
            universe=self.universe,
        )
        self.assertEqual(getSasquatchFields(visitDataId), {"day_obs": str(LSSTCAM_IN_FOCUS.dayObs)})

    def test_getSasquatchDatastoresSearchesChains(self) -> None:
        # the +sasquatch repos chain the Sasquatch datastore after the file
        # datastore, and a chain can itself be a child of a chain
        sasquatch = MagicMock(spec=SasquatchDatastore)
        fileDatastore = MagicMock(spec=Datastore)
        innerChain = MagicMock(spec=ChainedDatastore)
        innerChain.datastores = [sasquatch]
        outerChain = MagicMock(spec=ChainedDatastore)
        outerChain.datastores = [fileDatastore, innerChain]
        self.assertEqual(getSasquatchDatastores(outerChain), [sasquatch])
        self.assertEqual(getSasquatchDatastores(fileDatastore), [])

    def test_addSasquatchFieldsAddsThenRestores(self) -> None:
        # the dataset_tag (from SASQUATCH_EXTRAS or the datastore config) must
        # survive, and once the quantum is done the payload's fields must go,
        # or they would end up on the next payload's records
        tagged = SimpleNamespace(extra_fields={"dataset_tag": "rapid_analysis_ci"})
        untagged = SimpleNamespace(extra_fields=None)
        datastores = cast(list[SasquatchDatastore], [tagged, untagged])
        with addSasquatchFields(datastores, self.fields):
            self.assertEqual(tagged.extra_fields, {"dataset_tag": "rapid_analysis_ci", **self.fields})
            self.assertEqual(untagged.extra_fields, self.fields)
        self.assertEqual(tagged.extra_fields, {"dataset_tag": "rapid_analysis_ci"})
        self.assertIsNone(untagged.extra_fields)

    def test_addSasquatchFieldsRestoresWhenTheQuantumFails(self) -> None:
        datastore = SimpleNamespace(extra_fields=None)
        with self.assertRaises(RuntimeError):
            with addSasquatchFields(cast(list[SasquatchDatastore], [datastore]), self.fields):
                raise RuntimeError("task failed")
        self.assertIsNone(datastore.extra_fields)

    def test_fieldsFillTheRecordsExistingColumns(self) -> None:
        # analysis_tools already sends exposure and day_obs in every record,
        # as strings and empty for an instrument-level bundle. The added
        # fields must fill those, adding no columns and changing no types.
        dispatcher = SasquatchDispatcher("https://sasquatch.invalid", "token", "lsst.dm")
        bundle = MetricMeasurementBundle({"biasMeanPerAmp": [Measurement("calib.biasMean", 1.0 * adu)]})

        def prepareRecord(extraFields: dict[str, str] | None) -> dict:
            records, _ = dispatcher._prepareBundle(
                bundle,
                run="run",
                datasetType="cpBiasCore_metrics",
                identifierFields={"instrument": "LSSTCam"},
                extraFields=extraFields,
            )
            (record,) = records["biasMeanPerAmp"]
            return record["value"]

        plain = prepareRecord(None)
        withFields = prepareRecord(self.fields)
        self.assertEqual(plain["exposure"], "")
        self.assertEqual(plain["day_obs"], "")
        self.assertEqual(withFields["exposure"], self.fields["exposure"])
        self.assertEqual(withFields["day_obs"], self.fields["day_obs"])
        self.assertEqual(withFields.keys(), plain.keys())


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
