# This file is part of summit_utils.
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

"""Test cases for pipeline building and running."""

from __future__ import annotations

import logging
import unittest
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import patch

from utils import CALIB_FIXTURE_EXPOSURES, getUserRunCollectionName

import lsst.utils.tests
from lsst.daf.butler import Butler, DataCoordinate, DimensionRecord, MissingDatasetTypeError
from lsst.pipe.base import PipelineGraph
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph
from lsst.rubintv.production.locationConfig import LocationConfig, getAutomaticLocationConfig
from lsst.rubintv.production.payloads import Payload
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.processingControl import PipelineComponents, buildPipelines
from lsst.summit.utils.utils import getSite

_LOG = logging.getLogger("lsst.rubintv.production.tests.test_pipelines")


@contextmanager
def swallowLogs() -> Iterator[None]:
    root = logging.getLogger()
    oldLevel = root.level
    handlerLevels = [h.level for h in root.handlers]

    try:
        root.setLevel(logging.CRITICAL + 1)
        for h in root.handlers:
            h.setLevel(logging.CRITICAL + 1)
        yield
    finally:
        root.setLevel(oldLevel)
        for h, lvl in zip(root.handlers, handlerLevels):
            h.setLevel(lvl)


HAS_BUTLER = False
if getSite() in ["staff-rsp", "rubin-devl"]:
    HAS_BUTLER = True

# This whole test class builds real pipelines against a real Butler repo
# seeded with fixture data; there is no meaningful way to run it on a
# laptop. Skip the whole class when we don't have a butler to talk to.
SKIP_NO_BUTLER_REASON = (
    "These tests require a real Butler repo (staff-rsp or rubin-devl); " f"getSite() returned {getSite()!r}."
)

EXPECTED_PIPELINES = [
    "BIAS",
    "DARK",
    "FLAT",
    "ISR",
    "SFM",
    "AOS_WCS_DANISH_BIN_1",
    "AOS_WCS_DANISH_BIN_2",
    "AOS_DANISH",
    "AOS_TIE",
    "AOS_REFIT_WCS",
    "AOS_AI_DONUT_BINNED2",
    "AOS_AI_DONUT_UNBINNED",
    "AOS_TARTS_UNPAIRED",
    "AOS_FAM_TIE",
    "AOS_FAM_DANISH",
    "AOS_UNPAIRED_DANISH",
]

EXPECTED_AOS_PIPELINES = [p for p in EXPECTED_PIPELINES if p.startswith("AOS")]
EXPECTED_FAM_PIPEPLINES = [p for p in EXPECTED_AOS_PIPELINES if "FAM" in p]
EXPECTED_UNPAIRED_PIPELINES = [p for p in EXPECTED_AOS_PIPELINES if "UNPAIRED" in p]
EXPECTED_AOS_NON_FAM_PIPELINES = [
    p for p in EXPECTED_AOS_PIPELINES if "FAM" not in p and "UNPAIRED" not in p and "AOS" in p
]

# TODO: still need to add step1b tests for all the other pipelines


@unittest.skipIf(not HAS_BUTLER, SKIP_NO_BUTLER_REASON)
class TestPipelineGeneration(lsst.utils.tests.TestCase):
    # Declared on the class body so mypy can see attributes that are
    # actually assigned in setUpClass via `cls.foo = ...`.
    locationConfig: LocationConfig
    instrument: str
    minimalButler: Butler
    graphs: list[PipelineGraph]
    pipelines: dict[str, PipelineComponents]
    records: dict[str, DimensionRecord]
    intraDetector: int
    extraDetector: int
    scienceDetector: int
    podDetails: PodDetails
    step1aRunner: SingleCorePipelineRunner
    step1bRunner: SingleCorePipelineRunner

    @classmethod
    def _makeMinimalButler(cls) -> Butler:
        butler = Butler.from_config(
            cls.locationConfig.lsstCamButlerPath,
            instrument=cls.instrument,
            collections=[
                f"{cls.instrument}/defaults",
            ],
        )
        return butler

    def _makeButler(self, pipelineName: str) -> Butler:
        # A fresh, writeable Butler is built per pipeline at the point of use
        # because each pipeline needs its own per-user RUN collection to ensure
        # that we're starting afresh, and outputs from previous runs can't be
        # used (otherwise failing tests might look like they passed, because of
        # picking up previous outputs from sucessful runs), and that name
        # varies by pipeline. The minimalButler held on the class only carries
        # the defaults collection and is read-only, so it cannot be reused
        # here. Pre-building a butler per pipeline in setUpClass would require
        # enumerating every known pipeline name twice and is brittle, so we
        # lazily construct one each time runTest dispatches to a pipeline and
        # patch it onto the runner.
        runCollection = getUserRunCollectionName(pipelineName)
        butler = Butler.from_config(
            self.locationConfig.lsstCamButlerPath,
            instrument=self.instrument,
            collections=[
                f"{self.instrument}/defaults",
                runCollection,
            ],
            writeable=True,
        )
        return butler

    # All fixture construction lives in setUpClass rather than setUp because
    # buildPipelines() takes ~20s and its output is identical for every test
    # method in this class. Running it once per class instead of once per
    # test method cuts the wall-clock for this file roughly 3x. The runner
    # objects are also held on the class because constructing them is non-
    # trivial; runTest mutates runner.butler/runner.runCollection at the
    # point of use, which is safe in a single process (tests run serially
    # within a worker) and equally safe under pytest-xdist (each worker is
    # its own process with its own copy of the class).
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.locationConfig = getAutomaticLocationConfig()
        cls.instrument = "LSSTCam"
        cls.minimalButler = cls._makeMinimalButler()
        cls.graphs, cls.pipelines = buildPipelines("LSSTCam", cls.locationConfig, cls.minimalButler)

        onSkyIds = {"inFocus": 2025111500226, "intra": 2025111500227, "extra": 2025111500228}
        calibIds = {pipelineName.lower(): expId for pipelineName, expId in CALIB_FIXTURE_EXPOSURES.items()}
        fixtureIds = sorted(set(onSkyIds.values()) | set(calibIds.values()))
        where = f"exposure in ({','.join(str(i) for i in fixtureIds)}) AND instrument='LSSTCam'"
        records = cls.minimalButler.query_dimension_records("exposure", where=where)
        assert len(records) == len(
            fixtureIds
        ), f"Expected {len(fixtureIds)} fixture records, got {len(records)}"
        rd = {r.id: r for r in records}
        # keyed by what the record is used as: "inFocus", "intra", "extra"
        # and the calib pipeline each calib fixture belongs to ("bias" etc.)
        cls.records = {imageType: rd[expId] for imageType, expId in (onSkyIds | calibIds).items()}
        cls.intraDetector = 192
        cls.extraDetector = 191
        cls.scienceDetector = 94
        cls.podDetails = PodDetails(
            instrument="FAKE_INSTRUMENT", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=0, depth=0
        )

        cls.step1aRunner = SingleCorePipelineRunner(
            butler=cls.minimalButler,
            locationConfig=cls.locationConfig,
            instrument=cls.instrument,
            step="step1a",
            awaitsDataProduct="raw",
            podDetails=cls.podDetails,
            doRaise=False,
        )
        cls.step1bRunner = SingleCorePipelineRunner(
            butler=cls.minimalButler,
            locationConfig=cls.locationConfig,
            instrument=cls.instrument,
            step="step1b",
            awaitsDataProduct=None,
            podDetails=cls.podDetails,
            doRaise=False,
        )

    def testExpectedPipelinesArePresent(self) -> None:
        """Check that exactly the expected set of pipelines was built.

        Asserts both that every name in ``EXPECTED_PIPELINES`` is present
        and that no unexpected pipelines slipped in, so that adding or
        renaming a pipeline elsewhere in the code fails loudly here until
        ``EXPECTED_PIPELINES`` is updated to match.
        """
        for pipelineName in EXPECTED_PIPELINES:
            self.assertIn(pipelineName, self.pipelines)

        # check no unexpected pipelines either so that we're always explicit
        # that we're testing all the ones we know about.
        for pipelineName in self.pipelines.keys():
            self.assertIn(pipelineName, EXPECTED_PIPELINES, f"Unexpected pipeline {pipelineName} found")

    def testCalibPipelines(self) -> None:
        """Calib step1a runs cp_verify's ISR plus its per-detector verify
        task.

        The task labels are e.g. ``verifyBiasIsr``/``verifyBiasDet`` but the
        execution quanta are named by class, so the labels are checked via
        ``taskExpectations`` and the classes via ``quantaExpectations``.
        """
        for pipelineName in CALIB_FIXTURE_EXPOSURES:
            calibType = pipelineName.lower().capitalize()  # e.g. Bias
            taskExpectations: dict[str, int] = {f"verify{calibType}Isr": 1, f"verify{calibType}Det": 1}
            quantaExpectations: dict[str, int] = {"isr": 1, f"cpverify{calibType}task": 1}
            self.runTest(
                step="step1a",
                imageType=pipelineName.lower(),
                detector=self.scienceDetector,
                pipelinesToRun=[pipelineName],
                taskExpectations=taskExpectations,
                quantaExpectations=quantaExpectations,
            )

    def testCalibPipelinesStep1b(self) -> None:
        """Calib step1b merges the per-detector results for the exposure,
        runs the (instrument-level) run merge on top, and then the
        analysis_tools task that produces the metric bundle for Sasquatch.

        Unlike SFM/AOS step1b these run on an exposure dataId, as calibs
        have no visits. Requires the per-pipeline test collections to hold
        the step1a outputs (``verify<Type>DetStats`` etc.), i.e. to have
        been built by ``createUnitTestCollections.py`` with both step1a
        labels; ``runTest`` checks that up front and says so if not, as
        otherwise the merge quanta silently have no inputs.
        """
        expectations: dict[str, tuple[dict[str, int], dict[str, int]]] = {
            "BIAS": (
                {"verifyBiasExp": 1, "verifyBias": 1, "analyzeBiasCore": 1},
                {"cpverifyexpmergetask": 1, "cpverifyrunmergetask": 1, "verifycalibanalysistask": 1},
            ),
            "DARK": (
                {"verifyDarkExp": 1, "verifyDark": 1, "analyzeDarkCore": 1},
                {"cpverifyexpmergetask": 1, "cpverifyrunmergetask": 1, "verifycalibanalysistask": 1},
            ),
            "FLAT": (
                {"verifyFlatExp": 1, "verifyFlat": 1, "analyzeFlatDetCore": 1},
                {
                    "cpverifyflatexpmergetask": 1,
                    "cpverifyrunmergebyfiltertask": 1,
                    "verifycalibanalysistaskbyfilter": 1,
                },
            ),
        }
        for pipelineName, (taskExpectations, quantaExpectations) in expectations.items():
            self.runTest(
                step="step1b",
                imageType=pipelineName.lower(),
                pipelinesToRun=[pipelineName],
                taskExpectations=taskExpectations,
                quantaExpectations=quantaExpectations,
                step1bDataIdKey="exposure",
            )

    def testIsrOnly(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.scienceDetector,
            pipelinesToRun=["ISR"],
            taskExpectations=taskExpectations,
        )

    def testAosSfmPipelinesStep1a(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1, "calibrateImage": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.scienceDetector,
            pipelinesToRun=["SFM"],
            taskExpectations=taskExpectations,
        )

    def testCalibsPipeline(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1}
        self.runTest(
            step="step1a",
            imageType="bias",
            detector=self.scienceDetector,
            pipelinesToRun=["ISR"],
            taskExpectations=taskExpectations,
        )

    def testAosFamPipelinesStep1aExtraFocal(self) -> None:
        taskExpectations: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="extra",
            detector=self.scienceDetector,
            pipelinesToRun=EXPECTED_FAM_PIPEPLINES,
            taskExpectations=taskExpectations,
        )

    def testAosFamPipelinesStep1aIntraFocal(self) -> None:
        # unpaired intra should have no calcZernikes
        taskExpectations: dict[str, int] = {"isr": 1, "calcZernikes": 0}
        self.runTest(
            step="step1a",
            imageType="intra",
            detector=self.scienceDetector,
            pipelinesToRun=EXPECTED_FAM_PIPEPLINES,
            taskExpectations=taskExpectations,
        )

    def testAosRegularPipelines(self) -> None:
        taskExpectationsExtra: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.extraDetector,
            pipelinesToRun=EXPECTED_AOS_NON_FAM_PIPELINES,
            taskExpectations=taskExpectationsExtra,
        )

        # no calcZernikes for intrafocal for unpaired pipelines
        taskExpectationsIntra: dict[str, int] = {"isr": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.intraDetector,
            pipelinesToRun=EXPECTED_AOS_NON_FAM_PIPELINES,
            taskExpectations=taskExpectationsIntra,
        )

    def testAosRegularUnpairedPipelines(self) -> None:
        taskExpectationsExtra: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.extraDetector,
            pipelinesToRun=EXPECTED_UNPAIRED_PIPELINES,
            taskExpectations=taskExpectationsExtra,
        )

        # calcZernikes *is* expected for intra detectors for unpaired pipelines
        taskExpectationsIntra: dict[str, int] = {"isr": 1, "calcZernikes": 1}
        self.runTest(
            step="step1a",
            imageType="inFocus",
            detector=self.intraDetector,
            pipelinesToRun=EXPECTED_UNPAIRED_PIPELINES,
            taskExpectations=taskExpectationsIntra,
        )

    def testAosRegularPipelinesStep1b(self) -> None:
        taskExpectations: dict[str, int] = {"plotAOSTask": 1}
        self.runTest(
            step="step1b",
            imageType="inFocus",
            pipelinesToRun=EXPECTED_AOS_NON_FAM_PIPELINES,
            taskExpectations=taskExpectations,
        )

    def testRaisingNonFAM(self) -> None:
        for pipeline in EXPECTED_AOS_NON_FAM_PIPELINES:
            # all detectors should fail for intra + extra images for non-FAM
            for imageType in ["intra", "extra"]:
                for detector in [self.intraDetector, self.extraDetector, self.scienceDetector]:
                    failingToFailMsg = f"Failed to raise for {pipeline=}, {imageType=}, {detector=}"
                    with self.assertRaises(ValueError, msg=failingToFailMsg):
                        self.runTest(
                            step="step1a",
                            imageType=imageType,
                            detector=detector,
                            pipelinesToRun=[pipeline],
                            taskExpectations={},
                        )

    def testRaisingFAM(self) -> None:
        for pipeline in EXPECTED_FAM_PIPEPLINES:
            # all images should fail for all corner chips for FAM
            for imageType in ["inFocus", "intra", "extra"]:
                for detector in [self.intraDetector, self.extraDetector]:
                    failingToFailMsg = f"Failed to raise for {pipeline=}, {imageType=}, {detector=}"
                    with self.assertRaises(ValueError, msg=failingToFailMsg):
                        self.runTest(
                            step="step1a",
                            imageType=imageType,
                            detector=detector,
                            pipelinesToRun=[pipeline],
                            taskExpectations={},
                        )

    def assertStep1aOutputsPresent(
        self, pipelineName: str, dataCoord: DataCoordinate, butler: Butler, runCollection: str
    ) -> None:
        """Fail with an actionable message if the step1a products a step1b
        consumes are not in the pipeline's test collection.

        A step1b graph built without its inputs is simply empty, which the
        quanta checks would report as a bare ``0 != 1`` with no hint that the
        cause is a stale or missing collection rather than the pipeline.
        """
        components = self.pipelines[pipelineName]
        producedByStep1a = {
            edge.parent_dataset_type_name
            for task in components.graphs["step1a"].tasks.values()
            for edge in task.outputs.values()
        }
        needed = sorted(
            name for name, _ in components.graphs["step1b"].iter_overall_inputs() if name in producedByStep1a
        )
        self.assertTrue(needed, f"{pipelineName} step1b consumes nothing its step1a produces, which is wrong")

        missing = []
        for name in needed:
            try:
                found = butler.query_datasets(
                    name, collections=[runCollection], data_id=dataCoord, explain=False
                )
            except MissingDatasetTypeError:  # never been written anywhere
                found = []
            if not found:
                missing.append(name)
        self.assertFalse(
            missing,
            f"{runCollection} holds no {missing} for {dataCoord}, so the {pipelineName} step1b graph would"
            " be empty. Rebuild the unit test collections with tests/createUnitTestCollections.py (which"
            " runs both step1a labels for the calibration pipelines).",
        )

    def runTest(
        self,
        *,
        step: str,
        imageType: str,
        pipelinesToRun: list[str],
        detector: int | None = None,
        taskExpectations: dict[str, int] | None = None,
        quantaExpectations: dict[str, int] | None = None,
        step1bDataIdKey: str = "visit",
    ) -> None:
        """Build the quantum graph for ``step`` of each pipeline and check
        the tasks and quanta it contains.

        ``step1bDataIdKey`` is the dimension the step1b dataId is keyed by:
        ``visit`` for SFM/AOS and ``exposure`` for the calibration pipelines,
        mirroring what the head node dispatches.
        """
        taskExpectations = taskExpectations or {}
        quantaExpectations = quantaExpectations or taskExpectations
        if step == "step1a":
            dataCoord = self.minimalButler.registry.expandDataId(
                exposure=self.records[imageType].id,
                detector=detector,
                instrument=self.instrument,
            )
        elif step == "step1b":
            dataCoord = self.minimalButler.registry.expandDataId(
                instrument=self.instrument,
                **{step1bDataIdKey: self.records[imageType].id},
            )
        else:
            raise ValueError(f"Unknown step {step}")

        with swallowLogs():
            for pipelineName in pipelinesToRun:
                runCollection = getUserRunCollectionName(pipelineName)
                extraInfo = (
                    f"{imageType=} in {step} with {dataCoord=} using {runCollection=} running {pipelineName}"
                )
                print(f"Checking {pipelineName}:{step} with {dataCoord}, expecting {taskExpectations}")
                self.assertIn(pipelineName, self.pipelines, f"Pipeline {pipelineName} not found")

                graph = self.pipelines[pipelineName].graphs[step]

                runner = self.step1aRunner if step == "step1a" else self.step1bRunner
                butler = self._makeButler(pipelineName)
                runner.butler = butler  # patch this in now, it's much quicker having runners premade
                runner.runCollection = runCollection
                # The runner would put the tip of the location config's
                # output chain (i.e. whatever the CI last wrote to /repo/main)
                # first in its input collections, so a step1b test could pass
                # by finding step1a outputs the CI happened to produce rather
                # than ones from this pipeline's own test collection. Pin the
                # inputs to the test collection so a pass means what it says.
                collections = [runCollection, f"{self.instrument}/defaults"]
                if step == "step1b":
                    self.assertStep1aOutputsPresent(pipelineName, dataCoord, butler, runCollection)
                payload = Payload(dataCoord, b"", "does not matter here", who="AOS")
                payload = Payload.from_json(payload.to_json(), self.minimalButler)  # fully formed
                with patch.object(runner, "getCollections", return_value=collections):
                    qgb, _, _, _ = runner.getQuantumGraphBuilder(payload, graph)
                qg = qgb.finish().assemble()
                self.assertIsInstance(qg, PredictedQuantumGraph)

                taskNames = list(qg.quanta_by_task.keys())
                # Check that all expected tasks are present
                for taskSubStringToExpect in taskExpectations.keys():
                    taskSubStringToExpect = taskSubStringToExpect.lower()
                    foundTask = False
                    for taskName in taskNames:
                        taskNameLower = taskName.lower()
                        if taskSubStringToExpect in taskNameLower:
                            foundTask = True
                            break
                    self.assertTrue(
                        foundTask,
                        f"Expected task containing '{taskSubStringToExpect}' not found in {taskNames}",
                    )

                # Check that expected tasks have the correct number of quanta
                for taskSubStringToExpect, numTasksToExpectForString in taskExpectations.items():
                    taskSubStringToExpect = taskSubStringToExpect.lower()
                    for taskName in taskNames:
                        taskNameLower = taskName.lower()
                        if taskSubStringToExpect in taskNameLower:
                            self.assertEqual(
                                len(qg.quanta_by_task[taskName]),
                                numTasksToExpectForString,
                                (
                                    f"Task '{taskName}' has {len(qg.quanta_by_task[taskName])} quanta,"
                                    f" expected {numTasksToExpectForString} in pipeline {pipelineName}"
                                    f" for {extraInfo}. Found tasks: {taskNames}"
                                ),
                            )

                executionQuanta = qg.build_execution_quanta()
                self.assertIsInstance(executionQuanta, dict)

                executionQuanta = qg.build_execution_quanta()

                # quantaTaskList deliberately may contain duplicates
                quantaTaskList = [
                    q.taskName.lower() for q in executionQuanta.values() if q.taskName is not None
                ]
                for taskSubStringToExpect, numTasksToExpectForString in quantaExpectations.items():
                    taskSubStringToExpect = taskSubStringToExpect.lower()
                    count = sum(1 for t in quantaTaskList if taskSubStringToExpect in t)
                    self.assertEqual(
                        count,
                        numTasksToExpectForString,
                        (
                            f"Execution quanta: Task containing '{taskSubStringToExpect}' has"
                            f" {count} quanta, expected {numTasksToExpectForString} for {extraInfo}."
                            f" Found tasks: {quantaTaskList}"
                        ),
                    )


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
