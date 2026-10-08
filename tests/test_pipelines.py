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
from typing import Any, Iterator

from utils import getUserRunCollectionName

import lsst.utils.tests
from lsst.daf.butler import Butler, DimensionRecord
from lsst.pipe.base import PipelineGraph
from lsst.pipe.base.quantum_graph import PredictedQuantumGraph
from lsst.rubintv.production.locationConfig import LocationConfig, getAutomaticLocationConfig
from lsst.rubintv.production.payloads import Payload
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.processingControl import (
    LATISS_PIPELINE_NAMES,
    PipelineComponents,
    buildPipelines,
)
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
    "AOS_BLITZ_BIN_1",
    "AOS_BLITZ_BIN_2",
]

EXPECTED_AOS_PIPELINES = [p for p in EXPECTED_PIPELINES if p.startswith("AOS")]
EXPECTED_FAM_PIPEPLINES = [p for p in EXPECTED_AOS_PIPELINES if "FAM" in p]
EXPECTED_UNPAIRED_PIPELINES = [p for p in EXPECTED_AOS_PIPELINES if "UNPAIRED" in p]
# blitz runs all the corner chips in one visit-level quantum with no step1b,
# so it is kept out of the per-detector lists and tested on its own below
EXPECTED_BLITZ_PIPELINES = [p for p in EXPECTED_AOS_PIPELINES if "BLITZ" in p]
EXPECTED_AOS_NON_FAM_PIPELINES = [
    p
    for p in EXPECTED_AOS_PIPELINES
    if "FAM" not in p and "UNPAIRED" not in p and "BLITZ" not in p and "AOS" in p
]
CORNER_DETECTORS = {191, 192, 195, 196, 199, 200, 203, 204}

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

        where = "exposure.day_obs=20251115 AND exposure.seq_num in (226..229,436) AND instrument='LSSTCam'"
        records = cls.minimalButler.query_dimension_records("exposure", where=where)
        assert len(records) == 5, f"Expected 5 fixture exposure records, got {len(records)}"
        rd = {r.seq_num: r for r in records}
        cls.records = {}
        cls.records["inFocus"] = rd[226]
        cls.records["intra"] = rd[227]
        cls.records["extra"] = rd[228]
        cls.records["dark"] = rd[436]
        cls.records["blitz"] = rd[229]  # the in-focus image the CI sends through blitz
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
        # calib pipelines run the verify<product>Isr tasks but the quanta that
        # they actually execute are isr quanta, so check they exist with the
        # right names, but check the quanta counts under 'isr'
        for pipelineName in ["BIAS", "DARK", "FLAT"]:
            taskName = f"verify{pipelineName.lower().capitalize()}Isr"
            taskExpectations: dict[str, int] = {taskName: 1}
            quantaExpectations: dict[str, int] = {"isr": 1}
            self.runTest(
                step="step1a",
                imageType="inFocus",
                detector=self.scienceDetector,
                pipelinesToRun=[pipelineName],
                taskExpectations=taskExpectations,
                quantaExpectations=quantaExpectations,
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
            imageType="dark",
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

    def testAosBlitzPipelines(self) -> None:
        # A blitz payload carries no detector, and must build exactly one
        # quantum for the corner set plus the reformatting quantum. More than
        # one corner quantum means the graph was built per-detector, or per
        # visit the exposure belongs to (see the test below).
        taskExpectations: dict[str, int] = {"donutBlitzCornerTask": 1, "formatBlitzTask": 1}
        for imageType in ("inFocus", "blitz"):
            self.runTest(
                step="step1a",
                imageType=imageType,
                pipelinesToRun=EXPECTED_BLITZ_PIPELINES,
                taskExpectations=taskExpectations,
            )

    def testAosBlitzQuantumIsForTheExposuresOwnVisit(self) -> None:
        # The blitz quantum is visit-level, but an exposure in a multi-exposure
        # sequence belongs to two visits: its own (keyed on the exposure ID, as
        # used everywhere else in rapid analysis) and the sequence's, keyed on
        # the sequence's first exposure. Constraining only the exposure built
        # a quantum for each, and the second wrote 229's Zernikes and plots
        # under visit 226, clobbering 226's own. The quantum must also get all
        # eight corner raws, and only them and only from this exposure, as
        # DonutBlitzCornerTask raises on a non-corner raw.
        for imageType in ("inFocus", "blitz"):
            expId = self.records[imageType].id
            for pipelineName in EXPECTED_BLITZ_PIPELINES:
                with self.subTest(imageType=imageType, pipeline=pipelineName):
                    qg = self.buildQuantumGraph(pipelineName, "step1a", imageType, detector=None)
                    blitzQuanta = [
                        q
                        for q in qg.build_execution_quanta().values()
                        if q.taskName is not None and "donutblitzcornertask" in q.taskName.lower()
                    ]
                    self.assertEqual(len(blitzQuanta), 1, f"Got quanta for {[q.dataId for q in blitzQuanta]}")
                    (quantum,) = blitzQuanta
                    assert quantum.dataId is not None
                    self.assertEqual(quantum.dataId["visit"], expId)

                    raws = quantum.inputs["raw"]
                    self.assertEqual({int(ref.dataId["detector"]) for ref in raws}, CORNER_DETECTORS)
                    self.assertEqual({ref.dataId["exposure"] for ref in raws}, {expId})

    def testAosBlitzBinning(self) -> None:
        # The binning is applied as a config override keyed on the task label,
        # and PipelineComponents silently skips overrides whose label isn't in
        # the pipeline. If donut_viz renamed the task, BIN_1 would quietly run
        # binned, so pin the binning that actually reaches each graph.
        for pipelineName, expectedBinning in (("AOS_BLITZ_BIN_1", 1), ("AOS_BLITZ_BIN_2", 2)):
            with self.subTest(pipeline=pipelineName):
                graph = self.pipelines[pipelineName].graphs["step1a"]
                config: Any = graph.tasks["donutBlitzCornerTask"].config
                self.assertEqual(config.wavefrontFit.binning, expectedBinning)

    def testRaisingBlitz(self) -> None:
        # FAM images go to the science sensors, never to blitz, so a blitz
        # payload for one means the head node's routing is broken.
        for pipeline in EXPECTED_BLITZ_PIPELINES:
            for imageType in ["intra", "extra"]:
                failingToFailMsg = f"Failed to raise for {pipeline=}, {imageType=}"
                with self.assertRaises(ValueError, msg=failingToFailMsg):
                    self.runTest(
                        step="step1a",
                        imageType=imageType,
                        pipelinesToRun=[pipeline],
                        taskExpectations={},
                    )

    def buildQuantumGraph(
        self, pipelineName: str, step: str, imageType: str, detector: int | None
    ) -> PredictedQuantumGraph:
        """Build the quantum graph a worker would build for a payload.

        Parameters
        ----------
        pipelineName : `str`
            The pipeline to build the graph for, e.g. ``"AOS_DANISH"``.
        step : `str`
            The step to build, either ``"step1a"`` or ``"step1b"``.
        imageType : `str`
            The key of the fixture exposure in ``self.records``.
        detector : `int`, optional
            The detector for a step1a payload. ``None`` makes an
            exposure-level payload, as sent for the blitz pipelines.

        Returns
        -------
        qg : `lsst.pipe.base.quantum_graph.PredictedQuantumGraph`
            The quantum graph.
        """
        if step == "step1a":
            dataId: dict[str, int | str] = {
                "instrument": self.instrument,
                "exposure": self.records[imageType].id,
            }
            if detector is not None:
                dataId["detector"] = detector
            dataCoord = self.minimalButler.registry.expandDataId(dataId)
        elif step == "step1b":
            dataCoord = self.minimalButler.registry.expandDataId(
                visit=self.records[imageType].id,
                instrument=self.instrument,
            )
        else:
            raise ValueError(f"Unknown step {step}")

        graph = self.pipelines[pipelineName].graphs[step]
        runner = self.step1aRunner if step == "step1a" else self.step1bRunner
        # patch this in now, it's much quicker having runners premade
        runner.butler = self._makeButler(pipelineName)
        runner.runCollection = getUserRunCollectionName(pipelineName)
        payload = Payload(dataCoord, b"", "does not matter here", who="AOS")
        payload = Payload.from_json(payload.to_json(), self.minimalButler)  # fully formed
        qgb, _, _, _ = runner.getQuantumGraphBuilder(payload, graph)
        return qgb.finish().assemble()

    def runTest(
        self,
        *,
        step: str,
        imageType: str,
        pipelinesToRun: list[str],
        detector: int | None = None,
        taskExpectations: dict[str, int] | None = None,
        quantaExpectations: dict[str, int] | None = None,
    ) -> None:
        taskExpectations = taskExpectations or {}
        quantaExpectations = quantaExpectations or taskExpectations

        with swallowLogs():
            for pipelineName in pipelinesToRun:
                runCollection = getUserRunCollectionName(pipelineName)
                extraInfo = (
                    f"{imageType=} {detector=} in {step} using {runCollection=} running {pipelineName}"
                )
                print(f"Checking {pipelineName}:{step} for {extraInfo}, expecting {taskExpectations}")
                self.assertIn(pipelineName, self.pipelines, f"Pipeline {pipelineName} not found")

                qg = self.buildQuantumGraph(pipelineName, step, imageType, detector)
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


@unittest.skipIf(not HAS_BUTLER, SKIP_NO_BUTLER_REASON)
class TestLatissPipelineGeneration(lsst.utils.tests.TestCase):
    """Pipeline building and QG generation for LATISS.

    LATISS's one AOS pipeline is the WEP monolith, whose quantum consumes
    both raws of a CWFS pair. These tests catch (1) the LATISS pipeline set
    drifting from LATISS_PIPELINE_NAMES, (2) the pair-spanning quantum graph
    no longer resolving to exactly one monolith quantum, and (3) the guards
    against AOS payloads for the wrong image type, or the wrong half of the
    pair, no longer raising.

    The fixture data is a real CWFS pair: exposures 2026062500012 (intra)
    and 2026062500013 (extra), the same pair the CI drip-feeds.
    """

    locationConfig: LocationConfig
    instrument: str
    butler: Butler
    graphs: list[PipelineGraph]
    pipelines: dict[str, PipelineComponents]
    intraRecord: DimensionRecord
    extraRecord: DimensionRecord
    step1aRunner: SingleCorePipelineRunner

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.locationConfig = getAutomaticLocationConfig()
        cls.instrument = "LATISS"
        cls.butler = Butler.from_config(
            cls.locationConfig.auxtelButlerPath,
            instrument=cls.instrument,
            collections=[f"{cls.instrument}/defaults"],
        )
        cls.graphs, cls.pipelines = buildPipelines(cls.instrument, cls.locationConfig, cls.butler)

        where = "exposure.day_obs=20260625 AND exposure.seq_num in (12, 13) AND instrument='LATISS'"
        records = cls.butler.query_dimension_records("exposure", where=where)
        assert len(records) == 2, f"Expected 2 fixture exposure records, got {len(records)}"
        rd = {r.seq_num: r for r in records}
        cls.intraRecord = rd[12]
        cls.extraRecord = rd[13]

        podDetails = PodDetails(
            instrument="FAKE_INSTRUMENT", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=0, depth=0
        )
        cls.step1aRunner = SingleCorePipelineRunner(
            butler=cls.butler,
            locationConfig=cls.locationConfig,
            instrument=cls.instrument,
            step="step1a",
            awaitsDataProduct="raw",
            podDetails=podDetails,
            doRaise=False,
        )

    def _makeAosPayload(self, expRecord: DimensionRecord) -> Payload:
        dataCoord = self.butler.registry.expandDataId(
            exposure=expRecord.id, detector=0, instrument=self.instrument
        )
        payload = Payload(dataCoord, b"", "does not matter here", who="AOS")
        return Payload.from_json(payload.to_json(), self.butler)  # fully formed

    def testExpectedPipelinesArePresent(self) -> None:
        # both directions, so that adding or renaming a LATISS pipeline fails
        # loudly here until LATISS_PIPELINE_NAMES is updated to match
        for pipelineName in LATISS_PIPELINE_NAMES:
            self.assertIn(pipelineName, self.pipelines)
        for pipelineName in self.pipelines.keys():
            self.assertIn(pipelineName, LATISS_PIPELINE_NAMES, f"Unexpected pipeline {pipelineName} found")

    def testLatissAosQuantumGraph(self) -> None:
        # The pair-spanning quantum graph: a payload carrying the extra-focal
        # image must resolve to exactly one monolith quantum, which consumes
        # the raws of both images of the pair.
        runCollection = getUserRunCollectionName("AOS_LATISS")
        butler = Butler.from_config(
            self.locationConfig.auxtelButlerPath,
            instrument=self.instrument,
            collections=[f"{self.instrument}/defaults", runCollection],
            writeable=True,
        )
        runner = self.step1aRunner
        runner.butler = butler
        runner.runCollection = runCollection

        payload = self._makeAosPayload(self.extraRecord)
        graph = self.pipelines["AOS_LATISS"].graphs["step1a"]
        with swallowLogs():
            qgb, where, _, _ = runner.getQuantumGraphBuilder(payload, graph)
            qg = qgb.finish().assemble()
        self.assertIsInstance(qg, PredictedQuantumGraph)

        # both exposures of the pair must be in the data query
        self.assertIn(str(self.intraRecord.id), where)
        self.assertIn(str(self.extraRecord.id), where)

        taskNames = list(qg.quanta_by_task.keys())
        self.assertEqual(len(taskNames), 1, f"Expected only the monolith task, got {taskNames}")
        self.assertIn("latissmonolith", taskNames[0].lower())
        self.assertEqual(len(qg.quanta_by_task[taskNames[0]]), 1)

        executionQuanta = qg.build_execution_quanta()
        (quantum,) = executionQuanta.values()
        rawRefs = [ref for refs in quantum.inputs.values() for ref in refs if ref.datasetType.name == "raw"]
        rawExpIds = {ref.dataId["exposure"] for ref in rawRefs}
        self.assertEqual(rawExpIds, {self.intraRecord.id, self.extraRecord.id})

    def testLatissAosRaisesOnIntraFocalPayload(self) -> None:
        # AOS payloads are only ever dispatched on the extra-focal image; an
        # intra-focal one means the head node trigger has gone wrong, so the
        # worker must refuse it rather than build a graph for the wrong pair
        payload = self._makeAosPayload(self.intraRecord)
        graph = self.pipelines["AOS_LATISS"].graphs["step1a"]
        with self.assertRaises(ValueError):
            self.step1aRunner.getQuantumGraphBuilder(payload, graph)

    def testLatissAosRaisesOnNonCwfsPayload(self) -> None:
        # guard against AOS payloads for non-CWFS images entirely
        where = "exposure.day_obs=20240813 AND exposure.seq_num=632 AND instrument='LATISS'"
        (scienceRecord,) = self.butler.query_dimension_records("exposure", where=where)
        payload = self._makeAosPayload(scienceRecord)
        graph = self.pipelines["AOS_LATISS"].graphs["step1a"]
        with self.assertRaises(ValueError):
            self.step1aRunner.getQuantumGraphBuilder(payload, graph)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
