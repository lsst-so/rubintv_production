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

"""Tests for the parts of `SingleCorePipelineRunner` that need no Butler.

These tests catch the step1a completion report going to the wrong place:
per-detector payloads must land in the exposure's tracking hash for the
head node's gather, while the detector-less payloads of the blitz pipelines
must be counted as a visit-level step1a. Before blitz, every step1a payload
was assumed to carry a detector, so a detector-less one raised KeyError in
both the success and the failure path.
"""

import logging
import unittest
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import fakeredis
from utils import getSampleExpRecord

import lsst.utils.tests
from lsst.daf.butler import Butler, DataCoordinate
from lsst.rubintv.production import redisUtils as redisUtilsModule
from lsst.rubintv.production.locationConfig import LocationConfig
from lsst.rubintv.production.payloads import Payload
from lsst.rubintv.production.pipelineRunning import SingleCorePipelineRunner
from lsst.rubintv.production.redisKeys import getVisitFailedCounterKey
from lsst.rubintv.production.redisUtils import RedisHelper


def _makeFakeRedis(*args: object, **kwargs: object) -> fakeredis.FakeStrictRedis:
    """Drop-in for `redis.Redis(...)` returning a fresh in-process client."""
    return fakeredis.FakeStrictRedis()


class ReportStep1aFinishedTestCase(lsst.utils.tests.TestCase):
    """`SingleCorePipelineRunner.reportStep1aFinished`, invoked unbound
    against a duck-typed ``self`` backed by a real `RedisHelper` over
    fakeredis, as constructing a runner needs a Butler.
    """

    def setUp(self) -> None:
        self._patcher = patch.object(redisUtilsModule.redis, "Redis", side_effect=_makeFakeRedis)
        self._patcher.start()
        self.helper = RedisHelper(butler=cast(Butler, None), locationConfig=cast(LocationConfig, None))
        self.record = getSampleExpRecord()
        self.runner = SimpleNamespace(
            instrument="LSSTCam",
            redisHelper=self.helper,
            log=logging.getLogger("test.reportStep1aFinished"),
        )

    def tearDown(self) -> None:
        self._patcher.stop()

    def _report(self, dataId: DataCoordinate, failed: bool = False) -> None:
        payload = Payload(dataId=dataId, pipelineGraphBytes=b"", run="test-run", who="AOS")
        SingleCorePipelineRunner.reportStep1aFinished(
            cast(SingleCorePipelineRunner, self.runner), payload, failed=failed
        )

    def test_perDetectorPayloadReportsDetector(self) -> None:
        # the existing behaviour, which the head node's AOS gather counts
        # against the expected detectors
        self._report(DataCoordinate.standardize(self.record.dataId, detector=191))

        info = self.helper.getExposureProcessingInfo("LSSTCam", self.record.id)
        assert info is not None
        self.assertEqual(info.getFinishedDetectors("AOS"), {191})
        self.assertEqual(info.getFailedDetectors("AOS"), set())
        self.assertEqual(self.helper.getNumVisitLevelFinished("LSSTCam", "step1a", "AOS"), 0)

    def test_exposureLevelPayloadReportsVisitLevel(self) -> None:
        # a blitz payload has no detector, so it must neither raise nor write
        # a per-detector field, which the head node would never be expecting
        self._report(self.record.dataId)

        self.assertEqual(self.helper.getNumVisitLevelFinished("LSSTCam", "step1a", "AOS"), 1)
        self.assertIsNone(self.helper.getExposureProcessingInfo("LSSTCam", self.record.id))
        self.assertIsNone(self.helper.redis.get(getVisitFailedCounterKey("LSSTCam", "step1a", "AOS")))

    def test_failedExposureLevelPayloadCountsFailure(self) -> None:
        # the failure path is the one that used to KeyError, masking the
        # original exception; a failure must count as both finished and
        # failed, matching the per-detector and step1b conventions
        self._report(self.record.dataId, failed=True)

        self.assertEqual(self.helper.getNumVisitLevelFinished("LSSTCam", "step1a", "AOS"), 1)
        failedCount = self.helper.redis.get(getVisitFailedCounterKey("LSSTCam", "step1a", "AOS"))
        self.assertEqual(int(failedCount or 0), 1)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
