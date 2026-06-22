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

"""Unit tests for the Redis-backed methods of `RedisHelper`.

These run against a `fakeredis` in-memory client patched in over the
real `redis.Redis` constructor, so the helper exercises real Redis
semantics (TTLs, set arithmetic, hash atomicity, blocking pops) without
needing a live Redis container.

Methods that depend on a real Butler (anything that calls
`Payload.from_json`, `removeDetector`, or `expRecordFromJson` on the
deserialisation path) are out of scope here — those land in the
integration suite under `tests/ci/`. The tests here cover the methods
whose only side effect is on the Redis side.
"""

from __future__ import annotations

import json
import time
import unittest
from typing import cast
from unittest.mock import patch

import fakeredis
from utils import getSampleExpRecord

import lsst.utils.tests
from lsst.daf.butler import Butler
from lsst.rubintv.production import redisUtils as redisUtilsModule
from lsst.rubintv.production.locationConfig import LocationConfig
from lsst.rubintv.production.payloads import Payload
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.redisKeys import (
    QUEUE_LENGTHS_KEY,
    TRACKING_INITIALIZED_FIELD,
    WITNESS_DETECTOR_KEY,
    getActiveExposuresKey,
    getButlerWatcherListKey,
    getConsDbAnnouncementField,
    getConsDbAnnouncementKey,
    getPodBusyKey,
    getPodExistsKey,
    getPodRunningKey,
    getTrackingExpectedField,
    getTrackingKey,
    getVisitSummaryStatsKey,
)
from lsst.rubintv.production.redisUtils import RedisHelper


def _makeFakeRedis(*args: object, **kwargs: object) -> fakeredis.FakeStrictRedis:
    """Drop-in for `redis.Redis(...)` that returns a fresh fakeredis client.

    Ignores the host/password/port the real constructor takes — a
    fakeredis client serves the same API in-process.
    """
    return fakeredis.FakeStrictRedis()


class _RedisHelperTestBase(lsst.utils.tests.TestCase):
    """Base class that wires up a fakeredis-backed RedisHelper."""

    def setUp(self) -> None:
        # Patch the redis.Redis constructor in the redisUtils module
        # namespace so RedisHelper._makeRedis returns a fakeredis client.
        self._patcher = patch.object(redisUtilsModule.redis, "Redis", side_effect=_makeFakeRedis)
        self._patcher.start()
        self.helper = RedisHelper(
            butler=cast(Butler, None),
            locationConfig=cast(LocationConfig, None),
            isHeadNode=True,
        )
        # Convenience handle on the underlying fake client.
        self.redis = self.helper.redis

    def tearDown(self) -> None:
        self._patcher.stop()


class PodLivenessTestCase(_RedisHelperTestBase):
    """affirmRunning / confirmRunning / announceBusy / announceFree /
    announceExistence — the per-pod lifecycle keys.

    These keys are how the head node distinguishes a live pod from a dead
    one and a busy pod from an idle one. The contract pinned here: a pod is
    "running" only after it affirms (the key is heartbeat-based), free
    clears busy but leaves the existence key intact, and announceExistence
    round-trips both directions."""

    def _pod(self) -> PodDetails:
        return PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=0)

    def test_affirmAndConfirmRunning(self) -> None:
        # confirmRunning is false until the pod affirms — the running key is
        # a heartbeat the pod must keep refreshing, not a one-shot flag, so a
        # pod that stops affirming should fall out of the "running" set.
        pod = self._pod()
        self.assertFalse(self.helper.confirmRunning(pod))
        self.helper.affirmRunning(pod, timePeriod=60)
        self.assertTrue(self.helper.confirmRunning(pod))

        # The running key must carry a TTL bounded by the affirm period: it
        # is set with SETEX so that a pod which stops affirming expires out
        # of the running set on its own. A missing TTL (-1) would leave a
        # dead pod looking alive forever; a TTL above the period would mean
        # the wrong expiry was applied.
        ttl = self.redis.ttl(getPodRunningKey(pod.queueName))
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 60)

    def test_announceBusyAndFree(self) -> None:
        pod = self._pod()
        self.helper.announceBusy(pod)
        self.assertIsNotNone(self.redis.get(getPodBusyKey(pod.queueName)))

        self.helper.announceFree(pod)
        # Free clears the busy key but sets exists.
        self.assertIsNone(self.redis.get(getPodBusyKey(pod.queueName)))
        self.assertIsNotNone(self.redis.get(getPodExistsKey(pod.queueName)))

    def test_announceExistenceRoundTrip(self) -> None:
        pod = self._pod()
        self.helper.announceExistence(pod)
        self.assertIsNotNone(self.redis.get(getPodExistsKey(pod.queueName)))

        self.helper.announceExistence(pod, remove=True)
        self.assertIsNone(self.redis.get(getPodExistsKey(pod.queueName)))


class PodSecondaryStatusTestCase(_RedisHelperTestBase):
    """The secondary status field is the pod's free-form badge (e.g.
    ``RESTARTING``) — the contract here is empty-string default on
    unset, round-trip on set, and clear on ``clearPodSecondaryStatus``.
    Empty-string is documented and relied on by callers, so a switch
    to ``None`` would be a silent breaking change."""

    def _pod(self) -> PodDetails:
        return PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=0)

    def test_setGetClear(self) -> None:
        pod = self._pod()
        # Unset returns the empty string by contract.
        self.assertEqual(self.helper.getPodSecondaryStatus(pod), "")

        self.helper.setPodSecondaryStatus(pod, "RESTARTING")
        self.assertEqual(self.helper.getPodSecondaryStatus(pod), "RESTARTING")

        self.helper.clearPodSecondaryStatus(pod)
        self.assertEqual(self.helper.getPodSecondaryStatus(pod), "")


class WorkerEnumerationTestCase(_RedisHelperTestBase):
    """getAllWorkers / getFreeWorkers — the head node uses these every
    fanout cycle to figure out where to send work."""

    def _registerPod(self, podFlavor: PodFlavor, detectorNumber: int, depth: int) -> PodDetails:
        pod = PodDetails(
            instrument="LSSTCam",
            podFlavor=podFlavor,
            detectorNumber=detectorNumber,
            depth=depth,
        )
        self.helper.announceExistence(pod)
        return pod

    def test_emptyClusterReturnsEmpty(self) -> None:
        self.assertEqual(self.helper.getAllWorkers("LSSTCam", PodFlavor.SFM_WORKER), [])
        self.assertEqual(self.helper.getFreeWorkers("LSSTCam", PodFlavor.SFM_WORKER), [])

    def test_getAllWorkersIncludesBusyAndFree(self) -> None:
        free = self._registerPod(PodFlavor.SFM_WORKER, 94, 0)
        busy = self._registerPod(PodFlavor.SFM_WORKER, 95, 0)
        self.helper.announceBusy(busy)

        all_ = self.helper.getAllWorkers("LSSTCam", PodFlavor.SFM_WORKER)
        self.assertEqual(set(all_), {free, busy})

        # getFreeWorkers must filter out the busy one.
        free_ = self.helper.getFreeWorkers("LSSTCam", PodFlavor.SFM_WORKER)
        self.assertEqual(free_, [free])

    def test_getAllWorkersFiltersByPodFlavor(self) -> None:
        sfm = self._registerPod(PodFlavor.SFM_WORKER, 94, 0)
        # Same instrument, different flavor — must not bleed across.
        aos = PodDetails(
            instrument="LSSTCam",
            podFlavor=PodFlavor.AOS_WORKER,
            detectorNumber=190,
            depth=0,
        )
        self.helper.announceExistence(aos)

        sfmWorkers = self.helper.getAllWorkers("LSSTCam", PodFlavor.SFM_WORKER)
        aosWorkers = self.helper.getAllWorkers("LSSTCam", PodFlavor.AOS_WORKER)
        self.assertEqual(sfmWorkers, [sfm])
        self.assertEqual(aosWorkers, [aos])

    def test_getSingleWorkerPrefersFree(self) -> None:
        # Dispatch policy: an idle worker is always preferred over a busy
        # one. If this regressed to picking any worker, work would pile up
        # on a busy pod while a free one sat doing nothing.
        free = self._registerPod(PodFlavor.SFM_WORKER, 94, 0)
        busy = self._registerPod(PodFlavor.SFM_WORKER, 95, 0)
        self.helper.announceBusy(busy)

        picked = self.helper.getSingleWorker("LSSTCam", PodFlavor.SFM_WORKER)
        self.assertEqual(picked, free)

    def test_getSingleWorkerFallsBackToBusy(self) -> None:
        # When nothing is free, getSingleWorker must still return a busy
        # worker rather than None — the work has to land *somewhere* (there
        # is no backlog queue yet). Returning None here would silently drop
        # the payload instead of queueing it behind existing work.
        busy = self._registerPod(PodFlavor.SFM_WORKER, 94, 0)
        self.helper.announceBusy(busy)

        picked = self.helper.getSingleWorker("LSSTCam", PodFlavor.SFM_WORKER)
        self.assertEqual(picked, busy)

    def test_getSingleWorkerReturnsNoneWhenNoWorkers(self) -> None:
        # Only when there is genuinely no pod of this flavor at all does the
        # caller get None — that's the signal to log-and-skip, distinct from
        # the "all busy" fallback above.
        self.assertIsNone(self.helper.getSingleWorker("LSSTCam", PodFlavor.SFM_WORKER))


class PayloadQueueTestCase(_RedisHelperTestBase):
    """enqueuePayload + getQueueLength.

    Skip dequeue here because dequeuePayload calls Payload.from_json,
    which needs a real Butler.registry.expandDataId — that path is
    covered by the integration suite.
    """

    def _pod(self) -> PodDetails:
        return PodDetails(instrument="LSSTCam", podFlavor=PodFlavor.SFM_WORKER, detectorNumber=94, depth=0)

    def _payload(self, who: str = "SFM") -> Payload:
        record = getSampleExpRecord()
        return Payload(
            dataId=record.dataId,
            run="test-run",
            pipelineGraphBytes=b"",
            who=who,
        )

    def test_enqueuePushesToQueueAndIncrementsCounter(self) -> None:
        pod = self._pod()
        payload = self._payload()

        self.helper.enqueuePayload(payload, pod, top=True)
        self.assertEqual(self.helper.getQueueLength(pod), 1)
        # The actual JSON is sitting at the head of the queue.
        head = self.redis.lindex(pod.queueName, 0)
        self.assertIsNotNone(head)

    def test_enqueueTopVsBottomOrdering(self) -> None:
        pod = self._pod()
        first = self._payload(who="FIRST")
        second = self._payload(who="SECOND")
        third = self._payload(who="THIRD")

        # Convention: top=True means LPUSH (i.e. inserted at index 0).
        self.helper.enqueuePayload(first, pod, top=True)
        self.helper.enqueuePayload(second, pod, top=False)
        self.helper.enqueuePayload(third, pod, top=True)

        # third is now at the head, then first, then second at the
        # tail.
        items = self.redis.lrange(pod.queueName, 0, -1)
        whos = [json.loads(item)["who"] for item in redisUtilsModule.decode_list(items)]
        self.assertEqual(whos, ["THIRD", "FIRST", "SECOND"])

    def test_queueLengthIsZeroWhenUntracked(self) -> None:
        pod = self._pod()
        self.assertEqual(self.helper.getQueueLength(pod), 0)

    def test_enqueueIncrementsLengthHash(self) -> None:
        pod = self._pod()
        payload = self._payload()

        for _ in range(3):
            self.helper.enqueuePayload(payload, pod)

        # The length is reported via the QUEUE_LENGTHS_KEY hash, not by
        # llen on the queue itself.
        rawLen = self.redis.hget(QUEUE_LENGTHS_KEY, pod.queueName)
        self.assertIsNotNone(rawLen)
        assert rawLen is not None  # for mypy
        self.assertEqual(int(rawLen), 3)


class ExposureTrackingTestCase(_RedisHelperTestBase):
    """initExposureTracking, setExpectedDetectors and friends — the
    per-exposure tracking hash.

    This hash is the head node's source of truth for which detectors are
    expected vs. finished for each exposure, and the gather logic gates on
    it. Drift in any of these read/write paths silently hangs an exposure
    (a detector that never registers as finished) or completes it early (a
    detector dropped from the expected set), so the round-trips below are
    pinning the wire-level contract, not just the Python API."""

    def test_initExposureTrackingSetsSentinelAndTtl(self) -> None:
        self.helper.initExposureTracking("LSSTCam", 1001)
        key = getTrackingKey("LSSTCam", 1001)
        # The sentinel field is set to "1".
        self.assertEqual(self.redis.hget(key, TRACKING_INITIALIZED_FIELD), b"1")
        # A TTL is attached. Redis returns -1 for "no TTL" and -2 for
        # "no key", so the value must be positive.
        self.assertGreater(self.redis.ttl(key), 0)
        # The exposure is in the active set.
        self.assertIn(b"1001", self.redis.smembers(getActiveExposuresKey("LSSTCam")))

    def test_setExpectedDetectorsOverwriteAndAppend(self) -> None:
        self.helper.setExpectedDetectors("LSSTCam", 1001, detectors=[1, 2, 3], who="SFM")
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "SFM"), [1, 2, 3])

        # Overwrite (default) replaces the whole set.
        self.helper.setExpectedDetectors("LSSTCam", 1001, detectors=[10, 20], who="SFM")
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "SFM"), [10, 20])

        # Append merges with what's there.
        self.helper.setExpectedDetectors("LSSTCam", 1001, detectors=[5, 10], who="SFM", append=True)
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "SFM"), [5, 10, 20])

    def test_setExpectedDetectorsScopedByWho(self) -> None:
        # SFM and AOS detectors must not bleed across whos.
        self.helper.setExpectedDetectors("LSSTCam", 1001, detectors=[1, 2], who="SFM")
        self.helper.setExpectedDetectors("LSSTCam", 1001, detectors=[3, 4], who="AOS")
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "SFM"), [1, 2])
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "AOS"), [3, 4])

    def test_removeExpectedDetectorsDropsRequestedDetector(self) -> None:
        self.helper.setExpectedDetectors("LSSTCam", 1001, detectors=[1, 2, 3, 4], who="SFM")
        self.helper.removeExpectedDetectors("LSSTCam", 1001, detectors=[2, 3], who="SFM")
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "SFM"), [1, 4])

    def test_getExpectedDetectorsReturnsEmptyForUnknownExposure(self) -> None:
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "SFM"), [])

    def test_emptyExpectedFieldRoundTrips(self) -> None:
        # If the field is set to the empty string (because every
        # detector got removed), it must read back as an empty list,
        # not as [0] or a parse failure.
        key = getTrackingKey("LSSTCam", 1001)
        self.redis.hset(key, getTrackingExpectedField("SFM"), "")
        self.assertEqual(self.helper.getExpectedDetectors("LSSTCam", 1001, "SFM"), [])

    def test_reportDetectorFinishedAndFailed(self) -> None:
        # A failed detector is *also* recorded as finished (detector 2 below
        # appears in both sets). This is load-bearing: the gather completes
        # on the finished set, so if a failure stopped counting as finished
        # the exposure would hang forever waiting on a detector that already
        # errored out.
        self.helper.setExpectedDetectors("LSSTCam", 1001, detectors=[1, 2, 3], who="SFM")
        self.helper.reportDetectorFinished("LSSTCam", 1001, "SFM", detector=1)
        self.helper.reportDetectorFinished("LSSTCam", 1001, "SFM", detector=2, failed=True)

        info = self.helper.getExposureProcessingInfo("LSSTCam", 1001)
        self.assertIsNotNone(info)
        assert info is not None  # for mypy
        self.assertEqual(info.getFinishedDetectors("SFM"), {1, 2})
        self.assertEqual(info.getFailedDetectors("SFM"), {2})

    def test_markStep1aDispatchedRoundTrips(self) -> None:
        self.helper.markStep1aDispatched("LSSTCam", 1001, who="SFM")
        info = self.helper.getExposureProcessingInfo("LSSTCam", 1001)
        assert info is not None
        self.assertTrue(info.isStep1aDispatched("SFM"))

    def test_markStep1bDispatchedAndFinished(self) -> None:
        self.helper.markStep1bDispatched("LSSTCam", 1001, who="SFM")
        self.helper.markStep1bFinished("LSSTCam", 1001, who="SFM")
        info = self.helper.getExposureProcessingInfo("LSSTCam", 1001)
        assert info is not None
        self.assertTrue(info.isStep1bDispatched("SFM"))
        self.assertTrue(info.isStep1bFinished("SFM"))

    def test_completeExposureRemovesFromActiveSet(self) -> None:
        self.helper.initExposureTracking("LSSTCam", 1001)
        self.assertIn(1001, self.helper.getActiveExposures("LSSTCam"))

        self.helper.completeExposure("LSSTCam", 1001)
        self.assertNotIn(1001, self.helper.getActiveExposures("LSSTCam"))

        # The tracking hash itself must NOT be deleted (the docstring is
        # explicit about this — async consumers still want to read it).
        key = getTrackingKey("LSSTCam", 1001)
        self.assertGreater(self.redis.exists(key), 0)

    def test_aosPipelineConfigRoundTrip(self) -> None:
        self.helper.setAosPipelineConfig("LSSTCam", 1001, "AOS_DANISH")
        self.assertEqual(self.helper.getAosPipelineConfig("LSSTCam", 1001), "AOS_DANISH")

    def test_aosPipelineConfigMissingReturnsNone(self) -> None:
        # Helper should warn (logged, not asserted) and return None.
        self.assertIsNone(self.helper.getAosPipelineConfig("LSSTCam", 1001))

    def test_getExposureProcessingInfoReturnsNoneIfMissing(self) -> None:
        self.assertIsNone(self.helper.getExposureProcessingInfo("LSSTCam", 9999))

    def test_binnedIsrAndMosaicTracking(self) -> None:
        # The binned-ISR count is a count of *distinct* detectors (each is
        # an idempotent HSET of a per-detector field), which is what the
        # mosaic gate compares against the expected set. A regression to a
        # plain INCR would double-count re-runs and trip the gate early.
        self.helper.reportBinnedIsrProduced("LSSTCam", 1001, detector=1)
        self.helper.reportBinnedIsrProduced("LSSTCam", 1001, detector=2)
        self.assertEqual(self.helper.getNumBinnedIsrProduced("LSSTCam", 1001), 2)

        self.helper.markMosaicDispatched("LSSTCam", 1001)
        info = self.helper.getExposureProcessingInfo("LSSTCam", 1001)
        assert info is not None
        self.assertTrue(info.isMosaicDispatched())


class ButlerWatcherListTestCase(_RedisHelperTestBase):
    """pushToButlerWatcherList / checkButlerWatcherList round-trip."""

    def test_pushAndCheckRoundTrip(self) -> None:
        record = getSampleExpRecord()
        # Initially: not seen.
        self.assertFalse(self.helper.checkButlerWatcherList("LATISS", record))

        self.helper.pushToButlerWatcherList("LATISS", record)
        self.assertTrue(self.helper.checkButlerWatcherList("LATISS", record))

        # The list contains the JSON form of the record.
        rawList = self.redis.lrange(getButlerWatcherListKey("LATISS"), 0, -1)
        self.assertEqual(len(rawList), 1)


class ConsDbAnnouncementsTestCase(_RedisHelperTestBase):
    """announceResultInConsDb / waitForResultInConsDb."""

    def test_announceMakesWaitReturnTrue(self) -> None:
        self.helper.announceResultInConsDb("LSSTCam", "exposure", obsId=2024_0101_00042)
        self.assertTrue(
            self.helper.waitForResultInConsDb("LSSTCam", "exposure", obsId=2024_0101_00042, timeout=0.1)
        )

    def test_waitTimesOutWhenNotAnnounced(self) -> None:
        # Use a tiny timeout so the test doesn't itself time out.
        start = time.monotonic()
        result = self.helper.waitForResultInConsDb("LSSTCam", "exposure", obsId=999_999_999, timeout=0.2)
        elapsed = time.monotonic() - start
        self.assertFalse(result)
        # Sanity-check the timeout actually elapsed.
        self.assertGreater(elapsed, 0.15)

    def test_announcementUsesCorrectKeyAndField(self) -> None:
        obsId = 2024_0101_00042
        self.helper.announceResultInConsDb("LSSTCam", "exposure", obsId)
        dayObs = obsId // 100_000
        announcementKey = getConsDbAnnouncementKey(dayObs)
        field = getConsDbAnnouncementField("LSSTCam", "exposure", obsId)
        self.assertEqual(self.redis.hget(announcementKey, field), b"1")

    def test_announceIsIdempotentDoesNotOverwriteValue(self) -> None:
        # The implementation uses HSETNX so a second announcement
        # cannot bump the field.
        obsId = 2024_0101_00042
        self.helper.announceResultInConsDb("LSSTCam", "exposure", obsId)
        # Tamper with the value to detect a wrongful overwrite.
        dayObs = obsId // 100_000
        self.redis.hset(
            getConsDbAnnouncementKey(dayObs),
            getConsDbAnnouncementField("LSSTCam", "exposure", obsId),
            "tamper",
        )
        self.helper.announceResultInConsDb("LSSTCam", "exposure", obsId)
        self.assertEqual(
            self.redis.hget(
                getConsDbAnnouncementKey(dayObs),
                getConsDbAnnouncementField("LSSTCam", "exposure", obsId),
            ),
            b"tamper",
        )


class IgnoredDetectorsTestCase(_RedisHelperTestBase):
    """The ignored-detector list is the head node's way of telling
    workers "skip these chip IDs" mid-night. Contract: empty-by-
    default (so a fresh start ignores nothing), set overwrites prior
    list (not append, since the operator's latest call is the
    authoritative one)."""

    def test_setAndGetIgnoredDetectors(self) -> None:
        self.helper.setDetectorsIgnoredByHeadNode("LSSTCam", [1, 2, 99])
        self.assertEqual(self.helper.getDetectorsIgnoredByHeadNode("LSSTCam"), [1, 2, 99])

    def test_getIgnoredDetectorsEmptyByDefault(self) -> None:
        self.assertEqual(self.helper.getDetectorsIgnoredByHeadNode("LSSTCam"), [])

    def test_setOverwritesPriorList(self) -> None:
        self.helper.setDetectorsIgnoredByHeadNode("LSSTCam", [1, 2])
        self.helper.setDetectorsIgnoredByHeadNode("LSSTCam", [9])
        self.assertEqual(self.helper.getDetectorsIgnoredByHeadNode("LSSTCam"), [9])


class MtaosZernikeTestCase(_RedisHelperTestBase):
    """Zernike-count handoff to MTAOS — a small but contract-bearing
    pair: ``None`` on miss (not 0, since 0 is a meaningful Zernike
    count from the MTAOS side) and exact round-trip on set."""

    def test_sendAndGetZernikeCount(self) -> None:
        self.helper.sendZernikeCountToMTAOS("LSSTCam", visitId=1001, zernikeCount=42)
        self.assertEqual(self.helper.getMTAOSZernikeCount("LSSTCam", 1001), 42)

    def test_getZernikeCountReturnsNoneIfUnset(self) -> None:
        self.assertIsNone(self.helper.getMTAOSZernikeCount("LSSTCam", 9999))


class VisitSummaryStatsTestCase(_RedisHelperTestBase):
    """reportVisitSummaryStats / getAllVisitSummaryStats /
    getAveragedStatsForVisit. Bypass reportVisitSummaryStats here
    because its `summaryStatsToDict` argument is an
    `lsst.afw.image.ExposureSummaryStats`, which is awkward to
    fabricate; instead populate the hash directly and exercise the
    read path, which is where the NaN-handling lives.
    """

    def _writeStats(self, visit: int, detector: int, stats: dict) -> None:
        self.redis.hset(getVisitSummaryStatsKey("LSSTCam", visit), str(detector), json.dumps(stats))

    def test_getAllVisitSummaryStatsReturnsEmptyByDefault(self) -> None:
        self.assertEqual(self.helper.getAllVisitSummaryStats("LSSTCam", 1001), {})

    def test_getAllVisitSummaryStatsRoundTrip(self) -> None:
        self._writeStats(1001, 0, {"psf": 1.5})
        self._writeStats(1001, 1, {"psf": 2.0})
        result = self.helper.getAllVisitSummaryStats("LSSTCam", 1001)
        self.assertEqual(result, {0: {"psf": 1.5}, 1: {"psf": 2.0}})

    def test_getAveragedStatsForVisitReturnsEmptyDictIfMissing(self) -> None:
        self.assertEqual(self.helper.getAveragedStatsForVisit("LSSTCam", 9999), {})

    def test_getAveragedStatsMediansNumericFields(self) -> None:
        self._writeStats(1001, 0, {"psf": 1.0, "ra": 0.5})
        self._writeStats(1001, 1, {"psf": 2.0, "ra": 1.0})
        self._writeStats(1001, 2, {"psf": 3.0, "ra": 1.5})
        averaged = self.helper.getAveragedStatsForVisit("LSSTCam", 1001)
        self.assertAlmostEqual(averaged["psf"], 2.0)
        self.assertAlmostEqual(averaged["ra"], 1.0)

    def test_getAveragedStatsSkipsNonNumeric(self) -> None:
        # raCorners / decCorners are lists in real data — they must be
        # silently skipped rather than crashing the average.
        self._writeStats(1001, 0, {"psf": 1.0, "raCorners": [0.0, 0.1]})
        self._writeStats(1001, 1, {"psf": 2.0, "raCorners": [0.0, 0.1]})
        averaged = self.helper.getAveragedStatsForVisit("LSSTCam", 1001)
        self.assertAlmostEqual(averaged["psf"], 1.5)
        self.assertNotIn("raCorners", averaged)

    def test_getAveragedStatsHandlesNanWithNanmedian(self) -> None:
        # nanmedian is documented to ignore NaN values rather than
        # propagating them.
        self._writeStats(1001, 0, {"psf": float("nan")})
        self._writeStats(1001, 1, {"psf": 4.0})
        self._writeStats(1001, 2, {"psf": 6.0})
        averaged = self.helper.getAveragedStatsForVisit("LSSTCam", 1001)
        self.assertAlmostEqual(averaged["psf"], 5.0)


class WitnessDetectorTestCase(_RedisHelperTestBase):
    """The hard-coded short-circuit branches don't need Redis at all,
    only the LSSTCam path does."""

    def test_latissReturnsZero(self) -> None:
        self.assertEqual(self.helper.getWitnessDetectorNumber("LATISS"), 0)

    def test_comCamReturnsFour(self) -> None:
        self.assertEqual(self.helper.getWitnessDetectorNumber("LSSTComCam"), 4)
        self.assertEqual(self.helper.getWitnessDetectorNumber("LSSTComCamSim"), 4)
        self.assertEqual(self.helper.getWitnessDetectorNumber("LSST-TS8"), 4)

    def test_unknownInstrumentRaises(self) -> None:
        with self.assertRaises(ValueError):
            self.helper.getWitnessDetectorNumber("NotARealCamera")

    def test_lsstCamRequiresCamera(self) -> None:
        with self.assertRaises(ValueError):
            self.helper.getWitnessDetectorNumber("LSSTCam")


class ClearTaskCountersTestCase(_RedisHelperTestBase):
    """clearTaskCounters wipes all *EDCOUNTER, *TRACKING* and
    *ACTIVE-EXPOSURES* keys."""

    def test_clearsTrackingActiveAndCounterKeys(self) -> None:
        # Plant one of each kind of key the function targets.
        self.redis.hset("LSSTCam-FINISHEDCOUNTER-step1", "f", "1")
        self.redis.hset("LSSTCam-FAILEDCOUNTER-step1", "f", "1")
        self.redis.hset(getTrackingKey("LSSTCam", 1001), TRACKING_INITIALIZED_FIELD, "1")
        self.redis.sadd(getActiveExposuresKey("LSSTCam"), "1001")
        # And one unrelated key that must survive.
        self.redis.set(WITNESS_DETECTOR_KEY, "94")

        self.helper.clearTaskCounters()

        self.assertEqual(self.redis.exists("LSSTCam-FINISHEDCOUNTER-step1"), 0)
        self.assertEqual(self.redis.exists("LSSTCam-FAILEDCOUNTER-step1"), 0)
        self.assertEqual(self.redis.exists(getTrackingKey("LSSTCam", 1001)), 0)
        self.assertEqual(self.redis.exists(getActiveExposuresKey("LSSTCam")), 0)
        # Unrelated key untouched.
        self.assertEqual(self.redis.get(WITNESS_DETECTOR_KEY), b"94")


class SmokeIsHeadNodeTestCase(_RedisHelperTestBase):
    """getExposureForFanout requires isHeadNode=True; this base sets
    that. Just pin the assertion that worker-mode helpers refuse to
    pop from the head-node queue."""

    def test_workerModeHelperRefusesFanoutPop(self) -> None:
        self.helper.isHeadNode = False
        with self.assertRaises(RuntimeError):
            self.helper.getExposureForFanout("LSSTCam")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
