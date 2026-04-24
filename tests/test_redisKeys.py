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

"""Test cases for redisKeys.

These tests pin every key shape that the package writes to or reads from
Redis. Because the keys are persisted across restarts and consumed by
multiple pods, drifting any of them silently in a refactor would break
in-flight processing — so the tests are exhaustive enough that any
accidental change to the format will fail loudly.
"""

import unittest

import lsst.utils.tests
from lsst.rubintv.production import redisKeys
from lsst.rubintv.production.redisKeys import (
    QUEUE_LENGTHS_KEY,
    TRACKING_BINNED_ISR_PREFIX,
    TRACKING_INITIALIZED_FIELD,
    TRACKING_MOSAIC_DISPATCHED_FIELD,
    TRACKING_PIPELINE_CONFIG_FIELD,
    WITNESS_DETECTOR_KEY,
    getActiveExposuresKey,
    getButlerWatcherListKey,
    getConsDbAnnouncementField,
    getConsDbAnnouncementKey,
    getIgnoredDetectorsKey,
    getMtaosZernikeResultKey,
    getNewDataQueueName,
    getNightlyRollupFinishedKey,
    getPodBusyKey,
    getPodExistsKey,
    getPodRunningKey,
    getPodSecondaryStatusKey,
    getTaskFailedCounterKey,
    getTaskFinishedCounterKey,
    getTrackingBinnedIsrField,
    getTrackingExpectedField,
    getTrackingFailedField,
    getTrackingFinishedField,
    getTrackingKey,
    getTrackingStep1aDispatchedField,
    getTrackingStep1bDispatchedField,
    getTrackingStep1bFinishedField,
    getVisitFailedCounterKey,
    getVisitFinishedCounterKey,
    getVisitSummaryStatsKey,
)


class ConstantsTestCase(lsst.utils.tests.TestCase):
    """Pin the literal value of every constant exposed by `redisKeys`."""

    def test_queueLengthsKey(self) -> None:
        self.assertEqual(QUEUE_LENGTHS_KEY, "_QUEUE-LENGTHS")

    def test_witnessDetectorKey(self) -> None:
        self.assertEqual(WITNESS_DETECTOR_KEY, "RUBINTV_CONTROL_WITNESS_DETECTOR")

    def test_trackingInitializedField(self) -> None:
        self.assertEqual(TRACKING_INITIALIZED_FIELD, "_initialized")

    def test_trackingPipelineConfigField(self) -> None:
        self.assertEqual(TRACKING_PIPELINE_CONFIG_FIELD, "pipeline_config")

    def test_trackingMosaicDispatchedField(self) -> None:
        self.assertEqual(TRACKING_MOSAIC_DISPATCHED_FIELD, "_mosaicDispatched")

    def test_trackingBinnedIsrPrefix(self) -> None:
        self.assertEqual(TRACKING_BINNED_ISR_PREFIX, "_binnedIsr:")

    def test_allListedInDunderAll(self) -> None:
        # Every public symbol the module exposes should be in __all__ so
        # `from redisKeys import *` is well-defined.
        for name in (
            "QUEUE_LENGTHS_KEY",
            "WITNESS_DETECTOR_KEY",
            "TRACKING_INITIALIZED_FIELD",
            "TRACKING_PIPELINE_CONFIG_FIELD",
        ):
            self.assertIn(name, redisKeys.__all__)


class PerInstrumentKeysTestCase(lsst.utils.tests.TestCase):
    """Pin the per-instrument top-level key shapes."""

    def test_getNewDataQueueName(self) -> None:
        self.assertEqual(getNewDataQueueName("LSSTCam"), "INCOMING-LSSTCam-raw")
        self.assertEqual(getNewDataQueueName("LATISS"), "INCOMING-LATISS-raw")

    def test_getButlerWatcherListKeyPreservesTypo(self) -> None:
        # The persisted key contains the historical typo "Wacher" (missing
        # the 't'). The helper must reproduce it exactly until a coordinated
        # migration is done.
        self.assertEqual(getButlerWatcherListKey("LSSTCam"), "LSSTCam-fromButlerWacher")
        self.assertEqual(getButlerWatcherListKey("LATISS"), "LATISS-fromButlerWacher")

    def test_getTaskFinishedCounterKey(self) -> None:
        self.assertEqual(
            getTaskFinishedCounterKey("LSSTCam", "isr"),
            "LSSTCam-isr-FINISHEDCOUNTER",
        )

    def test_getTaskFailedCounterKey(self) -> None:
        self.assertEqual(
            getTaskFailedCounterKey("LSSTCam", "isr"),
            "LSSTCam-isr-FAILEDCOUNTER",
        )

    def test_taskCounterPairAreReplaceCompatible(self) -> None:
        # Several callers in redisUtils derive the failed counter from the
        # finished counter via str.replace("FINISHEDCOUNTER", "FAILEDCOUNTER").
        # The two helpers must continue to honour that round-trip.
        finished = getTaskFinishedCounterKey("LSSTCam", "isr")
        failed = getTaskFailedCounterKey("LSSTCam", "isr")
        self.assertEqual(finished.replace("FINISHEDCOUNTER", "FAILEDCOUNTER"), failed)

    def test_getVisitFinishedCounterKeyPreservesTypo(self) -> None:
        # Pin the historical "FINISIHED" typo (extra I) — persisted in Redis.
        self.assertEqual(
            getVisitFinishedCounterKey("LSSTCam", "step1b", "SFM"),
            "LSSTCam-step1b-SFM-VISIT_FINISIHED_COUNTER",
        )

    def test_getVisitFailedCounterKey(self) -> None:
        self.assertEqual(
            getVisitFailedCounterKey("LSSTCam", "step1b", "SFM"),
            "LSSTCam-step1b-SFM-VISIT_FAILED_COUNTER",
        )

    def test_visitCounterPairAreReplaceCompatible(self) -> None:
        # Same replace-trick as for the task counters.
        finished = getVisitFinishedCounterKey("LSSTCam", "step1b", "SFM")
        failed = getVisitFailedCounterKey("LSSTCam", "step1b", "SFM")
        self.assertEqual(
            finished.replace("VISIT_FINISIHED_COUNTER", "VISIT_FAILED_COUNTER"),
            failed,
        )

    def test_getNightlyRollupFinishedKey(self) -> None:
        self.assertEqual(
            getNightlyRollupFinishedKey("LSSTCam", "SFM"),
            "LSSTCam-SFM-NIGHTLYROLLUP-FINISHEDCOUNTER",
        )

    def test_getTrackingKey(self) -> None:
        self.assertEqual(getTrackingKey("LSSTCam", 12345), "LSSTCam-TRACKING-12345")
        self.assertEqual(getTrackingKey("LATISS", 0), "LATISS-TRACKING-0")

    def test_getActiveExposuresKey(self) -> None:
        self.assertEqual(getActiveExposuresKey("LSSTCam"), "LSSTCam-ACTIVE-EXPOSURES")

    def test_getIgnoredDetectorsKey(self) -> None:
        self.assertEqual(
            getIgnoredDetectorsKey("LSSTCam"),
            "LSSTCam-HEADNODE-IGNORED_DETECTORS",
        )

    def test_getVisitSummaryStatsKey(self) -> None:
        self.assertEqual(
            getVisitSummaryStatsKey("LSSTCam", 2026040100123),
            "LSSTCam-VISIT_SUMMARY_STATS-2026040100123",
        )

    def test_getMtaosZernikeResultKeyUppercases(self) -> None:
        # MTAOS uses upper-cased instrument names. The helper must apply
        # `.upper()` regardless of the case of the input.
        self.assertEqual(
            getMtaosZernikeResultKey("LSSTCam"),
            "LSSTCAM_WEP_PROCESSING_RESULT",
        )
        self.assertEqual(
            getMtaosZernikeResultKey("lsstcam"),
            "LSSTCAM_WEP_PROCESSING_RESULT",
        )
        self.assertEqual(
            getMtaosZernikeResultKey("LATISS"),
            "LATISS_WEP_PROCESSING_RESULT",
        )


class PerPodKeysTestCase(lsst.utils.tests.TestCase):
    """Pin the per-pod key shapes built from a queue name."""

    QUEUE = "SFM_WORKER-LSSTCam-000-094"

    def test_getPodRunningKey(self) -> None:
        self.assertEqual(getPodRunningKey(self.QUEUE), f"{self.QUEUE}+IS_RUNNING")

    def test_getPodBusyKey(self) -> None:
        self.assertEqual(getPodBusyKey(self.QUEUE), f"{self.QUEUE}+IS_BUSY")

    def test_getPodExistsKey(self) -> None:
        self.assertEqual(getPodExistsKey(self.QUEUE), f"{self.QUEUE}+EXISTS")

    def test_getPodSecondaryStatusKey(self) -> None:
        self.assertEqual(
            getPodSecondaryStatusKey(self.QUEUE),
            f"{self.QUEUE}+SECONDARY_STATUS",
        )

    def test_helpersAcceptGlobPatterns(self) -> None:
        # The per-pod helpers take a string rather than a PodDetails so that
        # `getAllWorkers` can build a glob over multiple pods. Pinning this
        # is what allows that call site to migrate to the helpers.
        glob = "SFM_WORKER-LSSTCam-*-*"
        self.assertEqual(getPodExistsKey(glob), "SFM_WORKER-LSSTCam-*-*+EXISTS")
        self.assertEqual(getPodBusyKey(glob), "SFM_WORKER-LSSTCam-*-*+IS_BUSY")

    def test_suffixesAreUniqueAcrossPodKeys(self) -> None:
        # The four per-pod suffixes must remain distinct so that
        # `redis.keys(<glob>+<suffix>)` does not return multiple key types.
        suffixes = {
            getPodRunningKey("X").removeprefix("X"),
            getPodBusyKey("X").removeprefix("X"),
            getPodExistsKey("X").removeprefix("X"),
            getPodSecondaryStatusKey("X").removeprefix("X"),
        }
        self.assertEqual(len(suffixes), 4)


class ConsDbAnnouncementKeysTestCase(lsst.utils.tests.TestCase):
    """Pin the consDB announcement key and hash field shapes."""

    def test_getConsDbAnnouncementKey(self) -> None:
        self.assertEqual(getConsDbAnnouncementKey(20260410), "consdb-announcements-20260410")

    def test_getConsDbAnnouncementFieldLowercases(self) -> None:
        # The hash field is lower-cased so two callers writing the same
        # logical announcement with different case agree on the field name.
        self.assertEqual(
            getConsDbAnnouncementField("LSSTCam", "ccdvisit1_quicklook", 12345),
            "lsstcam-ccdvisit1_quicklook-12345",
        )
        self.assertEqual(
            getConsDbAnnouncementField("LSSTCAM", "CCDVisit1_QuickLook", 12345),
            "lsstcam-ccdvisit1_quicklook-12345",
        )


class TrackingHashFieldsTestCase(lsst.utils.tests.TestCase):
    """Pin the per-exposure tracking-hash field names.

    The fields below are parsed back out by
    `ExposureProcessingInfo.fromRedisHash` via a regex pinned to this exact
    format. Drifting them silently would silently break the parser, so they
    are pinned individually plus a round-trip-style assertion.
    """

    def test_expectedField(self) -> None:
        self.assertEqual(getTrackingExpectedField("SFM"), "SFM:expected")
        self.assertEqual(getTrackingExpectedField("AOS"), "AOS:expected")

    def test_finishedField(self) -> None:
        self.assertEqual(getTrackingFinishedField("SFM", 94), "SFM:finished:94")
        self.assertEqual(getTrackingFinishedField("AOS", 0), "AOS:finished:0")

    def test_failedField(self) -> None:
        self.assertEqual(getTrackingFailedField("SFM", 42), "SFM:failed:42")

    def test_step1aDispatchedField(self) -> None:
        self.assertEqual(getTrackingStep1aDispatchedField("SFM"), "SFM:step1aDispatched")

    def test_step1bDispatchedField(self) -> None:
        self.assertEqual(getTrackingStep1bDispatchedField("SFM"), "SFM:step1bDispatched")

    def test_step1bFinishedField(self) -> None:
        self.assertEqual(getTrackingStep1bFinishedField("SFM"), "SFM:step1bFinished")

    def test_binnedIsrField(self) -> None:
        self.assertEqual(getTrackingBinnedIsrField(94), "_binnedIsr:94")
        self.assertEqual(getTrackingBinnedIsrField(0), "_binnedIsr:0")

    def test_binnedIsrAndMosaicFieldsAreNotWhoKeyed(self) -> None:
        # These fields are deliberately *not* matched by the per-who
        # parser regex — they're pipeline-agnostic. If this assertion
        # starts failing, the parser in
        # `ExposureProcessingInfo.fromRedisHash` must route them via
        # the regex; today it handles them by explicit prefix/equality
        # checks before running the regex.
        import re

        pattern = re.compile(r"^([A-Z_]+):(\w+?)(?::(\d+))?$")
        self.assertIsNone(pattern.match(getTrackingBinnedIsrField(42)))
        self.assertIsNone(pattern.match(TRACKING_MOSAIC_DISPATCHED_FIELD))

    def test_fieldsAreParseableByExistingRegex(self) -> None:
        # `ExposureProcessingInfo.fromRedisHash` parses these field names
        # using `_TRACKING_FIELD_RE`, currently
        #     ^([A-Z_]+):(\w+?)(?::(\d+))?$
        # All field-name helpers must produce strings that match this
        # regex so the parser keeps working.
        #
        # The pattern is intentionally *hard-coded here* rather than
        # imported from redisUtils. Importing the live symbol would make
        # this test tautological: a future change that loosens the parser
        # regex and drifts a helper in a matching way would pass silently,
        # because both sides of the assertion moved together. Pinning the
        # pattern text turns this into a real contract — edits to the
        # parser regex fail this test and force a conscious decision about
        # whether the helpers (and all persisted Redis fields consumers
        # have already written) are still compatible. If the parser regex
        # genuinely needs to change, update the copy below to match.
        import re

        pattern = re.compile(r"^([A-Z_]+):(\w+?)(?::(\d+))?$")
        cases = [
            getTrackingExpectedField("SFM"),
            getTrackingFinishedField("SFM", 94),
            getTrackingFailedField("SFM", 42),
            getTrackingStep1aDispatchedField("SFM"),
            getTrackingStep1bDispatchedField("SFM"),
            getTrackingStep1bFinishedField("SFM"),
        ]
        for field in cases:
            self.assertIsNotNone(pattern.match(field), f"{field!r} does not match the parser regex")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
