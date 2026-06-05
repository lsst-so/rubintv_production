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

"""Unit tests for `ExposureProcessingInfo.fromRedisHash`.

The dataclass is a pure parser over the per-exposure tracking hash,
so it can be exercised end-to-end without Redis, a Butler, or any
other runtime dependency — just feed it a dict of decoded field
names and values like ``RedisHelper.getExposureProcessingInfo`` does.
"""

import unittest

import lsst.utils.tests
from lsst.rubintv.production.redisUtils import ExposureProcessingInfo


class FromRedisHashTestCase(lsst.utils.tests.TestCase):
    """Parsing correctness for the tracking-hash fields.

    The threat these catch is drift between the field-name format that
    ``RedisHelper`` *writes* (``SFM:expected``, ``SFM:finished:1``,
    ``_binnedIsr:1``, ``_mosaicDispatched``, ``pipeline_config``) and what
    this parser *reads*. If a writer changes a delimiter or prefix without
    the parser following, the fields silently stop being recognised and an
    exposure never registers as complete — a failure that only the CI
    integration suite would otherwise surface. Each test pins one field
    shape against that drift."""

    def test_parsesWhoKeyedFields(self) -> None:
        fields = {
            "_initialized": "1",
            "SFM:expected": "1,2,3",
            "SFM:finished:1": "1",
            "SFM:finished:2": "1",
            "SFM:failed:3": "1",
            "SFM:step1aDispatched": "1",
        }
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertEqual(info.expId, 42)
        self.assertEqual(info.getExpectedDetectors("SFM"), {1, 2, 3})
        self.assertEqual(info.getFinishedDetectors("SFM"), {1, 2})
        self.assertEqual(info.getFailedDetectors("SFM"), {3})
        self.assertTrue(info.isStep1aDispatched("SFM"))

    def test_parsesBinnedIsrAndMosaicFields(self) -> None:
        fields = {
            "_initialized": "1",
            "_binnedIsr:1": "1",
            "_binnedIsr:42": "1",
            "_mosaicDispatched": "1",
        }
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertEqual(info.getBinnedIsrProduced(), {1, 42})
        self.assertTrue(info.isMosaicDispatched())

    def test_missingBinnedIsrAndMosaicFieldsDefault(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(42, {"_initialized": "1"})
        self.assertEqual(info.getBinnedIsrProduced(), set())
        self.assertFalse(info.isMosaicDispatched())

    def test_getAllExpectedDetectorsUnionsAcrossWhos(self) -> None:
        fields = {
            "SFM:expected": "1,2,3",
            "AOS:expected": "3,4,5",
        }
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertEqual(info.getAllExpectedDetectors(), {1, 2, 3, 4, 5})


class AllGathersDispatchedTestCase(lsst.utils.tests.TestCase):
    """The mosaic gate in ``allGathersDispatched``.

    Keeping the exposure in the active set until the post-ISR mosaic
    has been dispatched is what makes it safe for the head node to run
    `dispatchPostIsrMosaic` alongside (rather than inside) the per-who
    gather loop.
    """

    def test_returnsTrueWhenNoExpectedDetectors(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(42, {})
        self.assertTrue(info.allGathersDispatched())

    def test_blocksOnUndispatchedWho(self) -> None:
        fields = {"SFM:expected": "1,2,3"}
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertFalse(info.allGathersDispatched())

    def test_passesWhenAllWhosDispatchedAndNoBinnedIsr(self) -> None:
        fields = {
            "SFM:expected": "1,2,3",
            "SFM:step1aDispatched": "1",
            "AOS:expected": "4,5",
            "AOS:step1aDispatched": "1",
        }
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertTrue(info.allGathersDispatched())

    def test_blocksOnUndispatchedMosaicWhenBinnedIsrExists(self) -> None:
        # All whos dispatched, binned ISR has started landing — the
        # mosaic gate must hold the exposure open until the mosaic is
        # dispatched. This is exactly the case the bug fix targets.
        fields = {
            "SFM:expected": "1,2",
            "SFM:step1aDispatched": "1",
            "_binnedIsr:1": "1",
            "_binnedIsr:2": "1",
        }
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertFalse(info.allGathersDispatched())

    def test_passesOnceMosaicIsDispatched(self) -> None:
        fields = {
            "SFM:expected": "1,2",
            "SFM:step1aDispatched": "1",
            "_binnedIsr:1": "1",
            "_binnedIsr:2": "1",
            "_mosaicDispatched": "1",
        }
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertTrue(info.allGathersDispatched())


class IsCompleteTestCase(lsst.utils.tests.TestCase):
    """``isComplete`` is the gating predicate used to know when a who's
    detector set has all landed."""

    def test_falseWhenNoExpected(self) -> None:
        # No expected detectors registered for the who -> can never be
        # "complete"; this is the documented contract.
        info = ExposureProcessingInfo.fromRedisHash(42, {})
        self.assertFalse(info.isComplete("SFM"))

    def test_falseWhenSomeMissing(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(42, {"SFM:expected": "1,2,3", "SFM:finished:1": "1"})
        self.assertFalse(info.isComplete("SFM"))

    def test_trueWhenAllFinished(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(
            42,
            {
                "SFM:expected": "1,2,3",
                "SFM:finished:1": "1",
                "SFM:finished:2": "1",
                "SFM:finished:3": "1",
            },
        )
        self.assertTrue(info.isComplete("SFM"))

    def test_trueWhenFinishedSupersetOfExpected(self) -> None:
        # finished >= expected is the test, so an extra finished
        # detector (e.g. one that snuck through after a head-node
        # rerun) doesn't break the predicate.
        info = ExposureProcessingInfo.fromRedisHash(
            42,
            {
                "SFM:expected": "1,2",
                "SFM:finished:1": "1",
                "SFM:finished:2": "1",
                "SFM:finished:99": "1",
            },
        )
        self.assertTrue(info.isComplete("SFM"))


class GetMissingDetectorsTestCase(lsst.utils.tests.TestCase):
    """``getMissingDetectors`` = expected minus finished, per who.

    This is what the head node logs (and could re-dispatch on) when an
    exposure stalls, so the contract pinned here is: unknown who yields the
    empty set (not a KeyError), a partial run yields exactly the detectors
    still outstanding, and a complete run yields the empty set."""

    def test_emptyForUnknownWho(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(42, {})
        self.assertEqual(info.getMissingDetectors("SFM"), set())

    def test_returnsExpectedMinusFinished(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(
            42,
            {
                "SFM:expected": "1,2,3,4",
                "SFM:finished:1": "1",
                "SFM:finished:3": "1",
            },
        )
        self.assertEqual(info.getMissingDetectors("SFM"), {2, 4})

    def test_emptyWhenAllFinished(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(
            42,
            {
                "SFM:expected": "1,2",
                "SFM:finished:1": "1",
                "SFM:finished:2": "1",
            },
        )
        self.assertEqual(info.getMissingDetectors("SFM"), set())


class Step1bAndPipelineConfigParsingTestCase(lsst.utils.tests.TestCase):
    """Parsing of the step1b flags, the pipeline-config field, and the
    parser's defensive handling of empty/junk fields.

    step1bDispatched and step1bFinished are independent flags scoped per
    who; pipelineConfig carries the AOS recipe name through to logging and
    downstream config. The empty-string and junk-field cases pin that the
    parser degrades gracefully (empty set / skip) rather than raising, since
    it runs over whatever happens to be in the hash."""

    def test_step1bDispatchedAndFinishedFlags(self) -> None:
        fields = {
            "SFM:step1bDispatched": "1",
            "SFM:step1bFinished": "1",
        }
        info = ExposureProcessingInfo.fromRedisHash(42, fields)
        self.assertTrue(info.isStep1bDispatched("SFM"))
        self.assertTrue(info.isStep1bFinished("SFM"))
        # Distinct who is unaffected.
        self.assertFalse(info.isStep1bDispatched("AOS"))

    def test_pipelineConfigParsed(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(42, {"pipeline_config": "AOS_DANISH"})
        self.assertEqual(info.pipelineConfig, "AOS_DANISH")

    def test_emptyExpectedFieldRoundTripsToEmptySet(self) -> None:
        # The wire format has been observed to use an empty string when
        # all expected detectors have been removed; that has to parse
        # back to an empty set rather than an unparseable error.
        info = ExposureProcessingInfo.fromRedisHash(42, {"SFM:expected": ""})
        self.assertEqual(info.getExpectedDetectors("SFM"), set())

    def test_unparseableFieldsAreIgnored(self) -> None:
        # The parser is defensive — junk fields (lower-case who, no
        # delimiter, etc.) are skipped. Pinned so changes to the regex
        # don't silently start picking them up.
        info = ExposureProcessingInfo.fromRedisHash(
            42,
            {
                "garbage": "1",
                "lowercase:expected": "1,2",
                "SFM:expected": "1,2",
            },
        )
        self.assertEqual(info.getExpectedDetectors("SFM"), {1, 2})
        self.assertEqual(info.getExpectedDetectors("lowercase"), set())


class ReprAndStrTestCase(lsst.utils.tests.TestCase):
    """Smoke tests on __repr__ and __str__ — they're consumed by the
    head-node logging, so a None-attribute regression must not silently
    break logs."""

    def _info(self) -> ExposureProcessingInfo:
        fields = {
            "SFM:expected": "1,2,3",
            "SFM:finished:1": "1",
            "SFM:failed:2": "1",
            "SFM:step1aDispatched": "1",
            "_binnedIsr:1": "1",
            "pipeline_config": "AOS_DANISH",
        }
        return ExposureProcessingInfo.fromRedisHash(42, fields)

    def test_reprMentionsExpId(self) -> None:
        text = repr(self._info())
        self.assertIn("ExposureProcessingInfo", text)
        self.assertIn("42", text)
        self.assertIn("SFM", text)

    def test_strMentionsExpIdAndStatus(self) -> None:
        text = str(self._info())
        self.assertIn("Exposure 42", text)
        self.assertIn("SFM", text)
        # Pipeline config is included.
        self.assertIn("AOS_DANISH", text)
        # The "in progress" status (only one of three detectors finished).
        self.assertIn("in progress", text)

    def test_strReportsCompleteWhenAllFinished(self) -> None:
        info = ExposureProcessingInfo.fromRedisHash(
            42,
            {
                "SFM:expected": "1,2",
                "SFM:finished:1": "1",
                "SFM:finished:2": "1",
            },
        )
        self.assertIn("complete", str(info))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
