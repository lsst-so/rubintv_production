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
    """Parsing correctness for the tracking-hash fields."""

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


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
