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

"""Test cases for consdbUtils."""

import threading
import unittest
from types import SimpleNamespace
from typing import Any, cast

import numpy as np

import lsst.utils.tests
from lsst.afw.image import ExposureSummaryStats
from lsst.daf.butler import DimensionRecord
from lsst.rubintv.production.consdbUtils import (
    CCD_VISIT_MAPPING,
    VISIT_MIN_MED_MAX_MAPPING,
    VISIT_MIN_MED_MAX_TOTAL_MAPPING,
    ConsDBPopulator,
    _removeNans,
    changeType,
)
from lsst.rubintv.production.locationConfig import LocationConfig
from lsst.rubintv.production.redisUtils import RedisHelper
from lsst.summit.utils.consdbClient import ConsDbClient
from lsst.summit.utils.packageVersions import (
    PACKAGE_VERSIONS_COLUMN,
    PACKAGE_VERSIONS_TABLE,
    PackageVersions,
)


class MappingShapeTestCase(lsst.utils.tests.TestCase):
    """Schema-shape tests for the consDB column mappings.

    These mappings describe how Python attribute names on ExposureSummaryStats
    are translated to consDB column names. The shape tests catch the most
    common ways for them to drift:

    - a key that is not actually present on `ExposureSummaryStats` (e.g. after
      a rename in afw)
    - a non-string key or value
    - duplicated source keys or duplicated destination columns
    - a typo where the same camelCase column is mapped to two different
      snake_case names in two different mappings
    """

    def test_ccdVisitMappingKeysExistOnExposureSummaryStats(self) -> None:
        stats = ExposureSummaryStats()
        for camelKey in CCD_VISIT_MAPPING:
            self.assertTrue(
                hasattr(stats, camelKey),
                f"CCD_VISIT_MAPPING key {camelKey!r} is not an attribute on "
                f"ExposureSummaryStats — has the field been renamed?",
            )

    def test_visitMinMedMaxMappingKeysExistOnExposureSummaryStats(self) -> None:
        stats = ExposureSummaryStats()
        for camelKey in VISIT_MIN_MED_MAX_MAPPING:
            self.assertTrue(
                hasattr(stats, camelKey),
                f"VISIT_MIN_MED_MAX_MAPPING key {camelKey!r} is not an "
                f"attribute on ExposureSummaryStats — has the field been "
                f"renamed?",
            )

    def test_visitMinMedMaxTotalMappingKeysExistOnExposureSummaryStats(self) -> None:
        stats = ExposureSummaryStats()
        for camelKey in VISIT_MIN_MED_MAX_TOTAL_MAPPING:
            self.assertTrue(
                hasattr(stats, camelKey),
                f"VISIT_MIN_MED_MAX_TOTAL_MAPPING key {camelKey!r} is not an "
                f"attribute on ExposureSummaryStats — has the field been "
                f"renamed?",
            )

    def test_allKeysAndValuesAreStrings(self) -> None:
        for name, mapping in (
            ("CCD_VISIT_MAPPING", CCD_VISIT_MAPPING),
            ("VISIT_MIN_MED_MAX_MAPPING", VISIT_MIN_MED_MAX_MAPPING),
            ("VISIT_MIN_MED_MAX_TOTAL_MAPPING", VISIT_MIN_MED_MAX_TOTAL_MAPPING),
        ):
            for k, v in mapping.items():
                self.assertIsInstance(k, str, f"{name}: key {k!r} is not a str")
                self.assertIsInstance(v, str, f"{name}: value {v!r} is not a str")

    def test_destinationColumnsUnique(self) -> None:
        # If the same consDB column shows up twice as a destination in a
        # single mapping, two different summary-stats values would clobber
        # each other on insert. Within a single mapping all destination
        # columns must be unique.
        for name, mapping in (
            ("CCD_VISIT_MAPPING", CCD_VISIT_MAPPING),
            ("VISIT_MIN_MED_MAX_MAPPING", VISIT_MIN_MED_MAX_MAPPING),
            ("VISIT_MIN_MED_MAX_TOTAL_MAPPING", VISIT_MIN_MED_MAX_TOTAL_MAPPING),
        ):
            values = list(mapping.values())
            self.assertEqual(
                len(values),
                len(set(values)),
                f"{name} has duplicate destination columns: {values}",
            )

    def test_overlappingKeysAgreeOnDestination(self) -> None:
        # The CCD_VISIT_MAPPING and VISIT_MIN_MED_MAX_MAPPING share most of
        # their source keys (one is per-detector, the other is per-visit
        # rolled up). Where a key appears in both, it must map to the same
        # consDB column name — otherwise we are silently producing two
        # column names for the same logical quantity.
        sharedKeys = set(CCD_VISIT_MAPPING) & set(VISIT_MIN_MED_MAX_MAPPING)
        self.assertTrue(sharedKeys, "Expected overlap between the two mappings")
        for key in sharedKeys:
            self.assertEqual(
                CCD_VISIT_MAPPING[key],
                VISIT_MIN_MED_MAX_MAPPING[key],
                f"Key {key!r} maps to different consDB columns in the two "
                f"mappings: CCD={CCD_VISIT_MAPPING[key]!r} vs "
                f"VISIT={VISIT_MIN_MED_MAX_MAPPING[key]!r}",
            )

    def test_visitMinMedMaxTotalIsSubsetOfMinMedMax(self) -> None:
        # The TOTAL mapping is for columns that *also* want a total computed
        # alongside the min/median/max — every key in it must therefore also
        # be present in the regular min/med/max mapping.
        for key in VISIT_MIN_MED_MAX_TOTAL_MAPPING:
            self.assertIn(
                key,
                VISIT_MIN_MED_MAX_MAPPING,
                f"VISIT_MIN_MED_MAX_TOTAL_MAPPING key {key!r} is not in " f"VISIT_MIN_MED_MAX_MAPPING",
            )
            self.assertEqual(
                VISIT_MIN_MED_MAX_TOTAL_MAPPING[key],
                VISIT_MIN_MED_MAX_MAPPING[key],
                f"Key {key!r} maps to different columns in the TOTAL mapping "
                f"vs the regular min/med/max mapping",
            )


class RemoveNansTestCase(lsst.utils.tests.TestCase):
    """Tests for the private `_removeNans` helper."""

    def test_dropsPlainFloatNan(self) -> None:
        out = _removeNans({"a": 1.0, "b": float("nan"), "c": 2.0})
        self.assertEqual(out, {"a": 1.0, "c": 2.0})

    def test_dropsNumpyFloatNan(self) -> None:
        out = _removeNans({"a": 1.0, "b": np.float64("nan"), "c": np.float32("nan")})
        self.assertEqual(out, {"a": 1.0})

    def test_keepsZeroAndNegative(self) -> None:
        # 0.0 and negative values are valid numbers, not NaN — they must
        # not be silently dropped along with the NaNs.
        out = _removeNans({"a": 0.0, "b": -1.5, "c": float("nan")})
        self.assertEqual(out, {"a": 0.0, "b": -1.5})

    def test_keepsInfinity(self) -> None:
        # Infinity is not NaN; the helper only filters out NaN.
        out = _removeNans({"a": float("inf"), "b": float("-inf"), "c": float("nan")})
        self.assertEqual(out, {"a": float("inf"), "b": float("-inf")})

    def test_keepsIntsAndStrings(self) -> None:
        out = _removeNans({"i": 42, "s": "hello", "f": float("nan")})
        self.assertEqual(out, {"i": 42, "s": "hello"})

    def test_emptyMapping(self) -> None:
        self.assertEqual(_removeNans({}), {})

    def test_returnsNewDict(self) -> None:
        # The helper builds a new dict rather than mutating its input.
        original = {"a": 1.0, "b": float("nan")}
        result = _removeNans(original)
        self.assertIsNot(result, original)
        self.assertIn("b", original)  # original untouched


class ChangeTypeTestCase(lsst.utils.tests.TestCase):
    """Tests for `changeType`."""

    def test_bigintReturnsInt(self) -> None:
        typeFunc = changeType("foo", {"foo": "BIGINT"})
        self.assertIs(typeFunc, int)
        self.assertEqual(typeFunc(3.7), 3)

    def test_integerReturnsInt(self) -> None:
        typeFunc = changeType("bar", {"bar": "INTEGER"})
        self.assertIs(typeFunc, int)

    def test_doublePrecisionReturnsFloat(self) -> None:
        typeFunc = changeType("baz", {"baz": "DOUBLE PRECISION"})
        self.assertIs(typeFunc, float)
        self.assertEqual(typeFunc(3), 3.0)

    def test_unknownTypeRaises(self) -> None:
        with self.assertRaises(ValueError):
            changeType("qux", {"qux": "TEXT"})
        with self.assertRaises(ValueError):
            changeType("qux", {"qux": "VARCHAR(64)"})
        with self.assertRaises(ValueError):
            changeType("qux", {"qux": ""})

    def test_missingKeyRaisesKeyError(self) -> None:
        # `changeType` does not catch the KeyError from looking up a missing
        # column in the type mapping; the caller is expected to know which
        # columns exist.
        with self.assertRaises(KeyError):
            changeType("missing", {"foo": "BIGINT"})


class _RecordingClient:
    """A stand-in for ``ConsDbClient`` that records ``insert`` calls.

    It can optionally block inside ``insert`` (until ``released`` is set) or
    raise, so the threading behaviour can be observed deterministically.
    """

    def __init__(self) -> None:
        self.inserts: list[dict[str, Any]] = []
        self.threadNames: list[str] = []
        self.entered = threading.Event()  # set on entry to insert()
        self.released = threading.Event()  # gates exit from insert() when block
        self.block = False
        self.exception: Exception | None = None
        # schema() support: records each call, returns schemaReturn (or raises
        # schemaException). The real client returns {column: (dbType, doc)}.
        self.schemaCalls: list[tuple[str, str]] = []
        self.schemaReturn: dict[str, tuple[str, str]] = {}
        self.schemaException: Exception | None = None

    def insert(self, **kwargs: Any) -> None:
        self.entered.set()
        if self.block:
            self.released.wait(timeout=5.0)
        if self.exception is not None:
            raise self.exception
        self.threadNames.append(threading.current_thread().name)
        self.inserts.append(kwargs)

    def schema(self, instrument: str, table: str) -> dict[str, tuple[str, str]]:
        self.schemaCalls.append((instrument, table))
        if self.schemaException is not None:
            raise self.schemaException
        return self.schemaReturn


class _RecordingRedisHelper:
    """A stand-in for ``RedisHelper`` that records consDB announcements."""

    def __init__(self) -> None:
        self.announced: list[tuple[str, str, int]] = []

    def announceResultInConsDb(self, instrument: str, table: str, obsId: int) -> None:
        self.announced.append((instrument, table, obsId))


class AsyncWriteTestCase(lsst.utils.tests.TestCase):
    """Tests for the asynchronous (background-thread) consDB write path.

    All consDB writes in the live processing pods are made fire-and-forget on
    a single background thread so that a slow or timing-out insert can never
    block the processing that triggered it. These tests pin down that
    contract: the
    write happens off the calling thread, the caller does not wait for it,
    failures are logged (with attributing context) rather than raised, and the
    synchronous path used by the backfill tooling is unchanged.
    """

    def _makePopulator(
        self,
        client: _RecordingClient,
        redis: _RecordingRedisHelper | None = None,
        location: str = "summit",
        asyncWrites: bool = True,
    ) -> ConsDBPopulator:
        if redis is None:
            redis = _RecordingRedisHelper()
        return ConsDBPopulator(
            cast(ConsDbClient, client),
            cast(RedisHelper, redis),
            cast(LocationConfig, SimpleNamespace(location=location)),
            asyncWrites=asyncWrites,
        )

    def test_populatePackageVersions(self) -> None:
        # the write path: the summit_utils blob (toDict) must reach the client
        # at the summit_utils table/column, keyed by the record's
        # (dayObs, seqNum) with the record's exposure id alongside. The id is
        # taken verbatim from expRecord.id - it used to be recomputed with a
        # hard-coded "O" controller, which lied for exposures taken under any
        # other controller - so a value no recomputation could produce is used
        # here. Table/column come from the summit_utils constants the read
        # side also uses, so this pins that the two agree.
        client = _RecordingClient()
        populator = self._makePopulator(client, asyncWrites=False)  # synchronous, like backfill
        pv = PackageVersions(versions={"ts_wep": "v17.6.1-alpha", "danish": "1.1.1"})
        expRecord = cast(
            DimensionRecord,
            SimpleNamespace(instrument="LSSTCam", day_obs=20250624, seq_num=123, id=5025062400123),
        )
        written = populator.populatePackageVersions(expRecord, pv)
        self.assertTrue(written)
        self.assertEqual(len(client.inserts), 1)
        insert = client.inserts[0]
        self.assertEqual(insert["table"], f"cdb_lsstcam.{PACKAGE_VERSIONS_TABLE}")
        self.assertEqual(insert["values"][PACKAGE_VERSIONS_COLUMN], pv.toDict())
        self.assertEqual(insert["values"]["exposure_id"], 5025062400123)
        self.assertEqual(insert["obs_id"], (20250624, 123))

    def test_populatePackageVersionsSkippedOffSummit(self) -> None:
        # the location gate suppresses the write where inserts aren't allowed
        client = _RecordingClient()
        populator = self._makePopulator(client, location="usdf", asyncWrites=False)
        expRecord = cast(
            DimensionRecord,
            SimpleNamespace(instrument="LSSTCam", day_obs=20250624, seq_num=123, id=5025062400123),
        )
        written = populator.populatePackageVersions(expRecord, PackageVersions(versions={"ts_wep": "v1"}))
        self.assertFalse(written)
        self.assertEqual(len(client.inserts), 0)

    def test_asyncWriteRunsOnBackgroundThread(self) -> None:
        client = _RecordingClient()
        populator = self._makePopulator(client)
        self.addCleanup(populator.shutdown)

        returned = populator._insertIfAllowed(
            "LSSTCam", "cdb_lsstcam.ccdvisit1_quicklook", 123, {"a": 1.0}, False
        )
        # async mode hands the write off, so it can't report success here
        self.assertFalse(returned)
        populator.flush()

        self.assertEqual(len(client.inserts), 1)
        self.assertEqual(client.inserts[0]["obs_id"], 123)
        # the write ran on the dedicated writer thread, not the caller's
        self.assertNotEqual(client.threadNames[0], threading.current_thread().name)
        self.assertTrue(client.threadNames[0].startswith("ConsDBWriter"))

    def test_asyncWriteDoesNotBlockCaller(self) -> None:
        client = _RecordingClient()
        client.block = True
        populator = self._makePopulator(client)
        # Order matters: releasing the worker must run *before* shutdown joins
        # it, so register shutdown first (cleanups run last-in-first-out).
        self.addCleanup(populator.shutdown)
        self.addCleanup(client.released.set)

        populator._insertIfAllowed("LSSTCam", "cdb_lsstcam.ccdvisit1_quicklook", 1, {"a": 1.0}, False)

        # The worker is now stuck inside insert(); the caller has already
        # returned, proving it did not wait for the (slow) write to complete.
        self.assertTrue(client.entered.wait(timeout=5.0))
        self.assertEqual(len(client.inserts), 0)

        client.released.set()
        populator.flush()
        self.assertEqual(len(client.inserts), 1)

    def test_asyncWriteFailureIsLoggedNotRaised(self) -> None:
        client = _RecordingClient()
        client.exception = RuntimeError("consdb is on fire")
        populator = self._makePopulator(client)
        self.addCleanup(populator.shutdown)

        with self.assertLogs("lsst.rubintv.production.consdbUtils", level="ERROR") as cm:
            # must not raise, even though the underlying insert blows up
            populator._insertIfAllowed("LSSTCam", "cdb_lsstcam.ccdvisit1_quicklook", 999, {"a": 1.0}, False)
            populator.flush()

        joined = "\n".join(cm.output)
        self.assertIn("Asynchronous consDB write", joined)
        self.assertIn("cdb_lsstcam.ccdvisit1_quicklook", joined)  # the table is named in the error
        self.assertIn("999", joined)  # and so is the obsId

    def test_onSuccessRunsOnWriterThreadAfterInsert(self) -> None:
        client = _RecordingClient()
        populator = self._makePopulator(client)
        self.addCleanup(populator.shutdown)

        calls: list[str] = []
        populator._insertIfAllowed(
            "LSSTCam",
            "cdb_lsstcam.ccdvisit1_quicklook",
            7,
            {"a": 1.0},
            False,
            onSuccess=lambda: calls.append(threading.current_thread().name),
        )
        populator.flush()
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0].startswith("ConsDBWriter"))

    def test_onSuccessNotCalledWhenInsertFails(self) -> None:
        client = _RecordingClient()
        client.exception = RuntimeError("nope")
        populator = self._makePopulator(client)
        self.addCleanup(populator.shutdown)

        called: list[bool] = []
        with self.assertLogs("lsst.rubintv.production.consdbUtils", level="ERROR"):
            populator._insertIfAllowed(
                "LSSTCam",
                "t",
                1,
                {"a": 1.0},
                False,
                onSuccess=lambda: called.append(True),
            )
            populator.flush()
        self.assertEqual(called, [])

    def test_writesArriveInSubmissionOrder(self) -> None:
        # The single FIFO worker must preserve submission order; some writes
        # (e.g. create-row then populate-row) depend on it.
        client = _RecordingClient()
        populator = self._makePopulator(client)
        self.addCleanup(populator.shutdown)

        for i in range(20):
            populator._insertIfAllowed("LSSTCam", "t", i, {"a": float(i)}, False)
        populator.flush()
        self.assertEqual([kw["obs_id"] for kw in client.inserts], list(range(20)))

    def test_skippedWhenLocationDisallowsInsert(self) -> None:
        client = _RecordingClient()
        populator = self._makePopulator(client, location="usdf")
        self.addCleanup(populator.shutdown)

        returned = populator._insertIfAllowed("LSSTCam", "t", 1, {"a": 1.0}, False)
        self.assertFalse(returned)
        populator.flush()
        self.assertEqual(len(client.inserts), 0)

    def test_emptyValuesSkipInsert(self) -> None:
        client = _RecordingClient()
        populator = self._makePopulator(client)
        self.addCleanup(populator.shutdown)

        returned = populator._insertIfAllowed("LSSTCam", "t", 1, {}, False)
        self.assertFalse(returned)
        populator.flush()
        self.assertEqual(len(client.inserts), 0)

    def test_syncModeReturnsTrueAndCallsOnSuccessInline(self) -> None:
        client = _RecordingClient()
        populator = self._makePopulator(client, asyncWrites=False)
        self.assertIsNone(populator._executor)  # no background thread when sync

        calls: list[str] = []
        returned = populator._insertIfAllowed(
            "LSSTCam",
            "t",
            5,
            {"a": 1.0},
            False,
            onSuccess=lambda: calls.append(threading.current_thread().name),
        )
        self.assertTrue(returned)
        self.assertEqual(len(client.inserts), 1)
        # synchronous: the write and callback both ran inline on this thread
        self.assertEqual(calls, [threading.current_thread().name])

    def test_syncModeReraisesInsertErrors(self) -> None:
        client = _RecordingClient()
        client.exception = RuntimeError("boom")
        populator = self._makePopulator(client, asyncWrites=False)
        with self.assertRaises(RuntimeError):
            populator._insertIfAllowed("LSSTCam", "t", 1, {"a": 1.0}, False)

    def test_flushAndShutdownAreNoOpsInSyncMode(self) -> None:
        populator = self._makePopulator(_RecordingClient(), asyncWrites=False)
        populator.flush()  # must not raise
        populator.shutdown()  # must not raise


class SchemaCacheTestCase(lsst.utils.tests.TestCase):
    """Tests for the cached consDB schema lookup (`_getTypeMapping`).

    A schema fetch is a network read whose result is needed before a row can
    be built, so it cannot be deferred like a write. Since table schemas are
    static, they are fetched once and cached, so the read only blocks the
    processing thread once per table rather than on every write.
    """

    def _makePopulator(self, client: _RecordingClient) -> ConsDBPopulator:
        return ConsDBPopulator(
            cast(ConsDbClient, client),
            cast(RedisHelper, _RecordingRedisHelper()),
            cast(LocationConfig, SimpleNamespace(location="summit")),
            asyncWrites=False,
        )

    def test_schemaFetchedOnceAndCached(self) -> None:
        client = _RecordingClient()
        client.schemaReturn = {"a": ("BIGINT", "doc"), "b": ("DOUBLE PRECISION", "doc")}
        populator = self._makePopulator(client)

        first = populator._getTypeMapping("LSSTCam", "visit1_quicklook")
        # a second lookup (with different-cased instrument) must hit the cache
        second = populator._getTypeMapping("lsstcam", "visit1_quicklook")

        self.assertEqual(first, {"a": "BIGINT", "b": "DOUBLE PRECISION"})
        self.assertEqual(second, first)
        self.assertEqual(len(client.schemaCalls), 1)  # only one network fetch

    def test_differentTablesFetchedSeparately(self) -> None:
        client = _RecordingClient()
        client.schemaReturn = {"a": ("INTEGER", "doc")}
        populator = self._makePopulator(client)

        populator._getTypeMapping("LSSTCam", "visit1_quicklook")
        populator._getTypeMapping("LSSTCam", "ccdvisit1_quicklook")
        self.assertEqual(len(client.schemaCalls), 2)

    def test_schemaFetchFailureIsNotCached(self) -> None:
        client = _RecordingClient()
        client.schemaException = RuntimeError("consdb is unreachable")
        populator = self._makePopulator(client)

        with self.assertRaises(RuntimeError):
            populator._getTypeMapping("LSSTCam", "visit1_quicklook")

        # a transient failure must not be cached: the next call retries and,
        # once consDB is healthy again, succeeds
        client.schemaException = None
        client.schemaReturn = {"a": ("INTEGER", "doc")}
        typeMapping = populator._getTypeMapping("LSSTCam", "visit1_quicklook")
        self.assertEqual(typeMapping, {"a": "INTEGER"})
        self.assertEqual(len(client.schemaCalls), 2)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
