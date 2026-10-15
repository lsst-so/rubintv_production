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

"""Tests for the non-Butler logic of the summit→USDF AOS sync.

These cover the parts of `summitSync` that can be exercised without a live
Butler, Redis, or filesystem staging area: the per-day status ledger, the
day-selection diff that drives gap-filling, the bundle path construction,
and the (opt-in) export.yaml collection-name prefix rewrite. The
Butler- and S3-touching `exportDay`/`deliverBundle`/`importDay` are
validated manually and end-to-end, not here.
"""

from __future__ import annotations

import os
import tempfile
import unittest

import lsst.utils.tests
from lsst.rubintv.production.summitSync import (
    COMPLETE_MARKER,
    EXPORTED,
    IMPORTED,
    SENT,
    SyncLedger,
    _runParallel,
    bundleDir,
    bundleRelFiles,
    daysNeedingWork,
    destinationChainName,
    prefixExportData,
    s3KeyPrefix,
)


class SyncLedgerTestCase(lsst.utils.tests.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self._tmp.name, "ledger.json")
        self.ledger = SyncLedger(self.path)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_missingFileLoadsEmpty(self) -> None:
        # A ledger that has never been written must read as empty rather
        # than raising, so a fresh deployment starts cleanly.
        self.assertEqual(self.ledger.load(), {})
        self.assertIsNone(self.ledger.status(20240101))
        self.assertEqual(self.ledger.daysWith(SENT), set())

    def test_markAndRoundTrip(self) -> None:
        self.ledger.mark(20240101, EXPORTED)
        self.ledger.mark(20240102, SENT)
        # A separate instance reading the same file must see the writes,
        # since the ledger is the only cross-restart state.
        reloaded = SyncLedger(self.path)
        self.assertEqual(reloaded.load(), {20240101: EXPORTED, 20240102: SENT})
        self.assertEqual(reloaded.status(20240102), SENT)

    def test_markOverwritesStatus(self) -> None:
        # The normal lifecycle overwrites a day's status as it progresses.
        self.ledger.mark(20240101, EXPORTED)
        self.ledger.mark(20240101, SENT)
        self.assertEqual(self.ledger.status(20240101), SENT)
        self.assertEqual(len(self.ledger.load()), 1)

    def test_daysWithFiltersByStatus(self) -> None:
        self.ledger.mark(20240101, SENT)
        self.ledger.mark(20240102, SENT)
        self.ledger.mark(20240103, EXPORTED)
        self.assertEqual(self.ledger.daysWith(SENT), {20240101, 20240102})
        self.assertEqual(self.ledger.daysWith(EXPORTED), {20240103})
        self.assertEqual(self.ledger.daysWith(IMPORTED), set())

    def test_keysAreIntsNotStrings(self) -> None:
        # dayObs are persisted as JSON string keys but must come back as
        # ints so callers can compare against getCurrentDayObsInt().
        self.ledger.mark(20240101, SENT)
        loaded = SyncLedger(self.path).load()
        self.assertIn(20240101, loaded)
        self.assertNotIn("20240101", loaded)


class DaysNeedingWorkTestCase(lsst.utils.tests.TestCase):
    def test_excludesDoneDays(self) -> None:
        result = daysNeedingWork({20240101, 20240102, 20240103}, {20240102})
        self.assertEqual(result, [20240101, 20240103])

    def test_sortedOldestFirst(self) -> None:
        # Backfill order matters: oldest first so progress is monotonic.
        result = daysNeedingWork({20240103, 20240101, 20240102}, set())
        self.assertEqual(result, [20240101, 20240102, 20240103])

    def test_beforeExcludesCurrentDay(self) -> None:
        # The exporter passes the current dayObs as ``before`` so the
        # in-progress day, which may still be accumulating data, is held back.
        result = daysNeedingWork({20240101, 20240102, 20240103}, set(), before=20240103)
        self.assertEqual(result, [20240101, 20240102])

    def test_noneBeforeKeepsAllPending(self) -> None:
        result = daysNeedingWork({20240101, 20240102}, set(), before=None)
        self.assertEqual(result, [20240101, 20240102])

    def test_emptyWhenAllDone(self) -> None:
        self.assertEqual(daysNeedingWork({20240101}, {20240101}), [])


class BundleDirTestCase(lsst.utils.tests.TestCase):
    def test_layout(self) -> None:
        self.assertEqual(
            bundleDir("/staging", "LSSTCam", 20240101),
            os.path.join("/staging", "LSSTCam", "20240101"),
        )


class BundleRelFilesTestCase(lsst.utils.tests.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.bundle = self._tmp.name
        # A nested layout like a transfer="copy" export produces, plus the
        # top-level manifest and completion marker.
        for rel in (
            "export.yaml",
            "LSSTCam/runs/quickLook/donutTable/a.fits",
            "LSSTCam/runs/quickLook/donutStampsIntra/b.fits",
            COMPLETE_MARKER,
        ):
            full = os.path.join(self.bundle, rel)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            with open(full, "w") as f:
                f.write("x")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_returnsRelativePathsSortedExcludingMarker(self) -> None:
        result = bundleRelFiles(self.bundle)
        self.assertEqual(
            result,
            [
                "LSSTCam/runs/quickLook/donutStampsIntra/b.fits",
                "LSSTCam/runs/quickLook/donutTable/a.fits",
                "export.yaml",
            ],
        )
        # The completion marker must never be in the data transfer list; it is
        # sent separately and last.
        self.assertNotIn(COMPLETE_MARKER, result)

    def test_emptyBundle(self) -> None:
        with tempfile.TemporaryDirectory() as empty:
            self.assertEqual(bundleRelFiles(empty), [])


class RunParallelTestCase(lsst.utils.tests.TestCase):
    def test_resultsInOrder(self) -> None:
        # Results must match input order even though work runs in parallel.
        self.assertEqual(_runParallel(lambda x: x * 2, [1, 2, 3, 4], 4), [2, 4, 6, 8])

    def test_emptyItems(self) -> None:
        self.assertEqual(_runParallel(lambda x: x, [], 8), [])

    def test_singleWorkerPath(self) -> None:
        self.assertEqual(_runParallel(str, [1, 2], 1), ["1", "2"])

    def test_propagatesFirstError(self) -> None:
        # A failed transfer must raise so the day is left un-marked, to retry.
        def boom(x: int) -> int:
            if x == 2:
                raise ValueError("boom")
            return x

        with self.assertRaises(ValueError):
            _runParallel(boom, [1, 2, 3], 4)


class S3KeyPrefixTestCase(lsst.utils.tests.TestCase):
    def test_layout(self) -> None:
        self.assertEqual(s3KeyPrefix("LSSTCam", 20240101), "summit_sync/LSSTCam/20240101")

    def test_relFilesAppendCleanly(self) -> None:
        # The exporter joins the prefix with bundleRelFiles entries to form S3
        # keys, and the importer strips "<prefix>/" back off; pin that they
        # compose without a doubled or missing slash.
        prefix = s3KeyPrefix("LSSTCam", 20240101)
        key = f"{prefix}/LSSTCam/runs/quickLook/donutTable/a.fits"
        self.assertEqual(key[len(prefix) + 1 :], "LSSTCam/runs/quickLook/donutTable/a.fits")


class DestinationChainNameTestCase(lsst.utils.tests.TestCase):
    def test_default(self) -> None:
        self.assertEqual(destinationChainName("LSSTCam"), "LSSTCam/runs/quickLook")

    def test_isolationPrefixApplied(self) -> None:
        # The prefix must match the prefixed RUN names the import created, so
        # the chain actually spans them.
        self.assertEqual(
            destinationChainName("LSSTCam", "summit_sync"),
            "summit_sync/LSSTCam/runs/quickLook",
        )

    def test_emptyPrefixIsNoPrefix(self) -> None:
        self.assertEqual(destinationChainName("LSSTCam", ""), "LSSTCam/runs/quickLook")


class PrefixExportDataTestCase(lsst.utils.tests.TestCase):
    def _sampleExport(self) -> dict:
        # A minimal stand-in for a parsed export.yaml exercising each entry
        # type whose collection name must be rewritten.
        return {
            "description": "Butler Data Repository Export",
            "version": "1.0.2",
            "data": [
                {"type": "dimension", "element": "visit", "records": [{"id": 1}]},
                {
                    "type": "collection",
                    "collection_type": "RUN",
                    "name": "LSSTCam/runs/quickLook/20240101/abc",
                },
                {
                    "type": "collection",
                    "collection_type": "CHAINED",
                    "name": "LSSTCam/runs/quickLook",
                    "children": ["LSSTCam/runs/quickLook/20240101/abc"],
                },
                {"type": "dataset_type", "name": "donutTable", "dimensions": ["visit", "detector"]},
                {
                    "type": "dataset",
                    "dataset_type": "donutTable",
                    "run": "LSSTCam/runs/quickLook/20240101/abc",
                    "records": [{"path": "x.fits"}],
                },
                {
                    "type": "associations",
                    "collection": "LSSTCam/runs/quickLook/20240101/abc",
                    "collection_type": "TAGGED",
                    "dataset_ids": [],
                },
            ],
        }

    def test_prefixesEveryCollectionName(self) -> None:
        result = prefixExportData(self._sampleExport(), "summit_sync")
        byType = {entry.get("type"): entry for entry in result["data"]}

        self.assertEqual(
            byType["collection"]["children"][0], "summit_sync/LSSTCam/runs/quickLook/20240101/abc"
        )
        self.assertEqual(byType["dataset"]["run"], "summit_sync/LSSTCam/runs/quickLook/20240101/abc")
        self.assertEqual(
            byType["associations"]["collection"], "summit_sync/LSSTCam/runs/quickLook/20240101/abc"
        )
        # Both collection entries (RUN and CHAINED) get prefixed names.
        names = [e["name"] for e in result["data"] if e.get("type") == "collection"]
        self.assertEqual(
            names,
            ["summit_sync/LSSTCam/runs/quickLook/20240101/abc", "summit_sync/LSSTCam/runs/quickLook"],
        )

    def test_doesNotTouchOtherFields(self) -> None:
        result = prefixExportData(self._sampleExport(), "summit_sync")
        byType = {entry.get("type"): entry for entry in result["data"]}
        # Dataset type names, dimensions, paths and dimension records are data,
        # not collections, and must be left untouched.
        self.assertEqual(byType["dataset_type"]["name"], "donutTable")
        self.assertEqual(byType["dataset"]["records"][0]["path"], "x.fits")
        self.assertEqual(byType["dimension"]["records"][0], {"id": 1})

    def test_trailingSlashOnPrefixIgnored(self) -> None:
        result = prefixExportData(self._sampleExport(), "summit_sync/")
        run = next(e["run"] for e in result["data"] if e.get("type") == "dataset")
        self.assertEqual(run, "summit_sync/LSSTCam/runs/quickLook/20240101/abc")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
