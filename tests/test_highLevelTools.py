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

"""Test cases for highLevelTools."""

import json
import os
import tempfile
import unittest
from dataclasses import dataclass
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

from astropy.table import Table

import lsst.utils.tests
from lsst.daf.butler import Butler, DimensionRecord
from lsst.rubintv.production.consdbUtils import ConsDBPopulator
from lsst.rubintv.production.highLevelTools import (
    backfillCcdVisit1QuicklookForDayHigherOrderMoments,
    backfillPackageVersions,
    checkCcdVisitQuicklookTable,
    checkVisitQuicklookTable,
)
from lsst.rubintv.production.packageVersions import PACKAGE_VERSIONS_METADATA_KEY
from lsst.summit.utils.consdbClient import ConsDbClient
from lsst.summit.utils.packageVersions import PackageVersions

# Module path used when patching the ConsDB query helpers that the table-check
# functions call, so the tests never need a live ConsDbClient.
_MODULE = "lsst.rubintv.production.highLevelTools"

# The query helpers are patched out in every test, so the client is never
# touched; a cast dummy keeps the call sites type-correct without a real one.
_FAKE_CLIENT = cast(ConsDbClient, object())


def makeEmptyTable(columns: list[str]) -> Table:
    """Build a table as ``ConsDbClient.query`` does for an empty result.

    The no-rows branch constructs ``Table(names=columns)``, which yields
    all-``float64`` columns and, crucially, an empty ``seq_num`` column.
    This is the shape ConsDB returns on a night with no on-sky images.

    Parameters
    ----------
    columns : `list` [`str`]
        The column names to create.

    Returns
    -------
    table : `astropy.table.Table`
        An empty table with the requested columns.
    """
    return Table(names=columns)


class QuicklookTableCheckTestCase(lsst.utils.tests.TestCase):
    """Tests for the ConsDB quicklook table-check helpers, focusing on the
    empty-table path that has no defined min/max seq_num."""

    def test_ccdVisitEmptyTableDoesNotRaise(self) -> None:
        # No on-sky images -> ccdvisit1_quicklook is empty. min()/max() over an
        # empty seq_num set used to raise ValueError; they must yield None now.
        table = makeEmptyTable(["seq_num", "detector"])
        with patch(f"{_MODULE}.getCcdVisitTableForDay", return_value=table):
            results = checkCcdVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums=set(), lastSeqNum=7)

        self.assertEqual(results.nEntries, 0)
        self.assertIsNone(results.minSeqNum)
        self.assertIsNone(results.maxSeqNum)
        self.assertFalse(results.exceedsButler)
        self.assertEqual(results.missingSeqNums, [])
        self.assertEqual(results.missingOnSkyInputs, [])

    def test_visitEmptyTableDoesNotRaise(self) -> None:
        # The wide visit table holds all images, not just on-sky ones, so it is
        # rarely empty, but a day with no ConsDB entries at all must not crash.
        table = makeEmptyTable(["seq_num", "can_see_sky", "n_inputs"])
        with patch(f"{_MODULE}.getWideQuicklookTableForDay", return_value=table):
            results = checkVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums=set(), lastSeqNum=7)

        self.assertEqual(results.nEntries, 0)
        self.assertIsNone(results.minSeqNum)
        self.assertIsNone(results.maxSeqNum)
        self.assertFalse(results.exceedsButler)

    def test_ccdVisitPopulatedTable(self) -> None:
        table = Table(rows=[[1, 0], [2, 0], [3, 0]], names=["seq_num", "detector"])
        with patch(f"{_MODULE}.getCcdVisitTableForDay", return_value=table):
            results = checkCcdVisitQuicklookTable(
                _FAKE_CLIENT, 20260601, onSkySeqNums={1, 2, 3, 4}, lastSeqNum=4
            )

        self.assertEqual(results.nEntries, 3)
        self.assertEqual(results.minSeqNum, 1)
        self.assertEqual(results.maxSeqNum, 3)
        self.assertFalse(results.exceedsButler)
        # seq_num 4 is on-sky per the butler but absent from the table.
        self.assertEqual(results.missingSeqNums, [4])
        self.assertEqual(results.missingOnSkyInputs, [4])

    def test_visitPopulatedTableFlagsMissingInputs(self) -> None:
        # seq_num 2 is on-sky and present but has zero inputs.
        table = Table(
            rows=[[1, True, 5], [2, True, 0], [3, True, 5]],
            names=["seq_num", "can_see_sky", "n_inputs"],
        )
        with patch(f"{_MODULE}.getWideQuicklookTableForDay", return_value=table):
            results = checkVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums={1, 2, 3}, lastSeqNum=3)

        self.assertEqual(results.minSeqNum, 1)
        self.assertEqual(results.maxSeqNum, 3)
        self.assertEqual(results.missingSeqNums, [])
        self.assertEqual(results.missingOnSkyInputs, [2])
        self.assertFalse(results.exceedsButler)

    def test_exceedsButlerWhenMaxSeqNumBeyondButler(self) -> None:
        table = Table(rows=[[5, 0]], names=["seq_num", "detector"])
        with patch(f"{_MODULE}.getCcdVisitTableForDay", return_value=table):
            results = checkCcdVisitQuicklookTable(_FAKE_CLIENT, 20260601, onSkySeqNums={5}, lastSeqNum=3)

        self.assertEqual(results.maxSeqNum, 5)
        self.assertTrue(results.exceedsButler)


class _RecordingPopulator:
    """A ``ConsDBPopulator`` stand-in that records writes.

    ``populateHigherOrderMoments`` reports a failed write for any detector in
    ``emptyDetectors``, as the real one does for a catalog with no finite
    moments.
    """

    def __init__(self, emptyDetectors: tuple[int, ...] = ()) -> None:
        self.calls: list[tuple[DimensionRecord, PackageVersions, bool]] = []
        self.momentCalls: list[tuple[DimensionRecord, int, object, bool]] = []
        self.emptyDetectors = emptyDetectors

    def populatePackageVersions(
        self,
        expRecord: DimensionRecord,
        packageVersions: PackageVersions,
        allowUpdate: bool = False,
    ) -> bool:
        self.calls.append((expRecord, packageVersions, allowUpdate))
        return True

    def populateHigherOrderMoments(
        self,
        expRecord: DimensionRecord,
        detectorNum: int,
        singleVisitStarFootprints: object,
        allowUpdate: bool = False,
    ) -> bool:
        self.momentCalls.append((expRecord, detectorNum, singleVisitStarFootprints, allowUpdate))
        return detectorNum not in self.emptyDetectors


class _FakeButler:
    """A ``Butler`` stand-in serving canned records and dataset refs for the
    day, where ``get`` returns a ``(visit, detector)`` tuple in place of the
    dataset.
    """

    def __init__(self, records: list[DimensionRecord], dRefs: list[SimpleNamespace] | None = None) -> None:
        self.records = records
        self.dRefs = dRefs or []
        self.datasetsLimit: int | None = -20_000

    def query_dimension_records(
        self, element: str, where: str = "", order_by: str = ""
    ) -> list[DimensionRecord]:
        return self.records

    def query_datasets(
        self, datasetType: str, where: str = "", limit: int | None = -20_000, explain: bool = True
    ) -> list[SimpleNamespace]:
        self.datasetsLimit = limit
        return self.dRefs

    def get(self, dRef: SimpleNamespace) -> tuple[int, int]:
        return (dRef.dataId["visit"], dRef.dataId["detector"])


class BackfillPackageVersionsTestCase(lsst.utils.tests.TestCase):
    def test_backfillPackageVersions(self) -> None:
        # backfill reads the merged sidecar and writes only the version-bearing
        # seqNums, recovering the exact versions (display marker stripped) and
        # passing the matching exposure record through, so the populator gets
        # the true exposure id off the record rather than recomputing it (the
        # id used to be recomputed with a hard-coded "O" controller)
        versions = {"ts_wep": "v17.6.1-alpha", "danish": "1.1.1"}
        metadata = {
            "12": {PACKAGE_VERSIONS_METADATA_KEY: {**versions, "DISPLAY_VALUE": "📖"}, "PSF": 1.2},
            "13": {"PSF": 1.3},  # no package versions recorded -> skipped
            "14": {PACKAGE_VERSIONS_METADATA_KEY: {**versions, "DISPLAY_VALUE": "📖"}},  # no record, below
        }
        record12 = cast(
            DimensionRecord,
            SimpleNamespace(instrument="LSSTCam", day_obs=20250624, seq_num=12, id=2025062400012),
        )
        record13 = cast(
            DimensionRecord,
            SimpleNamespace(instrument="LSSTCam", day_obs=20250624, seq_num=13, id=2025062400013),
        )
        # no record for seqNum 14: version-bearing metadata with no matching
        # exposure record must be skipped with a warning, not raise or write
        butler = _FakeButler([record12, record13])
        populator = _RecordingPopulator()
        with tempfile.TemporaryDirectory() as tmp:
            with open(os.path.join(tmp, "dayObs_20250624.json"), "w") as f:
                json.dump(metadata, f)
            with self.assertLogs(level="WARNING"):
                nWritten = backfillPackageVersions(
                    cast(Butler, butler), cast(ConsDBPopulator, populator), tmp, 20250624, "LSSTCam"
                )

        self.assertEqual(nWritten, 1)
        self.assertEqual(len(populator.calls), 1)
        expRecord, packageVersions, allowUpdate = populator.calls[0]
        self.assertIs(expRecord, record12)  # the actual record, exposure id and all
        self.assertEqual(packageVersions, PackageVersions(versions=versions))
        # the exposure_quicklook row already exists by backfill time, so the
        # write must be an update or every row would conflict
        self.assertTrue(allowUpdate)


@dataclass(frozen=True)
class _FakeVisitRecord:
    """A hashable visit-record stand-in, as the backfill keys its results by
    record.
    """

    instrument: str
    id: int


class BackfillHigherOrderMomentsTestCase(lsst.utils.tests.TestCase):
    """Tests for `backfillCcdVisit1QuicklookForDayHigherOrderMoments`.

    The backfill must hand each detector's own catalog to the populator for
    the right visit, record a detector as inserted only when the write
    happened, and report visits with no catalogs at all as missing data.
    """

    def test_backfillHigherOrderMoments(self) -> None:
        withData = cast(DimensionRecord, _FakeVisitRecord(instrument="LSSTCam", id=2025062400012))
        withoutData = cast(DimensionRecord, _FakeVisitRecord(instrument="LSSTCam", id=2025062400013))
        dRefs = [
            SimpleNamespace(dataId={"visit": withData.id, "detector": 94}),
            SimpleNamespace(dataId={"visit": withData.id, "detector": 4}),
        ]
        butler = _FakeButler([withData, withoutData], dRefs)
        # detector 94's catalog has no finite moments, so its write is skipped
        # and it must not be counted as inserted
        populator = _RecordingPopulator(emptyDetectors=(94,))

        rowsInserted, noData = backfillCcdVisit1QuicklookForDayHigherOrderMoments(
            cast(Butler, butler), cast(ConsDBPopulator, populator), 20250624
        )

        self.assertEqual(rowsInserted, {withData: [4]})
        self.assertEqual(noData, [withoutData])
        self.assertEqual(
            populator.momentCalls,
            [
                # the ccdvisit1_quicklook rows already exist by backfill time,
                # so every write must be an update
                (withData, 4, (withData.id, 4), True),
                (withData, 94, (withData.id, 94), True),
            ],
        )
        # query_datasets caps results at 20k by default and only warns when it
        # truncates, which a night of ~190 detectors per visit easily exceeds
        self.assertIsNone(butler.datasetsLimit)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
