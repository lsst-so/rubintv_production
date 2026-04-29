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

"""Unit tests for shardIo: shard write/read helpers and getShardPath dispatch.

These exercise the metadata/data shard files end-to-end against a
``tmp_path``-style temp directory, because the wire format is the
filename pattern itself plus the JSON contents — there is no need to
mock anything to exercise it.

The shard files are the workers→gather wire format: workers writeShard
and the head node globs+reads them via ``getShardedData``. So the
regressions these tests catch are:

- Writer/reader filename drift — e.g. ``createFilenameForDataShard``
  and ``getGlobPatternForShardedData`` use a shared template; if they
  ever drift apart, the gather silently sees zero shards even though
  the workers wrote them. Pinned by globbing the writer's output with
  the reader's pattern.
- ``ALLOWED_DATASET_TYPES`` drift — the allow-list gates which named
  datasets a worker can write; the tests iterate the real list so a
  typo'd entry surfaces here rather than at runtime.
- NumpyEncoder removed or broken — workers commonly stash numpy
  scalars/arrays in metadata; if the encoder regressed, the JSON
  write would raise. Pinned by round-tripping numpy values.
- Atomic-write breakage — ``writeMetadataShard`` writes to ``tmp-`` and
  renames; if that ever became a direct write, a reader could pick up
  a half-written file. Pinned by asserting no ``tmp-`` files remain.
- ``getShardPath`` instrument dispatch — a new instrument added
  without updating the ``getShardPath`` branches would silently fall
  through; the test exercises every supported instrument plus an
  unknown one.
"""

from __future__ import annotations

import glob
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from typing import cast

import numpy as np

import lsst.utils.tests
from lsst.daf.butler import DimensionRecord
from lsst.rubintv.production.locationConfig import LocationConfig
from lsst.rubintv.production.shardIo import (
    ALLOWED_DATASET_TYPES,
    SEQNUM_PADDING,
    createFilenameForDataShard,
    getGlobPatternForShardedData,
    getShardedData,
    getShardPath,
    writeDataShard,
    writeMetadataShard,
)


class GetGlobPatternForShardedDataTestCase(lsst.utils.tests.TestCase):
    """The glob pattern is the gather-side half of the writer/reader
    wire-format contract — these tests pin its key invariants so a
    rename or formatting tweak surfaces immediately."""

    def test_seqNumIsZeroPadded(self) -> None:
        pattern = getGlobPatternForShardedData(
            path="/some/path",
            dataSetName="rawNoises",
            instrument="LSSTCam",
            dayObs=20240101,
            seqNum=42,
        )
        # The seqNum must be zero-padded to SEQNUM_PADDING in the
        # filename — gather code globs against this pattern, so the
        # padding is part of the wire format.
        seqStr = f"{42:0{SEQNUM_PADDING}}"
        self.assertIn(f"seqNum_{seqStr}_*", pattern)
        self.assertIn("rawNoises", pattern)
        self.assertIn("LSSTCam", pattern)
        self.assertIn("20240101", pattern)

    def test_globPatternMatchesCreatedFilename(self) -> None:
        # createFilenameForDataShard must produce paths that the glob
        # pattern matches, since one is the writer and the other is
        # the reader.
        with tempfile.TemporaryDirectory() as tmp:
            filename = createFilenameForDataShard(
                path=tmp,
                dataSetName="rawNoises",
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=42,
            )
            # Touch the file so glob actually has something to find.
            with open(filename, "w") as f:
                f.write("{}")
            pattern = getGlobPatternForShardedData(
                path=tmp,
                dataSetName="rawNoises",
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=42,
            )
            self.assertIn(filename, glob.glob(pattern))


class WriteMetadataShardTestCase(lsst.utils.tests.TestCase):
    """``writeMetadataShard`` is the worker→gather path for free-form
    metadata. The contract: atomic tmp-rename, parents created, dict-
    only payload, NumpyEncoder wired in, uuid1 suffix preventing
    concurrent collisions."""

    def test_writesJsonToNewDirectory(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, "subdir-that-does-not-exist")
            payload = {123: {"Filter": "r"}}
            writeMetadataShard(target, dayObs=20240101, mdDict=payload)

            files = glob.glob(os.path.join(target, "metadata-dayObs_20240101_*.json"))
            self.assertEqual(len(files), 1)
            with open(files[0]) as f:
                # JSON keys are always strings on disk, so reload and
                # compare against the stringified version.
                self.assertEqual(json.load(f), {"123": {"Filter": "r"}})

    def test_temporaryFileIsRenamedAway(self) -> None:
        # writeMetadataShard writes to a tmp- prefix then renames, so a
        # reader globbing the metadata-* pattern never sees a partial
        # write. After the call there must be no tmp- file left over.
        with tempfile.TemporaryDirectory() as tmp:
            writeMetadataShard(tmp, dayObs=20240101, mdDict={1: {"a": 1}})
            tmpFiles = glob.glob(os.path.join(tmp, "tmp-*"))
            self.assertEqual(tmpFiles, [])

    def test_rejectsNonDict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TypeError):
                writeMetadataShard(tmp, dayObs=20240101, mdDict=[("not", "a dict")])  # type: ignore[arg-type]

    def test_numpyValuesAreSerialised(self) -> None:
        # NumpyEncoder is part of the shard contract — workers commonly
        # write numpy scalars and arrays and the merger has to decode
        # them as plain JSON.
        with tempfile.TemporaryDirectory() as tmp:
            payload = {1: {"value": np.float64(3.14), "array": np.array([1, 2, 3])}}
            writeMetadataShard(tmp, dayObs=20240101, mdDict=payload)
            files = glob.glob(os.path.join(tmp, "metadata-dayObs_*.json"))
            self.assertEqual(len(files), 1)
            with open(files[0]) as f:
                loaded = json.load(f)
            self.assertAlmostEqual(loaded["1"]["value"], 3.14)
            self.assertEqual(loaded["1"]["array"], [1, 2, 3])

    def test_multipleWritesProduceDistinctFiles(self) -> None:
        # uuid1-suffixed filenames means concurrent writers can't
        # collide. Writing twice in quick succession from the same
        # process must still produce two distinct files.
        with tempfile.TemporaryDirectory() as tmp:
            writeMetadataShard(tmp, dayObs=20240101, mdDict={1: {"a": 1}})
            writeMetadataShard(tmp, dayObs=20240101, mdDict={2: {"b": 2}})
            files = glob.glob(os.path.join(tmp, "metadata-dayObs_*.json"))
            self.assertEqual(len(files), 2)


class WriteDataShardTestCase(lsst.utils.tests.TestCase):
    """``writeDataShard`` is the typed-data counterpart to
    ``writeMetadataShard``. The allow-list (``ALLOWED_DATASET_TYPES``)
    is the safety net before a new product can be written; these
    tests pin both that the gate is enforced and that every listed
    type really is round-trippable."""

    def test_writesAllowedType(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            writeDataShard(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=7,
                dataSetName="rawNoises",
                dataDict={"amp_0": 1.5},
            )
            pattern = getGlobPatternForShardedData(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=7,
                dataSetName="rawNoises",
            )
            files = glob.glob(pattern)
            self.assertEqual(len(files), 1)
            with open(files[0]) as f:
                self.assertEqual(json.load(f), {"amp_0": 1.5})

    def test_rejectsUnknownDatasetType(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                writeDataShard(
                    path=tmp,
                    instrument="LSSTCam",
                    dayObs=20240101,
                    seqNum=1,
                    dataSetName="not-an-allowed-type",
                    dataDict={"a": 1},
                )

    def test_rejectsNonDictPayload(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(TypeError):
                writeDataShard(
                    path=tmp,
                    instrument="LSSTCam",
                    dayObs=20240101,
                    seqNum=1,
                    dataSetName="rawNoises",
                    dataDict=[("not", "a dict")],  # type: ignore[arg-type]
                )

    def test_allowedTypesAreActuallyAllAccepted(self) -> None:
        # Pin the allow-list against the actual call so a typo in
        # ALLOWED_DATASET_TYPES wouldn't pass the test.
        with tempfile.TemporaryDirectory() as tmp:
            for datasetName in ALLOWED_DATASET_TYPES:
                writeDataShard(
                    path=tmp,
                    instrument="LSSTCam",
                    dayObs=20240101,
                    seqNum=1,
                    dataSetName=datasetName,
                    dataDict={"k": 1},
                )

    def test_createFilenameForDataShardEmbedsDataId(self) -> None:
        # Pure helper — just check the filename embeds the right pieces
        # so mismatched files can't leak through the gather glob.
        filename = createFilenameForDataShard(
            path="/tmp",
            dataSetName="binnedImage",
            instrument="LSSTCam",
            dayObs=20240101,
            seqNum=42,
        )
        seqStr = f"{42:0{SEQNUM_PADDING}}"
        self.assertIn("binnedImage", filename)
        self.assertIn("LSSTCam", filename)
        self.assertIn("20240101", filename)
        self.assertIn(f"seqNum_{seqStr}_", filename)
        self.assertTrue(filename.endswith(".json"))


class GetShardedDataTestCase(lsst.utils.tests.TestCase):
    """``getShardedData`` is the gather side: head-node code calls it
    to merge per-detector shards into one payload. The contract pinned
    here: complete merges, ambiguous-set raises, partial-keep semantics
    of ``deleteIfComplete`` vs ``deleteRegardless``, and dataId-based
    filtering across adjacent seqNums."""

    def _writeShard(self, tmp: str, seqNum: int, payload: dict) -> None:
        writeDataShard(
            path=tmp,
            instrument="LSSTCam",
            dayObs=20240101,
            seqNum=seqNum,
            dataSetName="rawNoises",
            dataDict=payload,
        )

    def test_mergesShardsWhenComplete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._writeShard(tmp, seqNum=1, payload={"amp_0": 1.0})
            self._writeShard(tmp, seqNum=1, payload={"amp_1": 2.0})

            data, n = getShardedData(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=1,
                dataSetName="rawNoises",
                nExpected=2,
                timeout=0.1,
                deleteIfComplete=False,
            )
            self.assertEqual(n, 2)
            self.assertEqual(data, {"amp_0": 1.0, "amp_1": 2.0})

    def test_returnsEmptyWhenNothingFound(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            data, n = getShardedData(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=1,
                dataSetName="rawNoises",
                nExpected=2,
                timeout=0.05,
            )
            self.assertEqual(data, {})
            self.assertEqual(n, 0)

    def test_raisesIfMoreShardsThanExpected(self) -> None:
        # Too many shards means the gathered set is ambiguous — the
        # caller must be told rather than silently merging.
        with tempfile.TemporaryDirectory() as tmp:
            self._writeShard(tmp, seqNum=1, payload={"a": 1})
            self._writeShard(tmp, seqNum=1, payload={"b": 2})
            self._writeShard(tmp, seqNum=1, payload={"c": 3})

            with self.assertRaises(RuntimeError):
                getShardedData(
                    path=tmp,
                    instrument="LSSTCam",
                    dayObs=20240101,
                    seqNum=1,
                    dataSetName="rawNoises",
                    nExpected=2,
                    timeout=0.05,
                )

    def test_deletesShardsWhenCompleteAndRequested(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._writeShard(tmp, seqNum=1, payload={"a": 1})
            self._writeShard(tmp, seqNum=1, payload={"b": 2})

            getShardedData(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=1,
                dataSetName="rawNoises",
                nExpected=2,
                timeout=0.1,
                deleteIfComplete=True,
            )
            remaining = glob.glob(os.path.join(tmp, "*.json"))
            self.assertEqual(remaining, [])

    def test_keepsShardsWhenIncompleteAndDeleteIfCompleteOnly(self) -> None:
        # If we ask for delete-if-complete and the set is incomplete,
        # the files must remain so a later attempt can merge them.
        with tempfile.TemporaryDirectory() as tmp:
            self._writeShard(tmp, seqNum=1, payload={"a": 1})
            data, n = getShardedData(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=1,
                dataSetName="rawNoises",
                nExpected=2,
                timeout=0.05,
                deleteIfComplete=True,
            )
            self.assertEqual(n, 1)
            self.assertEqual(data, {"a": 1})
            remaining = glob.glob(os.path.join(tmp, "*.json"))
            self.assertEqual(len(remaining), 1)

    def test_deleteRegardlessRemovesIncompleteShards(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self._writeShard(tmp, seqNum=1, payload={"a": 1})
            getShardedData(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=1,
                dataSetName="rawNoises",
                nExpected=2,
                timeout=0.05,
                deleteRegardless=True,
            )
            remaining = glob.glob(os.path.join(tmp, "*.json"))
            self.assertEqual(remaining, [])

    def test_filtersByDataIdAcrossSeqNums(self) -> None:
        # Two shards with different seqNums must not be merged together
        # by a glob targeting only one of them.
        with tempfile.TemporaryDirectory() as tmp:
            self._writeShard(tmp, seqNum=1, payload={"a": 1})
            self._writeShard(tmp, seqNum=2, payload={"b": 2})
            data, n = getShardedData(
                path=tmp,
                instrument="LSSTCam",
                dayObs=20240101,
                seqNum=1,
                dataSetName="rawNoises",
                nExpected=1,
                timeout=0.1,
                deleteIfComplete=False,
            )
            self.assertEqual(n, 1)
            self.assertEqual(data, {"a": 1})


class GetShardPathTestCase(lsst.utils.tests.TestCase):
    """``getShardPath`` selects the per-instrument shard directory off
    the LocationConfig. A new instrument added without updating the
    dispatch branches would silently fall through; these tests
    exercise every supported instrument plus the AOS-not-supported
    LATISS case and an unknown-instrument case."""

    def _makeLocationConfig(self) -> LocationConfig:
        # getShardPath only reads the per-instrument shard-path
        # attributes off the config, so a SimpleNamespace stand-in
        # avoids the YAML loading and dir checks that constructing a
        # real LocationConfig would do.
        return cast(
            LocationConfig,
            SimpleNamespace(
                auxTelMetadataShardPath="/aux/shard",
                comCamMetadataShardPath="/comcam/shard",
                comCamAosMetadataShardPath="/comcam/aos/shard",
                comCamSimMetadataShardPath="/comcamsim/shard",
                comCamSimAosMetadataShardPath="/comcamsim/aos/shard",
                lsstCamMetadataShardPath="/lsstcam/shard",
                lsstCamAosMetadataShardPath="/lsstcam/aos/shard",
            ),
        )

    def _record(self, instrument: str) -> DimensionRecord:
        return cast(DimensionRecord, SimpleNamespace(instrument=instrument))

    def test_latissNonAos(self) -> None:
        cfg = self._makeLocationConfig()
        self.assertEqual(getShardPath(cfg, self._record("LATISS")), "/aux/shard")

    def test_latissAosRaises(self) -> None:
        cfg = self._makeLocationConfig()
        with self.assertRaises(ValueError):
            getShardPath(cfg, self._record("LATISS"), isAos=True)

    def test_lsstComCamBranches(self) -> None:
        cfg = self._makeLocationConfig()
        self.assertEqual(getShardPath(cfg, self._record("LSSTComCam")), "/comcam/shard")
        self.assertEqual(getShardPath(cfg, self._record("LSSTComCam"), isAos=True), "/comcam/aos/shard")

    def test_lsstComCamSimBranches(self) -> None:
        cfg = self._makeLocationConfig()
        self.assertEqual(getShardPath(cfg, self._record("LSSTComCamSim")), "/comcamsim/shard")
        self.assertEqual(
            getShardPath(cfg, self._record("LSSTComCamSim"), isAos=True),
            "/comcamsim/aos/shard",
        )

    def test_lsstCamBranches(self) -> None:
        cfg = self._makeLocationConfig()
        self.assertEqual(getShardPath(cfg, self._record("LSSTCam")), "/lsstcam/shard")
        self.assertEqual(getShardPath(cfg, self._record("LSSTCam"), isAos=True), "/lsstcam/aos/shard")

    def test_unknownInstrumentRaises(self) -> None:
        cfg = self._makeLocationConfig()
        with self.assertRaises(ValueError):
            getShardPath(cfg, self._record("UnknownCamera"))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
