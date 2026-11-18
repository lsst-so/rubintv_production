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

"""Test cases for consdbSqlWriter.

The files are loaded by hand into the summit consDB after backfills that
run for hours, so these catch: (1) literal rendering that would corrupt a
value or let one escape its quotes, (2) statements drifting from what the
REST insert endpoints do (the conflict key, which columns are set, the
ccdexposure key lookup, the order of the writes), (3) a bad output
directory only being found when the first file is written, (4) rows held
in memory rather than written as they come, and (5) a ``.sql`` file that
exists while incomplete, which a resumed run would skip.
"""

import os
import tempfile
import unittest

import numpy as np

import lsst.utils.tests
from lsst.rubintv.production.consdbSqlWriter import ConsDBSqlWriter, _toSqlLiteral

DAY = 20261111
VISIT = DAY * 100_000 + 12


def finishAndRead(writer: ConsDBSqlWriter) -> str:
    """Finish the writer's file and return its contents.

    Parameters
    ----------
    writer : `ConsDBSqlWriter`
        The writer, with a file started.

    Returns
    -------
    sql : `str`
        The finished file's contents.
    """
    with open(writer.finishFile()) as f:
        return f.read()


def makeWriter(rowsPerStatement: int = 1000) -> ConsDBSqlWriter:
    """Make a writer in a new directory, with a file started.

    Parameters
    ----------
    rowsPerStatement : `int`, optional
        Passed to the writer.

    Returns
    -------
    writer : `ConsDBSqlWriter`
        The writer.
    """
    writer = ConsDBSqlWriter(tempfile.mkdtemp(), rowsPerStatement=rowsPerStatement)
    writer.startFile(DAY, "test")
    return writer


class SqlLiteralTestCase(lsst.utils.tests.TestCase):
    """Values must reach the database unchanged, and strings must never
    escape their quotes.
    """

    def test_numbers(self) -> None:
        self.assertEqual(_toSqlLiteral(42), "42")
        self.assertEqual(_toSqlLiteral(np.int64(2026111100012)), "2026111100012")
        self.assertEqual(_toSqlLiteral(True), "TRUE")
        self.assertEqual(_toSqlLiteral(np.bool_(False)), "FALSE")
        # repr is the shortest string that round-trips, so the double in the
        # database is bit-identical to the one written
        for value in (0.1, -1.5e-12, 1e16, 31.200000000000003):
            self.assertEqual(float(_toSqlLiteral(value)), value)
        self.assertEqual(_toSqlLiteral(np.float64(0.25)), "0.25")  # not "np.float64(0.25)"
        self.assertEqual(float(_toSqlLiteral(np.float32(0.1))), float(np.float32(0.1)))

    def test_nonFiniteRaises(self) -> None:
        # the populator strips these first; reaching here would be a bug, and
        # "nan" is not a SQL number
        for value in (float("nan"), float("inf"), np.float32("-inf")):
            with self.assertRaises(ValueError):
                _toSqlLiteral(value)

    def test_stringsAreEscaped(self) -> None:
        self.assertEqual(_toSqlLiteral("v17.6.1"), "'v17.6.1'")
        self.assertEqual(_toSqlLiteral("it's"), "'it''s'")
        # E'' strings mean backslashes survive whatever
        # standard_conforming_strings is set to
        self.assertEqual(_toSqlLiteral("a\\b'c"), "E'a\\\\b''c'")

    def test_dictIsJsonString(self) -> None:
        # package versions are a jsonb column, written as a JSON string
        self.assertEqual(_toSqlLiteral({"ts_wep": "v1"}), '\'{"ts_wep": "v1"}\'')

    def test_unsupportedTypeRaises(self) -> None:
        with self.assertRaises(TypeError):
            _toSqlLiteral(object())


class AddRowTestCase(lsst.utils.tests.TestCase):
    """Rows are validated as they are added, mirroring how the REST API
    resolves the table and key, and are written in the order they came.
    """

    def setUp(self) -> None:
        self.writer = makeWriter()

    def tearDown(self) -> None:
        self.writer.discardFile()

    def test_unsafeColumnNameRejected(self) -> None:
        # column names are written unquoted, so anything else is refused
        with self.assertRaises(ValueError):
            self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"a; drop table x": 1.0}, True)

    def test_badValueRaisesAtTheWrite(self) -> None:
        # a value that can't be written must fail the write that made it,
        # not a later one that happens to flush it
        with self.assertRaises(TypeError):
            self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"visit_id": object()}, True)

    def test_hinfoTablesRejected(self) -> None:
        # exposure and ccdexposure belong to the hinfo service; the REST API
        # refuses writes to them, and so must the files
        for table in ("exposure", "cdb_lsstcam.ccdexposure"):
            with self.assertRaises(ValueError):
                self.writer.addRow("LSSTCam", table, (DAY, 12), {"a": 1.0}, True)

    def test_onlyUpsertsSupported(self) -> None:
        with self.assertRaises(ValueError):
            self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"a": 1.0}, False)

    def test_intObsIdOnlyForCcdVisitTables(self) -> None:
        # only ccdvisit tables have the ccdexposure key lookup implemented
        with self.assertRaises(ValueError):
            self.writer.addRow("LSSTCam", "visit1_quicklook", VISIT, {"a": 1.0}, True)
        with self.assertRaises(ValueError):  # the key comes from ccdexposure
            self.writer.addRow("LSSTCam", "ccdvisit1_quicklook", 5, {"detector": 4}, True)

    def test_writesKeepTheirOrder(self) -> None:
        # Loading must have the effect of the REST calls made in turn, so
        # statements follow the order of the writes: here the second write
        # to row 12 must land after the write to row 13, and so last
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"a": 1.0}, True)
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 13), {"b": 2.0}, True)
        self.writer.addRow("LSSTCam", "cdb_lsstcam.visit1_quicklook", (DAY, 12), {"a": 3.0}, True)
        sql = finishAndRead(self.writer)

        first, second, third = (
            sql.index(f"({DAY}, {seq}, {value})") for seq, value in ((12, 1.0), (13, 2.0), (12, 3.0))
        )
        self.assertLess(first, second)
        self.assertLess(second, third)

    def test_repeatedWriteToARowInABatchKeepsTheLater(self) -> None:
        # One multi-row upsert can't touch a row twice (Postgres refuses),
        # so a repeat write to a row in the pending batch replaces the
        # earlier one, which is what applying both in turn would do
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"a": 1.0}, True)
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"a": 3.0}, True)
        sql = finishAndRead(self.writer)

        self.assertEqual(sql.count("INSERT INTO"), 1)
        self.assertIn(f"({DAY}, 12, 3.0)", sql)
        self.assertNotIn(f"({DAY}, 12, 1.0)", sql)


class StatementTestCase(lsst.utils.tests.TestCase):
    """Each statement must do what the REST insert endpoint does for the
    same call: upsert on the addressed key, setting only the columns given.
    """

    def setUp(self) -> None:
        self.writer = makeWriter(rowsPerStatement=2)

    def tearDown(self) -> None:
        self.writer.discardFile()

    def test_seqNumKeyedUpsert(self) -> None:
        # by_seq_num: the key columns come from the obsId (overriding any in
        # the values) and are never in the SET list
        self.writer.addRow(
            "LSSTCam", "visit1_quicklook", (DAY, 12), {"visit_id": VISIT, "seq_num": 99, "x": 1.5}, True
        )
        sql = finishAndRead(self.writer)
        self.assertIn("INSERT INTO cdb_lsstcam.visit1_quicklook (day_obs, seq_num, visit_id, x)", sql)
        self.assertIn(f"({DAY}, 12, {VISIT}, 1.5)", sql)
        self.assertIn(
            "ON CONFLICT (day_obs, seq_num) DO UPDATE SET\n"
            "    visit_id = EXCLUDED.visit_id,\n"
            "    x = EXCLUDED.x;",
            sql,
        )

    def test_detectorKeyedUpsert(self) -> None:
        self.writer.addRow("LSSTCam", "ccdexposure_quicklook", (DAY, 12, 4), {"x": 1.5}, True)
        sql = finishAndRead(self.writer)
        self.assertIn(f"({DAY}, 12, 4, 1.5)", sql)
        self.assertIn("ON CONFLICT (day_obs, seq_num, detector) DO UPDATE SET", sql)

    def test_ccdVisitIdTakesKeyFromCcdexposure(self) -> None:
        # The obs/{id} endpoint looks the row's key up from ccdexposure; the
        # statement must join on it rather than guess at the id's encoding
        self.writer.addRow("LSSTCam", "ccdvisit1_quicklook", 51657051327, {"psf_coma_1": 6.0}, True)
        sql = finishAndRead(self.writer)
        self.assertIn(
            "INSERT INTO cdb_lsstcam.ccdvisit1_quicklook "
            "(day_obs, seq_num, detector, ccdvisit_id, psf_coma_1)\n"
            "SELECT c.day_obs, c.seq_num, c.detector, v.ccdvisit_id, v.psf_coma_1\n"
            "FROM (VALUES\n"
            "    (51657051327, 6.0)\n"
            ") AS v (ccdvisit_id, psf_coma_1)\n"
            "JOIN cdb_lsstcam.ccdexposure AS c ON c.ccdexposure_id = v.ccdvisit_id\n"
            "ON CONFLICT (day_obs, seq_num, detector) DO UPDATE SET\n"
            "    ccdvisit_id = EXCLUDED.ccdvisit_id,\n"
            "    psf_coma_1 = EXCLUDED.psf_coma_1;",
            sql,
        )

    def test_rowsBatchedUntilFullOrColumnsChange(self) -> None:
        # A multi-row VALUES needs one column list, so a row with different
        # columns (e.g. a NaN dropped) starts a new statement, as does a full
        # batch
        for seq in range(5):
            self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, seq), {"visit_id": seq, "x": 1.0}, True)
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 9), {"visit_id": 9}, True)
        sql = finishAndRead(self.writer)

        self.assertEqual(sql.count("INSERT INTO"), 4)
        self.assertEqual(sql.count("\n-- 2 rows\n"), 2)
        self.assertEqual(sql.count("\n-- 1 row\n"), 2)
        self.assertIn("-- statements: 4\n-- cdb_lsstcam.visit1_quicklook: 6 rows\n", sql)

    def test_fileIsOneTransaction(self) -> None:
        # a failed statement must roll the whole file back, so a partial
        # load can never happen
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"visit_id": VISIT}, True)
        sql = finishAndRead(self.writer)
        self.assertEqual(sql.count("BEGIN;"), 1)
        self.assertTrue(sql.endswith("\nCOMMIT;\n"))
        self.assertLess(sql.index("BEGIN;"), sql.index("INSERT INTO"))


class FileLifecycleTestCase(lsst.utils.tests.TestCase):
    """The output directory is checked before any work is done, rows reach
    disk as they come, and a ``.sql`` file only exists once complete, as a
    resumed run skips anything with one.
    """

    def setUp(self) -> None:
        self.outputDir = tempfile.mkdtemp()
        self.writer = ConsDBSqlWriter(self.outputDir, rowsPerStatement=2)
        self.finalPath = os.path.join(self.outputDir, str(DAY), "step.sql")

    def tearDown(self) -> None:
        self.writer.discardFile()

    def test_missingOutputDirFailsAtOnce(self) -> None:
        # a typo in the path must fail before hours of backfilling, not when
        # the first file is written
        with self.assertRaises(FileNotFoundError):
            ConsDBSqlWriter(os.path.join(self.outputDir, "typo"))

    @unittest.skipIf(os.geteuid() == 0, "root can write to read-only directories")
    def test_unwritableOutputDirFailsAtOnce(self) -> None:
        readOnly = tempfile.mkdtemp()
        os.chmod(readOnly, 0o500)
        self.addCleanup(os.chmod, readOnly, 0o700)
        with self.assertRaises(PermissionError):
            ConsDBSqlWriter(readOnly)

    def test_rowsWrittenAsTheyCome(self) -> None:
        # with rowsPerStatement=2 the first statement is on disk once the
        # second row is followed by a third, long before the file finishes
        self.writer.startFile(DAY, "step")
        for seq in range(3):
            self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, seq), {"visit_id": seq}, True)

        with open(f"{self.finalPath}.partial") as f:
            self.assertIn(f"({DAY}, 1, 1)", f.read())
        self.assertFalse(self.writer.isFileWritten(DAY, "step"))

        self.assertEqual(self.writer.finishFile(), self.finalPath)
        self.assertTrue(self.writer.isFileWritten(DAY, "step"))
        self.assertEqual(os.listdir(os.path.dirname(self.finalPath)), ["step.sql"])  # no .partial left
        self.assertFalse(self.writer.isFileOpen)

    def test_discardFileRemovesPartial(self) -> None:
        # a failed step's rows must not be left where they could be loaded
        self.writer.startFile(DAY, "step")
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"visit_id": VISIT}, True)
        self.writer.discardFile()

        self.assertFalse(self.writer.isFileOpen)
        self.assertEqual(os.listdir(os.path.dirname(self.finalPath)), [])

    def test_rewriteReplacesFinishedFileOnlyWhenDone(self) -> None:
        # rerunning a step keeps the old file until the new one is complete
        self.writer.startFile(DAY, "step")
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"visit_id": 1}, True)
        self.writer.finishFile()

        self.writer.startFile(DAY, "step")
        self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"visit_id": 2}, True)
        with open(self.finalPath) as f:
            self.assertIn(f"({DAY}, 12, 1)", f.read())
        self.writer.finishFile()
        with open(self.finalPath) as f:
            self.assertIn(f"({DAY}, 12, 2)", f.read())

    def test_writesNeedAnOpenFile(self) -> None:
        with self.assertRaises(RuntimeError):
            self.writer.addRow("LSSTCam", "visit1_quicklook", (DAY, 12), {"visit_id": VISIT}, True)
        with self.assertRaises(RuntimeError):
            self.writer.finishFile()

        self.writer.startFile(DAY, "step")
        with self.assertRaises(RuntimeError):  # one file at a time
            self.writer.startFile(DAY, "other")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
