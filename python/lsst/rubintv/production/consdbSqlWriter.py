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

"""Write consDB writes as SQL upsert files, for loading with ``psql``.

Each statement does what the consDB REST insert endpoint does server-side
for the same call, so loading a file has the same effect as making the
calls, in one transaction rather than one HTTP request per row.
"""

from __future__ import annotations

__all__ = ["ConsDBSqlWriter"]

import json
import math
import os
import re
import tempfile
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TextIO

import numpy as np

ObsId = int | tuple[int, ...]

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")

_FORBIDDEN_TABLES = frozenset({"exposure", "ccdexposure"})

_SEQ_NUM_KEY = ("day_obs", "seq_num")
_DETECTOR_KEY = ("day_obs", "seq_num", "detector")


def _getUtcNow() -> str:
    """Return the current UTC time, to the second, for file comments.

    Returns
    -------
    now : `str`
        The time, in ISO 8601 form.
    """
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _checkIdentifier(name: str) -> str:
    """Return ``name`` if it is a plain lowercase SQL identifier.

    Parameters
    ----------
    name : `str`
        The schema, table or column name.

    Returns
    -------
    name : `str`
        The name, unchanged.

    Raises
    ------
    ValueError
        Raised if the name would need quoting.
    """
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Refusing to write unsafe SQL identifier {name!r}")
    return name


def _qualifyTable(instrument: str, table: str) -> str:
    """Return the ``cdb_<instrument>.<table>`` name the REST API resolves to.

    Parameters
    ----------
    instrument : `str`
        The instrument, e.g. ``"LSSTCam"``.
    table : `str`
        The table, with or without its ``cdb_<instrument>.`` schema prefix.

    Returns
    -------
    qualifiedTable : `str`
        The schema-qualified, lowercase table name.
    """
    schema = f"cdb_{instrument.lower()}"
    table = table.lower()
    if table.startswith(f"{schema}."):
        table = table[len(schema) + 1 :]
    if table in _FORBIDDEN_TABLES:
        raise ValueError(f"{schema}.{table} is owned by the hinfo service and cannot be written")
    return f"{_checkIdentifier(schema)}.{_checkIdentifier(table)}"


def _toSqlLiteral(value: object) -> str:
    """Render a value as a SQL literal.

    Parameters
    ----------
    value : `object`
        A bool, int, finite float, str, or JSON-serialisable dict or list,
        including the numpy scalar equivalents.

    Returns
    -------
    literal : `str`
        The SQL literal.
    """
    if isinstance(value, (bool, np.bool_)):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"Cannot write non-finite value {number} to SQL")
        return repr(number)
    if isinstance(value, (dict, list)):
        return _toSqlLiteral(json.dumps(value))
    if isinstance(value, str):
        quoted = value.replace("'", "''")
        if "\\" in value:
            return "E'" + quoted.replace("\\", "\\\\") + "'"
        return "'" + quoted + "'"
    raise TypeError(f"Cannot write {type(value).__name__} value {value!r} to SQL")


class ConsDBSqlWriter:
    """Write consDB writes to SQL files as they are made.

    Pass one to `ConsDBPopulator` as its ``sqlWriter`` and the populator's
    writes come here instead of going to the REST API. They go into the file
    begun with `startFile`, as upserts of up to ``rowsPerStatement`` rows, in
    the order they were made, so loading the file has the same effect as
    making the calls. The file is written as ``<name>.sql.partial`` and
    renamed to ``<name>.sql`` by `finishFile`, so a ``.sql`` file is always
    complete.

    The statements mirror the REST insert endpoints with ``allow_update``:

    - a ``(day_obs, seq_num[, detector])`` obsId upserts on those columns,
      setting only the columns written;
    - an integer obsId (only supported for ``ccdvisit`` tables) is the
      ``ccdvisit_id``, and the row's ``day_obs``, ``seq_num`` and
      ``detector`` are looked up from ``ccdexposure`` in the same statement,
      as the server does. Rows with no ``ccdexposure`` parent are skipped,
      where the REST API would have returned an error.

    Parameters
    ----------
    outputDir : `str`
        The directory to write the per-dayObs directories into. It must
        already exist and be writable.
    rowsPerStatement : `int`, optional
        The maximum number of rows in each ``INSERT`` statement.

    Raises
    ------
    FileNotFoundError
        Raised if ``outputDir`` does not exist.
    PermissionError
        Raised if a file cannot be created in ``outputDir``.
    """

    def __init__(self, outputDir: str, rowsPerStatement: int = 1000) -> None:
        if not os.path.isdir(outputDir):
            raise FileNotFoundError(f"Output directory {outputDir} does not exist")
        try:
            with tempfile.TemporaryFile(dir=outputDir):
                pass
        except OSError as e:
            raise PermissionError(f"Cannot write to output directory {outputDir}") from e

        self.outputDir = outputDir
        self.rowsPerStatement = rowsPerStatement
        self._file: TextIO | None = None
        self._partialPath = ""
        self._batchKey: tuple[str, int, tuple[str, ...]] | None = None
        self._batchRows: dict[ObsId, dict[str, str]] = {}
        self._rowCounts: dict[str, int] = {}
        self._nStatements = 0

    @property
    def isFileOpen(self) -> bool:
        """Whether a file has been started and not yet finished (`bool`)."""
        return self._file is not None

    def getFilePath(self, dayObs: int, name: str) -> str:
        """Return the path a file is written to once finished.

        Parameters
        ----------
        dayObs : `int`
            The dayObs, which names the directory.
        name : `str`
            The file's name, without ``.sql``.

        Returns
        -------
        path : `str`
            The path.
        """
        return os.path.join(self.outputDir, str(dayObs), f"{name}.sql")

    def isFileWritten(self, dayObs: int, name: str) -> bool:
        """Return whether a file has been finished.

        Parameters
        ----------
        dayObs : `int`
            The dayObs, which names the directory.
        name : `str`
            The file's name, without ``.sql``.

        Returns
        -------
        written : `bool`
            Whether the finished file exists.
        """
        return os.path.exists(self.getFilePath(dayObs, name))

    def startFile(self, dayObs: int, name: str) -> None:
        """Start a file for the writes that follow, until `finishFile`.

        A partial file of the same name, left by an interrupted run, is
        replaced. A finished one is only replaced once this one finishes.

        Parameters
        ----------
        dayObs : `int`
            The dayObs, which names the directory.
        name : `str`
            The file's name, without ``.sql``.
        """
        if self._file is not None:
            raise RuntimeError(f"{self._partialPath} is still open; finish or discard it first")
        finalPath = self.getFilePath(dayObs, name)
        os.makedirs(os.path.dirname(finalPath), exist_ok=True)
        self._partialPath = f"{finalPath}.partial"
        self._rowCounts = {}
        self._nStatements = 0
        self._file = open(self._partialPath, "w")
        self._writeText(
            "-- consDB backfill written by rapid analysis\n"
            f"-- dayObs:  {dayObs}\n"
            f"-- name:    {name}\n"
            f"-- started: {_getUtcNow()}\n"
            "--\n"
            "-- Load with:  psql -v ON_ERROR_STOP=1 -f <this file>\n"
            "--\n"
            "-- The file is one transaction. Each statement is an upsert on the\n"
            "-- table's primary key that sets only the columns it lists, so other\n"
            "-- columns are left alone and loading a file twice is harmless.\n"
            "-- psql prints each statement's row count, which should match the\n"
            "-- comment above it. For ccdvisit tables a lower count means some\n"
            "-- rows had no ccdexposure row to take their key from, and were\n"
            "-- skipped.\n"
            "\n"
            "BEGIN;\n"
        )

    def addRow(
        self,
        instrument: str,
        table: str,
        obsId: ObsId,
        values: Mapping[str, object],
        allowUpdate: bool,
    ) -> None:
        """Write a row to the current file.

        Takes the same arguments as the populator's REST insert. Consecutive
        rows for the same table and columns are written together, once
        ``rowsPerStatement`` of them have been added, or when the next row
        differs.

        Parameters
        ----------
        instrument : `str`
            The instrument, e.g. ``"LSSTCam"``.
        table : `str`
            The table, with or without its ``cdb_<instrument>.`` prefix.
        obsId : `int` or `tuple` [`int`, ...]
            The ``ccdvisit_id``, or ``(day_obs, seq_num)`` or
            ``(day_obs, seq_num, detector)``.
        values : `Mapping` [`str`, `object`]
            The column values to write.
        allowUpdate : `bool`
            Must be ``True``, as every statement written is an upsert.
        """
        if self._file is None:
            raise RuntimeError("No file to write to; call startFile first")
        if not allowUpdate:
            raise ValueError("ConsDBSqlWriter only writes upserts, so allowUpdate must be True")
        qualifiedTable = _qualifyTable(instrument, table)

        row = {_checkIdentifier(column): _toSqlLiteral(value) for column, value in values.items()}
        if isinstance(obsId, tuple):
            if len(obsId) not in (2, 3):
                raise ValueError(f"obsId tuple must be (day_obs, seq_num[, detector]), got {obsId!r}")
            keyColumns = _SEQ_NUM_KEY if len(obsId) == 2 else _DETECTOR_KEY
            for column in keyColumns:
                row.pop(column, None)
        else:
            if not qualifiedTable.split(".")[1].startswith("ccdvisit"):
                raise ValueError(
                    f"Integer obsIds are only supported for ccdvisit tables, not {qualifiedTable}"
                )
            clashes = set(row) & {*_DETECTOR_KEY, "ccdvisit_id"}
            if clashes:
                raise ValueError(f"Key columns {sorted(clashes)} come from the obsId, not the values")

        keyLength = len(obsId) if isinstance(obsId, tuple) else 1
        batchKey = (qualifiedTable, keyLength, tuple(sorted(row)))
        batchIsFull = len(self._batchRows) >= self.rowsPerStatement and obsId not in self._batchRows
        if batchKey != self._batchKey or batchIsFull:
            self._flushBatch()
            self._batchKey = batchKey
        self._batchRows[obsId] = row

    def finishFile(self) -> str:
        """Write any rows still pending, commit, and move the file into place.

        Returns
        -------
        path : `str`
            The finished file.
        """
        if self._file is None:
            raise RuntimeError("No file to finish; call startFile first")
        self._flushBatch()
        counts = "".join(
            f"-- {table}: {n} row{'' if n == 1 else 's'}\n" for table, n in sorted(self._rowCounts.items())
        )
        self._writeText(
            f"\n-- finished:   {_getUtcNow()}\n-- statements: {self._nStatements}\n{counts}\nCOMMIT;\n"
        )
        self._file.close()
        self._file = None
        finalPath = self._partialPath.removesuffix(".partial")
        os.replace(self._partialPath, finalPath)
        return finalPath

    def discardFile(self) -> None:
        """Abandon the current file, deleting it. Does nothing if there is
        no current file.
        """
        if self._file is None:
            return
        try:
            self._file.close()
        finally:
            self._file = None
            self._batchKey = None
            self._batchRows = {}
            if os.path.exists(self._partialPath):
                os.remove(self._partialPath)

    def _writeText(self, text: str) -> None:
        """Write to the current file and flush it to disk.

        Parameters
        ----------
        text : `str`
            The text to write.
        """
        assert self._file is not None
        self._file.write(text)
        self._file.flush()

    def _flushBatch(self) -> None:
        """Write the pending rows as one statement, if there are any."""
        if not self._batchRows:
            return
        assert self._batchKey is not None
        table, keyLength, columns = self._batchKey
        batch = list(self._batchRows.items())
        if keyLength == 1:
            statement = self._renderCcdVisitStatement(table, columns, batch)
        else:
            statement = self._renderSeqNumStatement(table, keyLength, columns, batch)
        self._writeText(f"\n-- {len(batch)} row{'' if len(batch) == 1 else 's'}\n{statement}")
        self._rowCounts[table] = self._rowCounts.get(table, 0) + len(batch)
        self._nStatements += 1
        self._batchKey = None
        self._batchRows = {}

    @staticmethod
    def _renderSetClause(columns: tuple[str, ...]) -> str:
        """Return the ``ON CONFLICT`` action updating ``columns``.

        Parameters
        ----------
        columns : `tuple` [`str`, ...]
            The non-key columns being written.

        Returns
        -------
        clause : `str`
            The action, starting with ``DO``.
        """
        if not columns:
            return "DO NOTHING"
        return "DO UPDATE SET\n" + ",\n".join(f"    {column} = EXCLUDED.{column}" for column in columns)

    def _renderSeqNumStatement(
        self,
        table: str,
        keyLength: int,
        columns: tuple[str, ...],
        batch: list[tuple[ObsId, dict[str, str]]],
    ) -> str:
        """Render an upsert for rows keyed by ``day_obs`` and ``seq_num``,
        and ``detector`` if ``keyLength`` is 3.

        Parameters
        ----------
        table : `str`
            The schema-qualified table name.
        keyLength : `int`
            2 for ``(day_obs, seq_num)`` keys, 3 to include ``detector``.
        columns : `tuple` [`str`, ...]
            The non-key columns, the same for every row.
        batch : `list` [`tuple` [`ObsId`, `dict` [`str`, `str`]]]
            The rows, as ``(obsId, values)``, the values as SQL literals.

        Returns
        -------
        statement : `str`
            The ``INSERT`` statement.
        """
        keyColumns = _SEQ_NUM_KEY if keyLength == 2 else _DETECTOR_KEY
        valueRows = []
        for obsId, row in batch:
            assert isinstance(obsId, tuple)
            literals = [_toSqlLiteral(part) for part in obsId] + [row[c] for c in columns]
            valueRows.append(f"    ({', '.join(literals)})")
        return (
            f"INSERT INTO {table} ({', '.join(keyColumns + columns)})\n"
            "VALUES\n" + ",\n".join(valueRows) + "\n"
            f"ON CONFLICT ({', '.join(keyColumns)}) {self._renderSetClause(columns)};\n"
        )

    def _renderCcdVisitStatement(
        self,
        table: str,
        columns: tuple[str, ...],
        batch: list[tuple[ObsId, dict[str, str]]],
    ) -> str:
        """Render an upsert for rows keyed by ``ccdvisit_id``.

        Parameters
        ----------
        table : `str`
            The schema-qualified ``ccdvisit`` table name.
        columns : `tuple` [`str`, ...]
            The non-key columns, the same for every row.
        batch : `list` [`tuple` [`ObsId`, `dict` [`str`, `str`]]]
            The rows, as ``(ccdvisit_id, values)``, the values as SQL
            literals.

        Returns
        -------
        statement : `str`
            The ``INSERT ... SELECT`` statement.
        """
        schema = table.split(".")[0]
        valueRows = []
        for obsId, row in batch:
            literals = [_toSqlLiteral(obsId)] + [row[c] for c in columns]
            valueRows.append(f"    ({', '.join(literals)})")
        insertColumns = _DETECTOR_KEY + ("ccdvisit_id",) + columns
        selectColumns = [f"c.{c}" for c in _DETECTOR_KEY] + [f"v.{c}" for c in ("ccdvisit_id",) + columns]
        return (
            f"INSERT INTO {table} ({', '.join(insertColumns)})\n"
            f"SELECT {', '.join(selectColumns)}\n"
            "FROM (VALUES\n" + ",\n".join(valueRows) + "\n"
            f") AS v ({', '.join(('ccdvisit_id',) + columns)})\n"
            f"JOIN {schema}.ccdexposure AS c ON c.ccdexposure_id = v.ccdvisit_id\n"
            f"ON CONFLICT ({', '.join(_DETECTOR_KEY)}) {self._renderSetClause(('ccdvisit_id',) + columns)};\n"
        )
