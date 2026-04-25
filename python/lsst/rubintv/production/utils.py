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

"""Small utility helpers that operate on middleware objects.

These functions all take LSST middleware types as input (``Exposure``,
``ExposureSummaryStats``, ``DimensionRecord``, ``DataCoordinate``) but do
not themselves need a ``Butler`` to do their work — they just pick values
out of, or summarise, the objects they are handed. Grouping them here
keeps ``butlerQueries`` honest about containing only functions that
actually query a Butler.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np

from lsst.daf.butler import DataCoordinate, DimensionRecord

if TYPE_CHECKING:
    from lsst.afw.image import Exposure, ExposureSummaryStats


__all__ = [
    "summaryStatsToDict",
    "getAirmass",
    "getExpIdOrVisitId",
]


def summaryStatsToDict(stats: ExposureSummaryStats) -> dict[str, Any]:
    """Return a dictionary of summary statistics.

    Parameters
    ----------
    stats : `ExposureSummaryStats`
        The summary statistics object to convert to a dictionary.

    Returns
    -------
    statsDict : `dict`
        A dictionary containing the summary statistics, with keys as attribute
        names and values as the corresponding attribute values.
    """
    return {
        attr: getattr(stats, attr)
        for attr in dir(stats)
        if not attr.startswith("_") and not callable(getattr(stats, attr))
    }


def getAirmass(exp: Exposure) -> float | None:
    """Get the airmass of an exposure if available and finite, else None.

    Parameters
    ----------
    exp : `lsst.afw.image.Exposure`
        The exposure to get the airmass for.

    Returns
    -------
    airmass : `float` or `None`
        The airmass of the exposure if available and finite, else None.
    """
    vi = exp.info.getVisitInfo()
    airmass = vi.boresightAirmass
    if airmass is not None and np.isfinite(airmass):
        return float(airmass)
    return None


def getExpIdOrVisitId(obj: DimensionRecord | DataCoordinate) -> int:
    """Get the exposure ID or visit ID from an exposure record or data ID.

    Parameters
    ----------
    obj : `lsst.daf.butler.DimensionRecord` or
            `lsst.daf.butler.DataCoordinate`
        The record or data ID to get the ID from.

    Returns
    -------
    id : `int`
        The exposure ID if available, else the visit ID.
    """
    if isinstance(obj, DimensionRecord):
        return obj.id

    if obj.hasRecords():
        if "exposure" in obj.records:
            record = obj.records["exposure"]
            assert record is not None
            return record.id
        if "visit" in obj.records:
            record = obj.records["visit"]
            assert record is not None
            return record.id
    else:
        if "visit" in obj:
            return int(obj["visit"])
        elif "exposure" in obj:
            return int(obj["exposure"])
    raise ValueError(f"{obj} does not contain an exposure or visit ID")
