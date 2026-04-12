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

"""Small Butler-touching helpers used throughout the package.

Every function here takes a ``Butler`` and uses its registry, collections
or dimensions to answer a question. Helpers that only operate on objects
produced by a Butler (``DimensionRecord``, ``DataCoordinate``, ``Exposure``,
``ExposureSummaryStats``) and don't need a ``Butler`` themselves live in
``utils.py`` instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from lsst.daf.butler import Butler, DataCoordinate, DimensionGroup, DimensionRecord

from .predicates import runningScons

if TYPE_CHECKING:
    from .locationConfig import LocationConfig


__all__ = [
    "removeDetector",
    "getExpRecordFromVisitRecord",
    "getVisitRecordFromExpRecord",
    "getExpRecordFromId",
    "getCurrentOutputRun",
    "getEquivalentDataId",
]


def removeDetector(dataCoord: DataCoordinate, butler: Butler) -> DataCoordinate:
    """Remove the detector from a DataCoordinate and return it in minimal form.

    Parameters
    ----------
    dataCoord : `DataCoordinate`
        The data coordinate to remove the detector from.
    butler : `Butler`
        The butler to get the dimensions from.

    Returns
    -------
    minimalDataCoord : `DataCoordinate`
        The data coordinate with the detector removed.
    """
    noDetector = {k: v for k, v in dataCoord.required.items() if k != "detector"}
    return DataCoordinate.standardize(noDetector, universe=butler.dimensions)


def getExpRecordFromVisitRecord(visitRecord: DimensionRecord, butler: Butler) -> DimensionRecord:
    """Get the exposure record corresponding to a visit record.

    Parameters
    ----------
    visitRecord : `lsst.daf.butler.DimensionRecord`
        The visit record to get the exposure record for.
    butler : `lsst.daf.butler.Butler`
        The butler to use to retrieve the exposure record.

    Returns
    -------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record corresponding to the visit record.
    """
    (expRecord,) = butler.registry.queryDimensionRecords("exposure", dataId=visitRecord.dataId)
    return expRecord


def getVisitRecordFromExpRecord(expRecord: DimensionRecord, butler: Butler) -> DimensionRecord:
    """Get the visit record corresponding to an exposure record.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to get the visit record for.
    butler : `lsst.daf.butler.Butler`
        The butler to use to retrieve the visit record.

    Returns
    -------
    visitRecord : `lsst.daf.butler.DimensionRecord`
        The visit record corresponding to the exposure record.
    """
    if expRecord.can_see_sky is not True:
        raise ValueError("Cannot get visit record from non-sky exposure record")

    # this line *could* still fail if visits haven't been defined, but it now
    # never *should* as we've checked that it's on sky, so let that raise
    (visitRecord,) = butler.registry.queryDimensionRecords("visit", dataId=expRecord.dataId)
    return visitRecord


def getExpRecordFromId(expOrVisitId: int, instrument: str, butler: Butler) -> DimensionRecord:
    """Get the exposure record for an exposure ID (or matching visit ID).

    Parameters
    ----------
    expOrVisitId : `int`
        The exposure ID or visit ID to get the exposure record for.
    instrument : `str`
        The instrument name.
    butler : `lsst.daf.butler.Butler`
        The butler to use to retrieve the exposure record.

    Returns
    -------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record matching the given identifier.
    """
    (expR,) = butler.registry.queryDimensionRecords("exposure", exposure=expOrVisitId, instrument=instrument)
    return expR


def getCurrentOutputRun(butler: Butler, locationConfig: LocationConfig, instrument: str) -> str | None:
    """Get the RUN name at the tip of the current output collection for the
    given instrument.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to use to retrieve the collection info.
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location configuration.
    instrument : `str`
        The instrument name.

    Returns
    -------
    outputCollection : `str` or `None`
        The current output collection for the given instrument, or ``None``
        when running under scons (where there is no real chain to query).
    """
    if runningScons():
        return None
    return butler.collections.get_info(locationConfig.getOutputChain(instrument)).children[0]


def getEquivalentDataId(
    butler: Butler,
    exposureDataId: DataCoordinate,
    dimensions: list[str] | DimensionGroup,
) -> DataCoordinate:
    """Construct a data ID by replacing or augmenting the 'exposure' dimension
    with some combination of the 'visit' and 'group' that should correspond to
    the same observation.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to use to retrieve the dimension records.
    exposureDataId : `lsst.daf.butler.DataCoordinate`
        The exposure data ID to get an equivalent data ID for.
    dimensions : `list` of `str` or `lsst.daf.butler.DimensionGroup`
        The dimensions to use to get the equivalent data ID.

    Returns
    -------
    equivalentDataId : `lsst.daf.butler.DataCoordinate`
        A data ID that is equivalent to the given exposure data ID.
    """
    if "exposure" not in exposureDataId:
        raise ValueError("Input data ID must contain an 'exposure' dimension")

    exposureDataId = butler.registry.expandDataId(exposureDataId)  # no-op if already expanded
    return butler.registry.expandDataId(
        exposureDataId,
        dimensions=dimensions,
        visit=exposureDataId["exposure"],
        group=exposureDataId["group"],
    )
