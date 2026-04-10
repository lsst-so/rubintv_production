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

These helpers used to live in `utils.py`. They are grouped here because
they all need a Butler (or an `Exposure` / `ExposureSummaryStats` /
`DimensionRecord` produced by one) to do their job, so they cannot be
exercised in a unit test without either a real Butler or a substantial
mock. The integration suite under `tests/ci/` covers them end-to-end.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import numpy as np

from lsst.daf.butler import Butler, DataCoordinate, DimensionGroup, DimensionRecord
from lsst.resources import ResourcePath

from .predicates import runningScons

if TYPE_CHECKING:
    from logging import Logger

    from lsst.afw.image import Exposure, ExposureSummaryStats

    from .locationConfig import LocationConfig


__all__ = [
    "getNumExpectedItems",
    "removeDetector",
    "summaryStatsToDict",
    "getAirmass",
    "getExpIdOrVisitId",
    "getExpRecordFromVisitRecord",
    "getVisitRecordFromExpRecord",
    "getExpRecordFromId",
    "getCurrentOutputRun",
    "getEquivalentDataId",
]


def getNumExpectedItems(expRecord: DimensionRecord, logger: Logger | None = None) -> int:
    """A placeholder function for getting the number of expected items.

    For a given instrument, get the number of detectors which were read out or
    for which we otherwise expect to have data for.

    This method will be updated once we have a way of knowing, from the camera,
    how many detectors were actually read out (the plan is the CCS writes a
    JSON file with this info).

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record. This is currently unused, but will be used once
        we are doing this properly.
    logger : `logging.Logger`
        The logger, created if not supplied.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    instrument = expRecord.instrument

    fallbackValue = None
    if instrument == "LATISS":
        fallbackValue = 1
    elif instrument == "LSSTCam":
        fallbackValue = 201
    elif instrument in ["LSST-TS8", "LSSTComCam", "LSSTComCamSim"]:
        fallbackValue = 9
    else:
        raise ValueError(f"Unknown instrument {instrument}")

    if instrument == "LSSTComCamSim":
        return fallbackValue  # it's always nine (it's simulated), and this will all be redone soon anyway

    resourcePath = (
        f"s3://rubin-sts/{expRecord.instrument}/{expRecord.day_obs}/{expRecord.obs_id}/"
        f"{expRecord.obs_id}_expectedSensors.json"
    )
    try:
        url = ResourcePath(resourcePath)
        jsonData = url.read()
        data = json.loads(jsonData)
        nExpected = len(data["expectedSensors"])
        if nExpected != fallbackValue:
            # not a warning because this is it working as expected, but it's
            # nice to see when we have a partial readout
            logger.debug(
                f"Partial focal plane readout detected: expected number of items ({nExpected}) "
                f" is different from the nominal value of {fallbackValue} for {instrument}"
            )
        return nExpected
    except FileNotFoundError:
        if instrument in ["LSSTCam", "LSST-TS8"]:
            # these instruments are expected to have this info, the other are
            # not yet, so only warn when the file is expected and not found.
            logger.warning(
                f"Unable to get number of expected items from {resourcePath}, "
                f"using fallback value of {fallbackValue}"
            )
        return fallbackValue
    except Exception:
        logger.exception(
            "Error calculating expected number of items, using fallback value " f"of {fallbackValue}"
        )
        return fallbackValue


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
    """Get the exposure ID or visit ID from an exposure record.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord` or
                `lsst.daf.butler.DataCoordinate`
        The exposure record to get the ID from.

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
    if expRecord.can_see_sky is not True:
        raise ValueError("Cannot get visit record from non-sky exposure record")

    # this line *could* still fail if visits haven't been defined, but it now
    # never *should* as we've checked that it's on sky, so let that raise
    (visitRecord,) = butler.registry.queryDimensionRecords("visit", dataId=expRecord.dataId)
    return visitRecord


def getExpRecordFromId(expOrVisitId: int, instrument: str, butler: Butler) -> DimensionRecord:
    """Get the exposure record corresponding to a visit record.

    Parameters
    ----------
    expOrVisitId : `int``
        The exposure ID or visit ID to get the exposure record for.
    instrument : `str`
        The instrument name.
    butler : `lsst.daf.butler.Butler`
        The butler to use to retrieve the exposure record.

    Returns
    -------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record corresponding to the visit record.
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
    ignoreCiFlag : `bool`, optional
        If ``True``, ignore the CI environment flag when determining the output
        collection.

    Returns
    -------
    outputCollection : `str`
        The current output collection for the given instrument.
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
