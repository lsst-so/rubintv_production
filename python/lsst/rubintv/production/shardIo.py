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

"""Sharded data I/O — metadata and per-image data shard files.

The functions here implement the writer / reader / merger for the small
JSON shard files that workers drop into a shared directory and that the
TimedMetadataServer then merges and uploads. They used to live in
`utils.py` and are extracted here so the file logic is in one place and
testable independently of the rest of `utils.py`.

The constants `ALLOWED_DATASET_TYPES`, `SEQNUM_PADDING` and
`SHARDED_DATA_TEMPLATE` are part of the wire format and live alongside
the functions that produce them.
"""

from __future__ import annotations

import glob
import json
import logging
import os
import time
import uuid
from typing import TYPE_CHECKING, Any

from lsst.obs.lsst.translators.lsst import FILTER_DELIMITER

from .parsers import NumpyEncoder
from .predicates import isFileWorldWritable

if TYPE_CHECKING:
    from logging import Logger

    from lsst.daf.butler import DimensionRecord

    from .utils import LocationConfig


__all__ = [
    "ALLOWED_DATASET_TYPES",
    "SEQNUM_PADDING",
    "SHARDED_DATA_TEMPLATE",
    "writeMetadataShard",
    "writeExpRecordMetadataShard",
    "writeDataShard",
    "createFilenameForDataShard",
    "getShardedData",
    "getGlobPatternForShardedData",
    "getShardPath",
]


# ALLOWED_DATASET_TYPES is the types of data that can be written as sharded
# data. In principle, there is no reason this can't be or shouldn't be totally
# free-form, but because we need to delete any pre-existing data before
# processing starts, we maintain a list of allowed types so that we can loop
# over them and delete them. Having this list here and checking before writing
# ensures that the list is maintained, as new products can't be written without
# being added here first.
ALLOWED_DATASET_TYPES = ["rawNoises", "binnedImage"]
SEQNUM_PADDING = 6
SHARDED_DATA_TEMPLATE = os.path.join(
    "{path}", "dataShard-{dataSetName}-{instrument}-dayObs_{dayObs}" "_seqNum_{seqNum}_{suffix}.json"
)


def getGlobPatternForShardedData(
    path: str, dataSetName: str, instrument: str, dayObs: int, seqNum: int
) -> str:
    """Get a glob-style pattern for finding sharded data.

    These are the sharded data files used to store parts of the output data
    from the scatter part of the processing, ready to be gathered.

    Parameters
    ----------
    path : `str`
        The path find the sharded data.
    dataSetName : `str`
        The dataSetName to find the dataIds for, e.g. binnedImage.
    instrument : `str`
        The instrument to find the sharded data for.
    dayObs : `int`
        The dayObs to find the sharded data for.
    seqNum : `int`
        The seqNum to find the sharded data for.
    """
    seqNumFormatted = f"{seqNum:0{SEQNUM_PADDING}}" if seqNum != "*" else "*"
    return SHARDED_DATA_TEMPLATE.format(
        path=path,
        dataSetName=dataSetName,
        instrument=instrument,
        dayObs=dayObs,
        seqNum=seqNumFormatted,
        suffix="*",
    )


def writeMetadataShard(path: str, dayObs: int, mdDict: dict[int, dict[str, Any]]) -> None:
    """Write a piece of metadata for uploading to the main table.

    Parameters
    ----------
    path : `str`
        The path to write the file to.
    dayObs : `int`
        The dayObs.
    mdDict : `dict` of `dict`
        The metadata items to write, as a dict of dicts. Each key in the main
        dict should be a sequence number. Each value is a dict of values for
        that seqNum, as {'measurement_name': value}.

    Raises
    ------
    TypeError
        Raised if mdDict is not a dictionary.
    """
    if not isinstance(mdDict, dict):
        raise TypeError(f"mdDict must be a dict, not {type(mdDict)}")

    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)  # exist_ok True despite check to be concurrency-safe just in case

    suffix = uuid.uuid1()
    # the final filename pattern is relied upon elsewhere, so don't change it
    # without updating in at least the following places: mergeShardsAndUpload
    filename = os.path.join(path, f"metadata-dayObs_{dayObs}_{suffix}.json")
    tmpFilename = os.path.join(path, f"tmp-metadata-dayObs_{dayObs}_{suffix}.json")

    with open(tmpFilename, "w") as f:
        json.dump(mdDict, f, cls=NumpyEncoder)
    os.rename(tmpFilename, filename)
    try:
        if not isFileWorldWritable(tmpFilename):
            os.chmod(tmpFilename, 0o777)  # file may be deleted by another process, so make it world writable
    except FileNotFoundError:
        pass  # it was indeed deleted elsewhere, so just ignore
    return


def writeExpRecordMetadataShard(expRecord: DimensionRecord, metadataShardPath: str) -> None:
    """Write the exposure record metedata to a shard.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record.
    metadataShardPath : `str`
        The directory to write the shard to.
    """
    md = {}
    md["Exposure time"] = expRecord.exposure_time
    md["Darktime"] = expRecord.dark_time
    md["Image type"] = expRecord.observation_type
    md["Reason"] = expRecord.observation_reason
    md["Date begin"] = expRecord.timespan.begin.isot
    md["Program"] = expRecord.science_program
    md["Group name"] = expRecord.group
    md["Target"] = expRecord.target_name
    md["RA"] = expRecord.tracking_ra
    md["Dec"] = expRecord.tracking_dec
    md["Sky angle"] = expRecord.sky_angle
    md["Azimuth"] = expRecord.azimuth
    md["Zenith Angle"] = expRecord.zenith_angle if expRecord.zenith_angle else None
    md["Elevation"] = 90 - expRecord.zenith_angle if expRecord.zenith_angle else None
    md["Can see the sky?"] = f"{expRecord.can_see_sky}"
    if expRecord.can_see_sky is None:  # None is different to False, and means HeaderService/header problems
        md["_Can see the sky?"] = "bad"  # flag this cell as red as this should never happen

    if expRecord.instrument == "LATISS":
        filt, disperser = expRecord.physical_filter.split(FILTER_DELIMITER)
        md["Filter"] = filt
        md["Disperser"] = disperser
    else:
        md["Filter"] = expRecord.physical_filter

    seqNum = expRecord.seq_num
    dayObs = expRecord.day_obs
    shardData = {seqNum: md}
    writeMetadataShard(metadataShardPath, dayObs, shardData)


def writeDataShard(
    path: str, instrument: str, dayObs: int, seqNum: int, dataSetName: str, dataDict: dict[Any, Any]
) -> None:
    """Write some per-image data for merging later.

    Parameters
    ----------
    path : `str`
        The path to write to.
    instrument : `str`
        The instrument name, e.g. 'LSSTCam'.
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.
    dataSetName : `str`
        The name of the data, e.g. 'perAmpNoise'
    dataDict : `dict` of `dict`
        The data to write.

    Raises
    ------
    TypeError
        Raised if dataDict is not a dictionary.
    """
    if dataSetName not in ALLOWED_DATASET_TYPES:
        raise ValueError(
            f"dataSetName must be one of {ALLOWED_DATASET_TYPES}, not {dataSetName}. If you are"
            " trying to add a new one, simply add it to the list."
        )

    if not isinstance(dataDict, dict):
        raise TypeError(f"dataDict must be a dict, not {type(dataDict)}")

    if not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)  # exist_ok True despite check to be concurrency-safe just in case

    filename = createFilenameForDataShard(
        path=path,
        instrument=instrument,
        dataSetName=dataSetName,
        dayObs=dayObs,
        seqNum=seqNum,
    )

    with open(filename, "w") as f:
        json.dump(dataDict, f, cls=NumpyEncoder)
    if not isFileWorldWritable(filename):
        try:
            os.chmod(filename, 0o777)  # file may be deleted by another process, so make it world writable
        except FileNotFoundError:
            pass  # it was indeed deleted elsewhere, so just ignore
    return


def createFilenameForDataShard(path: str, dataSetName: str, instrument: str, dayObs: int, seqNum: int) -> str:
    """Get a filename to use for writing sharded data to.

    A filename is built from the SHARDED_DATA_TEMPLATE, with a random suffix
    added for sharding.

    Parameters
    ----------
    path : `str`
        The path to write to.
    dataSetName : `str`
        The name of the dataset, e.g. 'rawNoises'.
    instrument : `str`
        The name of the instrument, e.g. 'LSSTCam'.
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.

    Returns
    -------
    filename : `str`
        The filename to write the data to.
    """
    suffix = uuid.uuid1()
    seqNumFormatted = f"{seqNum:0{SEQNUM_PADDING}}"
    filename = SHARDED_DATA_TEMPLATE.format(
        path=path,
        dataSetName=dataSetName,
        instrument=instrument,
        dayObs=dayObs,
        seqNum=seqNumFormatted,
        suffix=suffix,
    )
    return filename


def getShardedData(
    path: str,
    instrument: str,
    dayObs: int,
    seqNum: int,
    dataSetName: str,
    nExpected: int,
    timeout: float,
    logger: Logger | None = None,
    deleteIfComplete: bool = True,
    deleteRegardless: bool = False,
) -> tuple[dict[Any, Any], int]:
    """Read back the sharded data for a given dayObs, seqNum, and dataset.

    Looks for ``nExpected`` files in the directory ``path``, merges their
    contents and returns the merged data. If ``nExpected`` files are not found
    after ``timeout`` seconds, the items which have been found within the time
    limit are merged and returned.

    Parameters
    ----------
    path : `str`
        The path to write to.
    instrument : `str`
        The name of the instrument, e.g. 'LSSTCam'.
    dayObs : `int`
        The dayObs.
    seqNum : `int`
        The seqNum.
    dataSetName : `str`
        The name of the data, e.g. 'perAmpNoise'
    nExpected : `int`
        The number of expected items to wait for. Once ``nExpected`` files have
        been collected, their contents are merged and returned immediately. If
        ``nExpected`` items are not found after ``timeout`` seconds, the items
        which have been found within the time limit are merged and returned.
    timeout : `float`
        The timeout period after which to give up waiting for files to land.
    logger : `logging.Logger`, optional
        The logger for logging warnings if files don't appear.
    deleteIfComplete : `bool`, optional
        Delete the input datafiles if there were the number expected?
    deleteRegardless : `bool`, optional
        If True, delete the files after reading them, regardless of whether the
        expected number of items were found.

    Returns
    -------
    data : `dict` of `dict`
        The merged data.
    nFiles : `int`
        The number of shard files found and merged into ``data``.

    Raises
    ------
    TypeError
        Raised if dataDict is not a dictionary.
    """
    pattern = getGlobPatternForShardedData(
        path=path, instrument=instrument, dataSetName=dataSetName, dayObs=dayObs, seqNum=seqNum
    )

    start = time.time()
    firstLoop = True
    files = []
    while firstLoop or (time.time() - start < timeout):
        firstLoop = False  # ensure we always run at least once
        files = glob.glob(pattern)
        if len(files) > nExpected:
            # it is ambiguous which to use to form a coherent set, so raise
            raise RuntimeError(f"Too many data files found for {dataSetName} for {dayObs=}-{seqNum=}")
        if len(files) == nExpected:
            break
        time.sleep(0.2)

    if len(files) != nExpected:
        if not logger:
            logger = logging.getLogger(__name__)
        logger.warning(
            f"Found {len(files)} files after waiting {timeout}s for {dataSetName}"
            f" for {dayObs=}-{seqNum=} but expected {nExpected}"
        )

    if not files:
        return {}, 0

    data = {}
    for dataShard in files:
        with open(dataShard) as f:
            shard = json.load(f)
        data.update(shard)
        if deleteRegardless or (deleteIfComplete and len(files) == nExpected):
            os.remove(dataShard)
    return data, len(files)


def getShardPath(locationConfig: LocationConfig, expRecord: DimensionRecord, isAos: bool = False) -> str:
    """Get the path to the metadata shard for the given exposure record.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to get the shard path for.

    Returns
    -------
    shardPath : `str`
        The path to write the metadata shard to.
    """
    match expRecord.instrument:
        case "LATISS":
            if isAos:
                raise ValueError("No AOS metadata for LATISS")
            return locationConfig.auxTelMetadataShardPath
        case "LSSTComCam":
            if isAos:
                return locationConfig.comCamAosMetadataShardPath
            return locationConfig.comCamMetadataShardPath
        case "LSSTComCamSim":
            if isAos:
                return locationConfig.comCamSimAosMetadataShardPath
            return locationConfig.comCamSimMetadataShardPath
        case "LSSTCam":
            if isAos:
                return locationConfig.lsstCamAosMetadataShardPath
            return locationConfig.lsstCamMetadataShardPath
        case _:
            raise ValueError(f"Unknown instrument {expRecord.instrument=}")
