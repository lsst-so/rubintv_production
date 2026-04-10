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

from __future__ import annotations

import glob
import io
import logging
import os
import pickle
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas as pd
from astropy.table import Column, MaskedColumn, Table
from galsim.zernike import zernikeRotMatrix
from tqdm import tqdm

from lsst.daf.butler import Butler, DatasetNotFoundError, DimensionRecord
from lsst.summit.utils.butlerUtils import getExpRecordFromDataId, getSeqNumsForDayObs, makeDefaultLatissButler
from lsst.summit.utils.consdbClient import getCcdVisitTableForDay, getWideQuicklookTableForDay
from lsst.summit.utils.dateTime import calcPreviousDay, dayObsIntToString, getCurrentDayObsInt
from lsst.summit.utils.efdUtils import getEfdData
from lsst.summit.utils.utils import computeCcdExposureId, setupLogging
from lsst.utils import getPackageDir
from lsst.utils.iteration import sequence_to_string

from .channels import CHANNELS, PREFIXES
from .consdbUtils import CCD_VISIT_MAPPING, ConsDBPopulator, changeType
from .formatters import FakeExposureRecord, expRecordToUploadFilename
from .locationConfig import LocationConfig
from .uploaders import Uploader

HAS_EFD_CLIENT = True
try:
    from lsst_efd_client import EfdClient
except ImportError:
    HAS_EFD_CLIENT = False

if TYPE_CHECKING:
    from lsst.summit.utils import ConsDbClient

    from .uploaders import MultiUploader

__all__ = [
    "getPlotSeqNumsForDayObs",
    "createChannelByName",
    "remakePlotByDataId",
    "remakeDay",
    "pushTestImageToCurrent",
    "remakeStarTrackerDay",
    "getDaysWithDataForPlotting",
    "getPlottingArgs",
    "syncBuckets",
    "checkConsDbContents",
]

# this file is for higher level utilities for use in notebooks, but also
# can be imported by other scripts and channels, especially the catchup ones.


@dataclass
class QuicklookTableResults:
    """Results from checking a ConsDB quicklook table."""

    table: Table
    """The full table from ConsDB."""
    nEntries: int
    """Number of entries in the table."""
    minSeqNum: int
    """Minimum seq_num in the table."""
    maxSeqNum: int
    """Maximum seq_num in the table."""
    exceedsButler: bool
    """Whether the max seq_num exceeds the butler's last seq_num."""
    missingSeqNums: list[int]
    """List of on-sky seq_nums missing from the table."""
    emptyColumns: list[str]
    """List of column names that are always empty."""
    missingOnSkyInputs: list[int]
    """List of on-sky seq_nums with no inputs (n_inputs is None or 0)."""


def getDaysWithDataForPlotting(path):
    """Get a list of the days for which we have data for prototyping plots.

    Parameters
    ----------
    path : `str`
        The path to look for data in.

    Returns
    -------
    days : `list` [`int`]
        The days for which we have data.
    """
    reportFiles = glob.glob(os.path.join(path, "report_*.pickle"))
    mdTableFiles = glob.glob(os.path.join(path, "dayObs_*.json"))
    ccdVisitTableFiles = glob.glob(os.path.join(path, "ccdVisitTable_*.pickle"))

    reportDays = [
        int(filename.removeprefix(path + "/report_").removesuffix(".pickle")) for filename in reportFiles
    ]
    mdDays = [
        int(filename.removeprefix(path + "/dayObs_").removesuffix(".json")) for filename in mdTableFiles
    ]
    ccdVisitDays = [
        int(filename.removeprefix(path + "/ccdVisitTable_").removesuffix(".pickle"))
        for filename in ccdVisitTableFiles
    ]

    days = set.intersection(set(reportDays), set(mdDays), set(ccdVisitDays))
    return list(days)


def getPlottingArgs(butler, path, dayObs):
    """Get the args which are passed to a night report plot.

    Checks if the data is available for the specified ``dayObs`` at the
    specified ``path``, and returns the args which are passed to the plot
    function. The ``butler`` is largely unused, but we must pass one to
    reinstantiate a NightReport.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler.
    path : `str`
        The path to look for data in.
    dayObs : `int`
        The dayObs to get the data for.

    Returns
    -------
    plottingArgs : `tuple`
        The args which are passed to the plot function. ``(report, mdTable,
        ccdVisitTable)`` A NightReport, a metadata table as a pandas dataframe,
        and a ccdVisit table.
    """
    from lsst.summit.utils import NightReport

    if dayObs not in getDaysWithDataForPlotting(path):
        raise ValueError(f"Data not available for {dayObs=} in {path}")

    reportFilename = os.path.join(path, f"report_{dayObs}.pickle")
    mdFilename = os.path.join(path, f"dayObs_{dayObs}.json")
    ccdVisitFilename = os.path.join(path, f"ccdVisitTable_{dayObs}.pickle")

    report = NightReport(butler, dayObs, reportFilename)

    mdTable = pd.read_json(mdFilename).T
    mdTable = mdTable.sort_index()

    with open(ccdVisitFilename, "rb") as input_file:
        ccdVisitTable = pickle.load(input_file)

    return report, mdTable, ccdVisitTable


def getPlotSeqNumsForDayObs(channel, dayObs, bucket=None):
    """Return the list of seqNums for which the plot exists in the bucket for
    the specified channel.

    Parameters
    ----------
    channel : `str`
        The channel.
    dayObs : `int`
        The dayObs.
    bucket : `google.cloud.storage.bucket.Bucket`, optional
        The GCS bucket, created if not supplied.

    Returns
    -------
    seqNums : `list` [`int`]
        Sorted list of ints of the seqNums for which the specified plot exists.

    Raises
    ------
    ValueError:
        Raised if the channel is unknown.
    """
    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}.")

    if not bucket:
        from google.cloud import storage

        client = storage.Client()
        # TODO: either make bucket a mandatory arg or take a locationConfig and
        # create it from bucketName
        bucket = client.get_bucket("rubintv_data")

    dayObsStr = dayObsIntToString(dayObs)

    prefix = f"{channel}/{PREFIXES[channel]}_dayObs_{dayObsStr}"
    blobs = list(bucket.list_blobs(prefix=prefix))
    existing = [int(b.name.split(f"{prefix}_seqNum_")[1].replace(".png", "")) for b in blobs]
    return sorted(existing)


def createChannelByName(location, instrument, channel, *, embargo=False, doRaise=False):
    """Create a RubinTV Channel object using the name of the channel.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    instrument : `str`
        The instrument, e.g. 'LATISS' or 'LSSTComCam'.
    channel : `str`
        The name of the channel, as found in lsst.rubintv.production.CHANNELS.
    embargo : `bool`, optional
        If True, use the embargo repo.
    doRaise : `bool`, optional
        Have the channel ``raise`` if errors are encountered while it runs.

    Returns
    -------
    channel : `lsst.rubintv.production.<Channel>`
        The lsst.rubintv.production Channel object.

    Raises
    ------
    ValueError:
        Raised if the channel is unknown, or creating by name is not supported
        for the channel in question.
    """
    from .rubinTv import (
        ImExaminerChannel,
        MetadataCreator,
        MonitorChannel,
        MountTorqueChannel,
        SpecExaminerChannel,
    )

    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}.")

    locationConfig = LocationConfig(location)

    match channel:
        case "summit_imexam":
            return ImExaminerChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "summit_specexam":
            return SpecExaminerChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "auxtel_mount_torques":
            return MountTorqueChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "auxtel_monitor":
            return MonitorChannel(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "auxtel_metadata":
            return MetadataCreator(
                locationConfig=locationConfig, instrument=instrument, embargo=embargo, doRaise=doRaise
            )
        case "all_sky_current":
            raise ValueError(f"{channel} is not a creatable by name.")
        case "all_sky_movies":
            raise ValueError(f"{channel} is not a creatable by name.")
        case _:
            raise ValueError(f"Unrecognized channel {channel}.")


def remakePlotByDataId(location, instrument, channel, dataId, embargo=False):
    """Remake the plot for the given channel for a single dataId.
    Reproduces the plot regardless of whether it exists. Raises on error.

    This method is very slow and inefficient for bulk processing, as it
    creates a Channel object for each plot - do *not* use in loops, use
    remakeDay() or write a custom scripts for bulk remaking.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    instrument : `str`
        The instrument, e.g. 'LATISS' or 'LSSTComCam'.
    channel : `str`
        The name of the channel.
    dataId : `dict`
        The dataId.
    embargo : `bool`, optional
        Use the embargo repo?
    """
    tvChannel = createChannelByName(location, instrument, channel, embargo=embargo, doRaise=True)
    expRecord = getExpRecordFromDataId(tvChannel.butler, dataId)
    tvChannel.callback(expRecord)


def remakeDay(
    location, instrument, channel, dayObs, *, remakeExisting=False, notebook=True, logger=None, embargo=False
):
    """Remake all the plots for a given day.

    Currently auxtel_metadata does not pull from the bucket to check what is
    in there, so remakeExisting is not supported.

    Parameters
    ----------
    location : `str`
        The location, for use with LocationConfig.
    instrument : `str`
        The instrument, e.g. 'LATISS' or 'LSSTComCam'.
    channel : `str`
        The name of the lsst.rubintv.production channel. The actual channel
        object is created internally.
    dayObs : `int`
        The dayObs.
    remakeExisting : `bool`, optional
        Remake all plots, regardless of whether they already exist in the
        bucket?
    notebook : `bool`, optional
        Is the code being run from within a notebook? Needed to correctly nest
        asyncio event loops in notebook-type environments.
    logger : `logging.Logger`, optional
        The logger to use, created if not provided.
    embargo : `bool`, optional
        Use the embargoed repo?

    Raises
    ------
    ValueError:
        Raised if the channel is unknown.
        Raised if remakeExisting is False and channel is auxtel_metadata.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    from google.cloud import storage

    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}")

    if remakeExisting is False and channel in ["auxtel_metadata"]:
        raise ValueError(
            f"Channel {channel} can currently only remake everything or nothing. "
            "If you would like to remake everything, please explicitly pass "
            "remakeExisting=True."
        )

    if notebook:
        # notebooks have their own eventloops, so this is necessary if the
        # function is being run from within a notebook type environment
        import nest_asyncio

        nest_asyncio.apply()
        setupLogging()

    client = storage.Client()
    locationConfig = LocationConfig(location)
    bucket = client.get_bucket(locationConfig.bucketName)
    butler = makeDefaultLatissButler(embargo=embargo)

    allSeqNums = set(getSeqNumsForDayObs(butler, dayObs))
    logger.info(f"Found {len(allSeqNums)} seqNums to potentially create plots for.")
    existing = set()
    if not remakeExisting:
        existing = set(getPlotSeqNumsForDayObs(channel, dayObs, bucket=bucket))
        nToMake = len(allSeqNums) - len(existing)
        logger.info(
            f"Found {len(existing)} in the bucket which will be skipped, " f"leaving {nToMake} to create."
        )

    toMake = sorted(allSeqNums - existing)
    if not toMake:
        logger.info(f"Nothing to do for {channel} on {dayObs}")
        return

    # doRaise is False because during bulk plot remaking we expect many fails
    # due to image types, short exposures, etc.
    tvChannel = createChannelByName(location, instrument, channel, doRaise=False, embargo=embargo)
    for seqNum in toMake:
        dataId = {"day_obs": dayObs, "seq_num": seqNum, "detector": 0}
        expRecord = getExpRecordFromDataId(butler, dataId)
        tvChannel.callback(expRecord)


def pushTestImageToCurrent(channel, bucketName, duration=15):
    """Push a test image to a channel to see if it shows up automatically.

    Leaves the test image in the bucket for ``duration`` seconds and then
    removes it. ``duration`` cannot be more than 60s, as test images should not
    be left in the bucket for long, and a minute should easily be long enough
    to see if things are working.

    NB: this function is designed for interactive use in notebooks and blocks
    for ``duration`` and then deletes the file.

    Parameters
    ----------
    channel : `str`
        The name of the lsst.rubintv.production channel. The actual channel
        object is created internally.
    bucketName : `str`
        The name of the GCS bucket to push the test image to.
    duration : `float`, optional
        The duration to leave the test image up for, in seconds.

    Raises
    ------
    ValueError: Raised when the channel is unknown or the channel does support
        test images being pushed to it, or is the requested duration for the
        test image to remain is too long (max of 60s).
    """
    # TODO: DM-43413 think about how you want the alternative to this to work
    # for S3 uploads, given there's a local and remote and many locations.
    # probably just want to use the MultiUploader auto magic, and then manually
    # set one of them to None if we don't want to use it, or something like
    # that. Will always have to be the remote which gets set to none, as local
    # is currently mandatory for the MultiUploader (though you could swap them)
    # in here if you wanted to sneakily use the same object.
    logger = logging.getLogger(__name__)

    from google.cloud import storage

    if channel not in CHANNELS:
        raise ValueError(f"Channel {channel} not in {CHANNELS}")
    if channel in [
        "auxtel_metadata",
        "auxtel_isr_runner",
        "all_sky_current",
        "all_sky_movies",
        "auxtel_movies",
    ]:
        raise ValueError(f"Pushing test data not supported for {channel}")
    if duration > 60:
        raise ValueError(f"Maximum time to leave test images in buckets is 60s, got {duration}")

    client = storage.Client()
    bucket = client.get_bucket(bucketName)
    prefix = f"{channel}/{PREFIXES[channel]}"
    blobs = list(bucket.list_blobs(prefix=prefix))

    logger.info(f"Found {len(blobs)} for channel {channel} in bucket")

    # names are like
    # 'auxtel_monitor/auxtel-monitor_dayObs_2021-07-06_seqNum_100.png'
    days = set([b.name.split(f"{prefix}_dayObs_")[1].split("_seqNum")[0] for b in blobs])
    days = [int(d.replace("-", "")) for d in days]  # days are like 2022-01-02
    recentDay = max(days)

    seqNums = getPlotSeqNumsForDayObs(channel, recentDay, bucket)
    newSeqNum = max(seqNums) + 1

    mockDataCoord = FakeExposureRecord(seq_num=newSeqNum, day_obs=recentDay)
    testCardFile = os.path.join(getPackageDir("rubintv_production"), "assets", "testcard_f.jpg")
    uploadAs = expRecordToUploadFilename(channel, mockDataCoord)
    uploader = Uploader(bucketName)

    logger.info(f"Uploading test card to {mockDataCoord} for channel {channel}")
    blob = uploader.googleUpload(channel, testCardFile, uploadAs, isLiveFile=True)

    logger.info(f"Upload complete, sleeping for {duration} for you to check...")
    time.sleep(duration)
    blob.delete()
    logger.info("Test card removed")


def remakeStarTrackerDay(
    *,
    dayObs,
    rootDataPath,
    outputRoot,
    metadataRoot,
    astrometryNetRefCatRoot,
    wide,
    remakeExisting=False,
    logger=None,
    forceMaxNum=None,
):
    """Remake all the star tracker plots for a given day.

    TODO: This needs updating post-refactor, but can wait for another ticket
    for now, as other work is required on the StarTracker side, and this will
    fit well with doing that.

    Parameters
    ----------
    dayObs : `int`
        The dayObs.
    rootDataPath : `str`
        The path at which to find the data, passed through to the channel.
    outputRoot : str``
        The path to write the results out to, passed through to the channel.
    metadataRoot : `str`
        The path to write metadata to, passed through to the channel.
    astrometryNetRefCatRoot : `str`
        The path to the astrometry.net reference catalogs. Do not include
        the /4100 or /4200, just the base directory.
    wide : `bool`
        Do this for the wide or narrow camera?
    remakeExisting : `bool`, optional
        Remake all plots, regardless of whether they already exist in the
        bucket?
    logger : `logging.Logger`, optional
        The logger.
    forceMaxNum : `int`
        Force the maximum seqNum to be this value. This is useful for remaking
        days from scratch or in full, rather than running as a catchup.
    """
    raise NotImplementedError("This needs updating post-refactor")
    from .starTracker import StarTrackerChannel, getRawDataDirForDayObs

    if not logger:
        logger = logging.getLogger("lsst.starTracker.remake")

    # doRaise is False because during bulk plot remaking we expect many fails
    tvChannel = StarTrackerChannel(
        wide=wide,
        rootDataPath=rootDataPath,
        metadataRoot=metadataRoot,
        outputRoot=outputRoot,
        astrometryNetRefCatRoot=astrometryNetRefCatRoot,
        doRaise=False,
    )

    _ifWide = "_wide" if wide else ""
    rawChannel = f"startracker{_ifWide}_raw"

    existing = getPlotSeqNumsForDayObs(rawChannel, dayObs)
    maxSeqNum = max(existing) if not forceMaxNum else forceMaxNum
    missing = [_ for _ in range(1, maxSeqNum) if _ not in existing]
    logger.info(f"Most recent = {maxSeqNum}, found {len(missing)} missing to create plots for: {missing}")

    dayPath = getRawDataDirForDayObs(rootDataPath=rootDataPath, wide=wide, dayObs=dayObs)

    files = glob.glob(os.path.join(dayPath, "*.fits"))
    foundFiles = {}
    for filename in files:
        # filenames are like GC101_O_20221114_000005.fits
        _, _, dayObs, seqNumAndSuffix = filename.split("_")
        seqNum = int(seqNumAndSuffix.removesuffix(".fits"))
        foundFiles[seqNum] = filename

    toRemake = missing if not remakeExisting else list(range(1, maxSeqNum))
    toRemake.reverse()  # always do the most recent ones first, burning down the list, not up

    for seqNum in toRemake:
        if seqNum not in foundFiles.keys():
            logger.warning(f"Failed to find raw file for {seqNum}, skipping...")
            continue
        filename = foundFiles[seqNum]
        logger.info(f"Processing {seqNum} from {filename}")
        tvChannel.callback(filename)


def syncBuckets(multiUploader: MultiUploader, locationConfig: LocationConfig) -> None:
    """Make sure all objects in the local bucket are also in the remote bucket.

    Call this function after a bad night to (slowly) send all the plots that
    didn't make it to USDF.

    Parameters
    ----------
    multiUploader : `MultiUploader`
        The multiUploader to use to sync the buckets.
    locationConfig : `LocationConfig`
        The location configuration to use, which contains the scratch path
        to exclude from the sync.
    """
    log = logging.getLogger(__name__)

    t0 = time.time()
    remoteBucket = multiUploader.remoteUploader._s3Bucket
    remoteObjects = set(o for o in remoteBucket.objects.all())
    log.info(f"Found {len(remoteObjects)} remote objects in {(time.time() - t0):.2f}s")

    t0 = time.time()
    localBucket = multiUploader.localUploader._s3Bucket
    localObjects = set(o for o in localBucket.objects.all())
    log.info(f"Found {len(localObjects)} local objects in {(time.time() - t0):.2f}s")

    # these are temp files, for local use only, and will be deleted in due
    # course anyway, so never sync the scratch area
    exclude = {o for o in localObjects if o.key.startswith(f"{locationConfig.scratchPath}")}

    remoteKeys = {o.key for o in remoteObjects}
    missing = {o for o in localObjects if o.key not in remoteKeys}
    missing -= exclude  # remove the scratch area from the missing list
    nMissing = len(missing)
    log.info(f"of which {nMissing} were missing from the remote. Copying missing items...")

    t0 = time.time()
    for i, obj in enumerate(missing):
        body = localBucket.Object(obj.key).get()["Body"].read()
        remoteBucket.Object(obj.key).upload_fileobj(io.BytesIO(body))
        del body
        if i % 100 == 0:
            log.info(f"Copied {i + 1} items of {len(missing)}, elapsed: {(time.time() - t0):.2f}s")

    log.info(f"Full copying took {(time.time() - t0):.2f} seconds")


def deleteAllSkyStills(bucket: Any) -> None:
    log = logging.getLogger(__name__)

    today = getCurrentDayObsInt()
    yesterday = calcPreviousDay(today)
    todayStr = dayObsIntToString(today)
    yesterdayStr = dayObsIntToString(yesterday)

    allSkyAll = [o for o in bucket.objects.filter(Prefix="allsky/")]
    stills = [o for o in allSkyAll if "still" in o.key]
    log.info(f"Found {len(stills)} all sky stills in total")
    filtered = [o for o in stills if todayStr not in o.key]
    filtered = [o for o in filtered if yesterdayStr not in o.key]
    log.info(f" of which {len(filtered)} are from before {yesterdayStr}")
    for i, obj in enumerate(filtered):
        obj.delete()
        if (i + 1) % 100 == 0:
            log.info(f"Deleted {i + 1} of {len(filtered)} stills...")
    log.info("Finished deleting stills")


def deleteNonFinalAllSkyMovies(bucket: Any) -> None:
    log = logging.getLogger(__name__)

    today = getCurrentDayObsInt()
    yesterday = calcPreviousDay(today)
    todayStr = dayObsIntToString(today)
    yesterdayStr = dayObsIntToString(yesterday)

    allSkyAll = [o for o in bucket.objects.filter(Prefix="allsky/")]
    movies = [o for o in allSkyAll if o.key.endswith(".mp4") and "final" not in o.key]
    log.info(f"Found {len(movies)} non-final all sky movies in total")
    filtered = [o for o in movies if todayStr not in o.key]
    filtered = [o for o in filtered if yesterdayStr not in o.key]
    log.info(f" of which {len(filtered)} are from before {yesterdayStr}")
    for i, obj in enumerate(filtered):
        obj.delete()
        if (i + 1) % 100 == 0:
            log.info(f"Deleted {i + 1} of {len(filtered)} non-final movies...")
    log.info("Finished deleting non-final movies")


def deleteConsDbColumn(client: ConsDbClient, instrument: str, table: str, column: str) -> None:
    """Delete all values in a column in a ConsDB table by setting them to NULL.

    Can only be used interactively, as it requires typing 'yes' to confirm.

    Parameters
    ----------
    client : `ConsDbClient`
        The ConsDbClient to use to connect to the database.
    instrument : `str`
        The instrument, e.g. 'latiss' or 'LSSTCam', case insensitive.
    table : `str`
        The table name, without the cdb_<instrument>. prefix.
    column : `str`
        The column name to delete values from.
    """
    inst = instrument.lower()
    table = table.lower()
    col = column.lower()

    print(f"Are you sure you want to clear the entire {col} column from cdb_{inst}.{table}?")
    print("This action cannot be undone. Type 'yes' to confirm.")
    response = input()
    if response != "yes":
        print("Aborted.")
        return

    query = (
        f"UPDATE cdb_{inst}.{table} SET {col} = NULL; "
        f"SELECT COUNT(*) FROM cdb_{inst}.{table} WHERE {col} IS NOT NULL;"
    )
    ret = client.query(query)
    print(f"{ret['count'].value[0]} rows remain with non-null values in {col}.")


def _isContentFree(col: Column) -> bool:
    """Check if a column contains no meaningful data.

    Parameters
    ----------
    col : `astropy.table.Column`
        The column to check.

    Returns
    -------
    isEmpty : `bool`
        True if the column is empty (all masked, None, or NaN).
    """
    if isinstance(col, MaskedColumn):
        return bool(col.mask.all())

    data = col.data

    if data.dtype == object:
        return all(v is None for v in data)

    if np.issubdtype(data.dtype, np.number):
        return np.isnan(data).all()

    return False


def checkVisitQuicklookTable(
    client: ConsDbClient, dayObs: int, onSkySeqNums: set[int], lastSeqNum: int
) -> QuicklookTableResults:
    """Check the visit1_quicklook table for a given dayObs.

    Parameters
    ----------
    client : `ConsDbClient`
        The ConsDB client.
    dayObs : `int`
        The dayObs to check.
    onSkySeqNums : `set` [`int`]
        Set of on-sky sequence numbers from the butler.
    lastSeqNum : `int`
        The last sequence number from the butler.

    Returns
    -------
    results : `QuicklookTableResults`
        Results of the visit1_quicklook table checks.
    """
    table = getWideQuicklookTableForDay(client, dayObs)
    seqNums: set[int] = set(table["seq_num"])

    missingSeqNums = sorted(onSkySeqNums - seqNums)

    contentFreeColumns = [name for name in table.colnames if _isContentFree(table[name])]
    emptyColumns = sorted(set([re.sub(r"_(min|max|median)$", "", c) for c in contentFreeColumns]))

    canSeeSkyMask = np.ma.filled(np.asarray(table["can_see_sky"]), False).astype(bool)

    tableOnSky = table[canSeeSkyMask]
    sentinelColumn = "n_inputs"
    col = tableOnSky[sentinelColumn]
    hasInputs = (col != None) & (col != 0)  # noqa: E711
    hasInputsSeqNums = set(tableOnSky[hasInputs]["seq_num"].tolist())
    missingOnSkyInputs = sorted(onSkySeqNums - hasInputsSeqNums)

    return QuicklookTableResults(
        table=table,
        nEntries=len(table),
        minSeqNum=int(min(seqNums)),
        maxSeqNum=int(max(seqNums)),
        exceedsButler=bool(table["seq_num"].max() > lastSeqNum),
        missingSeqNums=missingSeqNums,
        emptyColumns=emptyColumns,
        missingOnSkyInputs=missingOnSkyInputs,
    )


def checkCcdVisitQuicklookTable(
    client: ConsDbClient, dayObs: int, onSkySeqNums: set[int], lastSeqNum: int
) -> QuicklookTableResults:
    """Check the ccdvisit1_quicklook table for a given dayObs.

    Parameters
    ----------
    client : `ConsDbClient`
        The ConsDB client.
    dayObs : `int`
        The dayObs to check.
    onSkySeqNums : `set` [`int`]
        Set of on-sky sequence numbers from the butler.
    lastSeqNum : `int`
        The last sequence number from the butler.

    Returns
    -------
    results : `QuicklookTableResults`
        Results of the ccdvisit1_quicklook table checks.
    """
    table = getCcdVisitTableForDay(client, dayObs)
    seqNums: set[int] = set(table["seq_num"])

    missingSeqNums = sorted(onSkySeqNums - seqNums)

    emptyColumns = [name for name in table.colnames if _isContentFree(table[name])]

    # ccdvisit table is only expected for on-sky images, so this is just the
    # same as missingSeqNums for the ccdvisit table
    missingOnSkyInputs = missingSeqNums

    return QuicklookTableResults(
        table=table,
        nEntries=len(table),
        minSeqNum=int(min(seqNums)),
        maxSeqNum=int(max(seqNums)),
        exceedsButler=bool(max(seqNums) > lastSeqNum),
        missingSeqNums=missingSeqNums,
        emptyColumns=emptyColumns,
        missingOnSkyInputs=missingOnSkyInputs,
    )


def checkConsDbContents(butler: Butler, client: ConsDbClient, dayObs: int, verbose: bool = True) -> bool:
    """Check ConsDB contents for a given dayObs and report inconsistencies.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to query for exposure records.
    client : `ConsDbClient`
        The ConsDB client to query tables.
    dayObs : `int`
        The dayObs to check.

    Returns
    -------
    dayIsOk : `bool`
        True if no inconsistencies were found, False otherwise.
    """
    dayIsOk = True
    where = f"exposure.day_obs={dayObs} AND instrument='LSSTCam'"
    records = butler.query_dimension_records("exposure", where=where, order_by="-exposure.timespan.end")
    rd: dict[int, DimensionRecord] = {r.seq_num: r for r in records}
    assert len(rd) == len(records), "query_dimension_records returned duplicate seq_nums"

    allSeqNums = set(rd.keys())
    lastSeqNum = max(allSeqNums)
    onSkySeqNumsButler: set[int] = set(int(r.seq_num) for r in records if r.can_see_sky)

    if verbose:
        print(
            f"dayObs {dayObs} has:    {len(records)} records from {min(rd.keys())}-{lastSeqNum} "
            f"({sequence_to_string(list(onSkySeqNumsButler))} are on-sky)"
        )

    vResults = checkVisitQuicklookTable(client, dayObs, onSkySeqNumsButler, lastSeqNum)
    if verbose:
        print(
            f"visit1_quicklook table: {vResults.nEntries} entries from "
            f"{vResults.minSeqNum}-{vResults.maxSeqNum}"
        )
    if vResults.exceedsButler:
        print(f"🤯 ConsDB has max seq_num {vResults.maxSeqNum} greater than the butler's for {dayObs}!")
    if vResults.missingSeqNums and verbose:
        missing = vResults.missingSeqNums
        print(f"Missing within that range: ({len(missing)}): {sequence_to_string(missing)}")
    if vResults.emptyColumns and verbose:
        print(f"Always-empty columns (_min/median/max suffixes removed):\n {vResults.emptyColumns}")

    if vResults.missingOnSkyInputs:  # always enter this - it needs to print and set dayIsOk=False
        print(f"❌ On sky images with no entries: {sequence_to_string(vResults.missingOnSkyInputs)}")
        dayIsOk = False
    elif verbose:
        print("✅ No on-sky images with zero entries from visit1_quicklook table")

    cResults = checkCcdVisitQuicklookTable(client, dayObs, onSkySeqNumsButler, lastSeqNum)
    if verbose:
        print(
            f"\nccdvisit1_quicklook table: {cResults.nEntries} entries from seqNum "
            f"{cResults.minSeqNum}-{cResults.maxSeqNum}"
        )
    if cResults.missingSeqNums:  # always enter this - it needs to print and set dayIsOk=False
        missing = cResults.missingSeqNums
        print(f"❌ Missing within that range (all dets): ({len(missing)}): {sequence_to_string(missing)}")
        dayIsOk = False
    elif verbose:
        print("✅ No on-sky images with zero entries from ccdvisit1_quicklook table (any detector)")
    if cResults.emptyColumns and verbose:
        print(f"Empty columns: {cResults.emptyColumns}")

    return dayIsOk


def backfillVisit1QuicklookForDay(
    butler: Butler,
    populator: ConsDBPopulator,
    dayObs: int,
    efdClient: EfdClient,
) -> tuple[list[DimensionRecord], list[DimensionRecord]]:
    """Backfill the visit1_quicklook table for a given dayObs.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to query for exposure records.
    populator : `ConsDBPopulator`
        The ConsDBPopulator to use to populate the table.
    dayObs : `int`
        The dayObs to backfill.

    Returns
    -------
    rowsInserted : `list` [`DimensionRecord`]
        List of DimensionRecords which were successfully populated.
    noData : `list` [`DimensionRecord`]
        List of DimensionRecords which could not be populated due to missing
        data.
    """
    where = f"exposure.day_obs={dayObs} AND instrument='LSSTCam'"
    records = butler.query_dimension_records("exposure", where=where, order_by="-exposure.timespan.end")

    rowsInserted: list[DimensionRecord] = []
    noData: list[DimensionRecord] = []

    for record in tqdm(reversed(records), total=len(records), mininterval=60.0, ncols=120):
        try:
            populator.populateVisitRowWithButler(butler, record, True)
        except DatasetNotFoundError:
            noData.append(record)
            continue

        rowsInserted.append(record)

        data = getEfdData(efdClient, "lsst.sal.MTRotator.rotation", expRecord=record)
        if data.empty:
            continue
        physicalRotation = np.nanmean(data["actualPosition"])
        consDbValues = {"physical_rotator_angle": physicalRotation, "visit_id": record.id}
        populator.populateArbitrary(
            record.instrument,
            "visit1_quicklook",
            consDbValues,
            record.day_obs,
            record.seq_num,
            True,
        )

    return rowsInserted, noData


def backfillVisit1QuicklookForDayAos(
    butler: Butler, populator: ConsDBPopulator, dayObs: int
) -> tuple[list[DimensionRecord], list[DimensionRecord]]:
    """Backfill the visit1_quicklook table for a given dayObs.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to query for exposure records.
    populator : `ConsDBPopulator`
        The ConsDBPopulator to use to populate the table.
    dayObs : `int`
        The dayObs to backfill.

    Returns
    -------
    rowsInserted : `list` [`DimensionRecord`]
        List of DimensionRecords which were successfully populated.
    noData : `list` [`DimensionRecord`]
        List of DimensionRecords which could not be populated due to missing
        data.
    """
    from lsst.ts.ofc import OFCData
    from lsst.ts.wep.utils import convertZernikesToPsfWidth, makeDense

    ofcData = OFCData("lsst")

    where = f"exposure.day_obs={dayObs} AND instrument='LSSTCam'"
    records = butler.query_dimension_records("exposure", where=where, order_by="-exposure.timespan.end")

    rowsInserted: list[DimensionRecord] = []
    noData: list[DimensionRecord] = []

    for record in tqdm(reversed(records), total=len(records), mininterval=60.0, ncols=120):
        if not record.can_see_sky:
            continue
        try:
            zernikes = butler.get("aggregateZernikesAvg", visit=record.id)
        except DatasetNotFoundError:
            noData.append(record)
            continue

        rowSums = []

        # NOTE: this recipe is copied and pasted from
        # SingleCorePipelineRunner.postProcessAggregateZernikeTables - if
        # that recipe is updated, this needs to be updated too
        # TODO: refactor this for proper reuse and remove this note

        nollIndices = zernikes.meta["nollIndices"]
        maxNollIndex = np.max(zernikes.meta["nollIndices"])
        for row in zernikes:
            zkOcs = row["zk_deviation_OCS"]
            detector = row["detector"]
            zkDense = makeDense(zkOcs, nollIndices, maxNollIndex)
            zkDense -= ofcData.y2_correction[detector][: len(zkDense)]
            zkFwhm = convertZernikesToPsfWidth(zkDense)
            rowSums.append(np.sqrt(np.sum(zkFwhm**2)))

        average_result = np.nanmean(rowSums)
        residual = 1.06 * np.log(1 + average_result)  # adjustement per John Franklin's paper
        donutBlurFwhm = float("nan")  # needs to be defined for lower block but nans are removed on send
        if "estimatorInfo" in zernikes.meta and zernikes.meta["estimatorInfo"] is not None:
            # If danish is run then fwhm is in the metadata, if TIE then
            # it's not. danish models the width of the Kolmogorov profile
            # needed to convolve with the geometric donut model (the
            # optics) to match the donut. If AI_DONUT then "estimatorInfo"
            # might not be present.
            donutBlurFwhm = zernikes.meta["estimatorInfo"].get("fwhm")

        consDbValues = {"aos_fwhm": residual, "visit_id": record.id}
        if donutBlurFwhm:
            consDbValues["donut_blur_fwhm"] = donutBlurFwhm
        populator.populateArbitrary(
            record.instrument,
            "visit1_quicklook",
            consDbValues,
            record.day_obs,
            record.seq_num,
            True,  # insert into existing an row requires allowUpdate
        )
        rowsInserted.append(record)

    return rowsInserted, noData


def backfillCcdVisit1QuicklookForDay(
    butler: Butler, populator: ConsDBPopulator, dayObs: int
) -> tuple[dict[DimensionRecord, list[int]], list[DimensionRecord]]:
    """Backfill the visit1_quicklook table for a given dayObs.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to query for exposure records.
    populator : `ConsDBPopulator`
        The ConsDBPopulator to use to populate the table.
    dayObs : `int`
        The dayObs to backfill.

    Returns
    -------
    rowsInserted : `dict` [`DimensionRecord`, `list`[`int`]]
        Dictionary mapping DimensionRecord to list of detectors successfully
        populated for that record.
    noData : `list` [`DimensionRecord`]
        List of DimensionRecords which could not be populated due to missing
        data.
    """
    where = f"exposure.day_obs={dayObs} AND instrument='LSSTCam'"
    records = butler.query_dimension_records("exposure", where=where, order_by="-exposure.timespan.end")

    noData: list[DimensionRecord] = []
    rowsInserted: dict[DimensionRecord, list[int]] = {}

    table = "cdb_lsstcam.ccdvisit1_quicklook"
    schema = cast(dict[str, tuple[str, str]], populator.client.schema("lsstcam", "ccdvisit1_quicklook"))
    typeMapping: dict[str, str] = {k: v[0] for k, v in schema.items()}

    slowInserts = 0
    for i, record in enumerate(tqdm(reversed(records), total=len(records), mininterval=30.0, ncols=120)):

        try:
            visitSummary = butler.get("preliminary_visit_summary", visit=record.id)
        except DatasetNotFoundError:
            noData.append(record)
            continue

        visitSummary = visitSummary.asAstropy()

        t0 = time.time()  # deliberately time after the butler.get()
        for row in visitSummary:
            detNum = int(row["id"])
            obsId = computeCcdExposureId(record.instrument, record.id, detNum)

            values = {}
            for summaryKey, consDbKey in CCD_VISIT_MAPPING.items():
                typeFunc = changeType(consDbKey, typeMapping)
                values[consDbKey] = typeFunc(row[summaryKey])

            inserted = populator._insertIfAllowed(
                instrument=record.instrument,
                table=table,
                obsId=int(obsId),  # integer form required for ccd-type tables
                values=values,
                allowUpdate=True,
            )
            if inserted:
                if record not in rowsInserted:
                    rowsInserted[record] = []
                rowsInserted[record].append(detNum)

        insertTime = time.time() - t0  # time for all rows
        if insertTime > 12.5:
            slowInserts += 1
            time.sleep(30)  # give the DB some rest
            if slowInserts >= 3:
                print(f"Aborted after {i} inserts due to poor ConsDB performance")
                return rowsInserted, noData
        else:  # reset as soon as DB is performing well again
            slowInserts = 0

    return rowsInserted, noData


def backfillCcdVisit1QuicklookForDayAos(
    butler: Butler, populator: ConsDBPopulator, dayObs: int, efdClient: EfdClient
) -> tuple[dict[DimensionRecord, list[int]], dict[DimensionRecord, list[int]]]:
    """Backfill the visit1_quicklook table for a given dayObs.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler to query for exposure records.
    populator : `ConsDBPopulator`
        The ConsDBPopulator to use to populate the table.
    dayObs : `int`
        The dayObs to backfill.

    Returns
    -------
    rowsInserted : `dict` [`DimensionRecord`, `list`[`int`]]
        Dictionary mapping DimensionRecord to list of detectors successfully
        populated for that record.
    noData : `dict` [`DimensionRecord`, `list`[`int`]]
        Dictionary mapping DimensionRecord to list of detectors which could
        not be populated due to missing data.
    """
    from lsst.ts.wep.utils.zernikeUtils import makeDense

    where = f"exposure.day_obs={dayObs} AND instrument='LSSTCam'"
    records = butler.query_dimension_records("visit", where=where, order_by="-visit.id")

    detectors = (191, 192, 195, 196, 199, 200, 203, 204)

    rowsInserted: dict[DimensionRecord, list[int]] = {}
    noData: dict[DimensionRecord, list[int]] = {}

    slowInserts = 0
    for record in tqdm(reversed(records), total=len(records), mininterval=30.0, ncols=120):
        for detector in detectors:
            t0 = time.time()

            try:
                zkTable = butler.get("zernikes", visit=record.id, detector=detector)
            except DatasetNotFoundError:
                if record not in noData:
                    noData[record] = []
                noData[record].append(detector)
                continue

            # NOTE: this recipe is copied and pasted from
            # SingleCorePipelineRunner.postProcessCalcZernikes -
            # if that recipe is updated, this needs to be updated too

            # TODO: refactor this for proper reuse and remove this note
            MAX_NOLL_INDEX = 28

            data = getEfdData(efdClient, "lsst.sal.MTRotator.rotation", expRecord=record)
            physicalRotation = np.nanmean(data["actualPosition"])

            try:
                zkTable = zkTable[zkTable["label"] == "average"]
                zkColsHere = zkTable.meta["opd_columns"]
                nollIndicesHere = np.asarray(zkTable.meta["noll_indices"])
                # Grab Zernike values, convert to dense array, save
                zkSparse = zkTable[zkColsHere].to_pandas().values[0]
                zkDense = makeDense(zkSparse, nollIndicesHere, MAX_NOLL_INDEX)
                rotationMatrix = zernikeRotMatrix(MAX_NOLL_INDEX, -np.deg2rad(physicalRotation))
                # we only track z4 upwards and ConsDB only has slots for z4 to
                # z28
                zernikeValues: np.ndarray = zkDense / 1e3 @ rotationMatrix[4:, 4:]

                consDbValues: dict[str, float] = {}
                for i in range(len(zernikeValues)):  # these start at z4 and are dense so contain zeros
                    value = float(zernikeValues[i])  # make a real float for ConsDB
                    # skip the ones which were zero due to sparseness so
                    # they're null in the DB
                    if value == 0:
                        continue
                    consDbValues[f"z{i + 4}"] = float(zernikeValues[i])

                populator.populateCcdVisitRowZernikes(record, detector, consDbValues, allowUpdate=True)
            except IndexError:
                # ideally we wouldn't catch IndexError, but sometimes the
                # zernike table is empty and that raises IndexError when we
                # try to access the first row above
                if record not in noData:
                    noData[record] = []
                noData[record].append(detector)
                continue

            if record not in rowsInserted:
                rowsInserted[record] = []
            rowsInserted[record].append(detector)

            insertTime = time.time() - t0
            if insertTime > 2.5:
                slowInserts += 1
                time.sleep(30)  # give the DB some rest
                if slowInserts >= 3:
                    print(f"Aborted after {i} inserts due to poor ConsDB performance")
                    return rowsInserted, noData

            else:
                slowInserts = 0

    return rowsInserted, noData
