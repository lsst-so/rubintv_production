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
import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from functools import cached_property, wraps
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, Iterator

import numpy as np
import sentry_sdk
import yaml

from lsst.daf.butler import (
    Butler,
    DataCoordinate,
    DimensionGroup,
    DimensionRecord,
)
from lsst.obs.lsst.translators.lsst import FILTER_DELIMITER
from lsst.resources import ResourcePath
from lsst.utils import getPackageDir

from .parsers import NumpyEncoder
from .predicates import isFileWorldWritable, runningScons

if TYPE_CHECKING:
    from logging import Logger

    from lsst.afw.image import Exposure, ExposureSummaryStats


__all__ = [
    "setupSentry",
    "checkRubinTvExternalPackages",
    "writeMetadataShard",
    "writeDataShard",
    "getShardedData",
    "LocationConfig",
    "getAutomaticLocationConfig",
    "getNumExpectedItems",
    "getShardPath",
    "removeDetector",
    "ALLOWED_DATASET_TYPES",
    "logDuration",
    "timeFunction",
    "summaryStatsToDict",
]

EFD_CLIENT_MISSING_MSG = (
    "ImportError: lsst_efd_client not found. Please install with:\n" "    pip install lsst-efd-client"
)

GOOGLE_CLOUD_MISSING_MSG = (
    "ImportError: Google cloud storage not found. Please install with:\n"
    "    pip install google-cloud-storage"
)

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

# this file is for low level tools and should therefore not import
# anything from elsewhere in the package, this is strictly for importing from
# only.


def setupSentry() -> None:
    """Set up sentry"""
    sentry_sdk.init()
    client = sentry_sdk.get_client()  # never None, but inactive if failing to initialize
    if not client.is_active() or client.dsn is None:
        logger = logging.getLogger(__name__)
        logger.warning("Sentry DSN not found or client inactive — events will not be reported")


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


@dataclass(frozen=True)
class LocationConfig:
    """A frozen dataclass for holding location-based configurations.

    Note that all items which are used as paths *must* be decorated with
    @cached_property, otherwise they are will be method-type rather than
    str-type when they are accessed.
    """

    location: str
    log: logging.Logger = logging.getLogger("lsst.rubintv.production.utils.LocationConfig")

    def __post_init__(self) -> None:
        # Touch the _config after init to make sure the config file can be
        # read.
        # Any essential items can be touched here, but note they must all
        # exist in all the different locations, otherwise it will fail in some
        # locations and not others, so add things with caution.
        self._config
        self.plotPath

    def _checkDir(self, dirName: str, createIfMissing=True) -> None:
        """Check that a directory exists, optionally creating if it does not.

        Parameters
        ----------
        dirName : `str`
            The directory to check.
        createIfMissing : `bool`, optional
            If True, create the directory if it does not exist.

        Raises
            RuntimeError: raised if the directory does not exist and could not
            be created.

        TODO: Add a check for being world-writable here, and make the dir
        creation chmod to 777.
        """
        if not os.path.isdir(dirName):
            if createIfMissing:
                self.log.info(f"Directory {dirName} does not exist, creating it.")
                os.makedirs(dirName, exist_ok=True)  # exist_ok necessary due to potential startup race

                if not os.access(dirName, os.W_OK):  # check if dir is 777 but only when creating
                    try:  # attempt to chmod to 777
                        os.chmod(dirName, 0o777)
                    except PermissionError:
                        raise RuntimeError(
                            f"Directory {dirName} is not world-writable " "and could not be made so."
                        )

        # check whether we succeeded
        if not os.path.isdir(dirName):
            msg = f"Directory {dirName} does not exist"
            if createIfMissing:
                msg += " and could not be created."
            raise RuntimeError(msg)

    def _checkFile(self, filename):
        """Check that a file exists.

        Parameters
        ----------
        filename : `str`
            The file to check.

        Raises
            RuntimeError: raised if the file does not exist.
        """
        expanded = os.path.expandvars(filename)
        if not os.path.isfile(expanded):
            raise RuntimeError(f"Could not find file {filename} at {expanded}")

    @cached_property
    def _config(self):
        return _loadConfigFile(self.location)

    @cached_property
    def dimensionUniverseFile(self):
        file = self._config["dimensionUniverseFile"]
        return file

    @cached_property
    def scratchPath(self) -> str:
        """The scratch path for the location."""
        getShardPath = self._config["scratchPath"]
        return getShardPath

    @cached_property
    def auxtelButlerPath(self):
        return self._config["auxtelButlerPath"]

    @cached_property
    def ts8ButlerPath(self):
        file = self._config["ts8ButlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def botButlerPath(self):
        file = self._config["botButlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def metadataPath(self):
        directory = self._config["metadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def auxTelMetadataPath(self):
        directory = self._config["auxTelMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def auxTelMetadataShardPath(self):
        directory = self._config["auxTelMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def ts8MetadataPath(self):
        directory = self._config["ts8MetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def ts8MetadataShardPath(self):
        directory = self._config["ts8MetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def plotPath(self):
        directory = self._config["plotPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def bucketName(self):
        if self._config["bucketName"] == "":
            raise RuntimeError("Bucket name not set in config file")
        return self._config["bucketName"]

    @cached_property
    def binning(self):
        return self._config["binning"]

    @cached_property
    def consDBURL(self):
        return self._config["consDBURL"]

    # start of the summit migration stuff:
    # star tracker paths
    @cached_property
    def starTrackerDataPath(self) -> str:
        directory = self._config["starTrackerDataPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    @cached_property
    def starTrackerMetadataPath(self) -> str:
        directory = self._config["starTrackerMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def starTrackerMetadataShardPath(self) -> str:
        directory = self._config["starTrackerMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def starTrackerOutputPath(self) -> str:
        directory = self._config["starTrackerOutputPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def astrometryNetRefCatPath(self) -> str:
        directory = self._config["astrometryNetRefCatPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    # animation paths
    @cached_property
    def moviePngPath(self) -> str:
        directory = self._config["moviePngPath"]
        self._checkDir(directory)
        return directory

    # all sky cam paths
    @cached_property
    def allSkyRootDataPath(self) -> str:
        directory = self._config["allSkyRootDataPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    @cached_property
    def allSkyOutputPath(self) -> str:
        directory = self._config["allSkyOutputPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def nightReportPath(self) -> str:
        directory = self._config["nightReportPath"]
        self._checkDir(directory)
        return directory

    # ComCam stuff:
    @cached_property
    def comCamButlerPath(self) -> str:
        file = self._config["comCamButlerPath"]
        return file

    @cached_property
    def comCamMetadataPath(self) -> str:
        directory = self._config["comCamMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamMetadataShardPath(self) -> str:
        directory = self._config["comCamMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimMetadataPath(self) -> str:
        directory = self._config["comCamSimMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimMetadataShardPath(self) -> str:
        directory = self._config["comCamSimMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimAosMetadataPath(self) -> str:
        directory = self._config["comCamSimAosMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimAosMetadataShardPath(self) -> str:
        directory = self._config["comCamSimAosMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamAosMetadataPath(self) -> str:
        directory = self._config["comCamAosMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamAosMetadataShardPath(self) -> str:
        directory = self._config["comCamAosMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamAosMetadataPath(self) -> str:
        directory = self._config["lsstCamAosMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamAosMetadataShardPath(self) -> str:
        directory = self._config["lsstCamAosMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def raPerformanceDirectory(self) -> str:
        directory = self._config["raPerformanceDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def raPerformanceShardsDirectory(self) -> str:
        directory = self._config["raPerformanceShardsDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def guiderDirectory(self) -> str:
        directory = self._config["guiderDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def guiderShardsDirectory(self) -> str:
        directory = self._config["guiderShardsDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def botMetadataPath(self) -> str:
        directory = self._config["botMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def botMetadataShardPath(self) -> str:
        directory = self._config["botMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamMetadataPath(self) -> str:
        directory = self._config["lsstCamMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamMetadataShardPath(self) -> str:
        directory = self._config["lsstCamMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamButlerPath(self) -> str:
        directory = self._config["lsstCamButlerPath"]
        return directory

    # TMA config:
    @cached_property
    def tmaMetadataPath(self) -> str:
        directory = self._config["tmaMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def tmaMetadataShardPath(self) -> str:
        directory = self._config["tmaMetadataShardPath"]
        self._checkDir(directory)
        return directory

    def getOutputChain(self, instrument: str) -> str:
        return self._config["outputChains"][instrument]

    def getSfmPipelineFile(self, instrument: str) -> str:
        return self._config["sfmPipelineFile"][instrument]

    @cached_property
    def aosLSSTCamPipelineFileDanish(self) -> str:
        return self._config["aosLSSTCamPipelineFileDanish"]

    @cached_property
    def aosLSSTCamPipelineFileTie(self) -> str:
        return self._config["aosLSSTCamPipelineFileTie"]

    @cached_property
    def aosLSSTCamFullArrayModePipelineFileDanish(self) -> str:
        return self._config["aosLSSTCamFullArrayModePipelineFileDanish"]

    @cached_property
    def aosLSSTCamFullArrayModePipelineFileTie(self) -> str:
        return self._config["aosLSSTCamFullArrayModePipelineFileTie"]

    @cached_property
    def aosDataDir(self) -> str:
        return self._config["aosDataDir"]

    @cached_property
    def aosLSSTCamAiDonutPipelineFile(self) -> str:
        return self._config["aosLSSTCamAiDonutPipelineFile"]

    @cached_property
    def aosLSSTCamTartsPipelineFile(self) -> str:
        return self._config["aosLSSTCamTartsPipelineFile"]

    @cached_property
    def aosLSSTCamUnpairedDanishPipelineFile(self) -> str:
        return self._config["aosLSSTCamUnpairedDanishPipelineFile"]

    @cached_property
    def aosLSSTCamWcsDanishBin1PipelineFile(self) -> str:
        return self._config["aosLSSTCamWcsDanishBin1PipelineFile"]

    @cached_property
    def aosLSSTCamWcsDanishBin2PipelineFile(self) -> str:
        return self._config["aosLSSTCamWcsDanishBin2PipelineFile"]


def getAutomaticLocationConfig() -> LocationConfig:
    """Get a location config, based on RA location and command line args.

    If no command line args have been supplied, get the LocationConfig based on
    where the code is being run. If a command line arg was supplied, use that
    as an override value.

    Returns
    -------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration.
    """
    if len(sys.argv) >= 2:
        try:  # try using this, because anything could be in argv[1]
            location = sys.argv[1]
            return LocationConfig(location.lower())
        except FileNotFoundError:
            pass

    location = os.getenv("RAPID_ANALYSIS_LOCATION", "")
    if not location:
        raise RuntimeError("No location was supplied on the command line or via RAPID_ANALYSIS_LOCATION.")
    return LocationConfig(location.lower())


def _loadConfigFile(site: str) -> dict[str, str]:
    """Get the site configuration, given a site name.

    Parameters
    ----------
    site : `str`, optional
        The site. If not provided, the default is 'summit'.

    Returns
    -------
    config : `dict`
        The configuration, as a dict.
    """
    packageDir = getPackageDir("rubintv_production")
    configFile = os.path.join(packageDir, "config", f"config_{site}.yaml")
    with open(configFile, "rb") as f:
        config = yaml.safe_load(f)
    return config


def checkRubinTvExternalPackages(exitIfNotFound: bool = True, logger: Logger | None = None) -> None:
    """Check whether the prerequsite installs for RubinTV are present.

    Some packages which aren't distributed with any metapackage are required
    to run RubinTV. This function is used to check if they're present so
    that unprotected imports don't cause the package to fail to import. It also
    allows checking in a single place, given that all are necessary for
    RubinTV's running.

    Parameters
    ----------
    exitIfNotFound : `bool`
        Terminate execution if imports are not present? Useful in bin scripts.
    logger : `logging.Logger`, optional
        The logger used to warn if packages are not present.
    """
    if not logger:
        logger = logging.getLogger(__name__)

    hasGoogleStorage = False
    hasEfdClient = False
    try:
        from google.cloud import storage  # noqa: F401

        hasGoogleStorage = True
    except ImportError:
        pass

    try:
        from lsst_efd_client import EfdClient  # noqa: F401

        hasEfdClient = True
    except ImportError:
        pass

    if not hasGoogleStorage:
        logger.warning(GOOGLE_CLOUD_MISSING_MSG)

    if not hasEfdClient:
        logger.warning(EFD_CLIENT_MISSING_MSG)

    if exitIfNotFound and (not hasGoogleStorage or not hasEfdClient):
        exit()


def catchPrintOutput(functionToCall: Callable, *args, **kwargs) -> str:
    f = io.StringIO()
    with redirect_stdout(f):
        functionToCall(*args, **kwargs)
    return f.getvalue()


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


@dataclass
class DurationResult:
    duration: float | None = None


@contextmanager
def logDuration(logger: Logger, label: str) -> Iterator[DurationResult]:
    """Context manager to log the duration of a block of code.

    Example usage::

        with logDuration(log, "this block of code") as timing:
            doSomething()
        duration = timing.duration

    This will log the time taken to execute the block of code with the label
    message "<loggerName>.info this block of code took 1.23s" and return 1.23
    as the duration attribute of the yielded object.

    Parameters
    ----------
    logger : `logging.Logger`
        The logger to use for logging the duration.
    label : `str`
        A label for the block of code being timed, used in the log message.

    Returns
    -------
    result : `DurationResult`
        A context manager that returns a ``DurationResult`` when entered.
    """
    start = perf_counter()
    result = DurationResult()
    try:
        yield result
    finally:
        result.duration = perf_counter() - start
        logger.info("%s took %.3fs", label, result.duration)


def timeFunction(logger: Logger) -> Callable:
    """Decorator to log the duration of a function call.

    Example usage:
    @timeFunc(logger)
    def my_function():
        doSomething()

    This will log the time taken to execute the function with the label
    message "<loggerName>.info my_function took 1.23s".

    Parameters
    ----------
    logger : `logging.Logger`
        The logger to use for logging the duration of the function call.
    """

    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                logger.info("%s took %.3fs", func.__qualname__, (perf_counter() - start))

        return wrapper

    return decorate


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
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
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
