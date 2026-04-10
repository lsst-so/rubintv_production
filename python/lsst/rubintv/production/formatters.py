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

"""Filename, title, instrument and filter formatters used by the package.

These helpers used to live in `utils.py` and are mostly pure string-building
or small lookup-table functions. The two `makePlot*` helpers also touch the
filesystem (they create the parent directory) but the rest of the formatting
logic is independent and unit-testable.
"""

from __future__ import annotations

import itertools
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from lsst.summit.utils.dateTime import dayObsIntToString

from .channels import PREFIXES

if TYPE_CHECKING:
    from lsst.afw.cameraGeom import Camera
    from lsst.daf.butler import DimensionRecord

    from .utils import LocationConfig


__all__ = [
    "AOS_CCDS",
    "AOS_WORKER_MAPPING",
    "FakeExposureRecord",
    "expRecordToUploadFilename",
    "getRubinTvInstrumentName",
    "getFilterColorName",
    "mapAosWorkerNumber",
    "getPodWorkerNumber",
    "makePlotFile",
    "makePlotFileFromRecord",
    "makeWitnessDetectorTitle",
    "makeFocalPlaneTitle",
]


# AOS worker layout. The mapping flattens (depth, ccd) pairs from
# `itertools.product(range(9), AOS_CCDS)` so that a flat worker number can
# be turned back into a (depth, ccd) tuple by `mapAosWorkerNumber`.
AOS_CCDS = (191, 192, 195, 196, 199, 200, 203, 204)
AOS_WORKER_MAPPING = {n: (depth, ccd) for n, (depth, ccd) in enumerate(itertools.product(range(9), AOS_CCDS))}


@dataclass
class FakeExposureRecord:
    """A minimal dataclass for passing to expRecordToUploadFilename.

    expRecordToUploadFilename accesses seq_num and day_obs as properties rather
    than dict items, so this dataclass exists to allow using the same naming
    function when we do not have a real dataId.
    """

    seq_num: int
    day_obs: int

    def __repr__(self):
        return f"{{day_obs={self.day_obs}, seq_num={self.seq_num}}}"


def expRecordToUploadFilename(
    channel: str, expRecord: DimensionRecord | FakeExposureRecord, extension=".png", zeroPad=False
) -> str:
    """Convert an expRecord to a filename, for use when uploading to a channel.

    Names the file in way the frontend app expects, zeropadding the seqNum if
    ``zeroPad`` is set (because the All Sky Cam channel has zero-padded seqNums
    but others do not).

    Parameters
    ----------
    channel : `str`
        The name of the RubinTV channel.
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to convert to a filename.

    Returns
    -------
    filename : `str`
        The filename.
    """
    dayObs = expRecord.day_obs
    seqNum = expRecord.seq_num
    dayObsStr = dayObsIntToString(dayObs)
    seqNumStr = f"{seqNum:05}" if zeroPad else str(seqNum)
    filename = f"{PREFIXES[channel]}_dayObs_{dayObsStr}_seqNum_{seqNumStr}{extension}"
    return filename


def getRubinTvInstrumentName(instrument: str) -> str:
    """Get the RubinTV instrument name for a given instrument.

    Parameters
    ----------
    instrument : `str`
        The instrument name.

    Returns
    -------
    rubinTvInstrument : `str`
        The RubinTV instrument name.
    """
    instrument_map = {
        "LATISS": "auxtel",
        "LSSTCam": "lsstcam",
        "LSSTComCam": "comcam",
        "LSSTComCamSim": "comcam_sim",
    }
    rubinTvInstrument = instrument_map.get(instrument)
    if rubinTvInstrument is None:
        raise ValueError(f"Unknown instrument {instrument=}")
    return rubinTvInstrument


def getPodWorkerNumber() -> int:
    """Get the pod number from the environment or sys.argv.

    Returns
    -------
    workerNum : `int`
        The worker number.
    """
    workerName = os.getenv("WORKER_NAME")  # when using statefulSets
    if workerName:
        workerNum = int(workerName.split("-")[-1])
        print(f"Found WORKER_NAME={workerName} in the env, derived {workerNum=} from that")
        return workerNum
    else:
        # here for *forward* compatibility for next Kubernetes release
        workerNumFromEnv = os.getenv("WORKER_NUMBER")
        print(f"Found WORKER_NUMBER={workerNumFromEnv} in the env")
        if workerNumFromEnv is not None:
            workerNum = int(workerNumFromEnv)
        else:
            if len(sys.argv) < 2:
                raise RuntimeError(
                    "Must supply worker number either as WORKER_NUMBER env var or as a command line argument"
                )
            workerNum = int(sys.argv[2])

    return workerNum


def mapAosWorkerNumber(workerNum: int) -> tuple[int, int]:
    """Map the worker number to the AOS worker number.

    Parameters
    ----------
    workerNum : `int`
        The worker number.

    Returns
    -------
    depth : `int`
        The depth of the worker.
    detectorNum : `int`
        The detector number of the worker.
    """
    return AOS_WORKER_MAPPING[workerNum]


def getFilterColorName(physicalFilter: str) -> str | None:
    """Get the color name for a physical filter to color cells on RubinTV.

    If the color doesn't have a mapping, ``None`` is returned.

    Parameters
    ----------
    physicalFilter : `str`
        The physical filter name.

    Returns
    -------
    colorName : `str`
        The color name.
    """
    filterMap = {
        # ComCam filters:
        "u_02": "u_color",
        "g_01": "g_color",
        "r_03": "r_color",
        "i_06": "i_color",
        "z_03": "z_color",
        "y_04": "y_color",
        # LSSTCam filters:
        "ph_5": "white_color",  # pinhole filter
        "ef_43": "white_color",  # "empty" filter
        "u_24": "u_color",
        "g_6": "g_color",
        "r_57": "r_color",
        "i_39": "i_color",
        "z_20": "z_color",
        "y_10": "y_color",
    }
    return filterMap.get(physicalFilter)


def makePlotFileFromRecord(
    locationConfig: LocationConfig, record: DimensionRecord, plotType: str, suffix: str
) -> str:
    dayObs: int = record.day_obs
    seqNum: int = record.seq_num
    instrument: str = record.instrument
    return makePlotFile(locationConfig, instrument, dayObs, seqNum, plotType, suffix)


def makePlotFile(
    locationConfig: LocationConfig, instrument: str, dayObs: int, seqNum: int, plotType: str, suffix: str
) -> str:
    filename = (
        Path(locationConfig.plotPath)
        / instrument
        / str(dayObs)
        / f"{instrument}_{plotType}_dayObs_{dayObs}_seqNum_{seqNum:06}.{suffix}"
    )
    filename.parent.mkdir(mode=0o777, parents=True, exist_ok=True)
    # add a path.touch() here?
    return filename.as_posix()


def makeWitnessDetectorTitle(record: DimensionRecord, detector: int | str, camera: Camera) -> str:
    """Make a title for a plot based on the exp/visit record and detector.

    Parameters
    ----------
    record : `lsst.daf.butler.DimensionRecord`
        The exposure or visit record.
    detector : `int` or `str`
        The detector number or name.
    camera : `lsst.afw.cameraGeom.Camera`
        The camera object.

    Returns
    -------
    title : `str`
        The title for the plot.
    """
    d = camera[detector]  # gets the actual detector object. Camera supports indexing by name or numerical id
    detName = d.getName()
    detId = d.getId()
    r = record
    title = f"dayObs={r.day_obs} - seqNum={r.seq_num}\n"
    title += f"{detName}(#{detId}) {r.observation_type} image @ {r.exposure_time:.1f}s"
    return title


def makeFocalPlaneTitle(record: DimensionRecord) -> str:
    """Make a title for a plot based on the exp/visit record.

    Parameters
    ----------
    record : `lsst.daf.butler.DimensionRecord`
        The exposure or visit record.

    Returns
    -------
    title : `str`
        The title for the plot.
    """
    r = record
    title = f"dayObs={r.day_obs} - seqNum={r.seq_num}\n"
    title += f"{r.observation_type} image @ {r.exposure_time:.1f}s in filter {r.physical_filter}"
    return title
