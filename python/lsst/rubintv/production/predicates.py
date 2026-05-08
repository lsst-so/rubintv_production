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

"""Small predicate helpers used throughout the package.

This module collects the pure record/environment/file predicates and the
two error-control helpers (`raiseIf` / `getDoRaise`) that used to live in
`utils.py`. They are grouped here because they are all small, self-contained,
and easy to test in isolation — see `tests/test_utils.py` for the unit
tests pinning their behaviour.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import numpy as np
import sentry_sdk

from lsst.summit.utils.dateTime import getCurrentDayObsInt

if TYPE_CHECKING:
    from logging import Logger

    from lsst.daf.butler import DimensionRecord
    from lsst.pipe.base import PipelineGraph


__all__ = [
    "raiseIf",
    "getDoRaise",
    "isDayObsContiguous",
    "hasDayRolledOver",
    "isCalibration",
    "isWepImage",
    "hasRaDec",
    "isFileWorldWritable",
    "isFamPipeline",
    "runningCI",
    "runningScons",
    "runningPyTest",
]


def raiseIf(doRaise: bool, error: Exception, logger: Logger, msg: str = "") -> None:
    """Raises the error if ``doRaise`` otherwise logs it as a warning.

    Parameters
    ----------
    doRaise : `bool`
        Raise the error if True, otherwise logs it as a warning.
    error : `Exception`
        The error that has been raised.
    logger : `logging.Logger`
        The logger to warn with if ``doRaise`` is False.
    msg : `str`, optional
        Additional error message to log with the error.

    Raises
    ------
    Exception
        Re-raises ``error`` if ``doRaise`` is True, otherwise swallows and
        warns.
    """
    sentry_sdk.capture_exception(error)
    if not msg:
        msg = f"{error}"
    if doRaise:
        logger.exception(msg)
        raise error
    else:
        logger.exception(msg)


def getDoRaise() -> bool:
    """Get the value of ``RAPID_ANALYSIS_DO_RAISE`` as a bool from the env.

    Defaults to False if not present or if the value cannot be interpreted as a
    boolean.

    Returns
    -------
    doRaise : `bool`
        Whether to raise exceptions or not.
    """
    doRaiseString = os.getenv("RAPID_ANALYSIS_DO_RAISE", "False").strip().lower()
    return doRaiseString in ["true", "1", "yes"]


def isDayObsContiguous(dayObs: int, otherDayObs: int) -> bool:
    """Check if two dayObs integers are contiguous or not.

    DayObs take forms like 20220727 and therefore don't trivially compare.

    Parameters
    ----------
    dayObs : `int`
        The first dayObs to compare.
    otherDayObs : `int`
        The second dayObs to compare.

    Returns
    -------
    contiguous : `bool`
        Are the days contiguous?
    """
    d1 = datetime.strptime(str(dayObs), "%Y%m%d")
    d2 = datetime.strptime(str(otherDayObs), "%Y%m%d")
    deltaDays = d2.date() - d1.date()
    return deltaDays == timedelta(days=1) or deltaDays == timedelta(days=-1)


def hasDayRolledOver(dayObs: int, logger: Logger | None = None) -> bool:
    """Check if the dayObs has rolled over when running constantly.

    Checks if supplied dayObs is the current dayObs and returns False
    if it is.

    Parameters
    ----------
    dayObs : `int`
        The dayObs to check if current
    logger : `logging.Logger`, optional
        The logger, created if not supplied

    Returns
    -------
    hasDayRolledOver : `bool`
        Whether the day has rolled over?
    """
    if not logger:
        logger = logging.getLogger(__name__)
    currentDay = getCurrentDayObsInt()
    if currentDay == dayObs:
        return False
    elif currentDay == dayObs + 1:
        return True
    else:
        if not isDayObsContiguous(currentDay, dayObs):
            logger.warning(
                f"Encountered non-linear time! dayObs supplied was {dayObs}"
                f" and now the current dayObs is {currentDay}!"
            )
        return True  # the day has still rolled over, just in an unexpected way


def isCalibration(expRecord: DimensionRecord) -> bool:
    """Check if the exposure is a calibration exposure.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to check.

    Returns
    -------
    isCalibration : `bool`
        ``True`` if the exposure is a calibration exposure, else ``False``.
    """
    if expRecord.observation_type in ["bias", "dark", "flat"]:
        return True
    return False


def isWepImage(expRecord: DimensionRecord) -> bool:
    """Check if the exposure is one of a donut-pair.

    All images with a cwfs observation_type are one of a donut-pair, or
    otherwise destined for the WEP pipeline, and conversely, all images
    destined for the WEP pipeline have a cwfs observation_type. Other images
    can contain donuts, e.g. in focus sweeps, but these are not designed to
    have WEP run on them.

    Parameters
    ----------
    expRecord : `lsst.daf.butler.DimensionRecord`
        The exposure record to check.

    Returns
    -------
    isWepImage : `bool`
        ``True`` if the exposure is destined for the WEP pipeline, else
        ``False``.
    """
    return expRecord.observation_type.lower() == "cwfs"


def hasRaDec(record: DimensionRecord) -> bool:
    """Check if an exposure record has valid RA and Dec.

    Parameters
    ----------
    record : `lsst.daf.butler.DimensionRecord`
        The exposure record to check.

    Returns
    -------
    hasRaDec : `bool`
        True if the exposure record has valid RA and Dec, else False.
    """
    try:
        ra = float(record.tracking_ra)
        dec = float(record.tracking_dec)
    except (AttributeError, TypeError):  # AttributeError for missing, TypeError for None
        return False

    if not np.isfinite(ra) or not np.isfinite(dec):
        return False
    return True


def isFileWorldWritable(filename: str) -> bool:
    """Check that the file has the correct permissions for write access.

    Parameters
    ----------
    filename : `str`
        The filename to check.

    Returns
    -------
    ok : `bool`
        True if the file has the correct permissions, False otherwise.
    """
    # XXX remove this function once we've done the S3 move. Removing it will
    # also help find other bits of file use.
    stat = os.stat(filename)
    return stat.st_mode & 0o777 == 0o777


def isFamPipeline(pipelineGraph: PipelineGraph) -> bool:
    """Check if the pipeline graph is a FAM pipeline.

    Parameters
    ----------
    pipelineGraph : `lsst.pipe.base.PipelineGraph`
        The pipeline graph to check.

    Returns
    -------
    isFamPipeline : `bool`
        ``True`` if the pipeline graph is a FAM pipeline, else ``False``.
    """
    return pipelineGraph.task_subsets.get("visit-pair-merge-task") is not None


def runningCI() -> bool:
    """Check if the code is running in a CI environment.

    Returns
    -------
    runningCI : `bool`
        ``True`` if ``RAPID_ANALYSIS_CI`` is set to a truthy value.
    """
    return os.environ.get("RAPID_ANALYSIS_CI", "false").lower() == "true"


def runningScons() -> bool:
    """Check if the code is running under scons.

    Returns
    -------
    runningScons : `bool`
        ``True`` if ``SCONS_BUILDING`` is set to a truthy value.
    """
    return os.environ.get("SCONS_BUILDING", "false").lower() == "true"


def runningPyTest() -> bool:
    """Check if the code is running inside pytest.

    Returns
    -------
    runningPyTest : `bool`
        ``True`` if pytest has set ``PYTEST_CURRENT_TEST`` in the environment.
    """
    return "PYTEST_CURRENT_TEST" in os.environ
