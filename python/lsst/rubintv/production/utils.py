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

import io
import logging
from contextlib import contextmanager, redirect_stdout
from dataclasses import dataclass
from functools import wraps
from time import perf_counter
from typing import TYPE_CHECKING, Callable, Iterator

import sentry_sdk

if TYPE_CHECKING:
    from logging import Logger


__all__ = [
    "setupSentry",
    "checkRubinTvExternalPackages",
    "logDuration",
    "timeFunction",
]

EFD_CLIENT_MISSING_MSG = (
    "ImportError: lsst_efd_client not found. Please install with:\n" "    pip install lsst-efd-client"
)

GOOGLE_CLOUD_MISSING_MSG = (
    "ImportError: Google cloud storage not found. Please install with:\n"
    "    pip install google-cloud-storage"
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
