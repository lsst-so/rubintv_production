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

import logging
import time
from abc import ABC, abstractmethod
from time import sleep
from typing import TYPE_CHECKING, Any

from .watchers import RedisWatcher

if TYPE_CHECKING:
    from logging import Logger

    from lsst.daf.butler import Butler, DataCoordinate, LimitedButler

    from .locationConfig import LocationConfig
    from .podDefinition import PodDetails
    from .starTracker import StarTrackerWatcher


__all__ = [
    "BaseChannel",
    "BaseButlerChannel",
]


class BaseChannel(ABC):
    """Base class for all channels.

    Subclasses that need an S3 uploader create their own
    ``self.s3Uploader = MultiUploader()`` in their ``__init__``. The base
    class deliberately does not own one — that way mypy sees the
    attribute as a non-optional ``MultiUploader`` on the subclasses
    that have it, and the subclasses that don't never need to deal
    with it at all.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location configuration to use.
    log : `logging.Logger`
        The logger to use.
    watcher : `lsst.rubintv.production.watchers.RedisWatcher`
        The watcher to use.
    doRaise : `bool`
        If ``True``, raise exceptions. If ``False``, log them.
    """

    def __init__(
        self,
        *,
        locationConfig: LocationConfig,
        log: Logger,
        watcher: RedisWatcher | StarTrackerWatcher,
        doRaise: bool,
    ) -> None:
        self.locationConfig = locationConfig
        self.log = log
        self.watcher = watcher
        self.doRaise: bool = doRaise

    @abstractmethod
    def callback(self, arg, /):
        """The callback function, called as each new value of arg is found.

        ``arg`` is usually an exposure record, but can be, for example, a
        filename.

        Parameters
        ----------
        arg : `any`
            The argument to run the callback with.
        """
        raise NotImplementedError()

    def run(self) -> None:
        """Run continuously, calling the callback method with the latest
        expRecord.
        """
        self.watcher.run(self.callback)


class BaseButlerChannel(BaseChannel):
    """Base class for all channels that use a Butler.

    Subclasses that need ``_waitForDataProduct`` to actually wait for
    something should set ``self.dataProduct`` to the dataset type name
    after calling ``super().__init__``. The default of ``None`` causes
    ``_waitForDataProduct`` to short-circuit and return immediately,
    which is what every Redis-driven worker that doesn't pre-stage a
    dataProduct wants.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location configuration to use.
    butler : `lsst.daf.butler.Butler`
        The Butler to use.
    podDetails : `lsst.rubintv.production.podDefinition.PodDetails`
        The pod identity, used both to construct the Redis watcher and to
        derive the per-instance log name.
    doRaise : `bool`
        If ``True``, raise exceptions. If ``False``, log them.
    """

    def __init__(
        self,
        *,
        locationConfig: LocationConfig,
        butler: Butler,
        podDetails: PodDetails,
        doRaise: bool,
    ) -> None:
        watcher = RedisWatcher(
            butler=butler,
            locationConfig=locationConfig,
            podDetails=podDetails,
        )
        log = logging.getLogger(f"lsst.rubintv.production.{type(self).__name__}")
        super().__init__(locationConfig=locationConfig, log=log, watcher=watcher, doRaise=doRaise)
        self.butler = butler
        self.podDetails = podDetails
        # Subclasses that pre-wait on a dataProduct override this. The
        # default short-circuits ``_waitForDataProduct``.
        self.dataProduct: str | None = None

    @abstractmethod
    def callback(self, expRecord):
        raise NotImplementedError()

    def _waitForDataProduct(
        self, dataId: DataCoordinate, timeout: float = 20, gettingButler: Butler | LimitedButler | None = None
    ) -> Any:
        """Wait for a dataProduct to land inside a repo.

        Wait for a maximum of ``timeout`` seconds for a dataProduct to land,
        and returns the dataProduct if it does, or ``None`` if it doesn't.

        Parameters
        ----------
        dataId : `dict` or `lsst.daf.butler.DataCoordinate`
            The fully-qualified dataId of the product to wait for.
        timeout : `float`
            The timeout, in seconds, to wait before giving up and returning
            ``None``.
        gettingButler : `lsst.daf.butler.LimitedButler`
            The butler to use. If ``None``, uses the butler attribute. Provided
            so that a CachingLimitedButler can be used instead.

        Returns
        -------
        dataProduct : dataProduct or None
            Either the dataProduct being waited for, or ``None`` if timeout was
            exceeded.
        """
        if self.dataProduct is None:
            return

        cadence = 0.25
        start = time.time()
        while time.time() - start < timeout:
            if self.butler.exists(self.dataProduct, dataId):
                if gettingButler is None:
                    return self.butler.get(self.dataProduct, dataId)
                else:
                    ref = self.butler.find_dataset(self.dataProduct, dataId)
                    assert ref is not None, f"Registry error: could not find {self.dataProduct} for {dataId}"
                    return gettingButler.get(ref)
            else:
                sleep(cadence)
        self.log.warning(f"Waited {timeout}s for {self.dataProduct} for {dataId} to no avail")
        return None
