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

__all__ = ("RedisWatcher", "ButlerWatcher")

import logging
import sys
from time import perf_counter, sleep
from typing import TYPE_CHECKING

from lsst.daf.butler import Butler

from .locationConfig import LocationConfig
from .payloads import isRestartPayload
from .predicates import raiseIf
from .redisUtils import RedisHelper

if TYPE_CHECKING:
    from lsst.daf.butler import DimensionRecord

    from .payloads import Payload
    from .podDefinition import PodDetails


_LOG = logging.getLogger(__name__)


class RedisWatcher:
    """A redis-based watcher, looking for work in a redis queue from the
    HeadProcessController.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The butler used by the head node when sending work, used here to
        rehydrate dataIds attached to incoming payloads.
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location config for the running pod.
    podDetails : `lsst.rubintv.production.podDefinition.PodDetails`
        The pod identity that selects which Redis queue to consume from.
    """

    def __init__(self, butler: Butler, locationConfig: LocationConfig, podDetails: PodDetails) -> None:
        self.redisHelper = RedisHelper(butler, locationConfig)
        self.podDetails = podDetails
        self.cadence = 0.2  # seconds - there's 400+ workers, don't go too high!
        self.log = _LOG.getChild("redisWatcher")
        self.payload: Payload | None = None  # XXX that is this for?

    def run(self, callback, **kwargs) -> None:
        """Run forever, calling ``callback`` on each most recent Payload.

        Parameters
        ----------
        callback : `callable`
            The callback to run, with the most recent ``Payload`` as the
            argument.
        """
        while True:
            self.redisHelper.announceFree(self.podDetails)
            payload = self.redisHelper.dequeuePayload(self.podDetails)  # blocks for up to DEQUE_TIMEOUT sec
            if payload is not None:
                if isRestartPayload(payload):
                    # TODO: delete existence + free keys?
                    self.log.warning("Received RESTART_SIGNAL, exiting")
                    self.redisHelper.setPodSecondaryStatus(self.podDetails, payload.specialMessage)
                    sys.exit(0)
                try:
                    self.payload = payload  # XXX why is this being saved on the class?
                    self.redisHelper.announceBusy(self.podDetails)
                    self.redisHelper.setPodSecondaryStatus(self.podDetails, payload.specialMessage)
                    callback(payload)
                    self.payload = None
                except Exception as e:  # deliberately don't catch KeyboardInterrupt, SIGINT etc
                    self.log.exception(f"Error processing payload {payload}: {e}")
                finally:
                    self.redisHelper.announceFree(self.podDetails)
            else:  # only sleep when no work is found
                self.redisHelper.clearPodSecondaryStatus(self.podDetails)
                sleep(self.cadence)  # probably unnecessary now we use a blocking dequeue but it doesn't hurt


class ButlerWatcher:
    """A main watcher, which polls the butler for new data.

    Only one of these should be instantiated per-location and per-instrument.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location config.
    instrument : `str`
        The instrument name to watch for new exposures of.
    butler : `lsst.daf.butler.Butler`
        The butler used to query for the most recent exposure record.
    doRaise : `bool`, optional
        Raise exceptions or log them as warnings?
    """

    # look for new images every ``cadence`` seconds
    cadence = 1

    def __init__(
        self,
        locationConfig: LocationConfig,
        instrument: str,
        butler: Butler,
        doRaise=False,
    ) -> None:
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.butler = butler
        self.doRaise = doRaise
        self.log = _LOG.getChild("butlerWatcher")
        self.redisHelper = RedisHelper(butler, locationConfig, isHeadNode=True)

    def _getLatestExpRecord(self) -> DimensionRecord:
        """Get the most recent expRecord from the butler.

        Returns
        -------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The most recent exposure record, sorted by ``timespan.end``.
        """
        # runtime is ~200ms on the summit. If the dayObs were added and the
        # results and then sorted in python this would bring this to ~30ms, but
        # the change would then need to deal with the change in behaviour when
        # the list is empty
        records = self.butler.query_dimension_records("exposure", order_by="-exposure.timespan.end", limit=1)

        # we must sort using the timespan because:
        # we can't use exposure.id because it is calculated differently
        # for different instruments, e.g. TS8 is 10x bigger than AuxTel
        # and also C-controller data has expIds like 3YYYMMDDNNNNN so would
        # always be the "most recent".
        if len(records) != 1:
            raise RuntimeError(f"Found {len(records)} records for 'raw', expected 1")
        return records[0]

    def run(self) -> None:
        lastSeen = None
        while True:
            try:
                start = perf_counter()
                latestRecord = self._getLatestExpRecord()
                duration = perf_counter() - start

                if lastSeen is None:  # starting up for the first time
                    seenBefore = self.redisHelper.checkButlerWatcherList(self.instrument, latestRecord)
                    if seenBefore:
                        self.log.info(
                            f"Skipping dispatching {latestRecord.instrument}-{latestRecord.id} as"
                            " it was dispatched by a ButlerWatcher in a previous life. You should only"
                            " ever see this on pod startup."
                        )
                        lastSeen = latestRecord
                        continue

                if latestRecord == lastSeen:
                    sleep(self.cadence)
                    continue

                self.log.info(f"Found new exposure={latestRecord.id}, query took {duration * 1000:.1f} ms")
                self.redisHelper.pushNewExposureToHeadNode(latestRecord)
                self.redisHelper.pushToButlerWatcherList(self.instrument, latestRecord)
                lastSeen = latestRecord

            except Exception as e:
                sleep(1)  # in case we are in a tight loop of raising, don't hammer the butler
                raiseIf(self.doRaise, e, self.log)
