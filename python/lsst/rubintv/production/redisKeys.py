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

"""Pure helpers for constructing the Redis keys used by ``RedisHelper``.

This module deliberately has zero runtime dependencies — no ``redis``,
``butler``, ``LocationConfig`` or anything else from the package — so that
the key shapes can be imported and unit-tested without spinning up a Redis
client or a Butler. Every key string the rest of the package writes to or
reads from Redis should pass through one of the functions or constants
defined here.

Note: a couple of these key shapes contain *typos* that have been persisted
in production Redis. Until those are migrated they cannot be silently
corrected here, so the typos are reproduced verbatim and called out
explicitly. See the relevant function docstrings.
"""

from __future__ import annotations

__all__ = [
    "QUEUE_LENGTHS_KEY",
    "WITNESS_DETECTOR_KEY",
    "TRACKING_INITIALIZED_FIELD",
    "TRACKING_PIPELINE_CONFIG_FIELD",
    "TRACKING_MOSAIC_DISPATCHED_FIELD",
    "TRACKING_BINNED_ISR_PREFIX",
    "getNewDataQueueName",
    "getPodRunningKey",
    "getPodBusyKey",
    "getPodExistsKey",
    "getPodSecondaryStatusKey",
    "getButlerWatcherListKey",
    "getTaskFinishedCounterKey",
    "getTaskFailedCounterKey",
    "getVisitFinishedCounterKey",
    "getVisitFailedCounterKey",
    "getNightlyRollupFinishedKey",
    "getTrackingKey",
    "getActiveExposuresKey",
    "getIgnoredDetectorsKey",
    "getVisitSummaryStatsKey",
    "getMtaosZernikeResultKey",
    "getConsDbAnnouncementKey",
    "getConsDbAnnouncementField",
    "getTrackingExpectedField",
    "getTrackingFinishedField",
    "getTrackingFailedField",
    "getTrackingStep1aDispatchedField",
    "getTrackingStep1bDispatchedField",
    "getTrackingStep1bFinishedField",
    "getTrackingBinnedIsrField",
]


# ----------------------------------------------------------------------------
# Top-level constants
# ----------------------------------------------------------------------------

#: Hash key holding per-queue length counters maintained by RedisHelper.
QUEUE_LENGTHS_KEY = "_QUEUE-LENGTHS"

#: String key holding the witness detector for LSSTCam (set externally).
WITNESS_DETECTOR_KEY = "RUBINTV_CONTROL_WITNESS_DETECTOR"

#: Sentinel hash field written by ``initExposureTracking`` so that the
#: tracking hash exists immediately and the TTL can be set on it.
TRACKING_INITIALIZED_FIELD = "_initialized"

#: Hash field inside the per-exposure tracking hash holding the AOS
#: pipeline name (e.g. ``"AOS_DANISH"``) for the exposure.
TRACKING_PIPELINE_CONFIG_FIELD = "pipeline_config"

#: Hash field inside the per-exposure tracking hash marking that the
#: post-ISR focal-plane mosaic has been dispatched.
TRACKING_MOSAIC_DISPATCHED_FIELD = "_mosaicDispatched"

#: Prefix for per-detector hash fields marking that a binned post-ISR
#: image has been produced. The full field name is
#: ``_binnedIsr:{detector}`` (see ``getTrackingBinnedIsrField``). Tracked
#: separately from the per-who ``{who}:finished:{det}`` fields because
#: every pipeline contains an ISR quantum, so binned-ISR production is
#: driven by *any* step1a pipeline finishing for a detector, not by a
#: specific ``who``.
TRACKING_BINNED_ISR_PREFIX = "_binnedIsr:"


# ----------------------------------------------------------------------------
# Per-instrument top-level keys
# ----------------------------------------------------------------------------


def getNewDataQueueName(instrument: str) -> str:
    """Return the queue key the ButlerWatcher pushes new exposures onto.

    Parameters
    ----------
    instrument : `str`
        The instrument name.

    Returns
    -------
    key : `str`
        The Redis list key for the new-exposure queue.
    """
    return f"INCOMING-{instrument}-raw"


def getButlerWatcherListKey(instrument: str) -> str:
    """Return the key for the persistent ButlerWatcher seen-records list.

    Parameters
    ----------
    instrument : `str`
        The instrument name.

    Returns
    -------
    key : `str`
        The Redis list key.

    Notes
    -----
    The Redis key contains the historical typo ``fromButlerWacher``
    (missing the ``t``). This is persisted in production Redis and
    cannot be changed without a coordinated migration, so the typo is
    reproduced exactly. Do not silently fix it here.
    """
    return f"{instrument}-fromButlerWacher"


def getTaskFinishedCounterKey(instrument: str, taskName: str) -> str:
    """Return the hash key for per-task finished counters.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    taskName : `str`
        The pipeline task name.

    Returns
    -------
    key : `str`
        The Redis hash key.
    """
    return f"{instrument}-{taskName}-FINISHEDCOUNTER"


def getTaskFailedCounterKey(instrument: str, taskName: str) -> str:
    """Return the hash key for per-task failed counters.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    taskName : `str`
        The pipeline task name.

    Returns
    -------
    key : `str`
        The Redis hash key.
    """
    return f"{instrument}-{taskName}-FAILEDCOUNTER"


def getVisitFinishedCounterKey(instrument: str, step: str, who: str) -> str:
    """Return the key for the per-visit finished counter.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    step : `str`
        The pipeline step name.
    who : `str`
        The pipeline identifier (e.g. ``"SFM"`` or ``"AOS"``).

    Returns
    -------
    key : `str`
        The Redis string key.

    Notes
    -----
    The Redis key contains the historical typo ``VISIT_FINISIHED_COUNTER``
    (extra ``I``). This is persisted in production Redis and cannot be
    changed without a coordinated migration, so the typo is reproduced
    exactly. Do not silently fix it here.
    """
    return f"{instrument}-{step}-{who}-VISIT_FINISIHED_COUNTER"


def getVisitFailedCounterKey(instrument: str, step: str, who: str) -> str:
    """Return the key for the per-visit failed counter.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    step : `str`
        The pipeline step name.
    who : `str`
        The pipeline identifier (e.g. ``"SFM"`` or ``"AOS"``).

    Returns
    -------
    key : `str`
        The Redis string key.
    """
    return f"{instrument}-{step}-{who}-VISIT_FAILED_COUNTER"


def getNightlyRollupFinishedKey(instrument: str, who: str) -> str:
    """Return the key for the nightly rollup finished counter.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    who : `str`
        The pipeline identifier (e.g. ``"SFM"`` or ``"AOS"``).

    Returns
    -------
    key : `str`
        The Redis string key.
    """
    return f"{instrument}-{who}-NIGHTLYROLLUP-FINISHEDCOUNTER"


def getTrackingKey(instrument: str, expId: int) -> str:
    """Return the key for the per-exposure tracking hash.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    expId : `int`
        The exposure ID.

    Returns
    -------
    key : `str`
        The Redis hash key.
    """
    return f"{instrument}-TRACKING-{expId}"


def getActiveExposuresKey(instrument: str) -> str:
    """Return the key for the per-instrument active exposures set.

    Parameters
    ----------
    instrument : `str`
        The instrument name.

    Returns
    -------
    key : `str`
        The Redis set key.
    """
    return f"{instrument}-ACTIVE-EXPOSURES"


def getIgnoredDetectorsKey(instrument: str) -> str:
    """Return the key for the head-node ignored-detectors list.

    Parameters
    ----------
    instrument : `str`
        The instrument name.

    Returns
    -------
    key : `str`
        The Redis string key.
    """
    return f"{instrument}-HEADNODE-IGNORED_DETECTORS"


def getVisitSummaryStatsKey(instrument: str, visit: int) -> str:
    """Return the key for the per-visit summary statistics hash.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    visit : `int`
        The visit ID.

    Returns
    -------
    key : `str`
        The Redis hash key.
    """
    return f"{instrument}-VISIT_SUMMARY_STATS-{visit}"


def getMtaosZernikeResultKey(instrument: str) -> str:
    """Return the key for the MTAOS Zernike processing-result hash.

    Parameters
    ----------
    instrument : `str`
        The instrument name. Note: this key is built with
        ``instrument.upper()`` to match the historical convention used
        on the MTAOS side.

    Returns
    -------
    key : `str`
        The Redis hash key.
    """
    return f"{instrument.upper()}_WEP_PROCESSING_RESULT"


# ----------------------------------------------------------------------------
# Per-pod keys (built from a queue name string)
# ----------------------------------------------------------------------------
#
# The functions below take a ``queueName`` string rather than a ``PodDetails``
# instance so they can also be used to build glob patterns — for example when
# ``RedisHelper.getAllWorkers`` searches for ``...EXISTS`` keys with a
# wildcard queue name. Pass ``pod.queueName`` for a single pod, or a glob
# built from ``podDefinition.getQueueName`` for a wildcard search.


def getPodRunningKey(queueName: str) -> str:
    """Return the per-pod ``IS_RUNNING`` key.

    Parameters
    ----------
    queueName : `str`
        The pod's queue name (or a glob pattern over queue names).

    Returns
    -------
    key : `str`
        The Redis string key.
    """
    return f"{queueName}+IS_RUNNING"


def getPodBusyKey(queueName: str) -> str:
    """Return the per-pod ``IS_BUSY`` key.

    Parameters
    ----------
    queueName : `str`
        The pod's queue name (or a glob pattern over queue names).

    Returns
    -------
    key : `str`
        The Redis string key.
    """
    return f"{queueName}+IS_BUSY"


def getPodExistsKey(queueName: str) -> str:
    """Return the per-pod ``EXISTS`` key.

    Parameters
    ----------
    queueName : `str`
        The pod's queue name (or a glob pattern over queue names).

    Returns
    -------
    key : `str`
        The Redis string key.
    """
    return f"{queueName}+EXISTS"


def getPodSecondaryStatusKey(queueName: str) -> str:
    """Return the per-pod ``SECONDARY_STATUS`` key.

    Parameters
    ----------
    queueName : `str`
        The pod's queue name.

    Returns
    -------
    key : `str`
        The Redis string key.
    """
    return f"{queueName}+SECONDARY_STATUS"


# ----------------------------------------------------------------------------
# ConsDB announcements
# ----------------------------------------------------------------------------


def getConsDbAnnouncementKey(dayObs: int) -> str:
    """Return the per-day consDB announcements hash key.

    Parameters
    ----------
    dayObs : `int`
        The dayObs (typically derived as ``obsId // 100_000``).

    Returns
    -------
    key : `str`
        The Redis hash key.
    """
    return f"consdb-announcements-{dayObs}"


def getConsDbAnnouncementField(instrument: str, table: str, obsId: int) -> str:
    """Return the hash field name for a single consDB announcement.

    Parameters
    ----------
    instrument : `str`
        The instrument name.
    table : `str`
        The consDB table name.
    obsId : `int`
        The obsId associated with the insert.

    Returns
    -------
    field : `str`
        The hash field name (lower-cased to match the historical
        convention used on the consumer side).
    """
    return f"{instrument}-{table}-{obsId}".lower()


# ----------------------------------------------------------------------------
# Tracking-hash field names
# ----------------------------------------------------------------------------
#
# The fields below all live inside the per-exposure tracking hash returned by
# ``getTrackingKey``. They are parsed back out by
# ``ExposureProcessingInfo.fromRedisHash`` via a regex pinned to this format.


def getTrackingExpectedField(who: str) -> str:
    """Return the hash field for the comma-separated expected detectors.

    Parameters
    ----------
    who : `str`
        The pipeline identifier.

    Returns
    -------
    field : `str`
        The tracking-hash field name.
    """
    return f"{who}:expected"


def getTrackingFinishedField(who: str, detector: int) -> str:
    """Return the hash field marking a finished detector.

    Parameters
    ----------
    who : `str`
        The pipeline identifier.
    detector : `int`
        The detector number.

    Returns
    -------
    field : `str`
        The tracking-hash field name.
    """
    return f"{who}:finished:{detector}"


def getTrackingFailedField(who: str, detector: int) -> str:
    """Return the hash field marking a failed detector.

    Parameters
    ----------
    who : `str`
        The pipeline identifier.
    detector : `int`
        The detector number.

    Returns
    -------
    field : `str`
        The tracking-hash field name.
    """
    return f"{who}:failed:{detector}"


def getTrackingStep1aDispatchedField(who: str) -> str:
    """Return the hash field marking step1a as dispatched.

    Parameters
    ----------
    who : `str`
        The pipeline identifier.

    Returns
    -------
    field : `str`
        The tracking-hash field name.
    """
    return f"{who}:step1aDispatched"


def getTrackingStep1bDispatchedField(who: str) -> str:
    """Return the hash field marking step1b as dispatched.

    Parameters
    ----------
    who : `str`
        The pipeline identifier.

    Returns
    -------
    field : `str`
        The tracking-hash field name.
    """
    return f"{who}:step1bDispatched"


def getTrackingStep1bFinishedField(who: str) -> str:
    """Return the hash field marking step1b as finished.

    Parameters
    ----------
    who : `str`
        The pipeline identifier.

    Returns
    -------
    field : `str`
        The tracking-hash field name.
    """
    return f"{who}:step1bFinished"


def getTrackingBinnedIsrField(detector: int) -> str:
    """Return the hash field marking that a binned post-ISR image has
    been produced for ``detector``.

    This field is deliberately *not* keyed by ``who``: every step1a
    pipeline (SFM, AOS, ISR) contains an ISR quantum, so binned-ISR
    production is pipeline-agnostic.

    Parameters
    ----------
    detector : `int`
        The detector number.

    Returns
    -------
    field : `str`
        The tracking-hash field name.
    """
    return f"{TRACKING_BINNED_ISR_PREFIX}{detector}"
