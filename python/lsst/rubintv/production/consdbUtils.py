# This file is part of rubintv_production.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
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
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = [
    "CCD_VISIT_MAPPING",
    "VISIT_MIN_MED_MAX_MAPPING",
    "VISIT_MIN_MED_MAX_TOTAL_MAPPING",
    "ConsDBPopulator",
]

import itertools
import logging
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Callable, cast

import numpy as np
from astropy.table import Table
from requests import HTTPError

from lsst.afw.image import ExposureSummaryStats  # type: ignore
from lsst.afw.table import ExposureCatalog, SourceCatalog  # type: ignore
from lsst.daf.butler import Butler, DatasetNotFoundError, DimensionRecord
from lsst.summit.utils import ConsDbClient
from lsst.summit.utils.simonyi.mountAnalysis import MountErrors
from lsst.summit.utils.utils import computeCcdExposureId, getDetectorIds

from .redisUtils import RedisHelper

if TYPE_CHECKING:
    from .locationConfig import LocationConfig

logger = logging.getLogger(__name__)

# The mapping from ExposureSummaryStats columns to consDB columns
CCD_VISIT_MAPPING = {
    "effTime": "eff_time",
    "effTimePsfSigmaScale": "eff_time_psf_sigma_scale",
    "effTimeSkyBgScale": "eff_time_sky_bg_scale",
    "effTimeZeroPointScale": "eff_time_zero_point_scale",
    "magLim": "stats_mag_lim",
    "astromOffsetMean": "astrom_offset_mean",
    "astromOffsetStd": "astrom_offset_std",
    "pixelScale": "pixel_scale",
    "maxDistToNearestPsf": "max_dist_to_nearest_psf",
    "meanVar": "mean_var",
    "nPsfStar": "n_psf_star",
    "psfArea": "psf_area",
    "psfIxx": "psf_ixx",
    "psfIyy": "psf_iyy",
    "psfIxy": "psf_ixy",
    "psfSigma": "psf_sigma",
    "psfStarDeltaE1Median": "psf_star_delta_e1_median",
    "psfStarDeltaE1Scatter": "psf_star_delta_e1_scatter",
    "psfStarDeltaE2Median": "psf_star_delta_e2_median",
    "psfStarDeltaE2Scatter": "psf_star_delta_e2_scatter",
    "psfStarDeltaSizeMedian": "psf_star_delta_size_median",
    "psfStarDeltaSizeScatter": "psf_star_delta_size_scatter",
    "psfStarScaledDeltaSizeScatter": "psf_star_scaled_delta_size_scatter",
    "psfApFluxDelta": "psf_ap_flux_delta",
    "psfApCorrSigmaScaledDelta": "psf_ap_corr_sigma_scaled_delta",
    "psfTraceRadiusDelta": "psf_trace_radius_delta",
    "skyBg": "sky_bg",
    "skyNoise": "sky_noise",
    "zenithDistance": "zenith_distance",
    "zeroPoint": "zero_point",
}

# The mapping from ExposureCatalog columns to consDB columns where
# min/median/max are calculated
VISIT_MIN_MED_MAX_MAPPING = {
    "effTime": "eff_time",
    "effTimePsfSigmaScale": "eff_time_psf_sigma_scale",
    "effTimeSkyBgScale": "eff_time_sky_bg_scale",
    "effTimeZeroPointScale": "eff_time_zero_point_scale",
    "magLim": "stats_mag_lim",
    "astromOffsetMean": "astrom_offset_mean",
    "astromOffsetStd": "astrom_offset_std",
    "pixelScale": "pixel_scale",
    "maxDistToNearestPsf": "max_dist_to_nearest_psf",
    "meanVar": "mean_var",
    "nPsfStar": "n_psf_star",
    "psfArea": "psf_area",
    "psfIxx": "psf_ixx",
    "psfIyy": "psf_iyy",
    "psfIxy": "psf_ixy",
    "psfSigma": "psf_sigma",
    "psfStarDeltaE1Median": "psf_star_delta_e1_median",
    "psfStarDeltaE2Median": "psf_star_delta_e2_median",
    "psfStarDeltaE1Scatter": "psf_star_delta_e1_scatter",
    "psfStarDeltaE2Scatter": "psf_star_delta_e2_scatter",
    "psfStarDeltaSizeMedian": "psf_star_delta_size_median",
    "psfStarDeltaSizeScatter": "psf_star_delta_size_scatter",
    "psfStarScaledDeltaSizeScatter": "psf_star_scaled_delta_size_scatter",
    "psfApFluxDelta": "psf_ap_flux_delta",
    "psfApCorrSigmaScaledDelta": "psf_ap_corr_sigma_scaled_delta",
    "psfTraceRadiusDelta": "psf_trace_radius_delta",
    "skyNoise": "sky_noise",
    "skyBg": "sky_bg",
    "zeroPoint": "zero_point",
}

# The mapping from ExposureCatalog columns to consDB columns where
# min/median/max are calculated as well as the total
VISIT_MIN_MED_MAX_TOTAL_MAPPING = {
    "nPsfStar": "n_psf_star",
}


def _removeNans(
    values: Mapping[str, float | int | str | np.floating],
) -> dict[str, float | int | str | np.floating]:
    out: dict[str, float | int | str | np.floating] = {}
    for k, v in values.items():
        if isinstance(v, (float, np.floating)) and np.isnan(v):
            continue
        out[k] = v
    return out


def changeType(key: str, typeMapping: dict[str, str]) -> Callable[[int | float], int | float]:
    """Return a function to convert to the appropriate type for a ConsDB column

    Parameters
    ----------
    key : `str`
        The ConsDB column name.
    typeMapping : `dict` [`str`, `str`]
        A mapping of ConsDB column names to their database types.

    Returns
    -------
    typeFunc : `Callable` [[`int` or `float`], `int` or `float`]
        A function that converts a value to the appropriate type for the
        ConsDB column.
    """
    dbType = typeMapping[key]
    if dbType in ("BIGINT", "INTEGER"):
        return int
    elif dbType == "DOUBLE PRECISION":
        return float
    else:
        raise ValueError(f"Got unknown database type {dbType}")


class ConsDBPopulator:
    def __init__(
        self,
        client: ConsDbClient,
        redisHelper: RedisHelper,
        locationConfig: LocationConfig,
        asyncWrites: bool = False,
    ) -> None:
        """Populate consDB from rapid analysis.

        Parameters
        ----------
        client : `lsst.summit.utils.ConsDbClient`
            The client used to talk to consDB.
        redisHelper : `RedisHelper`
            Used to announce completed writes for cross-pod coordination.
        locationConfig : `LocationConfig`
            The location config; its ``location`` gates whether writes happen
            at all (only "summit", "bts" and "tts" insert).
        asyncWrites : `bool`, optional
            If ``True``, every write is handed off to a single background
            thread so a slow or timing-out insert never blocks processing.
            Enable in the live pods; leave ``False`` for synchronous tooling
            (e.g. backfill) that relies on the blocking back-pressure and the
            bool return value.
        """
        self.client = client
        self.redisHelper = redisHelper
        self.locationConfig = locationConfig
        # When asyncWrites is True every consDB write is handed off to a
        # single background thread, so that a slow or timing-out insert never
        # blocks the processing that triggered it. Nothing in rapid analysis
        # reads back the data it writes to consDB, so deferring (or, on
        # failure, dropping) a write is always safe; the trade-off is that
        # write failures surface as logged tracebacks from the background
        # thread rather than as exceptions at the call site (see
        # _backgroundInsert). A single worker (max_workers=1) keeps writes
        # serialised and in submission order, and means the worker thread is
        # the only thread ever calling client.insert, while the main thread
        # only ever calls client.schema (a read) — they never issue the same
        # request on the shared requests.Session, and rely on urllib3's
        # thread-safe connection pool.
        #
        # asyncWrites is opt-in (default False) because the backfill tooling
        # in highLevelTools deliberately relies on synchronous inserts: it
        # times them to back off when consDB is slow, and uses the bool
        # return value to record which rows were written.
        self._executor: ThreadPoolExecutor | None = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix="ConsDBWriter") if asyncWrites else None
        )
        # Cache of consDB table schemas (as {column: dbType} type mappings),
        # keyed by (instrument.lower(), table). A schema fetch is a network
        # read whose result is needed to coerce values before the row can be
        # built, so it cannot be made fire-and-forget like a write. But table
        # schemas are static for the lifetime of a pod, so fetching once and
        # caching keeps the read off the processing thread for every call
        # after the first — the same goal the background writer serves for the
        # writes. Only successful fetches are cached, so a transient failure is
        # retried. Populated and read on the calling (processing) thread only.
        self._schemaCache: dict[tuple[str, str], dict[str, str]] = {}

    def flush(self, timeout: float | None = None) -> None:
        """Block until all queued background consDB writes have completed.

        This is a no-op when async writes are disabled. Because the writer is a
        single FIFO thread, waiting on a no-op task submitted after the
        outstanding writes guarantees that those writes have all finished.

        Parameters
        ----------
        timeout : `float` or `None`, optional
            Maximum number of seconds to wait. ``None`` (the default) waits
            indefinitely.
        """
        if self._executor is None:
            return
        self._executor.submit(lambda: None).result(timeout=timeout)

    def shutdown(self, wait: bool = True) -> None:
        """Shut down the background writer thread.

        This is a no-op when async writes are disabled. After shutdown no
        further writes may be submitted.

        Parameters
        ----------
        wait : `bool`, optional
            If ``True`` (the default), block until all queued writes have
            completed before returning.
        """
        if self._executor is not None:
            self._executor.shutdown(wait=wait)

    def _getTypeMapping(self, instrument: str, table: str) -> dict[str, str]:
        """Return the ``{column: dbType}`` type mapping for a consDB table.

        The underlying schema is fetched from consDB once per
        ``(instrument, table)`` and cached (see `_schemaCache`); every call
        after the first is a pure in-memory lookup, so the network read only
        ever blocks the processing thread once per table for the lifetime of
        the pod.

        Parameters
        ----------
        instrument : `str`
            The instrument name (case-insensitive).
        table : `str`
            The table name within the instrument schema, without the
            ``cdb_<instrument>.`` prefix (e.g. ``"visit1_quicklook"``).

        Returns
        -------
        typeMapping : `dict` [`str`, `str`]
            Mapping of consDB column name to its database type string.
        """
        key = (instrument.lower(), table)
        typeMapping = self._schemaCache.get(key)
        if typeMapping is None:
            schema = cast(dict[str, tuple[str, str]], self.client.schema(instrument.lower(), table))
            typeMapping = {k: v[0] for k, v in schema.items()}
            self._schemaCache[key] = typeMapping  # only cache successful fetches
        return typeMapping

    def _shouldInsert(self) -> bool:
        """Check whether inserts to consDB are allowed at the current location.

        Returns
        -------
        allowed : `bool`
            True if location is one of "summit", "bts", or "tts".
        """
        location = self.locationConfig.location
        if location is None:
            logger.warning("LocationConfig.location is None; skipping consDB insert.")
            return False
        return str(location).lower() in ("summit", "bts", "tts")

    def _insertIfAllowed(
        self,
        instrument: str,
        table: str,
        obsId: int | tuple[int, int],
        values: Mapping[str, int | float | str],
        allowUpdate: bool,
        onSuccess: Callable[[], None] | None = None,
    ) -> bool:
        """
        Conditionally call self.client.insert() based on location.

        When the populator was created with ``asyncWrites=True`` the actual
        write (and ``onSuccess`` callback) is handed off to the background
        writer thread and this returns immediately; otherwise the write happens
        inline on the calling thread.

        Parameters
        ----------
        instrument : `str`
            Instrument name for the consDB schema.
        table : `str`
            Table name within the instrument schema.
        obsId : `int` or `tuple[int, int]`
            The primary key used by consDB for the row (visit/exposure id or
            (day_obs, seq_num)).
        values : `dict[str, int | float | str]`
            Column values to write; NaN values are removed.
        allowUpdate : `bool`
            Whether to allow updates to existing rows.
        onSuccess : `Callable` [[], `None`], optional
            A zero-argument callback run after the write succeeds (e.g. to
            announce the result in redis). It runs on whichever thread performs
            the write: the background writer thread for async writes, or the
            calling thread for synchronous writes.

        Returns
        -------
        inserted : `bool`
            ``True`` if an insert/update was attempted and succeeded. ``False``
            if skipped (no values, or disallowed at this location) or if the
            write was handed off to the background thread (in which case the
            outcome is not yet known — async call sites must not rely on the
            return value).
        """
        if not values:
            logger.warning(f"No values to insert into consDB for {instrument}.{table} with obsId {obsId}")
            return False

        if not self._shouldInsert():  # called here again for safety
            location = self.locationConfig.location
            logger.info(f"Skipping consDB insert at {location} for {instrument}.{table} for {obsId}")
            return False

        if self._executor is not None:
            # Hand the write off to the background thread and return at once.
            # Snapshot the values so the caller is free to mutate or discard
            # its dict the moment this returns. The bool return is meaningless
            # in async mode (the write has not happened yet); no async call
            # site consumes it.
            self._executor.submit(
                self._backgroundInsert,
                instrument,
                table,
                obsId,
                dict(values),
                allowUpdate,
                onSuccess,
            )
            return False

        # Synchronous path: do the write inline, let failures propagate, and
        # report success to the caller. Used by the backfill tooling, which
        # relies on both the back-pressure of a blocking insert and the bool.
        self._doInsert(instrument, table, obsId, values, allowUpdate)
        if onSuccess is not None:
            onSuccess()
        return True

    def _doInsert(
        self,
        instrument: str,
        table: str,
        obsId: int | tuple[int, int],
        values: Mapping[str, int | float | str],
        allowUpdate: bool,
    ) -> None:
        """Perform the actual consDB insert, raising on failure.

        This is the single point at which a row is written to consDB. It runs
        on the calling thread for synchronous writes and on the background
        writer thread (via `_backgroundInsert`) for async writes.
        """
        try:
            self.client.insert(
                instrument=instrument,
                table=table,
                obs_id=obsId,
                values=_removeNans(values),
                allow_update=allowUpdate,
            )
        except HTTPError as e:
            try:
                if e.response is not None:
                    print(e.response.json())
            except Exception:
                logger.exception("HTTPError during consDB insert and response JSON parse failed.")
            raise RuntimeError from e

    def _backgroundInsert(
        self,
        instrument: str,
        table: str,
        obsId: int | tuple[int, int],
        values: Mapping[str, int | float | str],
        allowUpdate: bool,
        onSuccess: Callable[[], None] | None,
    ) -> None:
        """Run a single consDB write on the background writer thread.

        This is the body executed by the writer thread for every async write.
        The future returned by ``submit`` is never awaited, so an exception
        raised here would otherwise vanish silently. Everything is therefore
        wrapped and logged with enough context (instrument, table, obsId) that
        if a traceback surfaces later — e.g. from an insert that timed out — it
        is unmistakably from an asynchronous rapid analysis consDB write, and
        not from the processing that queued it.
        """
        try:
            self._doInsert(instrument, table, obsId, values, allowUpdate)
        except Exception:
            logger.exception(
                f"Asynchronous consDB write to {instrument}.{table} (obsId={obsId}) failed on the "
                "background writer thread. Nothing in rapid analysis consumes consDB data, so this "
                "does not affect processing, but the row was not written."
            )
            return

        if onSuccess is None:
            return
        try:
            onSuccess()
        except Exception:
            logger.exception(
                f"The post-write callback for the asynchronous consDB write to {instrument}.{table} "
                f"(obsId={obsId}) failed on the background writer thread. The row was written, but the "
                "follow-up action (e.g. announcing the result in redis) did not complete."
            )

    def _createExposureRow(self, expRecord: DimensionRecord, allowUpdate: bool = False) -> None:
        """Create a row for the exp in the cdb_<instrument>.exposure table.

        This is expected to always be populated by observatory systems, and is
        therefore not a user-facing method.
        """
        exposureValues: dict[str, str | int] = {
            "exposure_id": expRecord.id,  # required key if updating
            "exposure_name": expRecord.obs_id,
            "controller": expRecord.obs_id.split("_")[1],
            "day_obs": expRecord.day_obs,
            "seq_num": expRecord.seq_num,
        }

        self._insertIfAllowed(
            instrument=expRecord.instrument,
            table=f"cdb_{expRecord.instrument.lower()}.exposure",
            # tuple-form for obsId required for updating non ccd-type tables
            obsId=(expRecord.day_obs, expRecord.seq_num),
            values=exposureValues,
            allowUpdate=allowUpdate,
        )

    def _createCcdExposureRows(
        self, expRecord: DimensionRecord, detectorNum: int | None = None, allowUpdate: bool = False
    ) -> None:
        """Create rows in all the relevant ccdexposure tables for the exp.

        This is expected to always be populated by observatory systems, and is
        therefore not a user-facing method.

        Parameters
        ----------
        expRecord : `DimensionRecord`
            The exposure record to populate the rows for.
        detectorNum : `int`, optional
            The detector number to populate the rows for. If ``None``, all
            detectors for the instrument are populated.
        allowUpdate : `bool`, optional
            Allow updating existing rows in the tables. Default is ``False``
        """
        if detectorNum is None:
            detectorNums = getDetectorIds(expRecord.instrument)
        else:
            detectorNums = [detectorNum]

        for detNum in detectorNums:
            obsId = computeCcdExposureId(expRecord.instrument, expRecord.id, detNum)
            self._insertIfAllowed(
                instrument=expRecord.instrument,
                table=f"cdb_{expRecord.instrument.lower()}.ccdexposure",
                obsId=obsId,  # integer form required for ccd-type tables
                values={"detector": detNum, "exposure_id": expRecord.id},
                allowUpdate=allowUpdate,
            )

    def populateCcdVisitRowWithButler(
        self,
        butler: Butler,
        expRecord: DimensionRecord,
        detectorNum: int,
        allowUpdate: bool = False,
    ) -> bool:
        try:
            summaryStats = butler.get(
                "preliminary_visit_image.summaryStats", visit=expRecord.id, detector=detectorNum
            )
        except DatasetNotFoundError:
            return False
        self.populateCcdVisitRow(expRecord, detectorNum, summaryStats, allowUpdate=allowUpdate)
        return True

    def populateCcdVisitRow(
        self,
        expRecord: DimensionRecord,
        detectorNum: int,
        summaryStats: ExposureSummaryStats,
        allowUpdate: bool = False,
    ) -> None:
        obsId = computeCcdExposureId(expRecord.instrument, expRecord.id, detectorNum)
        values = {value: getattr(summaryStats, key) for key, value in CCD_VISIT_MAPPING.items()}
        table = f"cdb_{expRecord.instrument.lower()}.ccdvisit1_quicklook"

        self._insertIfAllowed(
            instrument=expRecord.instrument,
            table=table,
            obsId=obsId,  # integer form required for ccd-type tables
            values=values,
            allowUpdate=allowUpdate,
            onSuccess=lambda: self.redisHelper.announceResultInConsDb(expRecord.instrument, table, obsId),
        )

    def populateHigherOrderMoments(
        self,
        expRecord: DimensionRecord,
        detectorNum: int,
        singleVisitStarFootprints: SourceCatalog | Table,
        allowUpdate: bool = False,
    ) -> None:
        # TODO: DM-54675 remove this whole function once we have these in
        # ExposureSummaryStats
        if isinstance(singleVisitStarFootprints, SourceCatalog):
            table = singleVisitStarFootprints.asAstropy()
        else:
            table = singleVisitStarFootprints

        m03 = table["ext_shapeHSM_HigherOrderMomentsSource_03"]
        m12 = table["ext_shapeHSM_HigherOrderMomentsSource_12"]
        m21 = table["ext_shapeHSM_HigherOrderMomentsSource_21"]
        m30 = table["ext_shapeHSM_HigherOrderMomentsSource_30"]
        m04 = table["ext_shapeHSM_HigherOrderMomentsSource_04"]
        m13 = table["ext_shapeHSM_HigherOrderMomentsSource_13"]
        m22 = table["ext_shapeHSM_HigherOrderMomentsSource_22"]
        m31 = table["ext_shapeHSM_HigherOrderMomentsSource_31"]
        m40 = table["ext_shapeHSM_HigherOrderMomentsSource_40"]

        coma_1 = float(np.nanmedian(m30 + m12))
        coma_2 = float(np.nanmedian(m21 + m03))
        trefoil_1 = float(np.nanmedian(m30 - 3 * m12))
        trefoil_2 = float(np.nanmedian(3 * m21 - m03))
        kurtosis = float(np.nanmedian(m40 + 2 * m22 + m04))
        e4_1 = float(np.nanmedian(m40 - m04))
        e4_2 = float(np.nanmedian(2 * (m31 + m13)))

        obsId = computeCcdExposureId(expRecord.instrument, expRecord.id, detectorNum)
        values = {
            "coma_1": coma_1,
            "coma_2": coma_2,
            "trefoil_1": trefoil_1,
            "trefoil_2": trefoil_2,
            "kurtosis": kurtosis,
            "e4_1": e4_1,
            "e4_2": e4_2,
        }
        table = f"cdb_{expRecord.instrument.lower()}.ccdvisit1_quicklook"

        self._insertIfAllowed(
            instrument=expRecord.instrument,
            table=table,
            obsId=obsId,  # integer form required for ccd-type tables
            values=values,
            allowUpdate=allowUpdate,
            onSuccess=lambda: self.redisHelper.announceResultInConsDb(expRecord.instrument, table, obsId),
        )

    def populateCcdVisitRowZernikes(
        self,
        visitRecord: DimensionRecord,
        detectorNum: int,
        zernikeValues: dict[str, float],
        allowUpdate: bool = False,
    ) -> None:
        """Populate a row in the cdb_<instrument>.ccdvisit1_quicklook table
        with Zernike values.

        Parameters
        ----------
        visitRecord : `DimensionRecord`
            The visit record to populate the row for.
        detectorNum : `int`
            The detector number to populate the row for.
        zernikeValues : `dict[str, float]`
            A dictionary containing Zernike values to populate the row with,
            where keys are Zernike names and values are the corresponding float
            values. Names are as in the consDB schema, e.g. "z4", "z5", etc.
        allowUpdate : `bool`, optional
            Allow updating existing rows in the table.
        """
        obsId = computeCcdExposureId(visitRecord.instrument, visitRecord.id, detectorNum)
        table = f"cdb_{visitRecord.instrument.lower()}.ccdvisit1_quicklook"

        self._insertIfAllowed(
            instrument=visitRecord.instrument,
            table=table,
            obsId=obsId,  # integer form required for ccd-type tables
            values=zernikeValues,
            allowUpdate=allowUpdate,
        )

    def populateAllCcdVisitRowsWithButler(
        self, butler: Butler, expRecord: DimensionRecord, createRows: bool = False, allowUpdate: bool = False
    ) -> int:
        if createRows:
            self._createExposureRow(expRecord, allowUpdate=allowUpdate)
            self._createCcdExposureRows(expRecord, allowUpdate=allowUpdate)
            print(f"Populated tables for exposure and ccdexposure for {expRecord.instrument}+{expRecord.id}")

        detectorNums = getDetectorIds(expRecord.instrument)
        nFilled = 0
        for detectorNum in detectorNums:
            nFilled += self.populateCcdVisitRowWithButler(
                butler, expRecord, detectorNum, allowUpdate=allowUpdate
            )
        return nFilled

    def populateVisitRowWithButler(
        self, butler: Butler, expRecord: DimensionRecord, allowUpdate: bool = False
    ) -> None:
        visitSummary = butler.get("preliminary_visit_summary", visit=expRecord.id)
        self.populateVisitRow(visitSummary, expRecord, allowUpdate=allowUpdate)

    def populateVisitRow(
        self, visitSummary: ExposureCatalog, expRecord: DimensionRecord, allowUpdate: bool = False
    ) -> None:
        instrument: str = expRecord.instrument
        if not self._shouldInsert():  # ugly but need to check this before accessing the schema
            location = self.locationConfig.location
            logger.info(f"Skipping consDB insert at {location} for {instrument}.visit1_quicklook")
            return

        typeMapping = self._getTypeMapping(instrument, "visit1_quicklook")

        visitSummary = visitSummary.asAstropy()
        visits = visitSummary["visit"]
        visit = visits[0]
        assert all(v == visit for v in visits)  # this has to be true, but let's be careful
        visit = int(visit)  # must be python int not np.int64

        values: dict[str, int | float] = {}
        for summaryKey, consDbKeyNoSuffix in itertools.chain(
            VISIT_MIN_MED_MAX_MAPPING.items(),
            VISIT_MIN_MED_MAX_TOTAL_MAPPING.items(),
        ):
            consDbKey = consDbKeyNoSuffix + "_min"
            typeFunc = changeType(consDbKey, typeMapping)
            values[consDbKey] = typeFunc(np.nanmin(visitSummary[summaryKey]))

            consDbKey = consDbKeyNoSuffix + "_max"
            typeFunc = changeType(consDbKey, typeMapping)
            values[consDbKey] = typeFunc(np.nanmax(visitSummary[summaryKey]))

            consDbKey = consDbKeyNoSuffix + "_median"
            typeFunc = changeType(consDbKey, typeMapping)
            values[consDbKey] = typeFunc(np.nanmedian(visitSummary[summaryKey]))

        for summaryKey, consDbKey in VISIT_MIN_MED_MAX_TOTAL_MAPPING.items():
            typeFunc = changeType(consDbKey + "_total", typeMapping)
            values[consDbKey + "_total"] = typeFunc(np.nansum(visitSummary[summaryKey]))

        nInputs = max([len(visitSummary[col]) for col in visitSummary.columns])
        minInputs = min([len(visitSummary[col]) for col in visitSummary.columns])
        if minInputs != nInputs:
            raise RuntimeError("preliminary_visit_summary is jagged - this should be impossible")

        values["n_inputs"] = nInputs
        values["visit_id"] = visit  # required key if updating
        table = f"cdb_{instrument.lower()}.visit1_quicklook"

        self._insertIfAllowed(
            instrument=instrument,
            table=table,
            # tuple-form for obsId required for updating non ccd-type tables
            obsId=(expRecord.day_obs, expRecord.seq_num),
            values=values,
            allowUpdate=allowUpdate,
            onSuccess=lambda: self.redisHelper.announceResultInConsDb(instrument, table, visit),
        )

    def populateArbitrary(
        self,
        instrument: str,
        table: str,
        values: dict[str, int | float],
        dayObs: int,
        seqNum: int,
        allowUpdate: bool = False,
    ) -> None:
        """Populate an arbitrary consDB table for a given visit or exposure.

        Parameters
        ----------
        instrument : `str`
            The instrument name, used to resolve the schema namespace (e.g.,
            "LATISS" or "lsstcam", case-insensitive).
        table : `str`
            The table name within the instrument schema (e.g.,
            "visit1_quicklook").
        values : `dict` [`str`, `int` or `float`]
            Mapping of consDB column names to values to write. Values are
            coerced to the database column types using the table schema; NaN
            values are dropped.
        dayObs : `int`
            The dayObs of the row to populate.
        seqNum : `int`
            The seqNum of the row to populate.
        allowUpdate : `bool`, optional
            If True, allow updating existing rows in the table. An error is
            raised if False and a value exists.
        """
        # validate before checking _shouldInsert() for better CI coverage
        if allowUpdate and "exposure" in table.lower() and "exposure_id" not in values:
            raise ValueError("When updating an exposure table, exposure_id must be in values")
        if allowUpdate and "visit" in table.lower() and "visit_id" not in values:
            raise ValueError("When updating a visit table, visit_id must be in values")

        if not self._shouldInsert():  # ugly but need to check this before accessing the schema
            location = self.locationConfig.location
            logger.info(f"Skipping consDB insert at {location} for {instrument}.{table}")
            return

        typeMapping = self._getTypeMapping(instrument, table)

        toSend: dict[str, int | float] = {}
        for consDbKey, value in values.items():
            if consDbKey not in typeMapping:
                raise ValueError(f"Key {consDbKey} not in consDB table {table}")

            typeFunc = changeType(consDbKey, typeMapping)
            toSend[consDbKey] = typeFunc(value)

        self._insertIfAllowed(
            instrument=instrument,
            table=table,
            # tuple-form for obsId required for updating non ccd-type tables
            obsId=(dayObs, seqNum),
            values=toSend,
            allowUpdate=allowUpdate,
            onSuccess=lambda: logger.info(
                f"Inserted consDB values into {instrument}.{table} for ({dayObs=}, {seqNum=})"
            ),
        )

    def populateMountErrors(
        self,
        expRecord: DimensionRecord,
        mountErrors: dict[str, float] | MountErrors,
        instrument: str,
    ) -> None:
        values: dict[str, float] = {}
        if isinstance(mountErrors, MountErrors):  # LSSTCam code path
            # image impact measurements
            imageError = (mountErrors.imageAzRms**2 + mountErrors.imageElRms**2) ** 0.5
            values["mount_motion_image_degradation"] = imageError
            values["mount_motion_image_degradation_az"] = mountErrors.imageAzRms
            values["mount_motion_image_degradation_el"] = mountErrors.imageElRms
            values["mount_motion_image_degradation_rot"] = mountErrors.imageRotRms

            # raw axis jitter values
            mountError = (mountErrors.azRms**2 + mountErrors.elRms**2) ** 0.5
            values["mount_jitter_rms"] = mountError
            values["mount_jitter_rms_az"] = mountErrors.azRms
            values["mount_jitter_rms_el"] = mountErrors.elRms
            values["mount_jitter_rms_rot"] = mountErrors.rotRms
            values["mount_jitter_rms_cam_hexapod"] = mountErrors.camHexRms
            values["mount_jitter_rms_m2_hexapod"] = mountErrors.m2HexRms
        elif isinstance(mountErrors, dict):  # LATISS code path until unified
            image_az_rms = mountErrors["image_az_rms"]
            image_el_rms = mountErrors["image_el_rms"]
            imageError = (image_az_rms**2 + image_el_rms**2) ** 0.5

            values["mount_motion_image_degradation"] = imageError
            values["mount_motion_image_degradation_az"] = mountErrors["image_az_rms"]
            values["mount_motion_image_degradation_el"] = mountErrors["image_el_rms"]

            az_rms = mountErrors["az_rms"]
            el_rms = mountErrors["el_rms"]
            mountError = (az_rms**2 + el_rms**2) ** 0.5
            values["mount_jitter_rms"] = mountError
            values["mount_jitter_rms_az"] = mountErrors["az_rms"]
            values["mount_jitter_rms_el"] = mountErrors["el_rms"]
            values["mount_jitter_rms_rot"] = mountErrors["rot_rms"]
        else:
            raise TypeError(f"Expected MountErrors or dict, got {type(mountErrors)}")

        table = f"cdb_{instrument.lower()}.exposure_quicklook"

        if "exposure_id" not in values:  # required key if updating
            values["exposure_id"] = expRecord.id

        self._insertIfAllowed(
            instrument=instrument,
            table=table,
            # tuple-form for obsId required for updating non ccd-type tables
            obsId=(expRecord.day_obs, expRecord.seq_num),
            values=values,
            # this should always be an update as it's going in the exposure
            # table which will always already be populated
            allowUpdate=True,
        )
