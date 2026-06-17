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

"""Sync AOS data products from the summit Butler to the USDF ``main`` repo.

AOS (active-optics) data products are produced at the summit and written
into the ``LSSTCam/runs/quickLook`` output chain. This module copies them
into the canonical ``/repo/main`` repo at USDF using the Butler Python
export/import API, one ``dayObs`` at a time.

Two roles share the machinery here:

- `SummitSyncExporter` runs at the summit. It exports each day's datasets to
  a self-contained bundle directory (a ``transfer="copy"`` export plus an
  ``export.yaml`` manifest) and uploads the bundle to an S3 scratch prefix in
  the summit embargo bucket, via the `MultiUploader`'s remote uploader.
- `SummitSyncImporter` runs at USDF. It reads completed bundles from that S3
  prefix (using a dedicated client, since the USDF pods need the summit
  embargo credentials to read it), downloads them, and imports them into the
  ``main`` repo.

The summit↔USDF link is intermittent, so the design is idempotent and
gap-filling: each side keeps a small local JSON `SyncLedger` of per-day
status, and every pass re-attempts any day not yet confirmed done and picks
up new days. The current ``dayObs`` is never exported, since it may still be
accumulating data.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import yaml
from boto3.s3.transfer import TransferConfig  # type: ignore[import-untyped]
from boto3.session import Session as S3Session  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]

from lsst.daf.butler import Butler, CollectionType, MissingDatasetTypeError
from lsst.resources import ResourcePath
from lsst.summit.utils.dateTime import getCurrentDayObsInt, offsetDayObs

from .predicates import raiseIf
from .timing import logDuration
from .uploaders import MultiUploader

if TYPE_CHECKING:
    from lsst.rubintv.production.locationConfig import LocationConfig


__all__ = [
    "DATASETS_TO_SYNC",
    "SyncLedger",
    "SummitSyncExporter",
    "SummitSyncImporter",
    "bundleDir",
    "bundleRelFiles",
    "destinationChainName",
    "prefixExportData",
    "s3KeyPrefix",
]

_LOG = logging.getLogger(__name__)

DATASETS_TO_SYNC = (
    "zernikes",
    "aggregateAOSVisitTableRaw",
    "aggregateAOSVisitTableAvg",
    "donutStampsIntra",
    "donutStampsExtra",
    "donutTable",
    "donutQualityTable",
)

INSTRUMENT = "LSSTCam"
EXPORT_FILENAME = "export.yaml"
PREFIXED_EXPORT_FILENAME = "export.prefixed.yaml"
COMPLETE_MARKER = "_COMPLETE"
LEDGER_FILENAME_EXPORT = "export_ledger.json"
LEDGER_FILENAME_IMPORT = "import_ledger.json"
SYNC_INTERVAL = 6 * 60 * 60  # seconds to sleep between full sync passes

# S3 transport. Bundles are uploaded to this key prefix in the summit embargo
# bucket. The importer reads them with a dedicated client built from the
# summit embargo profile, which the USDF pods must have installed in their
# credentials file (they have no summit-bucket credentials by default).
S3_BUNDLE_PREFIX = "summit_sync"
SUMMIT_EMBARGO_PROFILE = "rubin-rubintv-data-summit-embargo"
SUMMIT_EMBARGO_BUCKET = "rubin-rubintv-data-summit"
SUMMIT_EMBARGO_ENDPOINT = "https://sdfembs3.sdf.slac.stanford.edu/"
# Threads used to upload/download a bundle's files. The transfers are
# latency-bound (each small file is one S3 round trip, with the GIL released
# during the network/TLS C calls), so parallelism helps even on a single core.
S3_TRANSFER_WORKERS = 8
# Connection pool size for the S3 clients (botocore's default is 10). Sized
# well above the worker count so warm connections are retained and reused
# across files rather than discarded ("connection pool is full" churn), which
# dominates the cost when transferring many small files. Leaves headroom for
# the occasional multipart transfer (which itself opens several connections).
S3_MAX_POOL_CONNECTIONS = 30
# Transfer tuning: raise the multipart threshold so the (small) AOS files
# transfer as a single PUT/GET on one reusable connection, instead of boto3's
# 8 MB default that multiparts them into many short-lived part connections. The
# larger chunk size keeps any genuinely big file to a handful of parts.
S3_TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=1024 * 1024 * 1024,  # 1 GiB
    multipart_chunksize=256 * 1024 * 1024,  # 256 MiB
)
# The exporter prunes S3 bundles older than this (the importer's read-only
# client cannot). Keep it generous: a deleted-but-not-yet-imported day needs a
# manual re-export to recover, so the window must exceed any plausible time
# the importer could be behind.
S3_RETENTION_DAYS = 30

# Ledger statuses.
EXPORTED = "exported"  # bundle written locally on the summit
SENT = "sent"  # bundle uploaded to the S3 scratch prefix
IMPORTED = "imported"  # bundle imported into the USDF main repo


# Register the same "!uuid" YAML tag handling that Butler uses, so the
# (opt-in) prefix rewrite can round-trip an export.yaml through safe_load /
# safe_dump. Butler reads export files with yaml.safe_load and the only
# non-native scalar in them is the "!uuid" dataset_id, so these two
# registrations are all that is needed.
def _uuidRepresenter(dumper: yaml.SafeDumper, data: uuid.UUID) -> yaml.Node:
    return dumper.represent_scalar("!uuid", str(data))


def _uuidConstructor(loader: yaml.SafeLoader, node: yaml.Node) -> uuid.UUID | None:
    value = loader.construct_scalar(node)  # type: ignore[arg-type]
    return uuid.UUID(hex=value) if value else None


yaml.SafeLoader.add_constructor("!uuid", _uuidConstructor)
yaml.SafeDumper.add_representer(uuid.UUID, _uuidRepresenter)


def bundleDir(stagingPath: str, instrument: str, dayObs: int) -> str:
    """Return the per-day bundle directory inside a staging path.

    Parameters
    ----------
    stagingPath : `str`
        The root staging directory.
    instrument : `str`
        The instrument name, e.g. ``"LSSTCam"``.
    dayObs : `int`
        The ``dayObs`` (``YYYYMMDD``) the bundle is for.

    Returns
    -------
    path : `str`
        The bundle directory ``<stagingPath>/<instrument>/<dayObs>``.
    """
    return os.path.join(stagingPath, instrument, str(dayObs))


def bundleRelFiles(bundlePath: str) -> list[str]:
    """Return a bundle's file paths relative to its root, sorted.

    The completion marker is excluded, since it is uploaded separately and
    last. These relative paths are both the S3 key suffixes the files are
    uploaded under and the manifest the importer verifies against.

    Parameters
    ----------
    bundlePath : `str`
        The bundle directory to walk.

    Returns
    -------
    relFiles : `list` [`str`]
        File paths relative to ``bundlePath``, sorted, excluding the
        `COMPLETE_MARKER`.
    """
    relFiles: list[str] = []
    for root, _dirs, files in os.walk(bundlePath):
        for name in files:
            if name == COMPLETE_MARKER:
                continue
            relFiles.append(os.path.relpath(os.path.join(root, name), bundlePath))
    return sorted(relFiles)


def s3KeyPrefix(instrument: str, dayObs: int) -> str:
    """Return the S3 key prefix a day's bundle is stored under.

    Parameters
    ----------
    instrument : `str`
        The instrument name, e.g. ``"LSSTCam"``.
    dayObs : `int`
        The ``dayObs`` (``YYYYMMDD``) the bundle is for.

    Returns
    -------
    prefix : `str`
        The key prefix, e.g. ``"summit_sync/LSSTCam/20240101"`` (no trailing
        slash).
    """
    return f"{S3_BUNDLE_PREFIX}/{instrument}/{dayObs}"


def _runParallel(func: Callable[[Any], Any], items: list[Any], maxWorkers: int) -> list[Any]:
    """Apply ``func`` to each item across a thread pool, results in order.

    Used to parallelise the per-file S3 uploads and downloads, which are
    latency-bound rather than CPU-bound. The first exception any call raises
    propagates out (so a failed transfer leaves the day un-marked and is
    retried next pass).

    Parameters
    ----------
    func : `~collections.abc.Callable`
        The callable to apply to each item.
    items : `list`
        The items to process.
    maxWorkers : `int`
        The maximum number of worker threads.

    Returns
    -------
    results : `list`
        The results of ``func`` for each item, in the order of ``items``.
    """
    if not items:
        return []
    workers = max(1, min(maxWorkers, len(items)))
    if workers == 1:
        return [func(item) for item in items]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(func, items))


def destinationChainName(instrument: str, isolationPrefix: str = "") -> str:
    """Return the CHAINED collection name the importer maintains at USDF.

    This mirrors the summit's quickLook output chain so the imported data is
    queryable the same way it is at the summit. When an isolation prefix is in
    effect it is prepended, matching the prefixed RUN names that the import
    created.

    Parameters
    ----------
    instrument : `str`
        The instrument name, e.g. ``"LSSTCam"``.
    isolationPrefix : `str`, optional
        The isolation prefix, or empty to preserve the verbatim namespace.

    Returns
    -------
    chain : `str`
        The CHAINED collection name, e.g. ``"LSSTCam/runs/quickLook"``.
    """
    chain = f"{instrument}/runs/quickLook"
    return f"{isolationPrefix}/{chain}" if isolationPrefix else chain


def daysNeedingWork(
    availableDays: set[int],
    doneDays: set[int],
    before: int | None = None,
) -> list[int]:
    """Return the sorted list of dayObs that still need processing.

    Parameters
    ----------
    availableDays : `set` [`int`]
        All dayObs that have data available to process.
    doneDays : `set` [`int`]
        The dayObs already recorded as done in the ledger.
    before : `int` or `None`, optional
        If given, only dayObs strictly less than this are returned. The
        exporter passes the current dayObs here so the in-progress day,
        which may still be accumulating data, is never sent.

    Returns
    -------
    days : `list` [`int`]
        The dayObs needing work, oldest first.
    """
    pending = set(availableDays) - set(doneDays)
    if before is not None:
        pending = {day for day in pending if day < before}
    return sorted(pending)


def prefixExportData(exportData: dict[str, Any], prefix: str) -> dict[str, Any]:
    """Prefix every collection name in a parsed export.yaml.

    Used to isolate imported datasets under a dedicated namespace at the
    destination so they cannot collide with anything the destination repo
    already contains. Operates on the loaded YAML structure, prepending
    ``<prefix>/`` to every RUN/CHAINED/TAGGED collection name, every dataset
    ``run`` field, every chained-collection child, and every dataset
    association ``collection``. Nothing else is touched.

    Parameters
    ----------
    exportData : `dict` [`str`, `Any`]
        The parsed contents of an ``export.yaml`` file.
    prefix : `str`
        The namespace prefix, e.g. ``"summit_sync"``. A trailing slash is
        ignored.

    Returns
    -------
    prefixed : `dict` [`str`, `Any`]
        The same mapping with all collection names prefixed. The input is
        mutated in place and also returned for convenience.
    """
    prefix = prefix.rstrip("/")

    def _prefixed(name: str) -> str:
        return f"{prefix}/{name}"

    for entry in exportData.get("data", []):
        entryType = entry.get("type")
        if entryType == "collection":
            entry["name"] = _prefixed(entry["name"])
            if "children" in entry:  # CHAINED collections
                entry["children"] = [_prefixed(child) for child in entry["children"]]
        elif entryType == "dataset":
            entry["run"] = _prefixed(entry["run"])
        elif entryType == "associations":
            entry["collection"] = _prefixed(entry["collection"])
    return exportData


class SyncLedger:
    """A small JSON-backed record of per-dayObs sync status.

    The ledger is the non-Butler local state that lets a sync pass be
    idempotent and gap-filling: it records which days have reached which
    status so a restarted or re-run service does not redo finished work and
    does retry days that never completed. Writes are atomic (tmp file +
    ``os.replace``).

    Parameters
    ----------
    path : `str`
        Absolute path to the JSON ledger file. The file need not exist yet;
        it is created on the first `mark`.
    """

    def __init__(self, path: str) -> None:
        self.path = path

    def load(self) -> dict[int, str]:
        """Return the ledger contents as a ``{dayObs: status}`` mapping.

        Returns
        -------
        data : `dict` [`int`, `str`]
            The mapping, empty if the file does not exist or is empty.
        """
        if not os.path.isfile(self.path) or os.path.getsize(self.path) == 0:
            return {}
        with open(self.path) as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict):
            return {}
        return {int(k): str(v) for k, v in loaded.items()}

    def mark(self, dayObs: int, status: str) -> None:
        """Record ``status`` for ``dayObs``, persisting atomically.

        Parameters
        ----------
        dayObs : `int`
            The dayObs to record.
        status : `str`
            The status string, e.g. `EXPORTED`, `SENT`, `IMPORTED`.
        """
        data = self.load()
        data[dayObs] = status
        tmpFile = f"{self.path}.tmp"
        with open(tmpFile, "w") as f:
            json.dump({str(k): v for k, v in sorted(data.items())}, f, indent=2)
        os.replace(tmpFile, self.path)

    def status(self, dayObs: int) -> str | None:
        """Return the recorded status for ``dayObs``, or `None`."""
        return self.load().get(dayObs)

    def daysWith(self, status: str) -> set[int]:
        """Return the set of dayObs currently recorded with ``status``."""
        return {day for day, value in self.load().items() if value == status}


class SummitSyncExporter:
    """Export AOS data products a day at a time and deliver them to USDF.

    Runs at the summit. Each pass discovers which dayObs have AOS data,
    exports each not-yet-sent day to a self-contained bundle, uploads the
    bundle to the S3 scratch prefix, and records progress in a local
    `SyncLedger`.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location configuration for the summit.
    instrument : `str`, optional
        The instrument to sync. Defaults to ``"LSSTCam"``.
    doRaise : `bool`, optional
        If `True`, re-raise per-day exceptions rather than logging and
        continuing to the next day.
    """

    def __init__(
        self,
        locationConfig: LocationConfig,
        instrument: str = INSTRUMENT,
        doRaise: bool = False,
    ) -> None:
        self.log = _LOG.getChild("SummitSyncExporter")
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.doRaise = doRaise

        self.stagingPath = locationConfig.summitSyncStagingPath
        self.collection = locationConfig.getOutputChain(instrument)
        self.ledger = SyncLedger(os.path.join(self.stagingPath, LEDGER_FILENAME_EXPORT))

        # The MultiUploader's remote uploader writes to the summit embargo
        # bucket at USDF (which is what the importer reads). Its top-level
        # upload() fires the remote write in a background thread and also
        # writes to the summit-local bucket, so we drive the remote uploader
        # directly for confirmed, summit-bucket-free delivery.
        self.uploader = MultiUploader(maxPoolConnections=S3_MAX_POOL_CONNECTIONS)
        if not self.uploader.hasRemote:
            raise RuntimeError("Summit sync export needs the remote (USDF) S3 uploader, which is unavailable")

        self.butler = Butler.from_config(
            locationConfig.lsstCamButlerPath,
            instrument=instrument,
            collections=[self.collection],
            writeable=False,
        )

    def discoverDays(self) -> set[int]:
        """Return the set of dayObs that have any AOS data to sync.

        Returns
        -------
        days : `set` [`int`]
            Every dayObs for which at least one `DATASETS_TO_SYNC` dataset
            exists in the source collection.
        """
        self.log.info(f"Discovering days with AOS data in {self.collection} ...")
        days: set[int] = set()
        for datasetType in DATASETS_TO_SYNC:
            try:
                with logDuration(self.log, f"Querying {datasetType!r}"):
                    refs = self.butler.query_datasets(
                        datasetType,
                        collections=self.collection,
                        find_first=False,
                        limit=None,
                        explain=False,
                    )
            except MissingDatasetTypeError:
                self.log.info(f"Dataset type {datasetType!r} not registered; skipping in discovery")
                continue
            typeDays = {int(ref.dataId["day_obs"]) for ref in refs}
            self.log.info(f"  {datasetType}: {len(refs)} datasets across {len(typeDays)} day(s)")
            days.update(typeDays)
        self.log.info(f"Discovery complete: AOS data on {len(days)} day(s)")
        return days

    def exportDay(self, dayObs: int) -> int:
        """Export all AOS datasets for ``dayObs`` to a fresh bundle directory.

        Parameters
        ----------
        dayObs : `int`
            The dayObs to export.

        Returns
        -------
        nDatasets : `int`
            The number of datasets written to the bundle. Zero means the day
            had no AOS data and no bundle was produced.
        """
        self.log.info(f"Exporting {dayObs=} ...")
        localDir = bundleDir(self.stagingPath, self.instrument, dayObs)
        if os.path.isdir(localDir):  # start clean in case a prior attempt was partial
            shutil.rmtree(localDir)
        os.makedirs(localDir)

        refs = []
        for datasetType in DATASETS_TO_SYNC:
            try:
                found = self.butler.query_datasets(
                    datasetType,
                    where=f"instrument='{self.instrument}' AND day_obs={dayObs}",
                    collections=self.collection,
                    find_first=False,
                    limit=None,
                    explain=False,
                )
            except MissingDatasetTypeError:
                continue
            if found:
                self.log.info(f"  {datasetType}: {len(found)} datasets")
            refs.extend(found)

        if not refs:
            self.log.info(f"No AOS datasets found for {dayObs=}; nothing to export")
            shutil.rmtree(localDir)
            return 0

        exportFile = os.path.join(localDir, EXPORT_FILENAME)
        self.log.info(f"Writing manifest and copying {len(refs)} datasets to {localDir} ...")
        with logDuration(self.log, f"Export of {dayObs=}"):
            # Write the manifest only: export() defaults to transfer=None,
            # recording the (relative) datastore paths without copying.
            with self.butler.export(filename=exportFile) as export:
                export.saveDatasets(refs)
            # Copy the artifacts with retrieveArtifacts, which transfers them
            # in parallel (ResourcePath.mtransfer) rather than the serial
            # per-file copy of export(transfer="copy"). preserve_path=True
            # writes each file to the same relative path the manifest records,
            # so the bundle imports cleanly. Worker count comes from
            # LSST_RESOURCES_NUM_WORKERS (default CPU+2); raise it for these
            # I/O-bound S3 copies.
            self.butler.retrieveArtifacts(
                refs,
                ResourcePath(localDir, forceDirectory=True),
                transfer="copy",
                preserve_path=True,
                overwrite=True,
            )
        self.ledger.mark(dayObs, EXPORTED)
        self.log.info(f"Exported {len(refs)} datasets for {dayObs=} to {localDir}")
        return len(refs)

    def deliverBundle(self, dayObs: int) -> None:
        """Upload a day's bundle to the S3 scratch prefix, then mark it sent.

        The data files are uploaded first; the `COMPLETE_MARKER` is written
        and uploaded last so the importer never sees a partially-uploaded
        bundle. The marker contains the bundle's manifest (the relative file
        list) so the importer can verify completeness before importing.

        Because the underlying ``upload`` is best-effort (it logs and swallows
        failures), every object is verified present in the bucket before the
        day is marked sent; a missing object raises, leaving the day unsent so
        the next pass retries it. The local bundle is pruned once sent (it is
        now in S3, and re-exportable from the Butler if ever needed).

        Parameters
        ----------
        dayObs : `int`
            The dayObs whose bundle should be delivered.
        """
        localDir = bundleDir(self.stagingPath, self.instrument, dayObs)
        keyPrefix = s3KeyPrefix(self.instrument, dayObs)
        relFiles = bundleRelFiles(localDir)
        self.log.info(f"Uploading {len(relFiles)} files for {dayObs=} to s3 prefix {keyPrefix!r} ...")

        def _upload(rel: str) -> None:
            self.uploader.remoteUploader.upload(
                f"{keyPrefix}/{rel}", os.path.join(localDir, rel), transferConfig=S3_TRANSFER_CONFIG
            )

        # Upload in parallel: each file is one latency-bound S3 round trip, so
        # a thread pool fills the link far better than the serial loop. The
        # shared bucket resource drives transfers through its thread-safe
        # client.
        with logDuration(self.log, f"Upload of {dayObs=}"):
            _runParallel(_upload, relFiles, S3_TRANSFER_WORKERS)

        # Write the manifest into the completion marker and upload it last, so
        # the importer only ever sees a complete, verifiable bundle.
        self.log.info("Data uploaded; writing and uploading the completion marker")
        markerPath = os.path.join(localDir, COMPLETE_MARKER)
        with open(markerPath, "w") as f:
            f.write("\n".join(relFiles) + "\n")
        self.uploader.remoteUploader.upload(f"{keyPrefix}/{COMPLETE_MARKER}", markerPath)

        self._verifyUploaded(keyPrefix, relFiles + [COMPLETE_MARKER])
        self.ledger.mark(dayObs, SENT)
        self.log.info(f"Uploaded and verified bundle for {dayObs=} ({len(relFiles)} files); pruning local")
        shutil.rmtree(localDir)

    def _verifyUploaded(self, keyPrefix: str, relFiles: list[str]) -> None:
        """Check every expected object is present in the bucket.

        Parameters
        ----------
        keyPrefix : `str`
            The S3 key prefix the bundle was uploaded under.
        relFiles : `list` [`str`]
            The relative paths expected under ``keyPrefix``.

        Raises
        ------
        RuntimeError
            Raised if any expected object is missing from the bucket.
        """
        present = self.uploader.remoteUploader.listFiles(f"{keyPrefix}/")
        missing = {f"{keyPrefix}/{rel}" for rel in relFiles} - present
        if missing:
            raise RuntimeError(
                f"Upload incomplete for {keyPrefix!r}: {len(missing)} object(s) missing, "
                f"e.g. {sorted(missing)[:3]}"
            )

    def cleanupOldBundles(self) -> None:
        """Delete S3 bundles older than `S3_RETENTION_DAYS` to reclaim scratch.

        The importer's read-only client cannot prune the S3 copies, so the
        exporter does it here (it has write credentials). Anything whose
        ``dayObs`` is at or before the retention cutoff is removed.
        """
        cutoff = offsetDayObs(getCurrentDayObsInt(), -S3_RETENTION_DAYS)
        prefix = f"{S3_BUNDLE_PREFIX}/{self.instrument}/"
        keysByDay: dict[int, list[str]] = {}
        for key in self.uploader.remoteUploader.listFiles(prefix):
            dayStr = key[len(prefix) :].split("/", 1)[0]
            if dayStr.isdigit():
                keysByDay.setdefault(int(dayStr), []).append(key)

        oldDays = sorted(day for day in keysByDay if day <= cutoff)
        if not oldDays:
            return
        self.log.info(f"Pruning S3 bundles for {len(oldDays)} day(s) at or before {cutoff}")
        for day in oldDays:
            keys = keysByDay[day]
            self.log.info(f"Deleting {len(keys)} S3 object(s) for old {day=}")
            self.uploader.remoteUploader.deleteFiles(keys)

    def runOnce(self) -> None:
        """Run a single export+deliver pass over all days needing work."""
        self.log.info("Starting AOS export pass")
        currentDayObs = getCurrentDayObsInt()
        available = self.discoverDays()
        todo = daysNeedingWork(available, self.ledger.daysWith(SENT), before=currentDayObs)
        self.log.info(f"{len(todo)} day(s) to sync: {todo if len(todo) <= 20 else f'{todo[:20]}...'}")
        for i, dayObs in enumerate(todo, start=1):
            self.log.info(f"=== Syncing {dayObs=} ({i}/{len(todo)}) ===")
            try:
                if self.exportDay(dayObs) == 0:
                    continue
                self.deliverBundle(dayObs)
            except Exception as e:
                msg = f"Failed to sync {dayObs=}: {e}"
                raiseIf(self.doRaise, e, self.log, msg)
        try:
            self.cleanupOldBundles()
        except Exception as e:
            raiseIf(self.doRaise, e, self.log, f"S3 bundle cleanup failed: {e}")
        self.log.info(f"Export pass complete; synced {len(todo)} day(s)")

    def run(self) -> None:
        """Run export passes forever, sleeping `SYNC_INTERVAL` between them."""
        while True:
            self.runOnce()
            self.log.info(f"Sleeping {SYNC_INTERVAL / 3600:.1f}h until the next export pass")
            time.sleep(SYNC_INTERVAL)


class SummitSyncImporter:
    """Import AOS bundles delivered from the summit into the USDF main repo.

    Runs at USDF. Each pass lists the S3 scratch prefix for bundles marked
    complete, downloads and imports each not-yet-imported day into the
    ``main`` repo, keeps the destination CHAINED collection pointed at the
    imported runs, and records progress in a local `SyncLedger`. The S3 read
    client is built from the summit embargo profile (`SUMMIT_EMBARGO_PROFILE`),
    which the USDF pods must have installed.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location configuration for USDF.
    mainRepo : `str`
        The destination Butler repo to import into. Supplied by the entry
        script since the importer only ever runs at USDF (e.g. ``/repo/main``).
    instrument : `str`, optional
        The instrument to sync. Defaults to ``"LSSTCam"``.
    isolationPrefix : `str`, optional
        Optional collection-name prefix applied on import to isolate the
        imported datasets from anything already in the destination repo.
        Empty (the default) preserves the original summit RUN names verbatim.
    doRaise : `bool`, optional
        If `True`, re-raise per-day exceptions rather than logging and
        continuing to the next day.
    """

    def __init__(
        self,
        locationConfig: LocationConfig,
        mainRepo: str,
        instrument: str = INSTRUMENT,
        isolationPrefix: str = "",
        doRaise: bool = False,
    ) -> None:
        self.log = _LOG.getChild("SummitSyncImporter")
        self.locationConfig = locationConfig
        self.instrument = instrument
        self.doRaise = doRaise

        self.stagingPath = locationConfig.summitSyncStagingPath
        self.isolationPrefix = isolationPrefix
        self.destinationChain = destinationChainName(instrument, isolationPrefix)
        self.ledger = SyncLedger(os.path.join(self.stagingPath, LEDGER_FILENAME_IMPORT))

        # Dedicated read client for the summit embargo bucket. This is NOT the
        # MultiUploader (the USDF pods need the summit embargo credentials,
        # which are separate); the profile must be present in the credentials
        # file.
        session = S3Session(profile_name=SUMMIT_EMBARGO_PROFILE)
        self.bucket = session.resource(
            "s3",
            endpoint_url=SUMMIT_EMBARGO_ENDPOINT,
            config=Config(max_pool_connections=S3_MAX_POOL_CONNECTIONS),
        ).Bucket(SUMMIT_EMBARGO_BUCKET)

        self.butler = Butler.from_config(mainRepo, writeable=True)

    def discoverDays(self) -> set[int]:
        """Return the set of dayObs with a complete bundle in the S3 prefix.

        Returns
        -------
        days : `set` [`int`]
            Every dayObs whose S3 prefix contains a `COMPLETE_MARKER`.
        """
        prefix = f"{S3_BUNDLE_PREFIX}/{self.instrument}/"
        self.log.info(f"Scanning s3 prefix {prefix!r} for completed bundles ...")
        markerSuffix = f"/{COMPLETE_MARKER}"
        days: set[int] = set()
        for obj in self.bucket.objects.filter(Prefix=prefix):
            if not obj.key.endswith(markerSuffix):
                continue
            dayStr = obj.key[len(prefix) :].split("/", 1)[0]
            if dayStr.isdigit():
                days.add(int(dayStr))
        self.log.info(f"Found {len(days)} completed bundle(s) in s3")
        return days

    def importDay(self, dayObs: int) -> None:
        """Download, import, and prune a day's bundle; refresh the chain.

        The bundle is downloaded from S3 to a fresh local directory, verified
        complete against its manifest, and imported into the ``main`` repo.
        The local download is removed afterwards (the ledger prevents
        reimport), and the destination CHAINED collection is re-pointed at the
        imported runs. The S3 copy is left in place (the read-only client
        cannot prune it).

        Parameters
        ----------
        dayObs : `int`
            The dayObs whose bundle should be imported.
        """
        self.log.info(f"Importing {dayObs=} ...")
        localDir = bundleDir(self.stagingPath, self.instrument, dayObs)
        if os.path.isdir(localDir):  # start clean in case a prior attempt was partial
            shutil.rmtree(localDir)
        os.makedirs(localDir)

        keyPrefix = s3KeyPrefix(self.instrument, dayObs) + "/"
        self.log.info(f"Downloading bundle from s3 prefix {keyPrefix!r} to {localDir} ...")
        downloads: list[tuple[str, str]] = []
        for obj in self.bucket.objects.filter(Prefix=keyPrefix):
            rel = obj.key[len(keyPrefix) :]
            if not rel:  # skip a zero-length "directory" placeholder key
                continue
            downloads.append((obj.key, os.path.join(localDir, rel)))

        # Pre-create the parent dirs single-threaded so the parallel downloads
        # below only write files (no mkdir races).
        for _, dest in downloads:
            os.makedirs(os.path.dirname(dest), exist_ok=True)

        def _download(item: tuple[str, str]) -> None:
            key, dest = item
            self.bucket.download_file(key, dest, Config=S3_TRANSFER_CONFIG)

        with logDuration(self.log, f"Download of {dayObs=}"):
            _runParallel(_download, downloads, S3_TRANSFER_WORKERS)

        self._verifyComplete(localDir)

        exportFile = os.path.join(localDir, EXPORT_FILENAME)
        if self.isolationPrefix:
            self.log.info(f"Applying isolation prefix {self.isolationPrefix!r} to export.yaml")
            exportFile = self._writePrefixedExport(localDir)

        self.log.info(f"Importing bundle from {localDir} into the main repo (transfer=copy) ...")
        with logDuration(self.log, f"Import of {dayObs=}"):
            self.butler.import_(directory=localDir, filename=exportFile, transfer="copy")
        self.ledger.mark(dayObs, IMPORTED)
        self.log.info(f"Imported bundle for {dayObs=}; pruning local download")
        shutil.rmtree(localDir)
        self.updateChain()

    def _verifyComplete(self, localDir: str) -> None:
        """Check a downloaded bundle matches the manifest in its marker.

        Parameters
        ----------
        localDir : `str`
            The downloaded bundle directory.

        Raises
        ------
        RuntimeError
            Raised if any file listed in the `COMPLETE_MARKER` manifest is
            missing from the download.
        """
        with open(os.path.join(localDir, COMPLETE_MARKER)) as f:
            expected = {line for line in f.read().splitlines() if line}
        missing = expected - set(bundleRelFiles(localDir))
        if missing:
            raise RuntimeError(
                f"Incomplete bundle in {localDir}: missing {len(missing)} file(s), "
                f"e.g. {sorted(missing)[:3]}"
            )

    def updateChain(self) -> None:
        """Point the destination CHAINED collection at the imported runs.

        Registers the chain if it does not exist, then redefines it to span
        every per-day RUN imported under it. Idempotent and self-healing: each
        call re-points the chain at all current runs, so a missed update is
        repaired by the next import.
        """
        self.butler.collections.register(self.destinationChain, CollectionType.CHAINED)
        runs = self.butler.collections.query(f"{self.destinationChain}/*", CollectionType.RUN)
        self.butler.collections.redefine_chain(self.destinationChain, runs)
        self.log.info(f"Chain {self.destinationChain} now spans {len(runs)} run(s)")

    def _writePrefixedExport(self, localDir: str) -> str:
        """Write a prefixed copy of a bundle's export.yaml and return its path.

        Parameters
        ----------
        localDir : `str`
            The bundle directory containing ``export.yaml``.

        Returns
        -------
        path : `str`
            The path to the prefixed export manifest.
        """
        with open(os.path.join(localDir, EXPORT_FILENAME)) as f:
            data = yaml.safe_load(f)
        prefixExportData(data, self.isolationPrefix)
        outFile = os.path.join(localDir, PREFIXED_EXPORT_FILENAME)
        with open(outFile, "w") as f:
            yaml.safe_dump(data, f, sort_keys=False)
        return outFile

    def runOnce(self) -> None:
        """Run one import pass over all complete, not-yet-imported days."""
        self.log.info("Starting AOS import pass")
        available = self.discoverDays()
        todo = daysNeedingWork(available, self.ledger.daysWith(IMPORTED))
        self.log.info(f"{len(todo)} day(s) to import: {todo if len(todo) <= 20 else f'{todo[:20]}...'}")
        for i, dayObs in enumerate(todo, start=1):
            self.log.info(f"=== Importing {dayObs=} ({i}/{len(todo)}) ===")
            try:
                self.importDay(dayObs)
            except Exception as e:
                msg = f"Failed to import {dayObs=}: {e}"
                raiseIf(self.doRaise, e, self.log, msg)
        self.log.info(f"Import pass complete; imported {len(todo)} day(s)")

    def run(self) -> None:
        """Run import passes forever, sleeping `SYNC_INTERVAL` between them."""
        while True:
            self.runOnce()
            self.log.info(f"Sleeping {SYNC_INTERVAL / 3600:.1f}h until the next import pass")
            time.sleep(SYNC_INTERVAL)
