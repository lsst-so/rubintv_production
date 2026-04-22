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
import re
import shutil
import time
from datetime import timedelta
from itertools import batched
from pathlib import Path
from typing import TYPE_CHECKING

import astropy.units as u  # type: ignore[import-untyped]
import humanize
from astroplan import Observer
from astropy.time import Time

from lsst.daf.butler import Butler
from lsst.obs.lsst.translators.lsst import SIMONYI_LOCATION
from lsst.summit.utils.dateTime import getCurrentDayObsInt, offsetDayObs

from .highLevelTools import deleteAllSkyStills, deleteNonFinalAllSkyMovies, syncBuckets
from .resources import getBasePath, getSubDirs, rmtree
from .uploaders import MultiUploader
from .utils import raiseIf

if TYPE_CHECKING:
    from lsst.rubintv.production.utils import LocationConfig


__all__ = ["TempFileCleaner"]

_LOG = logging.getLogger(__name__)

DATASETS_TO_CLEAN = (
    "isrStatistics",
    "isr_metadata",
    "calibrateImage_metadata",
    "preliminary_visit_image_background",
    "initial_astrometry_match_detector",
    "initial_photometry_match_detector",
    "preliminary_visit_mask",
    "verifyFlatIsrExpBin8",
    "verifyFlatIsrExpBin64",
    "verifyFlatIsr_metadata",
    "verifyBiasIsrExpBin64",
    "verifyBiasIsrExpBin8",
    "verifyBiasIsr_metadata",
    "verifyDarkIsrExpBin64",
    "verifyDarkIsrExpBin8",
    "verifyDarkIsr_metadata",
    "postISRCCD",
)
# TODO: switch to get collection chain from
# self.locationConfig.getOutputChain("LSSTCam") once unit test merge is done.
# Note the trailing slash - not sure how important it is, but check
COLLECTION_CHAIN = "LSSTCam/runs/quickLook/"
BATCHSIZE = 5000
SPEED_FACTOR = 0.5  # 0.5 = half-speed, 0.33 = one-third etc
SMALL_FILE_KEEP_DAYS = 14


def interruptibleSleep(duration: float, deadline: Time) -> bool:
    """Sleep up to ``duration`` seconds, returning early if ``deadline``
    passes.

    The deadline is checked at most every 5 seconds, so the call may
    continue sleeping for up to that long after ``deadline`` has passed.

    Parameters
    ----------
    duration : `float`
        The maximum number of seconds to sleep for.
    deadline : `astropy.time.Time`
        Absolute time after which the sleep should be cut short.

    Returns
    -------
    deadlineHit : `bool`
        ``True`` if the sleep was cut short because ``deadline`` had passed,
        ``False`` if the full ``duration`` was slept.
    """
    slept = 0.0
    while slept < duration:
        if Time.now() >= deadline:
            return True
        chunk = min(5.0, duration - slept)
        time.sleep(chunk)
        slept += chunk
    return False


def waitUntil(t: Time) -> None:
    """Block until the given absolute time has passed.

    Returns immediately if ``t`` is already in the past. This is a plain
    blocking sleep with no interruption mechanism.

    Parameters
    ----------
    t : `astropy.time.Time`
        The absolute time to wait for.
    """
    delta = (t - Time.now()).to(u.s).value
    if delta > 0:
        time.sleep(delta)


class TempFileCleaner:
    """Clean up temporary files, directories, and stale Butler datasets
    created by Rapid Analysis.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration for the site this cleaner is running at.
    doRaise : `bool`, optional
        If ``True``, re-raise exceptions encountered during cleanup rather
        than logging and continuing.
    """

    def __init__(self, locationConfig: LocationConfig, doRaise: bool = False) -> None:
        self.log = _LOG.getChild("TempFileCleaner")
        self.doRaise = doRaise
        self.locationConfig = locationConfig

        # TODO: probably move these to yaml when we do the LocationConfig
        # refactor
        self.nfsDirsToDelete = {
            "LATISSPlots": Path(locationConfig.plotPath) / "LATISS",
            "LSSTCamPlots": Path(locationConfig.plotPath) / "LSSTCam",
        }
        self.s3DirsToDelete = ("binnedImages/",)  # NB: must end in a trailing slash
        self.keepDaysS3temp = 2  # 2 means current dayObs and the day before
        self.keepDaysPixelProducts = 14  # keep the last two weeks for pixel products for now

        self.butler = Butler.from_config(
            locationConfig.lsstCamButlerPath,
            instrument="LSSTCam",
            collections=[
                "LSSTCam/defaults",
                locationConfig.getOutputChain("LSSTCam"),
            ],
            writeable=True,
        )
        self.observer = Observer(location=SIMONYI_LOCATION)

    def deletePixelProducts(self) -> None:
        """Delete old pixel data products for LSSTCam from the quickLook
        output chain.

        Only runs at summit-like sites (summit/BTS/TTS). The retention
        window is set by ``self.keepDaysPixelProducts``.
        """
        # TODO: add post_isr_image to main list and remove this function
        # entirely once that cleanup code is actually managing to finish before
        # sunset - the only reason this is separate it to do the big stuff
        # first while still making max progress on the small files
        site = self.locationConfig.location
        if site.lower() not in ("summit", "bts", "tts"):
            self.log.info(f"Pixel products are only deleted at summit/BTS/TTS sites, not {site}, skipping")
            return

        currentDayObs = getCurrentDayObsInt()
        deleteBefore = offsetDayObs(currentDayObs, -self.keepDaysPixelProducts)

        where = f"exposure.day_obs<={deleteBefore} AND instrument='LSSTCam'"
        for product in [
            "post_isr_image",
        ]:
            self.log.info(f"Querying for {product}s to delete before {deleteBefore}...")
            allDRefs = self.butler.query_datasets(
                product,
                where=where,
                limit=1_000_000_000,
                collections=self.locationConfig.getOutputChain("LSSTCam"),
                explain=False,  # sometimes there's nothing and this is expected
            )
            days = sorted(set(int(d.dataId["day_obs"]) for d in allDRefs))
            self.log.info(f"Found {len(allDRefs)} {product}s across {len(days)} days to delete")
            dayMap: dict[int, list] = {d: [] for d in days}
            for d in allDRefs:
                dayMap[int(d.dataId["day_obs"])].append(d)

            total = 0
            for dayObs, refs in dayMap.items():
                self.log.info(f"Removing {len(refs)} {product}s for {dayObs=}...")
                self.butler.pruneDatasets(
                    refs,
                    disassociate=True,
                    unstore=True,
                    purge=True,
                )
                total += len(refs)
                self.log.info(f"Deletion for {product} {100 * (total / len(allDRefs)):.1f}% complete")

    def cleanupPass(
        self,
        datasetsToClean: tuple[str, ...],
        deleteBefore: int,
        deadline: Time,
    ) -> int:
        """Run one pass of the deletion loop, bailing out when ``deadline``
        passes.

        Always finishes the in-flight ``pruneDatasets`` call before returning,
        so the function may overshoot ``deadline`` by up to the time taken to
        prune one batch.

        Parameters
        ----------
        datasetsToClean : `tuple` [`str`, ...]
            The dataset type names to delete.
        deleteBefore : `int`
            The dayObs cutoff: only datasets with ``day_obs <= deleteBefore``
            are deleted.
        deadline : `astropy.time.Time`
            Absolute time after which the pass should stop issuing new work.

        Returns
        -------
        totalDeletions : `int`
            The number of datasets deleted in this pass.
        """
        site = self.locationConfig.location
        if site.lower() not in ("summit", "bts", "tts"):
            self.log.info(f"Cleanup is only run at summit/BTS/TTS sites, not {site}, skipping")
            return 0

        totalDeletions = 0
        collections = self.butler.registry.queryCollections(f"*{COLLECTION_CHAIN}*")
        for dataset in datasetsToClean:
            for collection in collections:
                where = f"exposure.day_obs<={deleteBefore} AND instrument='LSSTCam'"
                t0 = time.time()
                dRefs = set(
                    self.butler.query_datasets(
                        dataset,
                        where=where,
                        limit=1_000_000_000_000,
                        collections=collection,
                        explain=False,
                        find_first=False,
                    )
                )
                if not dRefs:
                    continue
                self.log.info(f"Butler query found {len(dRefs)} {dataset}'s in {(time.time() - t0):.2f}s")

                for batch in batched(dRefs, BATCHSIZE):
                    if Time.now() >= deadline:
                        return totalDeletions
                    t0 = time.time()
                    self.butler.pruneDatasets(
                        batch,
                        disassociate=True,
                        unstore=True,
                        purge=True,
                    )
                    deletionTime = time.time() - t0
                    sleepDuration = deletionTime * (1 / SPEED_FACTOR - 1)
                    self.log.info(
                        f"Deleted {len(batch)} {dataset}'s in {deletionTime:.1f}s, "
                        f"sleeping for {sleepDuration:.1f}s"
                    )
                    totalDeletions += len(batch)
                    if interruptibleSleep(sleepDuration, deadline):
                        return totalDeletions
        return totalDeletions

    def deleteDirectories(self) -> None:
        """Delete dayObs-named subdirectories of the configured NFS paths
        that are older than ``self.keepDaysS3temp`` days.

        Only subdirectories whose names match ``YYYYMMDD`` and begin with
        ``2`` are considered. Regular files and non-matching directories
        are left untouched.
        """
        currentDayObs = getCurrentDayObsInt()
        deleteBefore = offsetDayObs(currentDayObs, -self.keepDaysS3temp)

        for locationName, dirPath in self.nfsDirsToDelete.items():
            self.log.info(f"Deleting old data from subdirectories in {dirPath}:")
            subDir = None
            try:
                subDirs = dirPath.iterdir()
                for subDir in subDirs:
                    if not subDir.is_dir():  # don't touch regular files
                        continue

                    dirName = subDir.name  # only delete dayObs type dirs
                    if not re.match(r"^2\d{7}$", dirName):
                        continue  # Skip if not in YYYYMMDD format and starting with a 2

                    day = int(dirName)
                    if day <= deleteBefore:
                        self.log.info(f"Deleting old data from {subDir}")
                        shutil.rmtree(subDir)
                    else:
                        self.log.info(f"Keeping {subDir} as it's not old enough yet")

            except Exception as e:
                msg = f"Error processing removing data from {subDir}: {e}"
                raiseIf(self.doRaise, e, self.log, msg)

    def deleteS3Directories(self) -> None:
        """Delete dayObs-named subdirectories of the configured S3 paths
        that are older than ``self.keepDaysS3temp`` days.

        Only subdirectories whose names match ``YYYYMMDD`` (with an
        optional trailing slash) and begin with ``2`` are considered.
        """
        currentDayObs = getCurrentDayObsInt()
        deleteBefore = offsetDayObs(currentDayObs, -self.keepDaysS3temp)

        basePath = getBasePath(self.locationConfig)
        for locationName in self.s3DirsToDelete:
            fullDirName = basePath.join(locationName)

            self.log.info(f"Deleting old data from subdirectories in {fullDirName}:")
            subDir = None
            try:
                subDirs = getSubDirs(fullDirName)
                for subDir in subDirs:
                    fullSubDir = fullDirName.join(subDir)
                    if not fullSubDir.isdir():  # don't touch regular files
                        continue

                    # only delete dayObs type dirs
                    if not re.match(r"^2\d{7}/?$", subDir):  # Allow optional trailing slash
                        continue  # Skip if not in YYYYMMDD format and starting with a 2

                    if subDir.endswith("/"):
                        subDir = subDir[:-1]

                    day = int(subDir)
                    if day <= deleteBefore:
                        self.log.info(f"Deleting old data from {fullSubDir}")
                        rmtree(fullSubDir)
                    else:
                        self.log.info(f"Keeping {fullSubDir} as it's not old enough yet")

            except Exception as e:
                msg = f"Error processing removing data from {subDir}: {e}"
                raiseIf(self.doRaise, e, self.log, msg)

    def cleanupAndSyncBuckets(self) -> None:
        """Delete stale S3 files and sync local and remote buckets.

        Delete any stale all sky stills and non-final movies from the buckets
        and sync the local bucket's objects to the remote.
        """
        # reinit the MultiUploader each time rather than holding one on the
        # class in case of connection problems
        mu = MultiUploader()

        remoteOk = mu.remoteUploader.checkAccess()
        if not remoteOk:
            self.log.warning("Cannot access remote bucket; skipping remote buck cleanup and sync")

        localBucket = mu.localUploader._s3Bucket
        remoteBucket = mu.remoteUploader._s3Bucket

        self.log.info("Deleting stale local all sky stills")
        deleteAllSkyStills(localBucket)

        self.log.info("Deleting local non-final movies")
        deleteNonFinalAllSkyMovies(localBucket)

        if remoteOk:
            self.log.info("Deleting stale remote all sky stills")
            deleteAllSkyStills(remoteBucket)

            self.log.info("Deleting remote non-final movies")
            deleteNonFinalAllSkyMovies(remoteBucket)

            self.log.info("Syncing remote bucket to local bucket's contents")
            syncBuckets(mu, self.locationConfig)  # always do the deletion before running the sync

        self.log.info("Finished bucket cleanup")

    def runEndOfNightCleanupAndSync(self) -> None:
        """Run all the fixed-cost cleanup chores.

        These are the tasks that run once per day at sunrise, before the
        throttled ``cleanupPass`` fills the remaining daylight hours.
        """
        self.deletePixelProducts()
        self.deleteDirectories()
        self.deleteS3Directories()
        try:
            # this can raise when there's USDF connection issues so don't
            # restart on this, the re-sync will happen the following day anyway
            self.cleanupAndSyncBuckets()
        except Exception as e:
            msg = f"Error during bucket cleanup and sync: {e}"
            raiseIf(self.doRaise, e, self.log, msg)
        self.log.info("Finished daily cleanup")

    def run(self) -> None:
        """Run sunrise chores then throttled cleanup until sunset, forever.

        At each sunrise: run the fixed chores (directory/bucket/pixel cleanup),
        then spend the rest of the daylight hours running ``cleanupPass`` on
        quickLook datasets. After sunset, sleep until the next sunrise.
        """
        while True:
            now = Time.now()
            nextSunrise = self.observer.sun_rise_time(now, which="next")
            nextSunset = self.observer.sun_set_time(now, which="next")

            if nextSunset < nextSunrise:
                sunset = nextSunset
            else:
                untilSunrise = humanize.precisedelta(timedelta(seconds=(nextSunrise - now).sec))
                self.log.info(f"Night-time; sleeping until sunrise at {nextSunrise.iso} (in {untilSunrise})")
                waitUntil(nextSunrise)
                sunset = self.observer.sun_set_time(Time.now(), which="next")

            self.log.info("Sunrise reached; running fixed daily chores")
            choresStart = time.time()
            self.runEndOfNightCleanupAndSync()
            choresElapsed = humanize.precisedelta(timedelta(seconds=time.time() - choresStart))
            self.log.info(f"Fixed chores finished in {choresElapsed}")

            if Time.now() >= sunset:
                self.log.warning("Sunset reached before cleanupPass could start; skipping deletions")
                continue

            currentDayObs = getCurrentDayObsInt()
            deleteBefore = offsetDayObs(currentDayObs, -SMALL_FILE_KEEP_DAYS)

            secondsUntilSunset = (sunset - Time.now()).sec
            untilSunset = humanize.precisedelta(timedelta(seconds=secondsUntilSunset))
            self.log.info(f"Running daytime deletions until sunset at {sunset.iso} " f"(in {untilSunset})")
            passStart = time.time()
            deleted = self.cleanupPass(
                DATASETS_TO_CLEAN,
                deleteBefore,
                deadline=sunset,
            )
            passElapsed = humanize.precisedelta(timedelta(seconds=time.time() - passStart))
            self.log.info(f"Pass ended; deleted {deleted} total datasets in {passElapsed}")
