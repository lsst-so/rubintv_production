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

"""Test cases for the dataclasses in clusterManagement."""

import unittest

import lsst.utils.tests
from lsst.rubintv.production.clusterManagement import (
    ClusterStatus,
    FlavorStatus,
    QueueItem,
    WorkerStatus,
)
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor


def _makeWorker(detector: int, depth: int = 0) -> PodDetails:
    """Build a PodDetails for a per-detector SFM worker."""
    return PodDetails(
        instrument="LSSTCam",
        podFlavor=PodFlavor.SFM_WORKER,
        detectorNumber=detector,
        depth=depth,
    )


def _makeWorkerStatus(
    detector: int,
    isBusy: bool = False,
    queueLength: int = 0,
) -> WorkerStatus:
    return WorkerStatus(
        worker=_makeWorker(detector),
        queueLength=queueLength,
        isBusy=isBusy,
        queueItems=[],
    )


def _makeFlavorStatus(
    name: str,
    statuses: tuple[WorkerStatus, ...],
) -> FlavorStatus:
    nFree = sum(1 for ws in statuses if not ws.isBusy)
    return FlavorStatus(name=name, nFreeWorkers=nFree, workerStatuses=statuses)


class QueueItemTestCase(lsst.utils.tests.TestCase):
    """Tests for the `QueueItem` dataclass."""

    def test_construction(self) -> None:
        item = QueueItem(index=3, who="SFM", dataIdInfo="exposure=12345")
        self.assertEqual(item.index, 3)
        self.assertEqual(item.who, "SFM")
        self.assertEqual(item.dataIdInfo, "exposure=12345")

    def test_equality(self) -> None:
        a = QueueItem(index=1, who="SFM", dataIdInfo="x")
        b = QueueItem(index=1, who="SFM", dataIdInfo="x")
        c = QueueItem(index=2, who="SFM", dataIdInfo="x")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)


class WorkerStatusTestCase(lsst.utils.tests.TestCase):
    """Tests for the `WorkerStatus` dataclass."""

    def test_construction(self) -> None:
        worker = _makeWorker(detector=42)
        items = [QueueItem(index=0, who="SFM", dataIdInfo="exposure=1")]
        status = WorkerStatus(worker=worker, queueLength=1, isBusy=True, queueItems=items)
        self.assertIs(status.worker, worker)
        self.assertEqual(status.queueLength, 1)
        self.assertTrue(status.isBusy)
        self.assertEqual(status.queueItems, items)


class FlavorStatusTestCase(lsst.utils.tests.TestCase):
    """Tests for the `FlavorStatus` dataclass and its derived properties."""

    def test_workersAndTotal(self) -> None:
        statuses = (
            _makeWorkerStatus(0, isBusy=False),
            _makeWorkerStatus(1, isBusy=True),
            _makeWorkerStatus(2, isBusy=False),
        )
        flavor = _makeFlavorStatus("SFM", statuses)
        self.assertEqual(flavor.totalWorkers, 3)
        self.assertEqual(len(flavor.workers), 3)
        self.assertEqual(flavor.workers, tuple(ws.worker for ws in statuses))

    def test_freeWorkersFiltersBusy(self) -> None:
        statuses = (
            _makeWorkerStatus(0, isBusy=False),
            _makeWorkerStatus(1, isBusy=True),
            _makeWorkerStatus(2, isBusy=False),
            _makeWorkerStatus(3, isBusy=True),
        )
        flavor = _makeFlavorStatus("SFM", statuses)
        free = flavor.freeWorkers
        self.assertEqual(len(free), 2)
        self.assertEqual({w.detectorNumber for w in free}, {0, 2})
        # The order from the source tuple is preserved.
        self.assertEqual(free, (statuses[0].worker, statuses[2].worker))

    def test_emptyFlavor(self) -> None:
        flavor = _makeFlavorStatus("SFM", ())
        self.assertEqual(flavor.totalWorkers, 0)
        self.assertEqual(flavor.workers, ())
        self.assertEqual(flavor.freeWorkers, ())

    def test_allBusy(self) -> None:
        statuses = (
            _makeWorkerStatus(0, isBusy=True),
            _makeWorkerStatus(1, isBusy=True),
        )
        flavor = _makeFlavorStatus("SFM", statuses)
        self.assertEqual(flavor.freeWorkers, ())
        self.assertEqual(flavor.totalWorkers, 2)

    def test_allFree(self) -> None:
        statuses = (
            _makeWorkerStatus(0, isBusy=False),
            _makeWorkerStatus(1, isBusy=False),
        )
        flavor = _makeFlavorStatus("SFM", statuses)
        self.assertEqual(len(flavor.freeWorkers), 2)
        self.assertEqual(flavor.freeWorkers, flavor.workers)


class ClusterStatusTestCase(lsst.utils.tests.TestCase):
    """Tests for the `ClusterStatus` dataclass."""

    def _makeCluster(self) -> tuple[ClusterStatus, dict[str, WorkerStatus]]:
        sfmStatuses = (
            _makeWorkerStatus(0, isBusy=False),
            _makeWorkerStatus(1, isBusy=True),
        )
        # AOS_WORKER also lives PER_DETECTOR — reuse the helper but with a
        # different flavor so the cluster has more than one flavor entry.
        aosWorker = PodDetails(
            instrument="LSSTCam",
            podFlavor=PodFlavor.AOS_WORKER,
            detectorNumber=192,
            depth=0,
        )
        aosStatuses = (WorkerStatus(worker=aosWorker, queueLength=0, isBusy=False, queueItems=[]),)
        cluster = ClusterStatus(
            instrument="LSSTCam",
            flavorStatuses={
                PodFlavor.SFM_WORKER: _makeFlavorStatus("SFM", sfmStatuses),
                PodFlavor.AOS_WORKER: _makeFlavorStatus("AOS", aosStatuses),
            },
            rawQueueLength=0,
        )
        return cluster, {
            "sfmFree": sfmStatuses[0],
            "sfmBusy": sfmStatuses[1],
            "aosFree": aosStatuses[0],
        }

    def test_isPodFreeReturnsTrueForFreePod(self) -> None:
        cluster, statuses = self._makeCluster()
        self.assertTrue(cluster.isPodFree(statuses["sfmFree"].worker))
        self.assertTrue(cluster.isPodFree(statuses["aosFree"].worker))

    def test_isPodFreeReturnsFalseForBusyPod(self) -> None:
        cluster, statuses = self._makeCluster()
        self.assertFalse(cluster.isPodFree(statuses["sfmBusy"].worker))

    def test_isPodFreeRaisesForUnknownPod(self) -> None:
        cluster, _ = self._makeCluster()
        # Same flavor as one of the registered workers, but a detector number
        # that does not appear in any FlavorStatus — `isPodFree` must raise
        # rather than silently lying about its busy-state.
        unknown = _makeWorker(detector=99)
        with self.assertRaises(ValueError):
            cluster.isPodFree(unknown)

    def test_isPodFreeRaisesForUnknownFlavor(self) -> None:
        cluster, _ = self._makeCluster()
        # A flavor that is entirely absent from the cluster status (e.g.
        # STEP1B_WORKER) raises a KeyError when `isPodFree` looks up the
        # flavor in `flavorStatuses`.
        step1b = PodDetails(
            instrument="LSSTCam",
            podFlavor=PodFlavor.STEP1B_WORKER,
            detectorNumber=None,
            depth=0,
        )
        with self.assertRaises(KeyError):
            cluster.isPodFree(step1b)

    def test_allWorkersFlattensAcrossFlavors(self) -> None:
        cluster, statuses = self._makeCluster()
        allWorkers = cluster.allWorkers
        self.assertEqual(len(allWorkers), 3)
        self.assertIn(statuses["sfmFree"].worker, allWorkers)
        self.assertIn(statuses["sfmBusy"].worker, allWorkers)
        self.assertIn(statuses["aosFree"].worker, allWorkers)

    def test_allWorkersEmptyClusterIsEmpty(self) -> None:
        cluster = ClusterStatus(instrument="LSSTCam", flavorStatuses={}, rawQueueLength=0)
        self.assertEqual(cluster.allWorkers, ())


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
