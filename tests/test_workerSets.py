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

"""Test cases for workerSets."""

import unittest

import lsst.utils.tests
from lsst.rubintv.production.clusterManagement import (
    ClusterStatus,
    FlavorStatus,
    WorkerStatus,
)
from lsst.rubintv.production.formatters import AOS_CCDS
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor
from lsst.rubintv.production.workerSets import (
    AosWorkerSet,
    BacklogWorkerSet,
    SfmWorkerSet,
    Step1bWorkerSet,
    WorkerSet,
)


def _sfmPod(detector: int, depth: int = 0, instrument: str = "LSSTCam") -> PodDetails:
    return PodDetails(
        instrument=instrument,
        podFlavor=PodFlavor.SFM_WORKER,
        detectorNumber=detector,
        depth=depth,
    )


def _aosPod(detector: int, depth: int = 0) -> PodDetails:
    return PodDetails(
        instrument="LSSTCam",
        podFlavor=PodFlavor.AOS_WORKER,
        detectorNumber=detector,
        depth=depth,
    )


def _step1bPod(depth: int) -> PodDetails:
    return PodDetails(
        instrument="LSSTCam",
        podFlavor=PodFlavor.STEP1B_WORKER,
        detectorNumber=None,
        depth=depth,
    )


def _makeWorkerStatus(pod: PodDetails, isBusy: bool = False, queueLength: int = 0) -> WorkerStatus:
    return WorkerStatus(worker=pod, queueLength=queueLength, isBusy=isBusy, queueItems=[])


def _makeClusterStatus(
    *,
    sfmPods: list[tuple[PodDetails, bool, int]] | None = None,
    aosPods: list[tuple[PodDetails, bool, int]] | None = None,
    step1bPods: list[tuple[PodDetails, bool, int]] | None = None,
    instrument: str = "LSSTCam",
) -> ClusterStatus:
    """Build a `ClusterStatus` from `(pod, isBusy, queueLength)` tuples."""
    flavorStatuses: dict[PodFlavor, FlavorStatus] = {}

    def addFlavor(flavor: PodFlavor, entries: list[tuple[PodDetails, bool, int]] | None) -> None:
        if entries is None:
            return
        statuses = tuple(_makeWorkerStatus(p, busy, qlen) for p, busy, qlen in entries)
        nFree = sum(1 for s in statuses if not s.isBusy)
        flavorStatuses[flavor] = FlavorStatus(name=flavor.name, nFreeWorkers=nFree, workerStatuses=statuses)

    addFlavor(PodFlavor.SFM_WORKER, sfmPods)
    addFlavor(PodFlavor.AOS_WORKER, aosPods)
    addFlavor(PodFlavor.STEP1B_WORKER, step1bPods)
    return ClusterStatus(instrument=instrument, flavorStatuses=flavorStatuses, rawQueueLength=0)


class WorkerSetPostInitTestCase(lsst.utils.tests.TestCase):
    """Tests for `WorkerSet.__post_init__` validation."""

    def test_validHomogeneousSet(self) -> None:
        pods = [_sfmPod(0), _sfmPod(1), _sfmPod(2)]
        ws = WorkerSet(
            instrument="LSSTCam",
            podFlavor=PodFlavor.SFM_WORKER,
            pods=pods,
            name="test",
        )
        self.assertEqual(ws.nWorkers(), 3)

    def test_emptySetIsValid(self) -> None:
        # An empty pod list trivially satisfies the validation; there is
        # nothing to mismatch on.
        ws = WorkerSet(
            instrument="LSSTCam",
            podFlavor=PodFlavor.SFM_WORKER,
            pods=[],
            name="empty",
        )
        self.assertEqual(ws.nWorkers(), 0)

    def test_instrumentMismatchRaises(self) -> None:
        pods = [_sfmPod(0, instrument="LSSTCam"), _sfmPod(1, instrument="LATISS")]
        with self.assertRaises(ValueError):
            WorkerSet(
                instrument="LSSTCam",
                podFlavor=PodFlavor.SFM_WORKER,
                pods=pods,
                name="bad",
            )

    def test_flavorMismatchRaises(self) -> None:
        pods = [_sfmPod(0), _aosPod(192)]
        with self.assertRaises(ValueError):
            WorkerSet(
                instrument="LSSTCam",
                podFlavor=PodFlavor.SFM_WORKER,
                pods=pods,
                name="bad",
            )


class WorkerSetAggregationTestCase(lsst.utils.tests.TestCase):
    """Tests for the aggregation methods on `WorkerSet`."""

    def setUp(self) -> None:
        # A small SFM set with three known detectors at depth 0.
        self.pod0 = _sfmPod(0)
        self.pod1 = _sfmPod(1)
        self.pod2 = _sfmPod(2)
        self.set = WorkerSet(
            instrument="LSSTCam",
            podFlavor=PodFlavor.SFM_WORKER,
            pods=[self.pod0, self.pod1, self.pod2],
            name="SFM test",
        )

    def test_nFreeWorkers(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 0),
                (self.pod1, True, 0),
                (self.pod2, False, 0),
            ]
        )
        self.assertEqual(self.set.nFreeWorkers(cluster), 2)

    def test_allFree(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 0),
                (self.pod1, False, 0),
                (self.pod2, False, 0),
            ]
        )
        self.assertTrue(self.set.allFree(cluster))

    def test_allFreeFalseWhenAnyBusy(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 0),
                (self.pod1, True, 0),
                (self.pod2, False, 0),
            ]
        )
        self.assertFalse(self.set.allFree(cluster))

    def test_allBusy(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, True, 0),
                (self.pod1, True, 0),
                (self.pod2, True, 0),
            ]
        )
        self.assertTrue(self.set.allBusy(cluster))

    def test_allBusyFalseWhenAnyFree(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, True, 0),
                (self.pod1, True, 0),
                (self.pod2, False, 0),
            ]
        )
        self.assertFalse(self.set.allBusy(cluster))

    def test_allFreeAndAllBusyVacuouslyTrueOnEmptySet(self) -> None:
        # An empty WorkerSet has nothing to be busy, so the universal
        # quantifiers vacuously hold for both `allFree` and `allBusy`.
        emptySet = WorkerSet(
            instrument="LSSTCam",
            podFlavor=PodFlavor.SFM_WORKER,
            pods=[],
            name="empty",
        )
        cluster = _makeClusterStatus(sfmPods=[])
        self.assertTrue(emptySet.allFree(cluster))
        self.assertTrue(emptySet.allBusy(cluster))

    def test_allFreeReturnsFalseWhenPodsMissing(self) -> None:
        # pod2 is registered to the set but absent from the cluster — the
        # set is incomplete and `allFree` must short-circuit to False.
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 0),
                (self.pod1, False, 0),
            ]
        )
        self.assertFalse(self.set.allFree(cluster))
        self.assertFalse(self.set.allBusy(cluster))
        self.assertFalse(self.set.allExist(cluster))
        self.assertEqual(self.set.getMissingPods(cluster), [self.pod2])

    def test_maxQueueLength(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 1),
                (self.pod1, False, 7),
                (self.pod2, False, 4),
            ]
        )
        self.assertEqual(self.set.maxQueueLength(cluster), 7)

    def test_maxQueueLengthIsZeroOnEmptyCluster(self) -> None:
        cluster = _makeClusterStatus(sfmPods=[])
        # Documents the current behaviour: zero is returned when the set
        # has no matching workers in the cluster (the implementation seeds
        # the running maximum at 0).
        self.assertEqual(self.set.maxQueueLength(cluster), 0)

    def test_minQueueLength(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 5),
                (self.pod1, False, 2),
                (self.pod2, False, 9),
            ]
        )
        self.assertEqual(self.set.minQueueLength(cluster), 2)

    def test_minQueueLengthSentinelOnEmptyCluster(self) -> None:
        # Documents the current behaviour: `minQueueLength` returns the
        # internal sentinel (9_999_999) when there are no workers in the
        # cluster, rather than 0 or None. This is almost certainly a bug
        # waiting to happen — pin it so any future fix has to be deliberate
        # and update this test at the same time.
        cluster = _makeClusterStatus(sfmPods=[])
        self.assertEqual(self.set.minQueueLength(cluster), 9_999_999)

    def test_totalQueuedItems(self) -> None:
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 1),
                (self.pod1, False, 2),
                (self.pod2, False, 3),
            ]
        )
        self.assertEqual(self.set.totalQueuedItems(cluster), 6)

    def test_totalQueuedItemsIgnoresOtherPods(self) -> None:
        # A worker that exists in the cluster but is not part of the set
        # must not contribute to the set's total.
        otherPod = _sfmPod(99)
        cluster = _makeClusterStatus(
            sfmPods=[
                (self.pod0, False, 1),
                (self.pod1, False, 2),
                (self.pod2, False, 3),
                (otherPod, False, 100),  # huge queue, but not in our set
            ]
        )
        self.assertEqual(self.set.totalQueuedItems(cluster), 6)


class WorkerSetGetWorkerForDetectorTestCase(lsst.utils.tests.TestCase):
    """Tests for `WorkerSet.getWorkerForDetector`."""

    def setUp(self) -> None:
        self.pod0 = _sfmPod(0)
        self.pod1 = _sfmPod(1)
        self.set = WorkerSet(
            instrument="LSSTCam",
            podFlavor=PodFlavor.SFM_WORKER,
            pods=[self.pod0, self.pod1],
            name="SFM",
        )

    def test_returnsWorkerForKnownDetector(self) -> None:
        cluster = _makeClusterStatus(sfmPods=[(self.pod0, False, 0), (self.pod1, False, 0)])
        self.assertEqual(self.set.getWorkerForDetector(0, cluster), self.pod0)
        self.assertEqual(self.set.getWorkerForDetector(1, cluster), self.pod1)

    def test_returnsNoneWhenDetectorNotInSet(self) -> None:
        cluster = _makeClusterStatus(sfmPods=[(self.pod0, False, 0), (self.pod1, False, 0)])
        self.assertIsNone(self.set.getWorkerForDetector(99, cluster))

    def test_returnsNoneWhenPodMissingFromCluster(self) -> None:
        # pod1 is in the set but missing from the cluster — should warn and
        # return None rather than the pod itself.
        cluster = _makeClusterStatus(sfmPods=[(self.pod0, False, 0)])
        self.assertIsNone(self.set.getWorkerForDetector(1, cluster))


class SfmWorkerSetCreateTestCase(lsst.utils.tests.TestCase):
    """Tests for `SfmWorkerSet.create`."""

    def test_createSet0(self) -> None:
        sfmSet = SfmWorkerSet.create("LSSTCam", depth=0)
        self.assertEqual(sfmSet.nWorkers(), 189)
        self.assertEqual(sfmSet.name, "SFM Set 1")
        self.assertEqual(sfmSet.podFlavor, PodFlavor.SFM_WORKER)
        self.assertEqual(sfmSet.instrument, "LSSTCam")
        for i, pod in enumerate(sfmSet.pods):
            self.assertEqual(pod.detectorNumber, i)
            self.assertEqual(pod.depth, 0)

    def test_createSetAtDifferentDepth(self) -> None:
        sfmSet = SfmWorkerSet.create("LSSTCam", depth=2)
        self.assertEqual(sfmSet.nWorkers(), 189)
        self.assertEqual(sfmSet.name, "SFM Set 3")  # depth + 1
        for pod in sfmSet.pods:
            self.assertEqual(pod.depth, 2)


class Step1bWorkerSetCreateTestCase(lsst.utils.tests.TestCase):
    """Tests for `Step1bWorkerSet.create`."""

    def test_createWithStep1bFlavor(self) -> None:
        s1b = Step1bWorkerSet.create("LSSTCam", PodFlavor.STEP1B_WORKER, count=4)
        self.assertEqual(s1b.nWorkers(), 4)
        self.assertEqual(s1b.podFlavor, PodFlavor.STEP1B_WORKER)
        self.assertEqual(s1b.name, "Step1b STEP1B_WORKER Set")
        for i, pod in enumerate(s1b.pods):
            self.assertIsNone(pod.detectorNumber)
            self.assertEqual(pod.depth, i)

    def test_createWithStep1bAosFlavor(self) -> None:
        s1b = Step1bWorkerSet.create("LSSTCam", PodFlavor.STEP1B_AOS_WORKER, count=2)
        self.assertEqual(s1b.nWorkers(), 2)
        self.assertEqual(s1b.podFlavor, PodFlavor.STEP1B_AOS_WORKER)
        self.assertEqual(s1b.name, "Step1b STEP1B_AOS_WORKER Set")


class AosWorkerSetCreateTestCase(lsst.utils.tests.TestCase):
    """Tests for `AosWorkerSet.create`.

    The AOS worker mapping flattens (depth, ccd) pairs from
    `itertools.product(range(9), AOS_CCDS)`, so a contiguous range of 8
    workers — `range(0, 8)` — covers the 8 CCDs at a single depth and
    produces a sensible "AOS Set N" name. The tests pin both the contents
    and the name math.
    """

    def test_createFirstSet(self) -> None:
        aosSet = AosWorkerSet.create("LSSTCam", workerRange=range(0, 8))
        self.assertEqual(aosSet.nWorkers(), 8)
        self.assertEqual(aosSet.name, "AOS Set 1")
        self.assertEqual(aosSet.podFlavor, PodFlavor.AOS_WORKER)
        # All 8 should be at depth 0, covering all the AOS CCDs.
        for pod in aosSet.pods:
            self.assertEqual(pod.depth, 0)
        ccds = sorted(p.detectorNumber for p in aosSet.pods if p.detectorNumber is not None)
        self.assertEqual(ccds, list(AOS_CCDS))

    def test_createSecondSet(self) -> None:
        aosSet = AosWorkerSet.create("LSSTCam", workerRange=range(8, 16))
        self.assertEqual(aosSet.nWorkers(), 8)
        self.assertEqual(aosSet.name, "AOS Set 2")
        for pod in aosSet.pods:
            self.assertEqual(pod.depth, 1)


class BacklogWorkerSetCreateTestCase(lsst.utils.tests.TestCase):
    """Tests for `BacklogWorkerSet.create`."""

    def test_create(self) -> None:
        backlog = BacklogWorkerSet.create("LSSTCam", count=3)
        self.assertEqual(backlog.nWorkers(), 3)
        self.assertEqual(backlog.podFlavor, PodFlavor.BACKLOG_WORKER)
        self.assertEqual(backlog.name, "Backlog Set")
        for i, pod in enumerate(backlog.pods):
            self.assertIsNone(pod.detectorNumber)
            self.assertEqual(pod.depth, i)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
