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

"""Test cases for the integration suite's single-source-of-truth dataset.

``tests/ci/ci_dataset.py`` defines the exposures the drip-feeder dispatches
and derives every end-of-run expectation (plots on disk, Redis data
products) from them. These tests pin the structural invariants both ends
rely on: (1) the dispatch order covers exactly the defined exposures and
preserves the load-bearing FAM intra-before-extra ordering, (2) the expId
arithmetic and the OCS pair-signal wire format, and (3) that the derived
expectations stay internally consistent (no duplicate plot paths, every
exposure checked for something, AOS/SFM visit sets in their documented
subset relations).
"""

import sys
import unittest
from pathlib import Path

import lsst.utils.tests

# ci_dataset.py lives under tests/ci/ and is part of the integration suite,
# not the importable lsst.rubintv.production package. Put its directory on
# sys.path so it can be imported here.
_TESTS_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(_TESTS_DIR / "ci"))
import ci_dataset as cd  # type: ignore[import-not-found]  # noqa: E402


class DispatchOrderTestCase(lsst.utils.tests.TestCase):
    """Pin the contract between the exposure set and the dispatch order."""

    def test_dispatchOrderIsPermutationOfExposures(self) -> None:
        # Every defined LSSTCam exposure must be dispatched exactly once:
        # an exposure added to LSSTCAM_EXPOSURES but not to the dispatch
        # order would have plots checked for but never be processed.
        self.assertEqual(set(cd.LSSTCAM_DISPATCH_ORDER), set(cd.LSSTCAM_EXPOSURES))
        self.assertEqual(len(cd.LSSTCAM_DISPATCH_ORDER), len(cd.LSSTCAM_EXPOSURES))

    def test_famIntraDispatchedBeforeExtra(self) -> None:
        # The single ordering constraint that actually matters (see the
        # comments on LSSTCAM_DISPATCH_ORDER): the intra-focal FAM image
        # must be dispatched before the extra-focal one so the pair is
        # processed in the right order on empty pods.
        order = list(cd.LSSTCAM_DISPATCH_ORDER)
        self.assertLess(order.index(cd.LSSTCAM_FAM_INTRA), order.index(cd.LSSTCAM_FAM_EXTRA))

    def test_lsstCamExposuresShareDayObs(self) -> None:
        # The drip-feeder queries all LSSTCam exposures with a single
        # day_obs constraint, so they must all be on the same night.
        self.assertEqual(len({exposure.dayObs for exposure in cd.LSSTCAM_EXPOSURES}), 1)


class ExpIdTestCase(lsst.utils.tests.TestCase):
    """Pin the expId arithmetic and the OCS donut-pair wire format."""

    def test_expId(self) -> None:
        # expId is dayObs * 100_000 + seqNum - the same construction the
        # butler uses for these single-snap visits, pinned against a known
        # value so the arithmetic can't silently drift.
        self.assertEqual(cd.LSSTCAM_SCIENCE.expId, 2025111500226)

    def test_famPairSignal(self) -> None:
        # The wire format of the simulated OCS announcement: comma-joined
        # expIds, intra-focal image first.
        self.assertEqual(cd.getFamPairSignal(), f"{cd.LSSTCAM_FAM_INTRA.expId},{cd.LSSTCAM_FAM_EXTRA.expId}")


class DerivedExpectationsTestCase(lsst.utils.tests.TestCase):
    """Pin the internal consistency of the derived end-of-run checks."""

    def test_expectedPlotPathsAreUnique(self) -> None:
        # A duplicate (plotType, exposure) pairing in the PlotSpec groups
        # would double-count a check; the derived paths must be unique.
        expected = cd.getExpectedPlots()
        paths = [path for path, _ in expected]
        self.assertEqual(len(paths), len(set(paths)))

    def test_everyExposureHasExpectedPlots(self) -> None:
        # Every exposure fed into the pipeline must be checked for at
        # least one plot - an exposure with no expectations at all means
        # a PlotSpec kinds-set has drifted.
        expectedPaths = [path for path, _ in cd.getExpectedPlots()]
        for exposure in cd.ALL_EXPOSURES:
            seqNumToken = f"seqNum_{exposure.seqNum:06}"
            matching = [
                path
                for path in expectedPaths
                if path.startswith(f"{exposure.instrument}/{exposure.dayObs}/") and seqNumToken in path
            ]
            self.assertTrue(matching, f"no expected plots derived for {exposure}")

    def test_visitSetRelations(self) -> None:
        # SFM runs on the in-focus science image only; AOS runs on all
        # on-sky images (CWFS for the science image, FAM for the pair), so
        # for LSSTCam the SFM visits must be a strict subset of the AOS
        # visits, and the Zernike-count check must cover exactly the AOS
        # visits.
        sfmVisits = set(cd.getSfmVisits("LSSTCam"))
        aosVisits = set(cd.getAosVisits("LSSTCam"))
        self.assertLess(sfmVisits, aosVisits)  # strict subset
        zernikeVisits = {exposure.expId for exposure in cd.getExpectedZernikeCounts()}
        self.assertEqual(zernikeVisits, aosVisits)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
