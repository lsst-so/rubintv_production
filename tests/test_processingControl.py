# This file is part of summit_utils.
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

"""Test cases for utils."""

import unittest

import lsst.utils.tests
from lsst.rubintv.production.processingControl import (
    PIPELINE_NAMES,
    CameraControlConfig,
    VisitProcessingMode,
    WorkerProcessingMode,
)


class CamearaControlConfigTestCase(lsst.utils.tests.TestCase):

    def test_behavior(self) -> None:
        """Checks that everything is properly applied on top of everything
        else and there are no unintended toggles, so be careful if reordering
        or refactoring into separate functions. Runtime is negligible so it's
        not worth splitting.
        """
        nWavefront = 8
        nGuiders = 8
        nImaging = 189
        nFullPhase0 = 96
        nFullPhase1 = 93
        nRaftPhase0 = 108
        nRaftPhase1 = 81
        ne2v = 117
        nITL = 72

        camConfig = CameraControlConfig()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nWavefront, camConfig.getNumEnabled())

        camConfig.setWavefrontOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setGuidersOn()
        self.assertEqual(nGuiders, camConfig.getNumEnabled())

        camConfig.setGuidersOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setGuidersOn()
        camConfig.setWavefrontOn()
        self.assertEqual(nWavefront + nGuiders, camConfig.getNumEnabled())

        camConfig.setGuidersOff()
        self.assertEqual(nWavefront, camConfig.getNumEnabled())

        camConfig.setAllOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setFullCheckerboard(phase=0)
        self.assertEqual(nFullPhase0, camConfig.getNumEnabled())

        camConfig.setFullCheckerboard(phase=1)
        self.assertEqual(nFullPhase1, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nFullPhase1 + nWavefront, camConfig.getNumEnabled())

        camConfig.setWavefrontOff()
        self.assertEqual(nFullPhase1, camConfig.getNumEnabled())

        camConfig.setAllImagingOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setAllImagingOn()
        self.assertEqual(nImaging, camConfig.getNumEnabled())

        camConfig.invertImagingSelection()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setE2Von()
        self.assertEqual(ne2v, camConfig.getNumEnabled())

        camConfig.invertImagingSelection()
        self.assertEqual(nITL, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nITL + nWavefront, camConfig.getNumEnabled())

        camConfig.setAllOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setAllImagingOn()
        self.assertEqual(nImaging, camConfig.getNumEnabled())

        camConfig.setAllOff()
        self.assertEqual(0, camConfig.getNumEnabled())

        camConfig.setRaftCheckerboard(phase=0)
        self.assertEqual(nRaftPhase0, camConfig.getNumEnabled())

        camConfig.setRaftCheckerboard(phase=1)
        self.assertEqual(nRaftPhase1, camConfig.getNumEnabled())

        camConfig.setWavefrontOn()
        self.assertEqual(nRaftPhase1 + nWavefront, camConfig.getNumEnabled())

        camConfig.setGuidersOn()
        self.assertEqual(nRaftPhase1 + nWavefront + nGuiders, camConfig.getNumEnabled())

    def test_plot(self) -> None:
        camConfig = CameraControlConfig()
        camConfig.plotConfig()

        camConfig.setRaftCheckerboard(phase=1)
        camConfig.plotConfig()

        camConfig.setWavefrontOn()
        camConfig.setRaftCheckerboard(phase=1)


class WorkerProcessingModeTestCase(lsst.utils.tests.TestCase):
    """Pin the integer values of `WorkerProcessingMode`.

    These values are persisted to Redis as the worker's processing mode and
    consumed by the runner. Pinning them prevents an accidental reorder of
    the enum members from silently flipping every running worker into a
    different mode.
    """

    def test_values(self) -> None:
        self.assertEqual(WorkerProcessingMode.WAITING.value, 0)
        self.assertEqual(WorkerProcessingMode.CONSUMING.value, 1)
        self.assertEqual(WorkerProcessingMode.MURDEROUS.value, 2)

    def test_isIntEnum(self) -> None:
        # IntEnum is what allows the integer comparison and JSON-serialisation
        # the rest of the code relies on.
        self.assertTrue(issubclass(WorkerProcessingMode, int))
        self.assertEqual(int(WorkerProcessingMode.WAITING), 0)

    def test_membersExhaustive(self) -> None:
        # If a new mode is added, this test forces the addition to be
        # explicit so other tests / Redis consumers can be updated.
        self.assertEqual(
            set(WorkerProcessingMode),
            {
                WorkerProcessingMode.WAITING,
                WorkerProcessingMode.CONSUMING,
                WorkerProcessingMode.MURDEROUS,
            },
        )


class VisitProcessingModeTestCase(lsst.utils.tests.TestCase):
    """Pin the integer values of `VisitProcessingMode`."""

    def test_values(self) -> None:
        self.assertEqual(VisitProcessingMode.CONSTANT.value, 0)
        self.assertEqual(VisitProcessingMode.ALTERNATING.value, 1)
        self.assertEqual(VisitProcessingMode.ALTERNATING_BY_TWOS.value, 2)

    def test_isIntEnum(self) -> None:
        self.assertTrue(issubclass(VisitProcessingMode, int))

    def test_membersExhaustive(self) -> None:
        self.assertEqual(
            set(VisitProcessingMode),
            {
                VisitProcessingMode.CONSTANT,
                VisitProcessingMode.ALTERNATING,
                VisitProcessingMode.ALTERNATING_BY_TWOS,
            },
        )


class PipelineNamesTestCase(lsst.utils.tests.TestCase):
    """Sanity tests for the `PIPELINE_NAMES` constant.

    PIPELINE_NAMES is consumed by the test helpers in tests/utils.py to
    validate user-RUN collection names, and by every pipeline-aware piece
    of code in the package. The tests below catch the easy ways for it to
    drift: duplicate entries (which would silently mask new pipelines),
    non-string entries, and lower-case strings (the convention is upper).
    """

    def test_isTuple(self) -> None:
        self.assertIsInstance(PIPELINE_NAMES, tuple)

    def test_allEntriesAreNonEmptyStrings(self) -> None:
        for name in PIPELINE_NAMES:
            self.assertIsInstance(name, str)
            self.assertTrue(name, "PIPELINE_NAMES contains an empty string")

    def test_noDuplicates(self) -> None:
        self.assertEqual(len(PIPELINE_NAMES), len(set(PIPELINE_NAMES)))

    def test_allUpperCase(self) -> None:
        for name in PIPELINE_NAMES:
            self.assertEqual(
                name,
                name.upper(),
                f"PIPELINE_NAMES entry {name!r} is not upper-case",
            )

    def test_sfmAlwaysPresent(self) -> None:
        # The "SFM" entry is the science pipeline and is referenced by
        # name throughout the package — guard it explicitly.
        self.assertIn("SFM", PIPELINE_NAMES)


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
