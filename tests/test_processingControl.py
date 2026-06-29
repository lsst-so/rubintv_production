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

import logging
import unittest
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import fakeredis

import lsst.utils.tests
from lsst.daf.butler import Butler
from lsst.rubintv.production import redisUtils as redisUtilsModule
from lsst.rubintv.production.locationConfig import LocationConfig
from lsst.rubintv.production.processingControl import (
    PIPELINE_NAMES,
    CameraControlConfig,
    HeadProcessController,
    VisitProcessingMode,
    WorkerProcessingMode,
)
from lsst.rubintv.production.redisKeys import getControlReadbackKey
from lsst.rubintv.production.redisUtils import RedisHelper


def _makeFakeRedis(*args: object, **kwargs: object) -> fakeredis.FakeStrictRedis:
    """Drop-in for `redis.Redis(...)` returning a fresh in-process client."""
    return fakeredis.FakeStrictRedis()


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


class RestoreAosPipelinesTestCase(lsst.utils.tests.TestCase):
    """`HeadProcessController.restoreAosPipelinesFromRedis` — the
    stickiness-across-restarts logic.

    A full `HeadProcessController` needs a Butler, Redis and S3, so rather
    than build one we invoke the unbound method against a duck-typed ``self``
    carrying just the attributes the method touches, backed by a real
    `RedisHelper` over fakeredis. That exercises the genuine cascade
    (persisted state -> readback -> default), the validation against
    ``self.pipelines``, and the readback re-assertion, without the heavy
    machinery.
    """

    # The two controls, as (controlKey, attribute, default).
    AOS = HeadProcessController._aosPipelineControls[0]
    FAM = HeadProcessController._aosPipelineControls[1]

    def setUp(self) -> None:
        self._patcher = patch.object(redisUtilsModule.redis, "Redis", side_effect=_makeFakeRedis)
        self._patcher.start()
        self.helper = RedisHelper(
            butler=cast(Butler, None),
            locationConfig=cast(LocationConfig, None),
            isHeadNode=True,
        )
        self.redis = self.helper.redis

    def tearDown(self) -> None:
        self._patcher.stop()

    def _controller(self, instrument: str = "LSSTCam") -> SimpleNamespace:
        """Build a minimal duck-typed stand-in for HeadProcessController."""
        return SimpleNamespace(
            instrument=instrument,
            redisHelper=self.helper,
            log=logging.getLogger("test.restoreAosPipelines"),
            pipelines={"AOS_DANISH", "AOS_TIE", "AOS_FAM_DANISH", "AOS_FAM_TIE"},
            _aosPipelineControls=HeadProcessController._aosPipelineControls,
            # the __init__ defaults that restore should overwrite (or keep)
            currentAosPipeline="AOS_DANISH",
            currentAosFamPipeline="AOS_FAM_DANISH",
        )

    def _restore(self, controller: SimpleNamespace) -> None:
        HeadProcessController.restoreAosPipelinesFromRedis(cast(HeadProcessController, controller))

    def test_restoresFromPersistedState(self) -> None:
        # The persisted state is the source of truth and must win on restart.
        self.helper.setControlState(self.AOS[0], "AOS_TIE")
        self.helper.setControlState(self.FAM[0], "AOS_FAM_TIE")

        controller = self._controller()
        self._restore(controller)

        self.assertEqual(controller.currentAosPipeline, "AOS_TIE")
        self.assertEqual(controller.currentAosFamPipeline, "AOS_FAM_TIE")
        # readback re-asserted to match the restored live value
        self.assertEqual(self.helper.getControlReadback(self.AOS[0]), "AOS_TIE")

    def test_migratesFromReadbackWhenNoState(self) -> None:
        # First boot after deploy: no _STATE key exists yet, but RubinTV's
        # readback holds the operator's last selection — adopt it so the
        # upgrade doesn't silently reset to the default.
        self.redis.set(getControlReadbackKey(self.AOS[0]), "AOS_TIE")

        controller = self._controller()
        self._restore(controller)

        self.assertEqual(controller.currentAosPipeline, "AOS_TIE")
        # and the value is healed into the persisted-state key for next time
        self.assertEqual(self.helper.getControlState(self.AOS[0]), "AOS_TIE")

    def test_fallsBackToDefaultWhenNothingStored(self) -> None:
        controller = self._controller()
        self._restore(controller)

        self.assertEqual(controller.currentAosPipeline, "AOS_DANISH")
        self.assertEqual(controller.currentAosFamPipeline, "AOS_FAM_DANISH")
        # the default is asserted onto state + readback so everything agrees
        self.assertEqual(self.helper.getControlState(self.AOS[0]), "AOS_DANISH")
        self.assertEqual(self.helper.getControlReadback(self.AOS[0]), "AOS_DANISH")

    def test_fallsBackToDefaultOnUnknownPipeline(self) -> None:
        # A corrupt/unknown persisted value must never be applied verbatim —
        # the head node would try to run a pipeline that doesn't exist.
        self.helper.setControlState(self.AOS[0], "AOS_NONSENSE")

        controller = self._controller()
        self._restore(controller)

        self.assertEqual(controller.currentAosPipeline, "AOS_DANISH")

    def test_clearsStaleRejectionMessage(self) -> None:
        # State holds the real value while readback was left showing a
        # rejection before the restart; state must win and readback must be
        # re-asserted so the scary REJECTED string doesn't persist forever.
        self.helper.setControlState(self.AOS[0], "AOS_TIE")
        self.helper.setControlReadbackMessage(self.AOS[0], "REJECTED_BETWEEN_PAIR!")

        controller = self._controller()
        self._restore(controller)

        self.assertEqual(controller.currentAosPipeline, "AOS_TIE")
        self.assertEqual(self.helper.getControlReadback(self.AOS[0]), "AOS_TIE")

    def test_noOpForNonLsstCam(self) -> None:
        # Only the LSSTCam head node consumes these controls; others must not
        # touch Redis or change their (ignored) defaults.
        self.helper.setControlState(self.AOS[0], "AOS_TIE")

        controller = self._controller(instrument="LATISS")
        self._restore(controller)

        self.assertEqual(controller.currentAosPipeline, "AOS_DANISH")  # unchanged default


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
