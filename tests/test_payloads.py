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

import json
import unittest
from typing import cast

from utils import getSampleExpRecord  # type: ignore[import]

import lsst.daf.butler as dafButler
import lsst.utils.tests
from lsst.daf.butler import DataCoordinate
from lsst.pipe.base import PipelineGraph
from lsst.rubintv.production.payloads import (
    RESTART_SIGNAL,
    Payload,
    RestartPayload,
    getDetectorId,
    isRestartPayload,
    pipelineGraphFromBytes,
    pipelineGraphToBytes,
)
from lsst.summit.utils.utils import getSite

NO_BUTLER = True
if getSite() in ["staff-rsp", "rubin-devl"]:
    NO_BUTLER = False


class TestPayload(unittest.TestCase):
    def setUp(self) -> None:
        self.butler = None
        if getSite() in ["staff-rsp", "rubin-devl"]:
            self.butler = dafButler.Butler("embargo_old", instrument="LATISS")  # type: ignore

        # this got harder because we now need a butler as well
        self.expRecord = getSampleExpRecord()
        self.pipelineBytes = "test".encode("utf-8")
        self.differentPipelineBytes = "different test".encode("utf-8")
        self.payload = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
        )
        self.validJson = self.payload.to_json()

    def test_constructor(self) -> None:
        payload = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
        )
        self.assertEqual(payload.dataId, self.expRecord.dataId)
        self.assertEqual(payload.pipelineGraphBytes, self.pipelineBytes)

        with self.assertRaises(TypeError):
            payload = Payload(
                dataId=self.expRecord.dataId,
                run="test run",
                pipelineGraphBytes=self.pipelineBytes,
                who="SFM",
                illegalKwarg="test",  # type: ignore[call-arg]  # that's the whole point here
            )

    def test_equality(self) -> None:
        payload1 = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
        )
        payload2 = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
        )
        payloadDiffRun = Payload(
            dataId=self.expRecord.dataId,
            run="other run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
        )
        payloadDiffPipeline = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.differentPipelineBytes,
        )

        self.assertEqual(payload1, payload2)
        self.assertNotEqual(payload1, payloadDiffPipeline)
        self.assertNotEqual(payload1, payloadDiffRun)
        self.assertNotEqual(payload1, payloadDiffPipeline)

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_roundtrip(self) -> None:
        # remove the ignore[arg-type] everywhere once there is a butler
        payload = Payload.from_json(self.validJson, self.butler)  # type: ignore[arg-type]
        payloadJson = payload.to_json()
        reconstructedPayload = Payload.from_json(payloadJson, self.butler)  # type: ignore[arg-type]
        self.assertEqual(payload, reconstructedPayload)

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_from_json(self) -> None:
        # remove the ignore[arg-type] everywhere once there is a butler
        payload = Payload.from_json(self.validJson, self.butler)  # type: ignore[arg-type]
        self.assertEqual(payload.dataId, self.expRecord.dataId)
        self.assertEqual(payload.pipelineGraphBytes, self.pipelineBytes)

    def test_taskName_defaults_to_none(self) -> None:
        # The graph-bearing flavour of Payload should not need to set
        # taskName; it must default to None so existing call sites that
        # only pass a real pipeline graph keep working.
        payload = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
        )
        self.assertIsNone(payload.taskName)

    def test_taskName_command_dispatch(self) -> None:
        # Command-style dispatch (used by the focal-plane mosaic plotters)
        # carries no graph and no output run, just the task name to invoke.
        payload = Payload(
            dataId=self.expRecord.dataId,
            run="",
            pipelineGraphBytes=b"",
            who="SFM",
            taskName="preliminary_visit_image",
        )
        self.assertEqual(payload.taskName, "preliminary_visit_image")
        self.assertEqual(payload.run, "")
        self.assertEqual(payload.pipelineGraphBytes, b"")

    def test_taskName_in_equality(self) -> None:
        payloadNoTask = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
        )
        payloadWithTask = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
            taskName="preliminary_visit_image",
        )
        payloadDifferentTask = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            who="SFM",
            pipelineGraphBytes=self.pipelineBytes,
            taskName="post_isr_image",
        )
        self.assertNotEqual(payloadNoTask, payloadWithTask)
        self.assertNotEqual(payloadWithTask, payloadDifferentTask)

    def test_taskName_serialised_in_to_json(self) -> None:
        payload = Payload(
            dataId=self.expRecord.dataId,
            run="",
            pipelineGraphBytes=b"",
            who="SFM",
            taskName="preliminary_visit_image",
        )
        decoded = json.loads(payload.to_json())
        self.assertIn("taskName", decoded)
        self.assertEqual(decoded["taskName"], "preliminary_visit_image")

        payloadNoTask = Payload(
            dataId=self.expRecord.dataId,
            run="test run",
            pipelineGraphBytes=self.pipelineBytes,
            who="SFM",
        )
        decodedNoTask = json.loads(payloadNoTask.to_json())
        self.assertIn("taskName", decodedNoTask)
        self.assertIsNone(decodedNoTask["taskName"])

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_taskName_roundtrip(self) -> None:
        payload = Payload(
            dataId=self.expRecord.dataId,
            run="",
            pipelineGraphBytes=b"",
            who="SFM",
            taskName="preliminary_visit_image",
        )
        reconstructed = Payload.from_json(payload.to_json(), self.butler)  # type: ignore[arg-type]
        self.assertEqual(reconstructed.taskName, "preliminary_visit_image")
        self.assertEqual(payload, reconstructed)

    @unittest.skipIf(NO_BUTLER, "Skipping butler-driven tests")
    def test_from_json_legacy_payload_without_taskName(self) -> None:
        # Old payloads on the wire (pre-taskName) won't have the key at
        # all. They must still deserialise, with taskName defaulting to
        # None, so a rolling rollout doesn't break in-flight work.
        legacyDict = json.loads(self.validJson)
        legacyDict.pop("taskName", None)
        legacyJson = json.dumps(legacyDict)
        payload = Payload.from_json(legacyJson, self.butler)  # type: ignore[arg-type]
        self.assertIsNone(payload.taskName)


class TestPipelineGraphRoundTrip(unittest.TestCase):
    """``pipelineGraphToBytes`` / ``pipelineGraphFromBytes`` are the
    Payload wire format for the pipeline graph. Byte-stability across
    a round-trip is the property workers rely on when forwarding a
    payload onwards (e.g. step1a → step1b), so a serialisation
    regression would surface as silent payload corruption."""

    def test_emptyPipelineGraphRoundTrip(self) -> None:
        # A bare PipelineGraph still serialises and deserialises cleanly,
        # which is the contract every wire-format consumer relies on.
        graph = PipelineGraph()
        data = pipelineGraphToBytes(graph)
        self.assertIsInstance(data, bytes)
        self.assertGreater(len(data), 0)

        recovered = pipelineGraphFromBytes(data)
        self.assertIsInstance(recovered, PipelineGraph)

    def test_roundTripIsByteStable(self) -> None:
        graph = PipelineGraph()
        data1 = pipelineGraphToBytes(graph)
        recovered = pipelineGraphFromBytes(data1)
        data2 = pipelineGraphToBytes(recovered)
        self.assertEqual(data1, data2)


class TestIsRestartPayload(unittest.TestCase):
    """``isRestartPayload`` runs on every payload pulled off a Redis
    queue. It must return True for both the typed ``RestartPayload``
    and for a post-JSON ``Payload`` whose run-or-who matches
    ``RESTART_SIGNAL`` (the type-information is gone after
    deserialisation). A regression here means a restart signal is
    treated as a regular payload — the pod doesn't restart."""

    def setUp(self) -> None:
        self.expRecord = getSampleExpRecord()

    def test_restartPayloadDetected(self) -> None:
        self.assertTrue(isRestartPayload(RestartPayload()))

    def test_normalPayloadIsNotRestart(self) -> None:
        payload = Payload(
            dataId=self.expRecord.dataId,
            run="some-run",
            pipelineGraphBytes=b"",
            who="SFM",
        )
        self.assertFalse(isRestartPayload(payload))

    def test_runFieldRestartSignalIsRestart(self) -> None:
        # The function is documented to inspect the run/who fields after
        # JSON round-trip, when the type-information of RestartPayload is
        # gone. Build that shape directly.
        payload = Payload(
            dataId=self.expRecord.dataId,
            run=RESTART_SIGNAL,
            pipelineGraphBytes=b"",
            who="not-the-restart-signal",
        )
        self.assertTrue(isRestartPayload(payload))

    def test_whoFieldRestartSignalIsRestart(self) -> None:
        payload = Payload(
            dataId=self.expRecord.dataId,
            run="some-run",
            pipelineGraphBytes=b"",
            who=RESTART_SIGNAL,
        )
        self.assertTrue(isRestartPayload(payload))


class TestGetDetectorId(unittest.TestCase):
    """``getDetectorId`` is called by worker code routing payloads to
    per-detector queues. The contract: ``None`` when the dataId has
    no detector key (exposure-level work), ``int`` when present.
    The string-to-int coercion matters because some upstream paths
    stringify detector numbers; switching to a string return would
    break ``%d``-style formatting at call sites."""

    def _makePayload(self, dataId: object) -> Payload:
        # Payload type-hints dataId as DataCoordinate but does no runtime
        # validation; getDetectorId only uses ``in`` and ``[]`` on the
        # dataId, both of which a plain dict supports. Casting keeps the
        # type checker happy.
        return Payload(
            dataId=cast(DataCoordinate, dataId),
            run="r",
            pipelineGraphBytes=b"",
            who="SFM",
        )

    def test_returnsNoneWhenNoDetectorInDataId(self) -> None:
        payload = self._makePayload({"instrument": "LSSTCam", "exposure": 1234})
        self.assertIsNone(getDetectorId(payload))

    def test_returnsIntWhenDetectorPresent(self) -> None:
        payload = self._makePayload({"instrument": "LSSTCam", "detector": 94})
        self.assertEqual(getDetectorId(payload), 94)

    def test_coercesStringDetectorToInt(self) -> None:
        # The `int(...)` conversion is part of the contract: callers can
        # rely on always getting an int back, even if the dataId carried
        # a stringified detector number.
        payload = self._makePayload({"instrument": "LSSTCam", "detector": "42"})
        self.assertEqual(getDetectorId(payload), 42)


class TestRestartPayloadInstance(unittest.TestCase):
    """Pins the field values of ``RestartPayload`` so a future change
    to the constructor doesn't silently shift the signal value used
    on the wire."""

    def test_restartPayloadFieldsMatchSignal(self) -> None:
        payload = RestartPayload()
        self.assertEqual(payload.run, RESTART_SIGNAL)
        self.assertEqual(payload.who, RESTART_SIGNAL)
        self.assertEqual(payload.specialMessage, "RESTARTING")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
