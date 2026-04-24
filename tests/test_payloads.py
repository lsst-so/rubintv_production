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

from utils import getSampleExpRecord  # type: ignore[import]

import lsst.daf.butler as dafButler
import lsst.utils.tests
from lsst.rubintv.production.payloads import Payload
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


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
