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

"""Test cases for startupChecks.

Pins the Sentry environment resolution in ``getSentryEnvironment``. The
worker-set pods only get ``RAPID_ANALYSIS_LOCATION`` from the Helm charts
(not ``SENTRY_ENVIRONMENT``), and the Sentry SDK silently files events
under ``production`` when it can't resolve an environment — so a
regression here wouldn't fail anything visibly, it would just quietly
mislabel every worker's error reports again (DM-55462).

Also pins which integrations ``setupSentry`` hands to the SDK. Sentry's
auto-enabled integrations import every matching library installed in the
image at pod startup, and the image's Jupyter stack installs plenty that
rapid analysis never uses. A regression here shows up only as new
warnings or import cost in every pod's startup log, not as a failure.
"""

import unittest
from collections.abc import Mapping
from typing import Any
from unittest.mock import patch

from sentry_sdk.integrations.redis import RedisIntegration

import lsst.utils.tests
from lsst.rubintv.production.startupChecks import getSentryEnvironment, setupSentry


class GetSentryEnvironmentTestCase(lsst.utils.tests.TestCase):
    """Tests for the SENTRY_ENVIRONMENT / RAPID_ANALYSIS_LOCATION fallback
    chain.
    """

    def testSentryEnvironmentTakesPrecedence(self) -> None:
        env = {"SENTRY_ENVIRONMENT": "SOMEWHERE", "RAPID_ANALYSIS_LOCATION": "SUMMIT"}
        with patch.dict("os.environ", env, clear=True):
            self.assertEqual(getSentryEnvironment(), "SOMEWHERE")

    def testFallsBackToRapidAnalysisLocation(self) -> None:
        with patch.dict("os.environ", {"RAPID_ANALYSIS_LOCATION": "BTS"}, clear=True):
            self.assertEqual(getSentryEnvironment(), "BTS")

    def testEmptySentryEnvironmentFallsThrough(self) -> None:
        # The Helm charts quote the location value, so an unset location
        # would arrive as an empty string rather than an absent variable —
        # empty must mean "unset", not "the empty environment".
        env = {"SENTRY_ENVIRONMENT": "", "RAPID_ANALYSIS_LOCATION": "SUMMIT"}
        with patch.dict("os.environ", env, clear=True):
            self.assertEqual(getSentryEnvironment(), "SUMMIT")

    def testReturnsNoneWhenNothingSet(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            self.assertIsNone(getSentryEnvironment())

    def testReturnsNoneWhenOnlyEmptyStringsSet(self) -> None:
        env = {"SENTRY_ENVIRONMENT": "", "RAPID_ANALYSIS_LOCATION": ""}
        with patch.dict("os.environ", env, clear=True):
            self.assertIsNone(getSentryEnvironment())


class SetupSentryTestCase(lsst.utils.tests.TestCase):
    """Tests that ``setupSentry`` turns off auto-enabling and lists Redis
    as the only integration on top of Sentry's defaults.
    """

    def _initKwargs(self) -> Mapping[str, Any]:
        with (
            patch.dict("os.environ", {"RAPID_ANALYSIS_LOCATION": "BTS"}, clear=True),
            patch("lsst.rubintv.production.startupChecks.sentry_sdk.init") as mockInit,
        ):
            setupSentry()
        mockInit.assert_called_once()
        return mockInit.call_args.kwargs

    def testAutoEnablingIntegrationsOff(self) -> None:
        # The image installs mcp (via the Jupyter stack), so Sentry's
        # auto-enabled MCP integration imported fastmcp, and fastmcp 2.x's
        # authlib.jose import logged an AuthlibDeprecationWarning in every
        # pod. Turning auto-enabling back on reintroduces that.
        self.assertIs(self._initKwargs()["auto_enabling_integrations"], False)

    def testRedisIntegrationListedExplicitly(self) -> None:
        # Redis breadcrumbs (the queue operations leading up to an error)
        # are wanted, and with auto-enabling off the Redis integration is
        # only active if it is listed here.
        integrations = self._initKwargs()["integrations"]
        self.assertEqual([type(i) for i in integrations], [RedisIntegration])


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
