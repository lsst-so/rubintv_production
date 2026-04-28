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

"""Test cases for LocationConfig.

LocationConfig must fail fast: if any path it manages cannot be created
or read, construction must raise rather than letting some unrelated pod
blow up later when it happens to read the offending property. These
tests pin that contract.
"""

import os
import tempfile
import unittest
from unittest.mock import patch

import yaml

from lsst.rubintv.production import locationConfig as locationConfigModule
from lsst.rubintv.production.locationConfig import LocationConfig, _expandEnvVars


def _ciEnvForTmpdir(tmpdir: str) -> dict[str, str]:
    """Map the CI env vars in ``config_usdf_testing.yaml`` to ``tmpdir``."""
    return {
        "RA_CI_DATA_ROOT": os.path.join(tmpdir, "data_root"),
        "RA_CI_STAR_TRACKER_DATA_PATH": os.path.join(tmpdir, "star_tracker"),
        "RA_CI_ASTROMETRY_NET_REF_CAT_PATH": os.path.join(tmpdir, "astrometry"),
    }


def _preCreateNonAutoCreatedDirs(env: dict[str, str]) -> None:
    """Pre-create the directories whose ``_checkDir(createIfMissing=False)``
    calls in ``LocationConfig`` require them to exist before construction.
    """
    for path in (
        env["RA_CI_STAR_TRACKER_DATA_PATH"],
        env["RA_CI_ASTROMETRY_NET_REF_CAT_PATH"],
        os.path.join(env["RA_CI_DATA_ROOT"], "allsky", "raw"),
    ):
        os.makedirs(path, exist_ok=True)


class LocationConfigInitTestCase(unittest.TestCase):
    """Verify LocationConfig validates every YAML-declared path at __init__."""

    def test_initSucceedsAgainstRealConfigWithEnvVarsRedirected(self) -> None:
        """A real config with the CI env vars redirected to a tmpdir
        should construct cleanly and create the auto-created dirs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _ciEnvForTmpdir(tmpdir)
            _preCreateNonAutoCreatedDirs(env)

            with patch.dict(os.environ, env):
                cfg = LocationConfig("usdf_testing")

            self.assertTrue(os.path.isdir(cfg.plotPath))
            self.assertTrue(cfg.plotPath.startswith(env["RA_CI_DATA_ROOT"]))

    def test_initFailsEagerlyOnUnreachablePath(self) -> None:
        """If any path in the YAML cannot be created, init must raise.

        Regression test for the previous behaviour where only
        ``self._config`` and ``self.plotPath`` were touched in
        ``__post_init__``: an unreachable directory could be missed at
        init and only blow up much later when some unrelated pod first
        accessed the property.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            env = _ciEnvForTmpdir(tmpdir)
            _preCreateNonAutoCreatedDirs(env)
            # /dev/null is a char device, so makedirs under it raises.
            env["RA_CI_DATA_ROOT"] = "/dev/null/cannot_create_under_this"

            with patch.dict(os.environ, env):
                with self.assertRaises((RuntimeError, OSError)):
                    LocationConfig("usdf_testing")

    def test_initFailsWhenAYamlKeyIsMissing(self) -> None:
        """All configs share the same key set (enforced by the CI yaml-check),
        so a missing key is a real bug and must surface as a KeyError at
        init, not be silently swallowed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # A minimal config that's missing nearly every required key.
            sparseConfig = {"plotPath": os.path.join(tmpdir, "plots")}
            with patch.object(locationConfigModule, "_loadConfigFile", return_value=sparseConfig):
                with self.assertRaises(KeyError):
                    LocationConfig("sparse")


class ExpandEnvVarsTestCase(unittest.TestCase):
    """Verify ``${VAR}`` refs in YAML strings get expanded at load time."""

    def test_expandsTopLevelStringValues(self) -> None:
        with patch.dict(os.environ, {"_RA_TEST_ROOT": "/tmp/some/where"}):
            self.assertEqual(
                _expandEnvVars({"plotPath": "${_RA_TEST_ROOT}/plots"}),
                {"plotPath": "/tmp/some/where/plots"},
            )

    def test_recursesIntoNestedDictsAndLists(self) -> None:
        with patch.dict(os.environ, {"_RA_TEST_ROOT": "/tmp/x"}):
            node = {
                "outer": "${_RA_TEST_ROOT}/a",
                "nested": {"inner": "${_RA_TEST_ROOT}/b"},
                "listy": ["${_RA_TEST_ROOT}/c", "${_RA_TEST_ROOT}/d"],
            }
            self.assertEqual(
                _expandEnvVars(node),
                {
                    "outer": "/tmp/x/a",
                    "nested": {"inner": "/tmp/x/b"},
                    "listy": ["/tmp/x/c", "/tmp/x/d"],
                },
            )

    def test_leavesNonStringValuesAlone(self) -> None:
        node = {"port": 6111, "enabled": True, "ratio": 0.5, "missing": None}
        self.assertEqual(_expandEnvVars(node), node)


class RealYamlSanityTestCase(unittest.TestCase):
    """Sanity-check that the on-disk ``config_usdf_testing.yaml`` parses
    and contains the env-var placeholders we depend on for redirection."""

    def test_configUsdfTestingHasRedirectableEnvVars(self) -> None:
        cfgPath = os.path.join(os.path.dirname(__file__), "..", "config", "config_usdf_testing.yaml")
        with open(cfgPath) as f:
            raw = yaml.safe_load(f)
        # plotPath should be expressed in terms of RA_CI_DATA_ROOT so the
        # env-var-redirection trick used by the init tests is valid.
        self.assertIn("${RA_CI_DATA_ROOT}", raw["plotPath"])


if __name__ == "__main__":
    unittest.main()
