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

"""Tests for resources.getBasePath site-branch selection.

``getBasePath`` is the per-site entry point that decides which S3
bucket + endpoint to use, based on ``getSite()``. The regressions to
catch:

- A site added to one of the ``ENDPOINTS`` / ``PROFILE_NAMES`` /
  ``BUCKET_NAMES`` tables but not the others — every call from that
  site would then fail with ``KeyError`` at runtime. Pinned by
  ``test_endpointTablesSyncedToProfileTables``.
- A new site supported by ``getSite()`` but unknown to the three
  tables — pinned by ``test_unknownSiteRaises``.
- The ``S3_ENDPOINT_URL`` side-effect drifting (e.g. silently
  removed) — pinned by every per-site branch asserting the env var
  is set.
- Suffix normalisation drifting — adding/removing the trailing slash
  changes which S3 prefix every uploader writes under.
"""

from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from typing import cast
from unittest.mock import patch

import lsst.utils.tests
from lsst.rubintv.production.locationConfig import LocationConfig
from lsst.rubintv.production.resources import (
    BUCKET_NAMES,
    ENDPOINTS,
    PROFILE_NAMES,
    getBasePath,
)


def _fakeLocationConfig(scratchPath: str = "/tmp/scratch") -> LocationConfig:
    # The function only reads ``scratchPath`` off the LocationConfig, so
    # a SimpleNamespace stand-in dodges the YAML loading and directory
    # checks that constructing a real LocationConfig would do.
    return cast(LocationConfig, SimpleNamespace(scratchPath=scratchPath))


class GetBasePathTestCase(lsst.utils.tests.TestCase):
    """One test per supported site, plus the unknown-site case and the
    table-consistency invariant. The site list is intentionally
    explicit (rather than iterating ``ENDPOINTS``) so adding a new
    site requires adding a corresponding test."""

    def setUp(self) -> None:
        # getBasePath sets S3_ENDPOINT_URL as a side effect; keep tests
        # from leaking state across runs.
        self._savedEnv = os.environ.get("S3_ENDPOINT_URL")
        if "S3_ENDPOINT_URL" in os.environ:
            del os.environ["S3_ENDPOINT_URL"]

    def tearDown(self) -> None:
        if self._savedEnv is not None:
            os.environ["S3_ENDPOINT_URL"] = self._savedEnv
        elif "S3_ENDPOINT_URL" in os.environ:
            del os.environ["S3_ENDPOINT_URL"]

    def test_summitBranch(self) -> None:
        cfg = _fakeLocationConfig(scratchPath="scratch")
        with patch("lsst.rubintv.production.resources.getSite", return_value="summit"):
            path = getBasePath(cfg)
        self.assertEqual(
            str(path),
            f"s3://{PROFILE_NAMES['summit']}@{BUCKET_NAMES['summit']}/scratch/",
        )
        self.assertEqual(os.environ["S3_ENDPOINT_URL"], ENDPOINTS["summit"])

    def test_baseBranch(self) -> None:
        cfg = _fakeLocationConfig(scratchPath="s")
        with patch("lsst.rubintv.production.resources.getSite", return_value="base"):
            path = getBasePath(cfg)
        self.assertEqual(str(path), f"s3://{PROFILE_NAMES['base']}@{BUCKET_NAMES['base']}/s/")
        self.assertEqual(os.environ["S3_ENDPOINT_URL"], ENDPOINTS["base"])

    def test_tucsonBranch(self) -> None:
        cfg = _fakeLocationConfig(scratchPath="s")
        with patch("lsst.rubintv.production.resources.getSite", return_value="tucson"):
            path = getBasePath(cfg)
        self.assertEqual(str(path), f"s3://{PROFILE_NAMES['tucson']}@{BUCKET_NAMES['tucson']}/s/")
        self.assertEqual(os.environ["S3_ENDPOINT_URL"], ENDPOINTS["tucson"])

    def test_rubinDevlBranch(self) -> None:
        cfg = _fakeLocationConfig(scratchPath="s")
        with patch("lsst.rubintv.production.resources.getSite", return_value="rubin-devl"):
            path = getBasePath(cfg)
        self.assertEqual(str(path), f"s3://{PROFILE_NAMES['rubin-devl']}@{BUCKET_NAMES['rubin-devl']}/s/")
        self.assertEqual(os.environ["S3_ENDPOINT_URL"], ENDPOINTS["rubin-devl"])

    def test_usdfK8sBranch(self) -> None:
        cfg = _fakeLocationConfig(scratchPath="s")
        with patch("lsst.rubintv.production.resources.getSite", return_value="usdf-k8s"):
            path = getBasePath(cfg)
        self.assertEqual(str(path), f"s3://{PROFILE_NAMES['usdf-k8s']}@{BUCKET_NAMES['usdf-k8s']}/s/")
        self.assertEqual(os.environ["S3_ENDPOINT_URL"], ENDPOINTS["usdf-k8s"])

    def test_unknownSiteRaises(self) -> None:
        cfg = _fakeLocationConfig()
        with patch("lsst.rubintv.production.resources.getSite", return_value="not-a-real-site"):
            with self.assertRaises(KeyError):
                getBasePath(cfg)

    def test_suffixIsAppendedAndNormalised(self) -> None:
        cfg = _fakeLocationConfig(scratchPath="scratch")
        with patch("lsst.rubintv.production.resources.getSite", return_value="summit"):
            withTrailing = getBasePath(cfg, suffix="sub/")
            withoutTrailing = getBasePath(cfg, suffix="sub")
        # The function appends a trailing slash to the suffix if missing,
        # so the two must produce the same final path.
        self.assertEqual(str(withTrailing), str(withoutTrailing))
        self.assertTrue(str(withTrailing).endswith("scratch/sub/"))

    def test_emptySuffixHasNoTrailingSubdir(self) -> None:
        cfg = _fakeLocationConfig(scratchPath="scratch")
        with patch("lsst.rubintv.production.resources.getSite", return_value="summit"):
            path = getBasePath(cfg)
        self.assertTrue(str(path).endswith("scratch/"))

    def test_endpointTablesSyncedToProfileTables(self) -> None:
        # If a site is added to one mapping but not the others, every
        # call from that site fails with KeyError. Pin the invariant.
        self.assertEqual(set(ENDPOINTS), set(PROFILE_NAMES))
        self.assertEqual(set(ENDPOINTS), set(BUCKET_NAMES))


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
