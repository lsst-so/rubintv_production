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

"""Tests for the small helpers in timedServices.py.

Right now this only covers ``deep_update``, the recursive dict-merge
used to layer per-pod overrides over a shared base config. The
regression to catch is dict-vs-non-dict overwrite semantics drifting
(e.g. a refactor that recursed into non-dict values, or stopped
mutating in place) — callers in the timed-services event loop rely
on both behaviours.
"""

from __future__ import annotations

import unittest

import lsst.utils.tests
from lsst.rubintv.production.timedServices import deep_update


class DeepUpdateTestCase(lsst.utils.tests.TestCase):
    """The four pinned properties: dict-on-dict recurses, dict-on-non-
    dict overwrites wholesale, the call mutates the base in place AND
    returns it, and either side empty leaves the other intact."""

    def test_topLevelKeyAdded(self) -> None:
        base = {"a": 1}
        result = deep_update(base, {"b": 2})
        self.assertEqual(result, {"a": 1, "b": 2})

    def test_topLevelKeyOverwritten(self) -> None:
        base = {"a": 1}
        result = deep_update(base, {"a": 99})
        self.assertEqual(result, {"a": 99})

    def test_nestedDictMerged(self) -> None:
        # The whole point of deep_update vs dict.update: nested dicts are
        # merged key-by-key rather than wholesale-replaced.
        base = {"outer": {"a": 1, "b": 2}}
        result = deep_update(base, {"outer": {"b": 22, "c": 3}})
        self.assertEqual(result, {"outer": {"a": 1, "b": 22, "c": 3}})

    def test_deeplyNestedDictMerged(self) -> None:
        base = {"l1": {"l2": {"l3": {"a": 1, "b": 2}}}}
        result = deep_update(base, {"l1": {"l2": {"l3": {"b": 99, "c": 3}}}})
        self.assertEqual(result, {"l1": {"l2": {"l3": {"a": 1, "b": 99, "c": 3}}}})

    def test_dictReplacesNonDict(self) -> None:
        # If the target value isn't a dict, the new value (dict or not)
        # replaces it wholesale — only dict-on-dict triggers the recursion.
        base = {"a": 1}
        result = deep_update(base, {"a": {"nested": True}})
        self.assertEqual(result, {"a": {"nested": True}})

    def test_nonDictReplacesDict(self) -> None:
        base = {"a": {"nested": True}}
        result = deep_update(base, {"a": 1})
        self.assertEqual(result, {"a": 1})

    def test_updatesInPlace(self) -> None:
        # The function is documented to return the dict, but it also
        # mutates the input. Pinning this so a refactor to return-only
        # cleanly doesn't go unnoticed at call sites that rely on the
        # mutation.
        base = {"a": 1}
        result = deep_update(base, {"b": 2})
        self.assertIs(result, base)
        self.assertEqual(base, {"a": 1, "b": 2})

    def test_emptyUpdateLeavesBaseUnchanged(self) -> None:
        base = {"a": 1, "b": {"c": 2}}
        result = deep_update(base, {})
        self.assertEqual(result, {"a": 1, "b": {"c": 2}})

    def test_emptyBaseTakesAllOfUpdate(self) -> None:
        result = deep_update({}, {"a": 1, "b": {"c": 2}})
        self.assertEqual(result, {"a": 1, "b": {"c": 2}})


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
