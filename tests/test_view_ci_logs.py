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

"""Tests for the pure helpers in tests/ci/view_ci_logs.py."""

import sys
import tempfile
import unittest
from pathlib import Path

import lsst.utils.tests

# view_ci_logs.py lives under tests/ci/ and is a standalone script, not part of
# the importable lsst.rubintv.production package. Put its directory on sys.path
# so the helpers can be imported here.
_TESTS_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(_TESTS_DIR / "ci"))
import view_ci_logs as vcl  # type: ignore[import-not-found]  # noqa: E402

_REAL_LOGS = _TESTS_DIR / "data" / "ci_logs"
_SYNTH_LOGS = _TESTS_DIR / "data" / "ci_logs_synthetic"

_GUIDER_LOG = _REAL_LOGS / "LSSTCam_runGuiderAnalysis_usdf_testing_pid_1969951.log"
_ZERNIKE_LOG = _REAL_LOGS / "LSSTCam_runZernikePredictionPlotting_usdf_testing_pid_1969732.log"
_STEP1B_LOG = _REAL_LOGS / "LATISS_runStep1bWorker_usdf_testing_0_pid_1969712.log"
_DRIP_LOG = _REAL_LOGS / "ci_drip_feed_data_usdf_testing_pid_1969954.log"
_HEADNODE_LOG = _REAL_LOGS / "LSSTCam_runHeadNode_usdf_testing_pid_1969947.log"

_SYNTH_MIXED = _SYNTH_LOGS / "synth_mixed_warnings_pid_1.log"
_SYNTH_CHAINED = _SYNTH_LOGS / "synth_chained_traceback_pid_2.log"
_SYNTH_EOF_TB = _SYNTH_LOGS / "synth_traceback_at_eof_pid_3.log"
_SYNTH_EMPTY = _SYNTH_LOGS / "synth_empty_pid_4.log"
_SYNTH_META = _SYNTH_LOGS / "synth_meta_skip_pid_5.log"


class ParseRunNameTestCase(lsst.utils.tests.TestCase):
    def test_labelPlusTimestamp(self) -> None:
        label, timestamp = vcl.parseRunName("myrun_20240315_120034")
        self.assertEqual(label, "myrun")
        self.assertEqual(timestamp, "20240315_120034")

    def test_noLabel(self) -> None:
        label, timestamp = vcl.parseRunName("20240315_120034")
        self.assertIsNone(label)
        self.assertEqual(timestamp, "20240315_120034")

    def test_noTimestamp(self) -> None:
        label, timestamp = vcl.parseRunName("just-a-label")
        self.assertEqual(label, "just-a-label")
        self.assertEqual(timestamp, "unknown")

    def test_multiUnderscoreLabel(self) -> None:
        label, timestamp = vcl.parseRunName("my_long_label_20240315_120034")
        self.assertEqual(label, "my_long_label")
        self.assertEqual(timestamp, "20240315_120034")


class ParseSelectionTestCase(lsst.utils.tests.TestCase):
    def test_single(self) -> None:
        self.assertEqual(vcl.parseSelection("1", 10), [0])

    def test_commaList(self) -> None:
        self.assertEqual(vcl.parseSelection("1,3,5", 10), [0, 2, 4])

    def test_range(self) -> None:
        self.assertEqual(vcl.parseSelection("1-3", 10), [0, 1, 2])

    def test_mixed(self) -> None:
        self.assertEqual(vcl.parseSelection("1,3-5,7", 10), [0, 2, 3, 4, 6])

    def test_deduplicatesAndSorts(self) -> None:
        self.assertEqual(vcl.parseSelection("3,1,2-3,1", 10), [0, 1, 2])

    def test_outOfRangeSingle(self) -> None:
        with self.assertRaises(ValueError):
            vcl.parseSelection("11", 10)

    def test_zeroInvalid(self) -> None:
        with self.assertRaises(ValueError):
            vcl.parseSelection("0", 10)

    def test_invalidRange(self) -> None:
        with self.assertRaises(ValueError):
            vcl.parseSelection("5-3", 10)

    def test_rangeEndOutOfRange(self) -> None:
        with self.assertRaises(ValueError):
            vcl.parseSelection("1-11", 10)


class FormatTimestampTestCase(lsst.utils.tests.TestCase):
    def test_valid(self) -> None:
        self.assertEqual(vcl.formatTimestamp("20240315_120034"), "2024-03-15 12:00:34")

    def test_invalidPassthrough(self) -> None:
        # Malformed input is returned unchanged so calling code never blows up
        # on unexpected run-directory names.
        self.assertEqual(vcl.formatTimestamp("nonsense"), "nonsense")


class ListTestRunsTestCase(lsst.utils.tests.TestCase):
    def test_newestFirstAndSkipsSymlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "20240101_120000").mkdir()
            (base / "20240315_120034").mkdir()
            (base / "other_20240201_120000").mkdir()
            (base / "latest").symlink_to("20240315_120034")
            runs = vcl.listTestRuns(base)
            self.assertEqual(
                runs,
                ["20240315_120034", "other_20240201_120000", "20240101_120000"],
            )

    def test_missingDirReturnsEmpty(self) -> None:
        self.assertEqual(vcl.listTestRuns(Path("/nonexistent/really-not-there")), [])

    def test_emptyDir(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(vcl.listTestRuns(Path(tmp)), [])


class GetLogDirTestCase(lsst.utils.tests.TestCase):
    def test_defaultFollowsLatestSymlink(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "20240315_120034").mkdir()
            (base / "latest").symlink_to("20240315_120034")
            self.assertEqual(vcl.getLogDir(base).name, "20240315_120034")

    def test_defaultFallsBackToNewest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "20240101_120000").mkdir()
            (base / "20240315_120034").mkdir()
            # No 'latest' symlink → should fall back to the newest run.
            self.assertEqual(vcl.getLogDir(base).name, "20240315_120034")

    def test_runIdentifierByIndex(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "20240101_120000").mkdir()
            (base / "20240315_120034").mkdir()
            # Index 1 = second-most-recent.
            self.assertEqual(vcl.getLogDir(base, "1").name, "20240101_120000")

    def test_runIdentifierByName(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "myrun_20240101_120000").mkdir()
            self.assertEqual(vcl.getLogDir(base, "myrun_20240101_120000").name, "myrun_20240101_120000")

    def test_missingRunRaises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                vcl.getLogDir(Path(tmp), "does_not_exist")

    def test_indexOutOfRangeRaises(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            (base / "20240315_120034").mkdir()
            with self.assertRaises(ValueError):
                vcl.getLogDir(base, "42")


class FindLogsByNameTestCase(lsst.utils.tests.TestCase):
    def test_caseInsensitiveMatch(self) -> None:
        logs = vcl.findLogsByName(_REAL_LOGS, "guideranalysis")
        self.assertEqual(len(logs), 1)
        self.assertIn("GuiderAnalysis", logs[0].name)

    def test_substring(self) -> None:
        logs = vcl.findLogsByName(_REAL_LOGS, "SfmRunner")
        self.assertEqual(len(logs), 2)  # one LATISS + one LSSTCam after pruning

    def test_noMatch(self) -> None:
        self.assertEqual(vcl.findLogsByName(_REAL_LOGS, "nothingmatcheshere"), [])


class FindLogsByPidTestCase(lsst.utils.tests.TestCase):
    def test_exactPid(self) -> None:
        logs = vcl.findLogsByPid(_REAL_LOGS, "1969951")
        self.assertEqual(len(logs), 1)
        self.assertIn("1969951", logs[0].name)

    def test_noMatch(self) -> None:
        self.assertEqual(vcl.findLogsByPid(_REAL_LOGS, "999999"), [])


class ListLogFilesTestCase(lsst.utils.tests.TestCase):
    def test_sortedLogs(self) -> None:
        logs = vcl.listLogFiles(_REAL_LOGS)
        names = [p.name for p in logs]
        self.assertEqual(names, sorted(names))
        # The curated fixture set is 22 files; guard against accidental drift.
        self.assertEqual(len(logs), 22)

    def test_missingDir(self) -> None:
        self.assertEqual(vcl.listLogFiles(Path("/nonexistent/really-not-there")), [])


class SearchInLogFileTestCase(lsst.utils.tests.TestCase):
    def test_caseInsensitiveFindsBothCases(self) -> None:
        ci = vcl.searchInLogFile(_HEADNODE_LOG, "warning", caseInsensitive=True)
        cs = vcl.searchInLogFile(_HEADNODE_LOG, "warning", caseInsensitive=False)
        # CI should find everything CS finds, and strictly more (since the file
        # uses uppercase WARNING far more often than lowercase 'warning').
        self.assertGreater(len(ci), len(cs))

    def test_noMatch(self) -> None:
        self.assertEqual(vcl.searchInLogFile(_HEADNODE_LOG, "zzz-nope-zzz"), [])

    def test_returnsLineNumbersOneBased(self) -> None:
        results = vcl.searchInLogFile(_HEADNODE_LOG, "WARNING", caseInsensitive=False)
        self.assertTrue(all(lineNum >= 1 for lineNum, _ in results))


class SearchAcrossAllLogsTestCase(lsst.utils.tests.TestCase):
    def test_findsMultipleFiles(self) -> None:
        results = vcl.searchAcrossAllLogs(_REAL_LOGS, "WARNING", caseInsensitive=False)
        self.assertGreater(len(results), 5)

    def test_noMatchReturnsEmpty(self) -> None:
        self.assertEqual(
            vcl.searchAcrossAllLogs(_REAL_LOGS, "zzz-nope-zzz", caseInsensitive=False),
            {},
        )


class ExtractTracebacksTestCase(lsst.utils.tests.TestCase):
    def test_realGuiderAnalysisHasFour(self) -> None:
        tbs = vcl.extractTracebacks(_GUIDER_LOG)
        self.assertEqual(len(tbs), 4)
        for tb in tbs:
            self.assertTrue(tb.tracebackLines[0].strip().startswith("Traceback"))
            self.assertIs(tb.logFile, _GUIDER_LOG)

    def test_cleanRealLogHasNoTracebacks(self) -> None:
        self.assertEqual(vcl.extractTracebacks(_DRIP_LOG), [])

    def test_py313PositionMarkersCaptured(self) -> None:
        # Python 3.13 tracebacks include "~~~^^^" position-marker lines
        # interleaved with the source snippets; they must be captured.
        tbs = vcl.extractTracebacks(_GUIDER_LOG)
        hasMarkers = any(any("~" in line and "^" in line for line in tb.tracebackLines) for tb in tbs)
        self.assertTrue(hasMarkers)

    def test_chainedException(self) -> None:
        tbs = vcl.extractTracebacks(_SYNTH_CHAINED)
        # Chained exceptions register as two distinct tracebacks.
        self.assertEqual(len(tbs), 2)
        self.assertIn("ValueError", "".join(tbs[0].tracebackLines))
        self.assertIn("RuntimeError", "".join(tbs[1].tracebackLines))

    def test_tracebackAtEof(self) -> None:
        tbs = vcl.extractTracebacks(_SYNTH_EOF_TB)
        self.assertEqual(len(tbs), 1)
        self.assertIn("KeyError", tbs[0].tracebackLines[-1])

    def test_emptyFile(self) -> None:
        self.assertEqual(vcl.extractTracebacks(_SYNTH_EMPTY), [])

    def test_preLinesCaptured(self) -> None:
        tbs = vcl.extractTracebacks(_GUIDER_LOG, contextLines=5)
        # Tracebacks beyond the top of the file should have 5 context lines.
        self.assertTrue(any(len(tb.preLines) == 5 for tb in tbs))


class FindAllTracebacksTestCase(lsst.utils.tests.TestCase):
    def test_realLogsOnlyGuiderHasTracebacks(self) -> None:
        byFile = vcl.findAllTracebacks(_REAL_LOGS)
        self.assertEqual(len(byFile), 1)
        (onlyFile,) = byFile
        self.assertEqual(onlyFile, _GUIDER_LOG)
        self.assertEqual(len(byFile[onlyFile]), 4)

    def test_metaFilesSkipped(self) -> None:
        byFile = vcl.findAllTracebacks(_SYNTH_LOGS)
        # The chained and EOF fixtures are included, the meta-named one is not.
        names = {p.name for p in byFile}
        self.assertIn(_SYNTH_CHAINED.name, names)
        self.assertIn(_SYNTH_EOF_TB.name, names)
        self.assertNotIn(_SYNTH_META.name, names)


class ExtractWarningsTestCase(lsst.utils.tests.TestCase):
    def test_realZernikeMixedPyAndLogWarnings(self) -> None:
        ws = vcl.extractWarnings(_ZERNIKE_LOG)
        self.assertEqual(len(ws), 5)
        # The three py-warnings come with a following source-line snippet.
        twoLine = [w for w in ws if len(w.warningLines) == 2]
        self.assertEqual(len(twoLine), 3)
        for w in twoLine:
            self.assertTrue(w.warningLines[1].startswith(" "))
            self.assertRegex(w.warningLines[0], r":\d+:\s*\w+Warning:")

    def test_realLogHasWarningClasses(self) -> None:
        ws = vcl.extractWarnings(_STEP1B_LOG)
        blob = "\n".join("\n".join(w.warningLines) for w in ws)
        # At least one Python-warnings-module entry should be present.
        self.assertRegex(blob, r":\d+:\s*\w+Warning:")

    def test_userAndDeprecationWarnings(self) -> None:
        ws = vcl.extractWarnings(_SYNTH_MIXED)
        userCount = sum(any("UserWarning" in line for line in w.warningLines) for w in ws)
        deprecCount = sum(any("DeprecationWarning" in line for line in w.warningLines) for w in ws)
        self.assertGreaterEqual(userCount, 3)  # raw + two stacked
        self.assertEqual(deprecCount, 1)

    def test_rawPyWarningWithSourceLine(self) -> None:
        ws = vcl.extractWarnings(_SYNTH_MIXED)
        # The raw-stderr py-warnings are the ones not prefixed by "WARNING -".
        raw = [w for w in ws if not w.warningLines[0].lstrip().startswith("20")]
        self.assertEqual(len(raw), 2)
        for w in raw:
            self.assertEqual(len(w.warningLines), 2)
            self.assertTrue(w.warningLines[1].startswith(" "))

    def test_stackedPyWarningsNoSourceLine(self) -> None:
        ws = vcl.extractWarnings(_SYNTH_MIXED)
        stacked = [w for w in ws if "stacked py-warnings" in w.warningLines[0]]
        self.assertEqual(len(stacked), 2)
        for w in stacked:
            self.assertEqual(len(w.warningLines), 1)

    def test_shortFormWarnMatches(self) -> None:
        ws = vcl.extractWarnings(_SYNTH_MIXED)
        short = [w for w in ws if " WARN -" in w.warningLines[0]]
        self.assertEqual(len(short), 1)

    def test_lowercaseWarningsNotMatched(self) -> None:
        # The "ignoring all remaining warnings" line uses lowercase 'warnings'
        # and must not be picked up — we only care about WARNING/WARN at log
        # level or the capitalised Python warning classes.
        ws = vcl.extractWarnings(_SYNTH_MIXED)
        joined = "\n".join(line for w in ws for line in w.warningLines)
        self.assertNotIn("ignoring all remaining warnings", joined)

    def test_uppercaseProseIsKnownFalsePositive(self) -> None:
        # Uppercase WARNING in plain prose is intentionally still matched; this
        # test pins the current behaviour so a future regex tightening is an
        # explicit decision rather than a silent change.
        ws = vcl.extractWarnings(_SYNTH_MIXED)
        joined = "\n".join(line for w in ws for line in w.warningLines)
        self.assertIn("scanning logs for the word WARNING in uppercase prose", joined)

    def test_emptyFile(self) -> None:
        self.assertEqual(vcl.extractWarnings(_SYNTH_EMPTY), [])

    def test_lineNumbersOneBased(self) -> None:
        ws = vcl.extractWarnings(_SYNTH_MIXED)
        for w in ws:
            self.assertGreaterEqual(w.lineNum, 1)


class FindAllWarningsTestCase(lsst.utils.tests.TestCase):
    def test_aggregatesAcrossRealLogs(self) -> None:
        byFile = vcl.findAllWarnings(_REAL_LOGS)
        # Most real pods emit at least one warning, so we should span many
        # files, not just the handful that also have tracebacks.
        self.assertGreater(len(byFile), 5)

    def test_metaFilesSkipped(self) -> None:
        byFile = vcl.findAllWarnings(_SYNTH_LOGS)
        names = {p.name for p in byFile}
        self.assertIn(_SYNTH_MIXED.name, names)
        self.assertNotIn(_SYNTH_META.name, names)


class TracebackReprTestCase(lsst.utils.tests.TestCase):
    def test_str(self) -> None:
        tb = vcl.Traceback(_GUIDER_LOG, [], ["Traceback (most recent call last):\n"])
        self.assertEqual(str(tb), f"Traceback from {_GUIDER_LOG.name}")


class WarningMatchReprTestCase(lsst.utils.tests.TestCase):
    def test_str(self) -> None:
        w = vcl.WarningMatch(_SYNTH_MIXED, 42, ["WARNING - foo\n"])
        self.assertEqual(str(w), f"Warning from {_SYNTH_MIXED.name}:42")


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):  # type: ignore[no-untyped-def]
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
