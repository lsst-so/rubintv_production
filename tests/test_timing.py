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
from contextlib import contextmanager
from typing import Iterator
from unittest.mock import patch

import lsst.utils.tests
from lsst.rubintv.production import timing as timingModule
from lsst.rubintv.production.timing import BoxCarTimer


class _FakeTimeModule:
    """A drop-in stand-in for the `time` module returning fixed timestamps.

    `BoxCarTimer` calls `time.time()` from inside the `timing` module
    namespace, which means we can swap that namespace's `time` reference
    out for an instance of this class to make every lap, pause and
    resume return a deterministic value. This avoids depending on
    `time.sleep()` precision (which on macOS routinely drifts more than
    the ±0.05 s tolerance the original tests required).

    Parameters
    ----------
    ticks : `list` [`float`]
        The sequence of timestamps to return on successive `time()`
        calls. The list must contain at least as many entries as the
        timer will call `time.time()` during the test.
    """

    def __init__(self, ticks: list[float]) -> None:
        self._ticks = list(ticks)
        self._index = 0

    def time(self) -> float:
        if self._index >= len(self._ticks):
            raise AssertionError(
                f"_FakeTimeModule exhausted: only {len(self._ticks)} ticks "
                f"configured but a {self._index + 1}th call was made."
            )
        value = self._ticks[self._index]
        self._index += 1
        return value


@contextmanager
def _fakeTime(ticks: list[float]) -> Iterator[None]:
    """Patch the timing module's `time` reference to a deterministic clock.

    Use as a context manager:

        with _fakeTime([0.0, 0.1, 0.3]):
            timer = BoxCarTimer(length=3)
            timer.start()  # consumes 0.0
            timer.lap()    # consumes 0.1, lap = 0.1
            timer.lap()    # consumes 0.3, lap = 0.2
    """
    with patch.object(timingModule, "time", _FakeTimeModule(ticks)):
        yield


class BoxCarTimerTestCase(lsst.utils.tests.TestCase):
    def test_lap(self) -> None:
        with _fakeTime([0.0, 0.1]):
            timer = BoxCarTimer(length=5)
            timer.start()
            timer.lap()
        self.assertEqual(len(timer._buffer), 1)
        self.assertAlmostEqual(timer._buffer[0], 0.1, places=10)

    def test_buffer_length(self) -> None:
        with _fakeTime([0.0, 0.1, 0.2, 0.3]):
            timer = BoxCarTimer(length=3)
            timer.start()
            timer.lap()
            timer.lap()
            timer.lap()
        self.assertEqual(len(timer._buffer), 3)

    def test_min(self) -> None:
        with _fakeTime([0.0, 0.1, 0.3]):
            timer = BoxCarTimer(length=3)
            timer.start()
            timer.lap()
            timer.lap()
            minValue = timer.min()
            assert minValue is not None
            self.assertAlmostEqual(minValue, 0.1, places=10)
            minFreq = timer.min(frequency=True)
            assert minFreq is not None
            self.assertAlmostEqual(minFreq, 10.0, places=10)

    def test_max(self) -> None:
        with _fakeTime([0.0, 0.1, 0.3]):
            timer = BoxCarTimer(length=3)
            timer.start()
            timer.lap()
            timer.lap()
            maxValue = timer.max()
            assert maxValue is not None
            self.assertAlmostEqual(maxValue, 0.2, places=10)
            maxFreq = timer.max(frequency=True)
            assert maxFreq is not None
            self.assertAlmostEqual(maxFreq, 5.0, places=10)

    def test_mean(self) -> None:
        with _fakeTime([0.0, 0.1, 0.3, 0.45]):
            timer = BoxCarTimer(length=3)
            timer.start()
            timer.lap()
            timer.lap()
            timer.lap()
            meanValue = timer.mean()
            assert meanValue is not None
            self.assertAlmostEqual(meanValue, 0.15, places=10)
            meanFreq = timer.mean(frequency=True)
            assert meanFreq is not None
            self.assertAlmostEqual(meanFreq, 1.0 / 0.15, places=10)

    def test_median(self) -> None:
        with _fakeTime([0.0, 0.1, 0.3, 0.45, 0.75]):
            timer = BoxCarTimer(length=5)
            timer.start()
            timer.lap()
            timer.lap()
            timer.lap()
            timer.lap()
            medianValue = timer.median()
            assert medianValue is not None
            self.assertAlmostEqual(medianValue, 0.175, places=10)
            medianFreq = timer.median(frequency=True)
            assert medianFreq is not None
            self.assertAlmostEqual(medianFreq, 1.0 / 0.175, places=10)

    def test_extreme_outliers(self) -> None:
        # Three short laps of 0.1 s and one long lap of 5.0 s. The mean
        # is dragged up to 1.325 s but the median is unaffected.
        with _fakeTime([0.0, 0.1, 0.2, 0.3, 5.3]):
            timer = BoxCarTimer(length=5)
            timer.start()
            timer.lap()
            timer.lap()
            timer.lap()
            timer.lap()
            meanValue = timer.mean()
            assert meanValue is not None
            self.assertAlmostEqual(meanValue, 1.325, places=10)
            medianValue = timer.median()
            assert medianValue is not None
            self.assertAlmostEqual(medianValue, 0.1, places=10)

    def test_overflow(self) -> None:
        # First lap is 1.0 s, the next five are 0.1 s each. The buffer
        # is length 5, so the 1.0 s lap drops out by the time we ask for
        # the mean and we should see 0.1 s exactly.
        with _fakeTime([0.0, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5]):
            timer = BoxCarTimer(length=5)
            timer.start()
            timer.lap()
            timer.lap()
            timer.lap()
            timer.lap()
            timer.lap()
            timer.lap()
            meanValue = timer.mean()
            assert meanValue is not None
            self.assertAlmostEqual(meanValue, 0.1, places=10)

    def test_empty_buffer(self) -> None:
        with _fakeTime([0.0]):
            timer = BoxCarTimer(length=3)
            timer.start()  # need to start for min/max etc to work at all
            self.assertIsNone(timer.min())
            self.assertIsNone(timer.max())
            self.assertIsNone(timer.mean())
            self.assertIsNone(timer.median())

    def test_last_lap_time(self) -> None:
        with _fakeTime([0.0, 0.1, 0.3]):
            timer = BoxCarTimer(length=5)
            timer.start()
            timer.lap()
            timer.lap()
            lastLap = timer.lastLapTime()
            assert lastLap is not None
            self.assertAlmostEqual(lastLap, 0.2, places=10)

    def test_pause_resume(self) -> None:
        # Sequence:
        #   start()  → tick 0 (lastTime = 0.0)
        #   pause()  → tick 1 (pauseStartTime = 0.1)
        #   resume() → tick 2 (pauseDuration = 0.2 → lastTime bumped to 0.2)
        #   lap()    → tick 3 (currentTime = 0.4 → elapsed = 0.2)
        #   pause()  → tick 4 (inside the assertRaises block)
        #   lap()    → raises before consuming a tick
        with _fakeTime([0.0, 0.1, 0.3, 0.4, 0.5]):
            timer = BoxCarTimer(length=5)
            timer.start()
            timer.pause()
            timer.resume()
            timer.lap()
            self.assertEqual(len(timer._buffer), 1)
            self.assertAlmostEqual(timer._buffer[0], 0.2, places=10)

            with self.assertRaises(RuntimeError):
                timer.pause()
                timer.lap()

    def test_lap_counting(self) -> None:
        # 1 start + 13 lap + 1 pause + 1 resume = 16 tick consumers, but
        # neither resume() nor lap() consume a tick when transitioning
        # state without a real elapsed measurement, so we'll just give it
        # plenty of room.
        with _fakeTime([float(i) for i in range(20)]):
            timer = BoxCarTimer(length=5)
            timer.start()
            for i in range(3):  # check the basics
                timer.lap()
            self.assertEqual(timer.totalLaps, 3)
            for i in range(10):  # check it works if we overrun the buffer
                timer.lap()
            self.assertEqual(timer.totalLaps, 13)
            timer.pause()
            timer.resume()
            self.assertEqual(timer.totalLaps, 13)  # check pause/resume doesn't add a lap

    def test_not_started(self) -> None:
        # No fake clock needed: every method we call here raises before
        # touching `time.time()` because the timer was never started.
        timer = BoxCarTimer(length=5)
        with self.assertRaises(RuntimeError):
            timer.lap()
        with self.assertRaises(RuntimeError):
            timer.pause()
        with self.assertRaises(RuntimeError):
            timer.resume()
        with self.assertRaises(RuntimeError):
            timer.min()
        with self.assertRaises(RuntimeError):
            timer.max()
        with self.assertRaises(RuntimeError):
            timer.mean()
        with self.assertRaises(RuntimeError):
            timer.median()
        with self.assertRaises(RuntimeError):
            timer.lastLapTime()


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
