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
from lsst.rubintv.production.timing import BoxCarTimer


class FakeClock:
    """Deterministic clock returning fixed timestamps on successive calls.

    Pass an instance to ``BoxCarTimer(clock=...)`` to avoid depending on
    real-clock timing in tests. The class is callable: each call returns
    the next entry in ``ticks``.

    Parameters
    ----------
    ticks : `list` [`float`]
        The sequence of timestamps to return on successive calls. Must
        contain at least as many entries as the timer will call the
        clock during the test.
    """

    def __init__(self, ticks: list[float]) -> None:
        self._ticks = list(ticks)
        self._index = 0

    def __call__(self) -> float:
        if self._index >= len(self._ticks):
            raise AssertionError(
                f"FakeClock exhausted: only {len(self._ticks)} ticks "
                f"configured but a {self._index + 1}th call was made."
            )
        value = self._ticks[self._index]
        self._index += 1
        return value


class BoxCarTimerTestCase(lsst.utils.tests.TestCase):
    def test_lap(self) -> None:
        timer = BoxCarTimer(length=5, clock=FakeClock([0.0, 0.1]))
        timer.start()
        timer.lap()
        self.assertEqual(len(timer._buffer), 1)
        self.assertAlmostEqual(timer._buffer[0], 0.1, places=7)

    def test_buffer_length(self) -> None:
        timer = BoxCarTimer(length=3, clock=FakeClock([0.0, 0.1, 0.2, 0.3]))
        timer.start()
        timer.lap()
        timer.lap()
        timer.lap()
        self.assertEqual(len(timer._buffer), 3)

    def test_min(self) -> None:
        timer = BoxCarTimer(length=3, clock=FakeClock([0.0, 0.1, 0.3]))
        timer.start()
        timer.lap()
        timer.lap()
        minValue = timer.min()
        assert minValue is not None
        self.assertAlmostEqual(minValue, 0.1, places=7)
        minFreq = timer.min(frequency=True)
        assert minFreq is not None
        self.assertAlmostEqual(minFreq, 10.0, places=7)

    def test_max(self) -> None:
        timer = BoxCarTimer(length=3, clock=FakeClock([0.0, 0.1, 0.3]))
        timer.start()
        timer.lap()
        timer.lap()
        maxValue = timer.max()
        assert maxValue is not None
        self.assertAlmostEqual(maxValue, 0.2, places=7)
        maxFreq = timer.max(frequency=True)
        assert maxFreq is not None
        self.assertAlmostEqual(maxFreq, 5.0, places=7)

    def test_mean(self) -> None:
        timer = BoxCarTimer(length=3, clock=FakeClock([0.0, 0.1, 0.3, 0.45]))
        timer.start()
        timer.lap()
        timer.lap()
        timer.lap()
        meanValue = timer.mean()
        assert meanValue is not None
        self.assertAlmostEqual(meanValue, 0.15, places=7)
        meanFreq = timer.mean(frequency=True)
        assert meanFreq is not None
        self.assertAlmostEqual(meanFreq, 1.0 / 0.15, places=7)

    def test_median(self) -> None:
        timer = BoxCarTimer(length=5, clock=FakeClock([0.0, 0.1, 0.3, 0.45, 0.75]))
        timer.start()
        timer.lap()
        timer.lap()
        timer.lap()
        timer.lap()
        medianValue = timer.median()
        assert medianValue is not None
        self.assertAlmostEqual(medianValue, 0.175, places=7)
        medianFreq = timer.median(frequency=True)
        assert medianFreq is not None
        self.assertAlmostEqual(medianFreq, 1.0 / 0.175, places=7)

    def test_extreme_outliers(self) -> None:
        # Three short laps of 0.1 s and one long lap of 5.0 s. The mean
        # is dragged up to 1.325 s but the median is unaffected.
        timer = BoxCarTimer(length=5, clock=FakeClock([0.0, 0.1, 0.2, 0.3, 5.3]))
        timer.start()
        timer.lap()
        timer.lap()
        timer.lap()
        timer.lap()
        meanValue = timer.mean()
        assert meanValue is not None
        self.assertAlmostEqual(meanValue, 1.325, places=7)
        medianValue = timer.median()
        assert medianValue is not None
        self.assertAlmostEqual(medianValue, 0.1, places=7)

    def test_overflow(self) -> None:
        # First lap is 1.0 s, the next five are 0.1 s each. The buffer
        # is length 5, so the 1.0 s lap drops out by the time we ask for
        # the mean and we should see 0.1 s exactly.
        timer = BoxCarTimer(length=5, clock=FakeClock([0.0, 1.0, 1.1, 1.2, 1.3, 1.4, 1.5]))
        timer.start()
        timer.lap()
        timer.lap()
        timer.lap()
        timer.lap()
        timer.lap()
        timer.lap()
        meanValue = timer.mean()
        assert meanValue is not None
        self.assertAlmostEqual(meanValue, 0.1, places=7)

    def test_empty_buffer(self) -> None:
        timer = BoxCarTimer(length=3, clock=FakeClock([0.0]))
        timer.start()  # need to start for min/max etc to work at all
        self.assertIsNone(timer.min())
        self.assertIsNone(timer.max())
        self.assertIsNone(timer.mean())
        self.assertIsNone(timer.median())

    def test_last_lap_time(self) -> None:
        timer = BoxCarTimer(length=5, clock=FakeClock([0.0, 0.1, 0.3]))
        timer.start()
        timer.lap()
        timer.lap()
        lastLap = timer.lastLapTime()
        assert lastLap is not None
        self.assertAlmostEqual(lastLap, 0.2, places=7)

    def test_pause_resume(self) -> None:
        timer = BoxCarTimer(length=5, clock=FakeClock([0.0, 0.1, 0.3, 0.4, 0.5]))
        timer.start()
        timer.pause()
        timer.resume()
        timer.lap()
        self.assertEqual(len(timer._buffer), 1)
        self.assertAlmostEqual(timer._buffer[0], 0.2, places=7)

        with self.assertRaises(RuntimeError):
            timer.pause()
            timer.lap()

    def test_lap_counting(self) -> None:
        # Tick values don't matter here — only the lap *count* is under
        # test — so give the clock a strictly-increasing sequence long
        # enough for 1 start + 13 lap + 1 pause + 1 resume = 16 calls.
        timer = BoxCarTimer(length=5, clock=FakeClock([float(i) for i in range(16)]))
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
        # touching the clock because the timer was never started.
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
