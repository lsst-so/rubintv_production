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

"""Tests for the DREAM cloud map reading, projection and dispatch.

These catch four kinds of regression:

- The url filter drifting so that other DREAM products in the LFA get
  plotted as cloud maps, or real cloud maps get skipped.
- The HDF5 reader drifting from DREAM's file layout (dataset and attribute
  names, time scale).
- The sky projection getting mirrored, rotated, or using the wrong HEALPix
  convention. This is invisible in a smoke test because every wrong
  orientation still renders a plausible-looking disc, so it is pinned
  numerically against maps whose values are a known function of alt/az.
- The channel's seqNum assignment and retry behaviour changing, which would
  renumber or duplicate plots on the frontend.

Nothing here touches the EFD, the LFA or S3: those are all mocked.
"""

from __future__ import annotations

import io
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import astropy.units as u  # type: ignore[import-untyped]
import h5py
import hpgeom
import numpy as np
import pandas as pd
from astropy.coordinates import AltAz, SkyCoord
from astropy.time import Time
from matplotlib.figure import Figure

import lsst.utils.tests
from lsst.rubintv.production import dream as dreamModule
from lsst.rubintv.production.dream import (
    DREAM_LOCATION,
    DreamCloudMap,
    DreamCloudMapChannel,
    getAltAzGrid,
    isCloudMapUrl,
    plotCloudMap,
    readCloudMapHdf5,
    sampleCloudMapAtAltAz,
)

CLOUD_MAP_URL = (
    "https://s3.cp.lsst.org/rubinobs-lfa-cp/DREAM/dream/2026/06/21/"
    "DREAM_dream_2026-06-22T10:49:18.546_B_cloud_sys_66611060_66611063_r0.hdf5"
)
MAP_TIME = Time("2026-06-22T04:00:00", scale="utc")
NSIDE = 32


def _makeHdf5Bytes(jd: float, extinction: np.ndarray) -> bytes:
    """Make the bytes of an HDF5 file laid out like a DREAM cloud map."""
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as f:
        f.attrs["jd"] = jd
        f["clouds_cleaned"] = extinction
        f["flags"] = np.zeros(len(extinction), dtype=np.int32)
    return buffer.getvalue()


def _makeMapOfAltAzFunction(function: str) -> DreamCloudMap:
    """Make a map whose value at each pixel is a function of its alt/az.

    Parameters
    ----------
    function : `str`
        One of ``"alt"``, ``"sinAz"`` or ``"cosAz"``.

    Returns
    -------
    cloudMap : `DreamCloudMap`
        The map, NaN below the horizon.
    """
    pixels = np.arange(hpgeom.nside_to_npixel(NSIDE))
    ra, dec = hpgeom.pixel_to_angle(NSIDE, pixels, nest=True)
    frame = AltAz(obstime=MAP_TIME, location=DREAM_LOCATION)
    altAz = SkyCoord(ra=ra * u.deg, dec=dec * u.deg, frame="icrs").transform_to(frame)
    alt = altAz.alt.deg
    az = altAz.az.rad
    values = {"alt": alt, "sinAz": np.sin(az), "cosAz": np.cos(az)}[function]
    values = np.where(alt > 0, values, np.nan)
    return DreamCloudMap(time=MAP_TIME, extinction=values)


class IsCloudMapUrlTestCase(lsst.utils.tests.TestCase):
    """Only the HDF5 cloud_sys files are cloud maps; DREAM puts other
    products in the same LFA topic."""

    def test_realCloudMapUrlAccepted(self) -> None:
        # The url from a real event, as seen in the EFD.
        self.assertTrue(isCloudMapUrl(CLOUD_MAP_URL))

    def test_otherProductsRejected(self) -> None:
        self.assertFalse(isCloudMapUrl(CLOUD_MAP_URL.replace(".hdf5", ".fits")))
        self.assertFalse(isCloudMapUrl(CLOUD_MAP_URL.replace("cloud_sys", "image")))

    def test_directoryNameNotMatched(self) -> None:
        # Only the filename identifies the product: a cloud_sys directory
        # holding some other HDF5 product must not be picked up.
        url = "https://s3.cp.lsst.org/rubinobs-lfa-cp/DREAM/cloud_sys/DREAM_dream_image.hdf5"
        self.assertFalse(isCloudMapUrl(url))


class ReadCloudMapHdf5TestCase(lsst.utils.tests.TestCase):
    def test_roundTrip(self) -> None:
        # Pins DREAM's file layout: the jd attribute is UTC, and the map is
        # the clouds_cleaned dataset with NaN gaps preserved.
        extinction = np.linspace(0, 2, hpgeom.nside_to_npixel(NSIDE))
        extinction[:100] = np.nan
        cloudMap = readCloudMapHdf5(_makeHdf5Bytes(MAP_TIME.jd, extinction))

        self.assertEqual(cloudMap.time.scale, "utc")
        self.assertAlmostEqual((cloudMap.time - MAP_TIME).sec, 0, delta=1e-3)
        np.testing.assert_array_equal(cloudMap.extinction, extinction)
        self.assertEqual(cloudMap.nside, NSIDE)


class AltAzGridTestCase(lsst.utils.tests.TestCase):
    """The rendered image is the sky seen from below: zenith in the centre,
    north at the top, east on the left, row 0 at the bottom."""

    def setUp(self) -> None:
        self.size = 64
        self.alt, self.az = getAltAzGrid(self.size)
        self.middle = self.size // 2

    def test_zenithAtCentre(self) -> None:
        self.assertGreater(self.alt[self.middle, self.middle], 85)

    def test_cornersOutsideHorizon(self) -> None:
        for row, col in ((0, 0), (0, -1), (-1, 0), (-1, -1)):
            with self.subTest(row=row, col=col):
                self.assertTrue(np.isnan(self.alt[row, col]))
                self.assertTrue(np.isnan(self.az[row, col]))

    def test_cardinalDirections(self) -> None:
        # Getting this wrong mirrors or rotates every plot, which looks
        # completely plausible to anyone not checking against the sky. The
        # difference is wrapped as north sits on the 0/360 seam.
        for (row, col), expectedAz in (
            ((-1, self.middle), 0),
            ((self.middle, 0), 90),
            ((0, self.middle), 180),
            ((self.middle, -1), 270),
        ):
            with self.subTest(expectedAz=expectedAz):
                wrappedDifference = (self.az[row, col] - expectedAz + 180) % 360 - 180
                self.assertLess(abs(wrappedDifference), 2)


class SampleCloudMapAtAltAzTestCase(lsst.utils.tests.TestCase):
    """Sampling must recover a map that is a known function of alt/az.

    This pins the HEALPix conventions (NEST ordering, equatorial
    coordinates) and the image orientation together: a RING/NEST mix-up
    scrambles the alt map, an east/west mirror flips the sign of sin(az),
    and a north/south flip flips the sign of cos(az).
    """

    def setUp(self) -> None:
        self.alt, self.az = getAltAzGrid(64)
        # Near the zenith azimuth changes too quickly across one HEALPix
        # pixel to compare, and at the horizon pixel centres can fall below
        # it.
        self.comparable = (self.alt > 5) & (self.alt < 70)

    def _assertSampledMatches(self, function: str, expected: np.ndarray, tolerance: float) -> None:
        sampled = sampleCloudMapAtAltAz(_makeMapOfAltAzFunction(function), self.alt, self.az, DREAM_LOCATION)
        self.assertFalse(np.ma.getmaskarray(sampled)[self.comparable].any())
        difference = np.abs(sampled.data[self.comparable] - expected[self.comparable])
        self.assertLess(difference.max(), tolerance)

    def test_altitudeRecovered(self) -> None:
        self._assertSampledMatches("alt", self.alt, tolerance=2.5)

    def test_eastWestOrientation(self) -> None:
        self._assertSampledMatches("sinAz", np.sin(np.radians(self.az)), tolerance=0.15)

    def test_northSouthOrientation(self) -> None:
        self._assertSampledMatches("cosAz", np.cos(np.radians(self.az)), tolerance=0.15)

    def test_gapsAndBelowHorizonAreMasked(self) -> None:
        # NaN gaps in the map must stay masked so they get the "no data"
        # colour, rather than being drawn as some extinction value.
        cloudMap = DreamCloudMap(time=MAP_TIME, extinction=np.full(hpgeom.nside_to_npixel(NSIDE), np.nan))
        sampled = sampleCloudMapAtAltAz(cloudMap, self.alt, self.az, DREAM_LOCATION)
        self.assertTrue(np.ma.getmaskarray(sampled).all())


class PlotCloudMapTestCase(lsst.utils.tests.TestCase):
    def test_plotsAndSaves(self) -> None:
        fig = Figure()
        plotCloudMap(_makeMapOfAltAzFunction("alt"), fig, DREAM_LOCATION, imageSize=64, vmin=0, vmax=90)
        buffer = io.BytesIO()
        fig.savefig(buffer, format="png")
        self.assertGreater(len(buffer.getvalue()), 0)

    def test_figureReusedWithoutAccumulatingAxes(self) -> None:
        # The channel reuses one figure all night. If the figure were not
        # cleared, every plot would add another colourbar axis, leaking
        # memory and squashing the plot.
        fig = Figure()
        cloudMap = _makeMapOfAltAzFunction("alt")
        plotCloudMap(cloudMap, fig, DREAM_LOCATION, imageSize=32)
        nAxes = len(fig.axes)
        plotCloudMap(cloudMap, fig, DREAM_LOCATION, imageSize=32)
        self.assertEqual(len(fig.axes), nAxes)


class DreamCloudMapChannelTestCase(lsst.utils.tests.TestCase):
    """seqNums are the 1-based time order of the maps within the dayObs,
    each map is processed once, and a failed map is not retried."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.dayObs = 20260621
        self.urls = [CLOUD_MAP_URL.replace("66611060", str(n)) for n in range(3)]
        self.mapBytes = _makeHdf5Bytes(MAP_TIME.jd, np.zeros(hpgeom.nside_to_npixel(NSIDE)))

        patchers = [
            patch.object(dreamModule, "makeEfdClient"),
            patch.object(dreamModule, "MultiUploader"),
            patch.object(dreamModule, "getEfdData"),
            patch.object(dreamModule, "ResourcePath"),
            patch.object(dreamModule, "plotCloudMap"),
        ]
        mocks = [patcher.start() for patcher in patchers]
        for patcher in patchers:
            self.addCleanup(patcher.stop)
        self.getEfdData = mocks[2]
        self.resourcePath = mocks[3]
        self.resourcePath.return_value.read.return_value = self.mapBytes

        locationConfig = MagicMock()
        locationConfig.dreamOutputPath = self._tmp.name
        self.channel = DreamCloudMapChannel(locationConfig)
        self.channel.fig = MagicMock()
        self.uploader = self.channel.s3Uploader

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _setEfdUrls(self, urls: list[str], minutes: list[int]) -> None:
        index = pd.to_datetime([MAP_TIME.datetime], utc=True).repeat(len(urls))
        index = index + pd.to_timedelta(minutes, unit="min")
        self.getEfdData.return_value = pd.DataFrame({"url": urls}, index=index)

    def _getUploadedSeqNums(self) -> list[int]:
        return [call.kwargs["seqNum"] for call in self.uploader.uploadPerSeqNumPlot.call_args_list]

    def test_seqNumsFollowTimeOrder(self) -> None:
        # The EFD rows arrive out of order here, with a duplicate event and a
        # non-cloud-map product mixed in. seqNums must follow event time and
        # count only distinct cloud maps, otherwise a restarted pod would
        # number the same map differently and overwrite another plot.
        otherProduct = CLOUD_MAP_URL.replace("cloud_sys", "image")
        self._setEfdUrls(
            [self.urls[1], self.urls[0], otherProduct, self.urls[0]],
            [10, 0, 5, 20],
        )
        self.channel.processDay(self.dayObs)

        readUrls = [call.args[0] for call in self.resourcePath.call_args_list]
        self.assertEqual(readUrls, [self.urls[0], self.urls[1]])
        self.assertEqual(self._getUploadedSeqNums(), [1, 2])
        for call in self.uploader.uploadPerSeqNumPlot.call_args_list:
            self.assertEqual(call.kwargs["instrument"], "dream")
            self.assertEqual(call.kwargs["dayObs"], self.dayObs)
            self.assertTrue(
                call.kwargs["filename"].startswith(os.path.join(self._tmp.name, str(self.dayObs)))
            )

    def test_onlyNewMapsProcessedOnNextPoll(self) -> None:
        self._setEfdUrls(self.urls[:2], [0, 10])
        self.channel.processDay(self.dayObs)
        self._setEfdUrls(self.urls, [0, 10, 20])
        self.channel.processDay(self.dayObs)
        self.assertEqual(self._getUploadedSeqNums(), [1, 2, 3])

    def test_failedMapNotRetriedAndDoesNotBlockOthers(self) -> None:
        # One unreadable file must not stop later maps from being plotted,
        # and must not be retried on every poll for the rest of the night.
        self.resourcePath.return_value.read.side_effect = [OSError("LFA unavailable"), self.mapBytes]
        self._setEfdUrls(self.urls[:2], [0, 10])
        self.channel.processDay(self.dayObs)
        self.channel.processDay(self.dayObs)

        self.assertEqual(self.resourcePath.call_count, 2)
        self.assertEqual(self._getUploadedSeqNums(), [2])

    def test_noEventsIsQuiet(self) -> None:
        # During the day there are no events at all; this must be a no-op,
        # not an error.
        self.getEfdData.return_value = pd.DataFrame()
        self.channel.processDay(self.dayObs)
        self.uploader.uploadPerSeqNumPlot.assert_not_called()


class TestMemory(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: object) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
