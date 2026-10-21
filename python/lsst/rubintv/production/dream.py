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

"""Plotting of the cloud maps made by DREAM, the summit cloud camera.

DREAM writes each cloud map to the LFA as an HDF5 file and announces it with
a ``largeFileObjectAvailable`` event in the EFD. Any reader that returns a
`DreamCloudMap` can feed the same plotting, whatever the source product.
"""

from __future__ import annotations

__all__ = [
    "DreamCloudMap",
    "DreamCloudMapChannel",
    "getAltAzGrid",
    "isCloudMapUrl",
    "plotCloudMap",
    "readCloudMapHdf5",
    "sampleCloudMapAtAltAz",
]

import io
import logging
import os
import posixpath
from dataclasses import dataclass
from time import sleep
from typing import TYPE_CHECKING, Any

import astropy.units as u  # type: ignore[import-untyped]
import h5py
import hpgeom
import matplotlib
import numpy as np
from astropy.coordinates import AltAz, SkyCoord
from astropy.time import Time
from matplotlib.figure import Figure
from matplotlib.patches import Circle

from lsst.obs.lsst.translators.latiss import AUXTEL_LOCATION
from lsst.resources import ResourcePath
from lsst.summit.utils.dateTime import getCurrentDayObsInt
from lsst.summit.utils.efdUtils import getEfdData, makeEfdClient

from .predicates import hasDayRolledOver, raiseIf
from .uploaders import MultiUploader

if TYPE_CHECKING:
    from astropy.coordinates import EarthLocation

    from .locationConfig import LocationConfig

_LOG = logging.getLogger(__name__)

LFA_TOPIC = "lsst.sal.DREAM.logevent_largeFileObjectAvailable"
CLOUD_MAP_PRODUCT = "cloud_sys"
DREAM_LOCATION = AUXTEL_LOCATION

ALTITUDE_RINGS = (10, 30, 50, 70)
AZIMUTH_SPOKE_STEP = 30
CARDINAL_AZIMUTHS = {"N": 0, "E": 90, "S": 180, "W": 270}


@dataclass(frozen=True)
class DreamCloudMap:
    """A DREAM cloud extinction map.

    Parameters
    ----------
    time : `astropy.time.Time`
        The time the map is for.
    extinction : `numpy.ndarray`
        The extinction in magnitudes, as a NEST-ordered HEALPix map in
        equatorial coordinates, with NaN where there is no data.
    """

    time: Time
    extinction: np.ndarray

    @property
    def nside(self) -> int:
        return hpgeom.npixel_to_nside(len(self.extinction))


def isCloudMapUrl(url: str) -> bool:
    """Check whether an LFA url is for a DREAM HDF5 cloud map.

    Parameters
    ----------
    url : `str`
        The url from the ``largeFileObjectAvailable`` event.

    Returns
    -------
    isCloudMap : `bool`
        Whether the url is for a cloud map.
    """
    filename = posixpath.basename(url)
    return filename.endswith(".hdf5") and CLOUD_MAP_PRODUCT in filename


def readCloudMapHdf5(data: bytes) -> DreamCloudMap:
    """Read a DREAM HDF5 cloud map.

    Parameters
    ----------
    data : `bytes`
        The contents of the HDF5 file.

    Returns
    -------
    cloudMap : `DreamCloudMap`
        The cloud map.
    """
    with h5py.File(io.BytesIO(data), "r") as f:
        jd = float(f.attrs["jd"])
        extinction = np.asarray(f["clouds_cleaned"][:], dtype=float)
    return DreamCloudMap(time=Time(jd, format="jd", scale="utc"), extinction=extinction)


def getAltAzGrid(size: int) -> tuple[np.ndarray, np.ndarray]:
    """Get the altitude and azimuth of each pixel of a zenith-centred image.

    The image is an orthographic projection of the sky as seen from below,
    with north up and east left, and with row 0 at the bottom. Pixels
    outside the horizon are NaN.

    Parameters
    ----------
    size : `int`
        The width and height of the image, in pixels.

    Returns
    -------
    alt : `numpy.ndarray`
        The altitude of each pixel, in degrees.
    az : `numpy.ndarray`
        The azimuth of each pixel, in degrees.
    """
    pixelCentres = (np.arange(size) + 0.5) / size * 2 - 1
    x, y = np.meshgrid(pixelCentres, pixelCentres)
    rho = np.hypot(x, y)
    outsideHorizon = rho > 1

    alt = np.degrees(np.arccos(np.clip(rho, 0, 1)))
    az = np.degrees(np.arctan2(-x, y)) % 360
    alt[outsideHorizon] = np.nan
    az[outsideHorizon] = np.nan
    return alt, az


def sampleCloudMapAtAltAz(
    cloudMap: DreamCloudMap, alt: np.ndarray, az: np.ndarray, location: EarthLocation
) -> np.ma.MaskedArray:
    """Sample a cloud map at the given altitudes and azimuths.

    Parameters
    ----------
    cloudMap : `DreamCloudMap`
        The cloud map to sample.
    alt : `numpy.ndarray`
        The altitudes to sample at, in degrees. NaN values are masked.
    az : `numpy.ndarray`
        The azimuths to sample at, in degrees.
    location : `astropy.coordinates.EarthLocation`
        The location of the camera.

    Returns
    -------
    extinction : `numpy.ma.MaskedArray`
        The extinction at each point, masked where there is no data.
    """
    valid = np.isfinite(alt) & np.isfinite(az)
    frame = AltAz(obstime=cloudMap.time, location=location)
    equatorial = SkyCoord(alt=alt[valid] * u.deg, az=az[valid] * u.deg, frame=frame).icrs
    pixels = hpgeom.angle_to_pixel(cloudMap.nside, equatorial.ra.deg, equatorial.dec.deg, nest=True)

    extinction = np.full(alt.shape, np.nan)
    extinction[valid] = cloudMap.extinction[pixels]
    return np.ma.masked_invalid(extinction)


def plotCloudMap(
    cloudMap: DreamCloudMap,
    fig: Figure,
    location: EarthLocation,
    imageSize: int = 500,
    vmin: float = 0.0,
    vmax: float = 2.0,
) -> None:
    """Plot a cloud map as a zenith-centred all-sky view.

    Clears the figure before plotting.

    Parameters
    ----------
    cloudMap : `DreamCloudMap`
        The cloud map to plot.
    fig : `matplotlib.figure.Figure`
        The figure to plot on.
    location : `astropy.coordinates.EarthLocation`
        The location of the camera.
    imageSize : `int`, optional
        The width and height of the rendered sky image, in pixels.
    vmin : `float`, optional
        The extinction at the bottom of the colour scale, in magnitudes.
    vmax : `float`, optional
        The extinction at the top of the colour scale, in magnitudes.
    """
    alt, az = getAltAzGrid(imageSize)
    extinction = sampleCloudMapAtAltAz(cloudMap, alt, az, location)

    fig.clear()
    ax = fig.add_subplot()
    cmap = matplotlib.colormaps["Blues_r"].with_extremes(bad="#222222")
    image = ax.imshow(
        extinction,
        origin="lower",
        extent=(-1, 1, -1, 1),
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        interpolation="nearest",
    )
    horizon = Circle((0, 0), 1, facecolor="none", edgecolor="black", linewidth=1)
    ax.add_patch(horizon)
    image.set_clip_path(horizon)

    gridStyle: dict[str, Any] = {"color": "grey", "linewidth": 0.8, "alpha": 0.7}
    for ringAlt in ALTITUDE_RINGS:
        radius = np.cos(np.radians(ringAlt))
        ax.add_patch(Circle((0, 0), radius, fill=False, **gridStyle))
        labelAngle = np.radians(-30)
        ax.text(
            radius * np.cos(labelAngle),
            radius * np.sin(labelAngle),
            f"{ringAlt}°",
            color="grey",
            fontsize=9,
            ha="left",
            va="top",
        )
    for spokeAz in np.radians(np.arange(0, 360, AZIMUTH_SPOKE_STEP)):
        ax.plot([0, -np.sin(spokeAz)], [0, np.cos(spokeAz)], **gridStyle)

    for label, cardinalAz in CARDINAL_AZIMUTHS.items():
        cardinalAzRad = np.radians(cardinalAz)
        ax.text(
            -1.07 * np.sin(cardinalAzRad),
            1.07 * np.cos(cardinalAzRad),
            label,
            fontsize=16,
            fontweight="bold",
            ha="center",
            va="center",
        )

    fig.colorbar(image, ax=ax, shrink=0.8, label="Extinction [mag]")
    ax.set_title(f"DREAM cloud map {cloudMap.time.utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    ax.set_xlim(-1.15, 1.15)
    ax.set_ylim(-1.15, 1.15)
    ax.set_aspect("equal")
    ax.set_axis_off()


class DreamCloudMapChannel:
    """Plot and upload each DREAM cloud map as it lands in the LFA.

    Polls the EFD for DREAM's ``largeFileObjectAvailable`` events and plots
    every cloud map not yet processed. The seqNum of each plot is the 1-based
    position of its map within the dayObs, so it is stable across restarts.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The LocationConfig containing the relevant path items:
            ``dreamOutputPath`` : `str`
            Where to write the plots before uploading them.
    doRaise : `bool`, optional
        Raise on error?
    """

    instrument = "dream"
    # TODO: DM-55019 replace with the plot name to use on the frontend
    plotName = "cloud_map"
    cadenceSeconds = 30

    def __init__(self, locationConfig: LocationConfig, doRaise: bool = False) -> None:
        self.locationConfig = locationConfig
        self.doRaise = doRaise
        self.log = _LOG.getChild("dreamCloudMapChannel")
        self.outputPath = locationConfig.dreamOutputPath
        self.client = makeEfdClient()
        self.s3Uploader = MultiUploader()
        self.fig = Figure(figsize=(10, 9))
        self.processedUrls: set[str] = set()

    def getCloudMapUrls(self, dayObs: int) -> list[str]:
        """Get the urls of all the cloud maps for a dayObs, in time order.

        Parameters
        ----------
        dayObs : `int`
            The dayObs.

        Returns
        -------
        urls : `list` [`str`]
            The cloud map urls.
        """
        data = getEfdData(
            self.client,
            LFA_TOPIC,
            columns=["url"],
            dayObs=dayObs,
            warn=False,
            raiseIfTopicNotInSchema=False,
        )
        if data.empty:
            return []
        urls = [url for url in data.sort_index()["url"] if isCloudMapUrl(url)]
        return list(dict.fromkeys(urls))

    def getPlotFilename(self, dayObs: int, seqNum: int) -> str:
        """Get the local filename to save a plot to, creating its directory.

        Parameters
        ----------
        dayObs : `int`
            The dayObs.
        seqNum : `int`
            The seqNum.

        Returns
        -------
        filename : `str`
            The full path to save the plot to.
        """
        directory = os.path.join(self.outputPath, str(dayObs))
        os.makedirs(directory, exist_ok=True)
        return os.path.join(directory, f"dream_{self.plotName}_{dayObs}_{seqNum:06}.png")

    def processCloudMap(self, url: str, dayObs: int, seqNum: int) -> None:
        """Read, plot and upload a single cloud map.

        Parameters
        ----------
        url : `str`
            The LFA url of the cloud map.
        dayObs : `int`
            The dayObs to upload the plot for.
        seqNum : `int`
            The seqNum to upload the plot as.
        """
        self.log.info(f"Plotting DREAM cloud map {seqNum} for {dayObs}: {url}")
        cloudMap = readCloudMapHdf5(ResourcePath(url).read())
        plotCloudMap(cloudMap, self.fig, DREAM_LOCATION)
        filename = self.getPlotFilename(dayObs, seqNum)
        self.fig.savefig(filename, bbox_inches="tight")
        self.s3Uploader.uploadPerSeqNumPlot(
            instrument=self.instrument,
            plotName=self.plotName,
            dayObs=dayObs,
            seqNum=seqNum,
            filename=filename,
        )

    def processDay(self, dayObs: int) -> None:
        """Process all the not-yet-processed cloud maps for a dayObs.

        Maps which fail are logged and not retried.

        Parameters
        ----------
        dayObs : `int`
            The dayObs.
        """
        for seqNum, url in enumerate(self.getCloudMapUrls(dayObs), start=1):
            if url in self.processedUrls:
                continue
            try:
                self.processCloudMap(url, dayObs, seqNum)
            except Exception as e:
                raiseIf(self.doRaise, e, self.log, msg=f"Failed to process DREAM cloud map {url}")
            finally:
                self.processedUrls.add(url)

    def run(self) -> None:
        """Run forever, processing new cloud maps as they land."""
        dayObs = getCurrentDayObsInt()
        while True:
            try:
                if hasDayRolledOver(dayObs):
                    self.processDay(dayObs)
                    dayObs = getCurrentDayObsInt()
                    self.processedUrls.clear()
                self.processDay(dayObs)
            except Exception as e:
                raiseIf(self.doRaise, e, self.log)
            sleep(self.cadenceSeconds)
