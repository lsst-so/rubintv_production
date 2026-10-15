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

"""The exposures the unit tests and the CI run on, defined once.

Fixtures are identified by full exposure id, so they need not share a night,
and they don't: the calibs are from a much newer night than the on-sky images,
as older raw headers lack information cp_verify needs. Nothing else may
hard-code a dayObs or seqNum.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from lsst.rubintv.production.formatters import getPlotRelativePath

__all__ = [
    "ExposureKind",
    "FixtureExposure",
    "LSSTCAM_IN_FOCUS",
    "LSSTCAM_FAM_INTRA",
    "LSSTCAM_FAM_EXTRA",
    "LSSTCAM_BIAS",
    "LSSTCAM_DARK",
    "LSSTCAM_FLAT",
    "LATISS_ON_SKY",
    "LATISS_CWFS_INTRA",
    "LATISS_CWFS_EXTRA",
    "CALIB_EXPOSURES",
    "LSSTCAM_EXPOSURES",
    "LATISS_EXPOSURES",
]


class ExposureKind(Enum):
    """What processing an exposure gets, and so what outputs to expect."""

    SCIENCE = auto()  # in-focus, on-sky: gets SFM and corner-sensor (CWFS) AOS processing
    FAM_INTRA = auto()  # intra-focal image of a full-array-mode CWFS pair
    FAM_EXTRA = auto()  # extra-focal image of a full-array-mode CWFS pair
    BIAS = auto()
    DARK = auto()
    FLAT = auto()
    CWFS_PAIR_INTRA = auto()  # intra-focal image of a LATISS CWFS pair: gets ISR only
    CWFS_PAIR_EXTRA = auto()  # extra-focal image of a LATISS CWFS pair: gets ISR, and completes the pair


@dataclass(frozen=True)
class FixtureExposure:
    """An exposure the tests run on.

    Parameters
    ----------
    instrument : `str`
        The instrument name, e.g. "LSSTCam".
    id : `int`
        The exposure id, e.g. 2026070200203, which encodes the dayObs and
        seqNum as ``dayObs * 100000 + seqNum``. The on-sky fixtures are
        single-snap visits, so their visit id is this number too.
    role : `str`
        What the exposure is used for, for log and check messages.
    kind : `ExposureKind`
        What processing the exposure gets.
    """

    instrument: str
    id: int
    role: str
    kind: ExposureKind

    @property
    def isFam(self) -> bool:
        """Whether this is one of the images of a full-array-mode pair."""
        return self.kind in (ExposureKind.FAM_INTRA, ExposureKind.FAM_EXTRA)

    @property
    def dayObs(self) -> int:
        """The dayObs the exposure was taken on, e.g. 20260702."""
        return self.id // 100000

    @property
    def seqNum(self) -> int:
        """The exposure's sequence number within its dayObs."""
        return self.id % 100000

    def getPlotPath(self, plotType: str, suffix: str) -> str:
        """Get the path, relative to the plot root, of one of this exposure's
        plots.

        Parameters
        ----------
        plotType : `str`
            The plot type as used in the filename, e.g. "focal_plane_mosaic".
        suffix : `str`
            The file extension without the dot, e.g. "jpg".

        Returns
        -------
        path : `str`
            The path relative to ``locationConfig.plotPath``.
        """
        return getPlotRelativePath(self.instrument, self.dayObs, self.seqNum, plotType, suffix)

    def __str__(self) -> str:
        return f"{self.instrument} {self.dayObs}/{self.seqNum} ({self.role})"


LSSTCAM_IN_FOCUS = FixtureExposure(
    "LSSTCam", 2025111500226, "in-focus on-sky image, goes to SFM", ExposureKind.SCIENCE
)
LSSTCAM_FAM_INTRA = FixtureExposure(
    "LSSTCam", 2025111500227, "intra-focal FAM CWFS image", ExposureKind.FAM_INTRA
)
LSSTCAM_FAM_EXTRA = FixtureExposure(
    "LSSTCam", 2025111500228, "extra-focal FAM CWFS image", ExposureKind.FAM_EXTRA
)
LSSTCAM_BIAS = FixtureExposure("LSSTCam", 2026070200203, "bias", ExposureKind.BIAS)
LSSTCAM_DARK = FixtureExposure("LSSTCam", 2026070200201, "dark", ExposureKind.DARK)
LSSTCAM_FLAT = FixtureExposure("LSSTCam", 2026070200192, "flat", ExposureKind.FLAT)
LATISS_ON_SKY = FixtureExposure("LATISS", 2024081300632, "on-sky image", ExposureKind.SCIENCE)
LATISS_CWFS_INTRA = FixtureExposure(
    "LATISS", 2026062500012, "intra-focal CWFS image", ExposureKind.CWFS_PAIR_INTRA
)
LATISS_CWFS_EXTRA = FixtureExposure(
    "LATISS", 2026062500013, "extra-focal CWFS image, completes the pair", ExposureKind.CWFS_PAIR_EXTRA
)

CALIB_EXPOSURES: dict[str, FixtureExposure] = {
    "BIAS": LSSTCAM_BIAS,
    "DARK": LSSTCAM_DARK,
    "FLAT": LSSTCAM_FLAT,
}
"""The calibration exposure each calibration pipeline runs on, keyed by the
pipeline name the head node uses."""

LSSTCAM_EXPOSURES: tuple[FixtureExposure, ...] = (
    LSSTCAM_IN_FOCUS,
    LSSTCAM_FAM_INTRA,
    LSSTCAM_FAM_EXTRA,
    LSSTCAM_BIAS,
    LSSTCAM_DARK,
    LSSTCAM_FLAT,
)
"""All the LSSTCam exposures the CI feeds."""

LATISS_EXPOSURES: tuple[FixtureExposure, ...] = (
    LATISS_ON_SKY,
    LATISS_CWFS_INTRA,
    LATISS_CWFS_EXTRA,
)
"""All the LATISS exposures the CI feeds, in the order it feeds them: the
intra-focal image of the CWFS pair must land before the extra-focal one, as it
does on the mountain, since it is the extra-focal image completing the pair
that triggers the AOS_LATISS processing."""
