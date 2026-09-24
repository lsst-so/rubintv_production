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

Each is identified by its full exposure id, which encodes the dayObs and
seqNum, so nothing that uses them assumes they share a night. They don't: the
calibration frames are from a much newer night than the on-sky images, because
older raw headers lack information cp_verify needs. Change an id here and
every butler query, expected plot path and Redis check follows, as none of
those may hard-code a dayObs or seqNum.

Both the unit tests (``tests/``) and the CI (``tests/ci/``) import this; the
CI adds this directory to ``sys.path`` to do so.
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "FixtureExposure",
    "LSSTCAM_IN_FOCUS",
    "LSSTCAM_FAM_INTRA",
    "LSSTCAM_FAM_EXTRA",
    "LSSTCAM_BIAS",
    "LSSTCAM_DARK",
    "LSSTCAM_FLAT",
    "LATISS_ON_SKY",
    "CALIB_EXPOSURES",
    "LSSTCAM_EXPOSURES",
]


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
    """

    instrument: str
    id: int
    role: str

    @property
    def dayObs(self) -> int:
        """The dayObs the exposure was taken on, e.g. 20260702."""
        return self.id // 100000

    @property
    def seqNum(self) -> int:
        """The exposure's sequence number within its dayObs."""
        return self.id % 100000

    def plotPath(self, plotType: str, suffix: str) -> str:
        """Get the path, relative to the plot root, of one of this exposure's
        plots.

        Mirrors ``formatters.makePlotFile``, which is what rapid analysis
        writes the plots with; ``test_fixtureExposures.py`` pins the two
        together.

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
        filename = f"{self.instrument}_{plotType}_dayObs_{self.dayObs}_seqNum_{self.seqNum:06}.{suffix}"
        return f"{self.instrument}/{self.dayObs}/{filename}"

    def __str__(self) -> str:
        return f"{self.instrument} {self.dayObs}/{self.seqNum} ({self.role})"


LSSTCAM_IN_FOCUS = FixtureExposure("LSSTCam", 2025111500226, "in-focus on-sky image, goes to SFM")
LSSTCAM_FAM_INTRA = FixtureExposure("LSSTCam", 2025111500227, "intra-focal FAM CWFS image")
LSSTCAM_FAM_EXTRA = FixtureExposure("LSSTCam", 2025111500228, "extra-focal FAM CWFS image")
LSSTCAM_BIAS = FixtureExposure("LSSTCam", 2026070200203, "bias")
LSSTCAM_DARK = FixtureExposure("LSSTCam", 2026070200201, "dark")
LSSTCAM_FLAT = FixtureExposure("LSSTCam", 2026070200192, "flat")
LATISS_ON_SKY = FixtureExposure("LATISS", 2024081300632, "on-sky image")

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
