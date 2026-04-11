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

"""Pure-function Zernike post-processing recipes shared by the live
SingleCorePipelineRunner post-processors and the high-level backfill
helpers in highLevelTools.

Both call sites need to convert raw outputs of the AOS pipeline (a
per-detector ``zernikes`` table or an aggregated ``aggregateZernikesAvg``
table) into the same shape of values that the ConsDB ``ccdvisit1_quicklook``
and ``visit1_quicklook`` columns expect. Previously the recipes were
copied between the two files with paired ``# NOTE: this recipe is copied
and pasted to ...`` warning comments — fragile and easy to drift.

These helpers deliberately do their ``lsst.ts.wep`` imports lazily inside
the function bodies so that callers do not pay the (substantial) ts_wep
import cost just by importing this module. The other dependencies
(``numpy``, ``galsim.zernike``) are imported eagerly because both call
sites already pay for them at module load.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from galsim.zernike import zernikeRotMatrix

if TYPE_CHECKING:
    from astropy.table import Table

    from lsst.ts.ofc import OFCData


__all__ = [
    "MAX_NOLL_INDEX",
    "computeRotatedZernikesForConsDB",
    "computeAosResidualFwhm",
]


# *Ideally* this would be pulled from ``maxconfig.nollIndices()`` but
# that is not accessible from here. In practice a) the AOS pipelines
# never run above 28, and b) ConsDB only has slots for z4..z28, so any
# higher Noll index would be truncated anyway.
MAX_NOLL_INDEX = 28


def computeRotatedZernikesForConsDB(
    zkTable: Table, physicalRotation: float, maxNollIndex: int = MAX_NOLL_INDEX
) -> dict[str, float]:
    """Convert a per-detector Zernike table into the dict ConsDB expects.

    Filters the table to its ``"average"`` row, expands the sparse Noll
    columns to a dense array, applies the rotator-derived rotation
    matrix to bring the values into the consDB frame, and emits a
    ``{"z4": ..., "z5": ..., ...}`` dict suitable for the
    ``ccdvisit1_quicklook`` row writer. Sparse zero entries are dropped
    so they end up as nulls in the database.

    Parameters
    ----------
    zkTable : `astropy.table.Table`
        The ``zernikes`` dataset from the AOS pipeline. Must contain a
        ``label == "average"`` row and metadata keys ``opd_columns`` and
        ``noll_indices``.
    physicalRotation : `float`
        Physical rotator angle, in degrees, used to build the rotation
        matrix that takes the Zernikes from the camera-frame back to
        the OCS frame.
    maxNollIndex : `int`, optional
        Maximum Noll index to expand the sparse Zernikes to. Defaults to
        ``MAX_NOLL_INDEX`` (28) — change at your own risk because this
        is the wire format ConsDB expects.

    Returns
    -------
    consDbValues : `dict` [`str`, `float`]
        Mapping ``{"z<n>": value}`` for ``n`` from 4 upwards, omitting
        any modes that were sparsely zero in the input.
    """
    from lsst.ts.wep.utils.zernikeUtils import makeDense

    average = zkTable[zkTable["label"] == "average"]
    zkCols = average.meta["opd_columns"]
    nollIndices = np.asarray(average.meta["noll_indices"])
    zkSparse = average[zkCols].to_pandas().values[0]
    zkDense = makeDense(zkSparse, nollIndices, maxNollIndex)
    rotationMatrix = zernikeRotMatrix(maxNollIndex, -np.deg2rad(physicalRotation))
    # We only track z4 upwards and ConsDB only has slots for z4..z28.
    zernikeValues: np.ndarray = zkDense / 1e3 @ rotationMatrix[4:, 4:]

    consDbValues: dict[str, float] = {}
    for i in range(len(zernikeValues)):
        value = float(zernikeValues[i])
        if value == 0:  # skip sparse zeros so they end up null in the DB
            continue
        consDbValues[f"z{i + 4}"] = value
    return consDbValues


def computeAosResidualFwhm(zernikes: Table, ofcData: OFCData) -> tuple[float, float]:
    """Compute the residual AOS FWHM and (best-effort) donut blur FWHM.

    Iterates the rows of an ``aggregateZernikesAvg`` table, subtracts
    the per-detector ``y2_correction`` from the dense Zernike values,
    converts those into per-detector PSF widths, and reduces them to
    a single residual figure of merit using the empirical adjustment
    ``residual = 1.06 * log(1 + mean_rowSum)`` from John Franklin's
    paper.

    Parameters
    ----------
    zernikes : `astropy.table.Table`
        The ``aggregateZernikesAvg`` dataset for the visit. Must
        contain ``zk_deviation_OCS`` and ``detector`` columns and a
        ``meta["nollIndices"]`` entry. ``meta["estimatorInfo"]`` is
        consulted for the optional ``fwhm`` field used as the donut
        blur estimate (Danish populates it; TIE does not; AI_DONUT may
        not have ``estimatorInfo`` at all).
    ofcData : `lsst.ts.ofc.OFCData`
        OFCData providing the per-detector ``y2_correction`` array.

    Returns
    -------
    residual : `float`
        The single-figure residual AOS FWHM, in the units the ConsDB
        ``aos_fwhm`` column expects.
    donutBlurFwhm : `float`
        The donut blur FWHM if Danish recorded one in the table
        metadata, otherwise ``float("nan")``.
    """
    from lsst.ts.wep.utils import convertZernikesToPsfWidth, makeDense

    nollIndices = zernikes.meta["nollIndices"]
    maxNollIndex = np.max(zernikes.meta["nollIndices"])
    rowSums: list[float] = []
    for row in zernikes:
        zkOcs = row["zk_deviation_OCS"]
        detector = row["detector"]
        zkDense = makeDense(zkOcs, nollIndices, maxNollIndex)
        zkDense -= ofcData.y2_correction[detector][: len(zkDense)]
        zkFwhm = convertZernikesToPsfWidth(zkDense)
        rowSums.append(np.sqrt(np.sum(zkFwhm**2)))

    averageResult = np.nanmean(rowSums)
    residual = 1.06 * np.log(1 + averageResult)  # adjustment per John Franklin's paper

    donutBlurFwhm = float("nan")
    if "estimatorInfo" in zernikes.meta and zernikes.meta["estimatorInfo"] is not None:
        # If Danish is run then ``fwhm`` is in the metadata; if TIE then
        # it is not. Danish models the width of the Kolmogorov profile
        # needed to convolve with the geometric donut model (the
        # optics) to match the donut. If AI_DONUT then ``estimatorInfo``
        # may not be present at all. We use a truthiness check rather
        # than ``is not None`` so that a zero value coming from a
        # degenerate fit also degrades to NaN — a zero donut blur is
        # never physical and was always skipped at the call site.
        if result := zernikes.meta["estimatorInfo"].get("fwhm"):
            donutBlurFwhm = float(result)
    return residual, donutBlurFwhm
