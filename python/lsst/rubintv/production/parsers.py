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

"""Small JSON / dimension-universe parsers and the NumpyEncoder.

These helpers used to live in `utils.py`. They are grouped here because
they all deal with serialising and deserialising data on the way to and
from JSON files (notably the dimension universe file used to roundtrip
DimensionRecords through Redis).
"""

from __future__ import annotations

import json
import math
import time
from typing import TYPE_CHECKING, Any

import numpy as np

from lsst.daf.butler import DimensionConfig, DimensionRecord, DimensionUniverse

if TYPE_CHECKING:
    from .utils import LocationConfig


__all__ = [
    "NumpyEncoder",
    "sanitizeNans",
    "safeJsonOpen",
    "expRecordFromJson",
    "getDimensionUniverse",
    "writeDimensionUniverseFile",
]


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def sanitizeNans(obj: Any) -> Any:
    """
    Recursively sanitize an object by replacing NaN values with None and coerce
    numeric-looking strings to floats.

    Nans are not JSON de-serializable, so this function replaces them with
    ``None``. In addition, strings that represent numeric values (including
    scientific notation) are converted to floats to ensure proper JSON numeric
    typing.

    Parameters
    ----------
    obj : `object`
        The object to santitize, expected to be dict-like: either a single
        ``dict`` or a ``list`` or ``dict`` of ``dict``s.

    Returns
    -------
    obj : `object`
        The object with any NaNs replaced with ``None``, and numeric strings
        converted to floats.
    """
    if isinstance(obj, list):
        return [sanitizeNans(o) for o in obj]
    elif isinstance(obj, dict):
        return {k: sanitizeNans(v) for k, v in obj.items()}
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, str):
        s = obj.strip()
        if not s:
            return obj
        # Try to coerce numeric-looking strings to floats; map NaN to None.
        try:
            value = float(s)
            if math.isnan(value):
                return None
            return value
        except Exception:
            return obj
    else:
        return obj


def safeJsonOpen(filename: str, timeout=0.3) -> str:
    """Open a JSON file, waiting for it to be populated if necessary.

    JSON doesn't like opening zero-byte files, so try to open it, and if it's
    empty, add a series of small waits until it's not empty and reads
    correctly, or the timeout is reachecd.

    Parameters
    ----------
    filename : `str`
        The filename to open.
    timeout : `float`, optional
        The timeout period after which to give up waiting for the contents to
        populate.

    Returns
    -------
    jsonData : `str`
        The data from the json file as a string, i.e. not put back into a
        python object.

    Raises
    ------
    RuntimeError: Raised if the file is not populated within the timeout.
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            with open(filename, "r") as f:
                jsonData = json.load(f)
                return jsonData
        except (RuntimeError, json.decoder.JSONDecodeError):
            pass
        time.sleep(0.1)
    raise RuntimeError(f"Failed to load data from {filename} after {timeout}s")


def writeDimensionUniverseFile(butler, locationConfig: LocationConfig) -> None:
    """Run on butler watcher startup.

    This assumes that all repos in a give location are on the same version, but
    we will make sure to keep that always true.
    """
    with open(locationConfig.dimensionUniverseFile, "w") as f:
        f.write(json.dumps(butler.dimensions.dimensionConfig.toDict()))


def getDimensionUniverse(locationConfig: LocationConfig) -> DimensionUniverse:
    duJson = safeJsonOpen(locationConfig.dimensionUniverseFile)
    return DimensionUniverse(DimensionConfig(duJson))


def expRecordFromJson(expRecordJson: str | bytes, locationConfig: LocationConfig) -> DimensionRecord | None:
    """Deserialize a DimensionRecord from a JSON string.

    expRecordJson : `str` or `bytes`
        The JSON string to deserialize, as either a string or bytes.
    locationConfig : `lsst.rubintv.production.utils.LocationConfig`
        The location configuration, used to determine the dimension universe.
    """
    if not expRecordJson:
        return None
    return DimensionRecord.from_json(expRecordJson, universe=getDimensionUniverse(locationConfig))
