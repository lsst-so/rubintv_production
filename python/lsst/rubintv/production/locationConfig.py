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

"""LocationConfig — the central per-location path/configuration object.

`LocationConfig` is the frozen dataclass that every other piece of the
package consults for filesystem paths, butler repositories, bucket
names, and pipeline file locations. It loads the per-location YAML
config from the package's `config/` directory on demand and exposes
the entries as `cached_property` accessors so that the directory
existence checks happen exactly once per access.

`getAutomaticLocationConfig` is the convenience entry point used by the
script wrappers; it picks the location either from `sys.argv[1]` (if
the script was invoked with one) or from the
`RAPID_ANALYSIS_LOCATION` environment variable.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from functools import cached_property

import yaml

from lsst.utils import getPackageDir

__all__ = [
    "LocationConfig",
    "getAutomaticLocationConfig",
]


@dataclass(frozen=True)
class LocationConfig:
    """A frozen dataclass for holding location-based configurations.

    Note that all items which are used as paths *must* be decorated with
    @cached_property, otherwise they will be method-type rather than
    str-type when they are accessed.
    """

    location: str
    log: logging.Logger = logging.getLogger("lsst.rubintv.production.locationConfig.LocationConfig")

    def __post_init__(self) -> None:
        # Touch the _config after init to make sure the config file can be
        # read.
        # Any essential items can be touched here, but note they must all
        # exist in all the different locations, otherwise it will fail in some
        # locations and not others, so add things with caution.
        self._config
        self.plotPath

    def _checkDir(self, dirName: str, createIfMissing: bool = True) -> None:
        """Check that a directory exists, optionally creating if it does not.

        Parameters
        ----------
        dirName : `str`
            The directory to check.
        createIfMissing : `bool`, optional
            If True, create the directory if it does not exist.

        Raises
        ------
        RuntimeError
            Raised if the directory does not exist and could not be created.

        TODO: Add a check for being world-writable here, and make the dir
        creation chmod to 777.
        """
        if not os.path.isdir(dirName):
            if createIfMissing:
                self.log.info(f"Directory {dirName} does not exist, creating it.")
                os.makedirs(dirName, exist_ok=True)  # exist_ok necessary due to potential startup race

                if not os.access(dirName, os.W_OK):  # check if dir is 777 but only when creating
                    try:  # attempt to chmod to 777
                        os.chmod(dirName, 0o777)
                    except PermissionError:
                        raise RuntimeError(
                            f"Directory {dirName} is not world-writable " "and could not be made so."
                        )

        # check whether we succeeded
        if not os.path.isdir(dirName):
            msg = f"Directory {dirName} does not exist"
            if createIfMissing:
                msg += " and could not be created."
            raise RuntimeError(msg)

    def _checkFile(self, filename: str) -> None:
        """Check that a file exists.

        Parameters
        ----------
        filename : `str`
            The file to check.

        Raises
        ------
        RuntimeError
            Raised if the file does not exist.
        """
        expanded = os.path.expandvars(filename)
        if not os.path.isfile(expanded):
            raise RuntimeError(f"Could not find file {filename} at {expanded}")

    @cached_property
    def _config(self):
        return _loadConfigFile(self.location)

    @cached_property
    def dimensionUniverseFile(self):
        file = self._config["dimensionUniverseFile"]
        return file

    @cached_property
    def scratchPath(self) -> str:
        """The scratch path for the location."""
        return self._config["scratchPath"]

    @cached_property
    def auxtelButlerPath(self):
        return self._config["auxtelButlerPath"]

    @cached_property
    def ts8ButlerPath(self):
        file = self._config["ts8ButlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def botButlerPath(self):
        file = self._config["botButlerPath"]
        self._checkFile(file)
        return file

    @cached_property
    def metadataPath(self):
        directory = self._config["metadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def auxTelMetadataPath(self):
        directory = self._config["auxTelMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def auxTelMetadataShardPath(self):
        directory = self._config["auxTelMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def ts8MetadataPath(self):
        directory = self._config["ts8MetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def ts8MetadataShardPath(self):
        directory = self._config["ts8MetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def plotPath(self):
        directory = self._config["plotPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def bucketName(self):
        if self._config["bucketName"] == "":
            raise RuntimeError("Bucket name not set in config file")
        return self._config["bucketName"]

    @cached_property
    def binning(self):
        return self._config["binning"]

    @cached_property
    def consDBURL(self):
        return self._config["consDBURL"]

    # start of the summit migration stuff:
    # star tracker paths
    @cached_property
    def starTrackerDataPath(self) -> str:
        directory = self._config["starTrackerDataPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    @cached_property
    def starTrackerMetadataPath(self) -> str:
        directory = self._config["starTrackerMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def starTrackerMetadataShardPath(self) -> str:
        directory = self._config["starTrackerMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def starTrackerOutputPath(self) -> str:
        directory = self._config["starTrackerOutputPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def astrometryNetRefCatPath(self) -> str:
        directory = self._config["astrometryNetRefCatPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    # animation paths
    @cached_property
    def moviePngPath(self) -> str:
        directory = self._config["moviePngPath"]
        self._checkDir(directory)
        return directory

    # all sky cam paths
    @cached_property
    def allSkyRootDataPath(self) -> str:
        directory = self._config["allSkyRootDataPath"]
        self._checkDir(directory, createIfMissing=False)
        return directory

    @cached_property
    def allSkyOutputPath(self) -> str:
        directory = self._config["allSkyOutputPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def nightReportPath(self) -> str:
        directory = self._config["nightReportPath"]
        self._checkDir(directory)
        return directory

    # ComCam stuff:
    @cached_property
    def comCamButlerPath(self) -> str:
        file = self._config["comCamButlerPath"]
        return file

    @cached_property
    def comCamMetadataPath(self) -> str:
        directory = self._config["comCamMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamMetadataShardPath(self) -> str:
        directory = self._config["comCamMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimMetadataPath(self) -> str:
        directory = self._config["comCamSimMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimMetadataShardPath(self) -> str:
        directory = self._config["comCamSimMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimAosMetadataPath(self) -> str:
        directory = self._config["comCamSimAosMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamSimAosMetadataShardPath(self) -> str:
        directory = self._config["comCamSimAosMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamAosMetadataPath(self) -> str:
        directory = self._config["comCamAosMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def comCamAosMetadataShardPath(self) -> str:
        directory = self._config["comCamAosMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamAosMetadataPath(self) -> str:
        directory = self._config["lsstCamAosMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamAosMetadataShardPath(self) -> str:
        directory = self._config["lsstCamAosMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def raPerformanceDirectory(self) -> str:
        directory = self._config["raPerformanceDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def raPerformanceShardsDirectory(self) -> str:
        directory = self._config["raPerformanceShardsDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def guiderDirectory(self) -> str:
        directory = self._config["guiderDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def guiderShardsDirectory(self) -> str:
        directory = self._config["guiderShardsDirectory"]
        self._checkDir(directory)
        return directory

    @cached_property
    def botMetadataPath(self) -> str:
        directory = self._config["botMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def botMetadataShardPath(self) -> str:
        directory = self._config["botMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamMetadataPath(self) -> str:
        directory = self._config["lsstCamMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamMetadataShardPath(self) -> str:
        directory = self._config["lsstCamMetadataShardPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def lsstCamButlerPath(self) -> str:
        directory = self._config["lsstCamButlerPath"]
        return directory

    # TMA config:
    @cached_property
    def tmaMetadataPath(self) -> str:
        directory = self._config["tmaMetadataPath"]
        self._checkDir(directory)
        return directory

    @cached_property
    def tmaMetadataShardPath(self) -> str:
        directory = self._config["tmaMetadataShardPath"]
        self._checkDir(directory)
        return directory

    def getOutputChain(self, instrument: str) -> str:
        return self._config["outputChains"][instrument]

    def getSfmPipelineFile(self, instrument: str) -> str:
        return self._config["sfmPipelineFile"][instrument]

    @cached_property
    def aosLSSTCamPipelineFileDanish(self) -> str:
        return self._config["aosLSSTCamPipelineFileDanish"]

    @cached_property
    def aosLSSTCamPipelineFileTie(self) -> str:
        return self._config["aosLSSTCamPipelineFileTie"]

    @cached_property
    def aosLSSTCamFullArrayModePipelineFileDanish(self) -> str:
        return self._config["aosLSSTCamFullArrayModePipelineFileDanish"]

    @cached_property
    def aosLSSTCamFullArrayModePipelineFileTie(self) -> str:
        return self._config["aosLSSTCamFullArrayModePipelineFileTie"]

    @cached_property
    def aosDataDir(self) -> str:
        return self._config["aosDataDir"]

    @cached_property
    def aosLSSTCamAiDonutPipelineFile(self) -> str:
        return self._config["aosLSSTCamAiDonutPipelineFile"]

    @cached_property
    def aosLSSTCamTartsPipelineFile(self) -> str:
        return self._config["aosLSSTCamTartsPipelineFile"]

    @cached_property
    def aosLSSTCamUnpairedDanishPipelineFile(self) -> str:
        return self._config["aosLSSTCamUnpairedDanishPipelineFile"]

    @cached_property
    def aosLSSTCamWcsDanishBin1PipelineFile(self) -> str:
        return self._config["aosLSSTCamWcsDanishBin1PipelineFile"]

    @cached_property
    def aosLSSTCamWcsDanishBin2PipelineFile(self) -> str:
        return self._config["aosLSSTCamWcsDanishBin2PipelineFile"]


def getAutomaticLocationConfig() -> LocationConfig:
    """Get a location config, based on RA location and command line args.

    If no command line args have been supplied, get the LocationConfig based on
    where the code is being run. If a command line arg was supplied, use that
    as an override value.

    Returns
    -------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The location configuration.
    """
    if len(sys.argv) >= 2:
        try:  # try using this, because anything could be in argv[1]
            location = sys.argv[1]
            return LocationConfig(location.lower())
        except FileNotFoundError:
            pass

    location = os.getenv("RAPID_ANALYSIS_LOCATION", "")
    if not location:
        raise RuntimeError("No location was supplied on the command line or via RAPID_ANALYSIS_LOCATION.")
    return LocationConfig(location.lower())


def _expandEnvVars(node):
    """Recursively expand ``${VAR}`` / ``$VAR`` references in YAML strings."""
    if isinstance(node, str):
        return os.path.expandvars(node)
    if isinstance(node, dict):
        return {k: _expandEnvVars(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_expandEnvVars(v) for v in node]
    return node


def _loadConfigFile(site: str) -> dict[str, str]:
    """Get the site configuration, given a site name.

    Parameters
    ----------
    site : `str`
        The site whose ``config_<site>.yaml`` file should be loaded.

    Returns
    -------
    config : `dict`
        The configuration, as a dict, with ``${VAR}`` style references in
        string values expanded against the current environment.
    """
    packageDir = getPackageDir("rubintv_production")
    configFile = os.path.join(packageDir, "config", f"config_{site}.yaml")
    with open(configFile, "rb") as f:
        config = yaml.safe_load(f)
    return _expandEnvVars(config)
