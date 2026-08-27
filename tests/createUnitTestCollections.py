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
import logging
import math
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils import getUserRunCollectionName, removeUserRunCollection

import lsst.summit.utils.butlerUtils as butlerUtils
from lsst.rubintv.production.locationConfig import getAutomaticLocationConfig
from lsst.rubintv.production.processingControl import PIPELINE_NAMES, PipelineComponents, buildPipelines
from lsst.summit.utils.utils import setupLogging

FAM_VISIT_QUERY = "visit in (2025111500227,2025111500228)"
SFM_VISIT_QUERY = "visit in (2025111500226)"
# calib frames don't get visit records defined, so query on exposure
CALIB_EXPOSURE_QUERY = "exposure in (2025111500436)"
# a LATISS CWFS intra/extra pair, for the AOS_LATISS (WEP monolith) pipeline
LATISS_AOS_EXPOSURE_QUERY = "exposure in (2026062500012, 2026062500013)"

INTRA_IDS = (192, 196, 200, 204)
EXTRA_IDS = (191, 195, 199, 203)
SFM_DETECTORS = (90, 91, 92, 93, 94, 95, 96, 97, 98)  # 1 raft

CORNER_DETECTORS = tuple([d for d in INTRA_IDS] + [d for d in EXTRA_IDS])
ALL_DETECTOR_IDS = tuple([d for d in INTRA_IDS] + [d for d in EXTRA_IDS] + [d for d in SFM_DETECTORS])

_LOG = logging.getLogger("lsst.rubintv.tests.createUnitTestCollections")

os.environ["RAPID_ANALYSIS_LOCATION"] = "usdf_testing"
os.environ["RAPID_ANALYSIS_CI"] = "true"
os.environ["RAPID_ANALYSIS_DO_RAISE"] = "True"
os.environ["TARTS_DATA_DIR"] = "/sdf/home/m/mfl/temp/TARTS"
os.environ["AI_DONUT_DATA_DIR"] = "/sdf/home/m/mfl/u/rubintv/aos_data/AI_DONUT"


PER_PIPELINE_EXTRAS: dict[str, list[str]] = {
    "AOS_DANISH": [
        "reassignCwfsCutoutsPairTask:customQG=False",
    ],
    "AOS_WCS_DANISH_BIN_2": [
        "reassignCwfsCutoutsPairTask:customQG=False",
    ],
    "AOS_WCS_DANISH_BIN_1": [
        "reassignCwfsCutoutsPairTask:customQG=False",
    ],
    "AOS_TIE": [
        "reassignCwfsCutoutsPairTask:customQG=False",
    ],
    "AOS_REFIT_WCS": [
        "reassignCwfsCutoutsPairTask:customQG=False",
    ],
    "AOS_AI_DONUT_BINNED2": [
        "reassignCwfsCutoutsPairTask:customQG=False",
    ],
    "AOS_AI_DONUT_UNBINNED": [
        "reassignCwfsCutoutsPairTask:customQG=False",
    ],
}


def runCommand(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=False)


def runCommands(pipelineCommands: dict[str, list[str]]) -> None:
    """Run pipeline commands in parallel.

    Each pipeline's status is logged as it completes, and once they have all
    run a summary of every pipeline's result is printed in one block, so the
    results can be read without picking them out from the interleaved pipeline
    output. Raises at the end if any pipeline failed.

    Parameters
    ----------
    pipelineCommands : `dict[str, list[str]]`
        Dictionary mapping pipeline names to their command lists.
    """
    with ThreadPoolExecutor(max_workers=len(pipelineCommands)) as pool:
        futures = {}
        for pipelineName, command in pipelineCommands.items():
            _LOG.info(f"Submitting pipeline '{pipelineName}':\n{' '.join(command)}\n")
            futures[pool.submit(runCommand, command)] = pipelineName

        results: dict[str, subprocess.CompletedProcess[str]] = {}
        for fut in as_completed(futures):
            pipelineName = futures[fut]
            result = fut.result()
            results[pipelineName] = result
            if result.returncode == 0:
                _LOG.info(f"✅ Pipeline '{pipelineName}' completed successfully")
            else:
                _LOG.error(
                    "❌ Pipeline '%s' failed (exit code %s)\nstdout:\n%s\nstderr:\n%s",
                    pipelineName,
                    result.returncode,
                    result.stdout,
                    result.stderr,
                )

    summaryLines = ["Summary of all pipeline runs:"]
    for pipelineName in pipelineCommands:  # original submission order, not completion order
        result = results[pipelineName]
        if result.returncode == 0:
            summaryLines.append(f"✅ {pipelineName}")
        else:
            summaryLines.append(f"❌ {pipelineName} (exit code {result.returncode})")
    _LOG.info("\n".join(summaryLines))

    failed = [name for name, result in results.items() if result.returncode != 0]
    if failed:
        raise RuntimeError(f"{len(failed)}/{len(pipelineCommands)} pipelines failed: {', '.join(failed)}")


def getDataQueryForPipeline(pipeline: PipelineComponents, pipelineName: str) -> tuple[str, int]:
    """Get the data query string for a given pipeline name and nCores to use.

    Parameters
    ----------
    pipeline : `PipelineComponents`
        The pipeline components object for which to generate the data query.
    pipelineName : `str`
        The name of the pipeline, e.g. "SFM", "BIAS", "AOS_DANISH".

    Returns
    -------
    query : `str`
        The data query string to use for this pipeline, to use with -d.
    nDetectors : `int`
        The number of detectors involved in this pipeline, to use with -j.
    """
    if pipelineName not in PIPELINE_NAMES:
        raise ValueError(f"Unknown pipeline name: {pipelineName}")

    query = ""

    detectors: tuple[int, ...] = ()
    if pipelineName in ("BIAS", "DARK", "FLAT"):  # calibs get the calib frame on the full focal plane
        detectors = ALL_DETECTOR_IDS
        query += CALIB_EXPOSURE_QUERY
    elif pipeline.isFullArrayMode:  # FAM gets science detectors and FAM images
        detectors = SFM_DETECTORS
        query += FAM_VISIT_QUERY
    elif not pipeline.isAosPipeline:  # non-AOS pipelines get inFocus image + science detectors
        detectors = SFM_DETECTORS
        query += SFM_VISIT_QUERY
    elif pipeline.isAosPipeline and not pipeline.isFullArrayMode:  # CWFS pipelines get corner chips
        detectors = CORNER_DETECTORS
        query += SFM_VISIT_QUERY
    else:
        raise RuntimeError(f"Unknown pipeline type for {pipelineName}")

    query += f" AND detector IN ({','.join(str(d) for d in detectors)})"
    query += " AND instrument='LSSTCam'"
    return query, len(detectors)


def main() -> None:
    """Create unit test collections for all head node pipelines."""
    setupLogging()
    _LOG.info("Building all head node pipelines...")
    butler = butlerUtils.makeDefaultButler("LSSTCam", embargo=False, writeable=True)
    locationConfig = getAutomaticLocationConfig()
    graphs, pipelines = buildPipelines("LSSTCam", locationConfig, butler)

    pipelineCommands = {}

    baseCommands = (
        "pipetask",
        "run",
        "-b",
        "main",
        "-i",
        "LSSTCam/defaults",
        "--register-dataset-types",
    )

    _LOG.info("Removing existing collections and building commands for pipelines...")
    totalCores = 0
    for pipelineName in PIPELINE_NAMES:  # pipelineName is e.g. "AOS_REFIT_WCS"
        _LOG.info(f"Preparing pipeline: {pipelineName}")
        pipeline = pipelines[pipelineName]
        commands = list(baseCommands)

        runCollection = getUserRunCollectionName(pipelineName)
        removeUserRunCollection(butler, pipelineName)

        # hard coding for now because we can't use #isr for bias/dark/flat
        # pipelines as they don't have these steps/labels, but we need #isr
        # for the isr pipeline because that will drop the quanta otherwise
        substep = "#isr" if pipelineName == "ISR" else ""  # TODO: remove hardcoding later
        if pipelineName == "BIAS":
            substep = "#verifyBiasIsr"
        if pipelineName == "DARK":
            substep = "#verifyDarkIsr"
        if pipelineName == "FLAT":
            substep = "#verifyFlatIsr"

        commands.extend(
            [
                "--output-run",
                f"{runCollection}",
                "-p",
                f"{pipeline.pipelineFile}{substep}",  # contains full path using e.g. $DONUT_VIZ_DIR/...
            ]
        )
        dataQuery, nCores = getDataQueryForPipeline(pipeline, pipelineName)
        nCores = math.ceil(nCores / 2)  # each quantum processes 2 detectors
        commands.extend(["-d", dataQuery, "-j", str(nCores)])
        totalCores += nCores

        if pipelineName in PER_PIPELINE_EXTRAS:
            configOptions = PER_PIPELINE_EXTRAS[pipelineName]
            for configOption in configOptions:
                commands.extend(["-c", configOption])

        pipelineCommands[pipelineName] = commands

    # LATISS runs its own, single AOS pipeline: the WEP monolith, which
    # processes a whole CWFS pair (both raws) in one quantum on detector 0
    _LOG.info("Preparing pipeline: AOS_LATISS")
    latissButler = butlerUtils.makeDefaultButler("LATISS", embargo=False, writeable=True)
    _, latissPipelines = buildPipelines("LATISS", locationConfig, latissButler)
    latissRunCollection = getUserRunCollectionName("AOS_LATISS")
    removeUserRunCollection(latissButler, "AOS_LATISS")
    pipelineCommands["AOS_LATISS"] = [
        "pipetask",
        "run",
        "-b",
        "main",
        "-i",
        "LATISS/defaults",
        "--register-dataset-types",
        "--output-run",
        f"{latissRunCollection}",
        "-p",
        f"{latissPipelines['AOS_LATISS'].pipelineFile}#step1a",
        "-d",
        f"{LATISS_AOS_EXPOSURE_QUERY} AND detector=0 AND instrument='LATISS'",
        "-j",
        "1",
    ]
    totalCores += 1

    _LOG.info(f"Running all pipelines using a total of {totalCores} cores 😅")
    runCommands(pipelineCommands)
    print("All done!")


if __name__ == "__main__":
    main()
