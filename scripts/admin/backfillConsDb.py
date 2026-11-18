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

"""Backfill LSSTCam consDB values for a range of dayObs as SQL files.

Each dayObs is run by ``backfillDay`` in a process of its own, with up to
``--n-workers`` running at once. Every step of every day writes its own file,
``<output-dir>/<dayObs>/<step>.sql``, to be loaded with ``psql``, and each
day's output goes to ``<log-dir>/<dayObs>.log``. A step whose file already
exists is skipped, so rerunning the same command after a failure or an
interruption runs only what is missing.

The days run in sessions of their own: Ctrl-C stops them all, each discarding
the step it was part way through, but closing the terminal leaves the running
days to finish, so run it under ``tmux`` or ``nohup`` to keep it going.

Given a single dayObs, it is run in this process, with its output going to
the terminal.

The output directory must already exist. Table schemas are still read from
consDB, but nothing is written to it.

Examples
--------
A month, six days at a time::

    python backfillConsDb.py 20261101 20261130 \\
        --output-dir /home/mfl/temp/consDB_backfill --n-workers 6

One day, in this process::

    python backfillConsDb.py 20261111 \\
        --output-dir /home/mfl/temp/consDB_backfill
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time

from lsst.daf.butler import Butler
from lsst.rubintv.production.consdbSqlWriter import ConsDBSqlWriter
from lsst.rubintv.production.consdbUtils import ConsDBPopulator
from lsst.rubintv.production.highLevelTools import BACKFILL_STEPS, backfillDay
from lsst.rubintv.production.locationConfig import getAutomaticLocationConfig
from lsst.rubintv.production.utils import getDayObsRange
from lsst.summit.utils import ConsDbClient
from lsst.summit.utils.efdUtils import makeEfdClient
from lsst.summit.utils.utils import setupLogging

INSTRUMENT = "LSSTCam"


def backfillOneDay(dayObs: int, outputDir: str, steps: list[str] | None, overwrite: bool) -> bool:
    """Backfill one dayObs in this process.

    Parameters
    ----------
    dayObs : `int`
        The dayObs to backfill.
    outputDir : `str`
        The directory to write the SQL files into.
    steps : `list` [`str`], optional
        The steps to run. Defaults to all of them.
    overwrite : `bool`
        Rerun steps whose SQL files already exist.

    Returns
    -------
    succeeded : `bool`
        Whether every step ran without an error.
    """
    locationConfig = getAutomaticLocationConfig()
    butler = Butler.from_config(
        locationConfig.lsstCamButlerPath,
        instrument=INSTRUMENT,
        collections=[f"{INSTRUMENT}/defaults", locationConfig.getOutputChain(INSTRUMENT)],
    )
    populator = ConsDBPopulator(
        ConsDbClient(locationConfig.consDBURL), None, locationConfig, sqlWriter=ConsDBSqlWriter(outputDir)
    )
    result = backfillDay(
        butler,
        populator,
        dayObs,
        makeEfdClient(),
        metadataPath=locationConfig.lsstCamAosMetadataPath,
        steps=steps,
        overwrite=overwrite,
    )

    print(f"\n{dayObs}: wrote {len(result.outputFiles)} files")
    for path in result.outputFiles:
        print(f"    {path}")
    if result.skippedSteps:
        print(f"Skipped, as already written: {', '.join(result.skippedSteps)}")
    if result.errors:
        print(f"FAILED: {', '.join(result.errors)} (tracebacks above)")
    return result.succeeded


def backfillDaysInParallel(days: list[int], nWorkers: int, childArgs: list[str], logDir: str) -> list[int]:
    """Backfill each dayObs by running this script for it in a new process.

    Up to ``nWorkers`` run at once, taking the next dayObs as each finishes,
    so busy and quiet nights balance out. Each process runs in a session of
    its own, so on Ctrl-C this signals each one exactly once, and starts no
    more.

    Parameters
    ----------
    days : `list` [`int`]
        The dayObs to backfill.
    nWorkers : `int`
        The maximum number of days to run at once.
    childArgs : `list` [`str`]
        The options to pass on to each process, after its dayObs.
    logDir : `str`
        The directory to write each day's log to, as ``<dayObs>.log``.

    Returns
    -------
    failedDays : `list` [`int`]
        The days whose process exited with an error.
    """
    os.makedirs(logDir, exist_ok=True)
    pending = list(days)
    running: dict[subprocess.Popen[bytes], int] = {}
    failedDays: list[int] = []
    nDone = 0
    try:
        while pending or running:
            while pending and len(running) < nWorkers:
                dayObs = pending.pop(0)
                with open(os.path.join(logDir, f"{dayObs}.log"), "a") as log:
                    child = subprocess.Popen(
                        [sys.executable, os.path.abspath(__file__), str(dayObs), *childArgs],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                running[child] = dayObs

            time.sleep(1)
            for child in [child for child in running if child.poll() is not None]:
                dayObs = running.pop(child)
                nDone += 1
                if child.returncode == 0:
                    status = "done"
                else:
                    failedDays.append(dayObs)
                    status = (
                        f"FAILED (exit code {child.returncode}), see {os.path.join(logDir, f'{dayObs}.log')}"
                    )
                print(f"[{nDone}/{len(days)}] {dayObs}: {status}", flush=True)
    except KeyboardInterrupt:
        print("Interrupted: stopping the running days, which discard their unfinished steps", flush=True)
        for child in running:
            child.send_signal(signal.SIGINT)
        for child in running:
            child.wait()
        raise
    return sorted(failedDays)


def main(argv: list[str] | None = None) -> int:
    """Run the backfill from the command line.

    Parameters
    ----------
    argv : `list` [`str`], optional
        The arguments, defaulting to those the script was run with.

    Returns
    -------
    exitCode : `int`
        0 if every step of every day succeeded, else 1.
    """
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("first_day_obs", type=int, help="The first dayObs to backfill, e.g. 20261101")
    parser.add_argument(
        "last_day_obs",
        type=int,
        nargs="?",
        help="The last dayObs to backfill, inclusive. Defaults to the first",
    )
    parser.add_argument(
        "--output-dir", required=True, help="The existing directory to write the SQL files into"
    )
    parser.add_argument("--n-workers", type=int, default=4, help="How many days to run at once (default: 4)")
    parser.add_argument("--log-dir", help="Where to write each day's log (default: <output-dir>/logs)")
    parser.add_argument(
        "--steps",
        nargs="+",
        choices=BACKFILL_STEPS,
        metavar="STEP",
        help=f"The steps to run, from: {', '.join(BACKFILL_STEPS)} (default: all)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Rerun steps whose SQL files already exist")
    args = parser.parse_args(argv)

    days = getDayObsRange(args.first_day_obs, args.last_day_obs or args.first_day_obs)
    ConsDBSqlWriter(args.output_dir)

    if len(days) == 1:
        setupLogging()
        return 0 if backfillOneDay(days[0], args.output_dir, args.steps, args.overwrite) else 1

    childArgs = ["--output-dir", args.output_dir]
    if args.steps:
        childArgs += ["--steps", *args.steps]
    if args.overwrite:
        childArgs.append("--overwrite")
    logDir = args.log_dir or os.path.join(args.output_dir, "logs")

    print(
        f"Backfilling {len(days)} days, {days[0]} to {days[-1]}, {args.n_workers} at a time; logs in {logDir}"
    )
    try:
        failedDays = backfillDaysInParallel(days, args.n_workers, childArgs, logDir)
    except KeyboardInterrupt:
        print("Stopped. Rerun the same command to carry on; the steps already written are skipped")
        return 130
    if failedDays:
        print(f"{len(failedDays)} days had failures: {failedDays}")
        print("Rerun the same command to retry them; the steps already written are skipped")
        return 1
    print("All days completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
