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

from __future__ import annotations

import io
import json
import os
from contextlib import redirect_stdout
from typing import TYPE_CHECKING, Any, Callable

import pandas as pd

from lsst.pipe.tasks.postprocess import MakeCcdVisitTableTask

try:
    from lsst_efd_client import EfdClient  # noqa: F401 just check we have it, but don't use it

    HAS_EFD_CLIENT = True
except ImportError:
    HAS_EFD_CLIENT = False

from lsst.summit.utils import NightReport
from lsst.summit.utils.dateTime import getCurrentDayObsInt

from .baseChannels import BaseButlerChannel
from .parsers import NumpyEncoder
from .plotting import latissNightReportPlots
from .predicates import hasDayRolledOver, raiseIf
from .uploaders import MultiUploader

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, DimensionRecord

    from .locationConfig import LocationConfig
    from .podDefinition import PodDetails

__all__ = [
    "NightReportChannel",
]


def _catchPrintOutput(functionToCall: Callable, *args: Any, **kwargs: Any) -> str:
    """Capture stdout from a function call into a string.

    Used by `NightReportChannel` to grab the printed output of helper
    methods like ``NightReport.printShutterTimes``.
    """
    f = io.StringIO()
    with redirect_stdout(f):
        functionToCall(*args, **kwargs)
    return f.getvalue()


class NightReportChannel(BaseButlerChannel):
    """Class for running the AuxTel Night Report channel on the rapid analysis
    backend.

    Parameters
    ----------
    locationConfig : `lsst.rubintv.production.locationConfig.LocationConfig`
        The locationConfig containing the path configs.
    butler : `lsst.daf.butler.Butler`
        The Butler to use for data access.
    instrument : `str`
        The instrument name.
    podDetails : `lsst.rubintv.production.podDefinition.PodDetails`
        The pod details identifying this worker.
    dayObs : `int`, optional
        The dayObs. If not provided, will be calculated from the current time.
        This should be supplied manually if running catchup or similar, but
        when running live it will be set automatically so that the current day
        is processed.
    doRaise : `bool`, optional
        If True, raise exceptions instead of logging them as warnings.
    """

    def __init__(
        self,
        locationConfig: LocationConfig,
        butler: Butler,
        instrument: str,
        podDetails: PodDetails,
        *,
        dayObs: int | None = None,
        doRaise: bool = False,
    ) -> None:
        super().__init__(
            locationConfig=locationConfig,
            butler=butler,
            podDetails=podDetails,
            doRaise=doRaise,
        )
        self.instrument = instrument
        self.s3Uploader: MultiUploader = MultiUploader()

        # we update when the quickLookExp lands, but we scrape for everything,
        # updating the CcdVisitSummaryTable in the hope that the
        # CalibrateCcdRunner is producing. Because that takes longer to run,
        # this means the summary table is often a visit behind, but the only
        # alternative is to block on waiting for preliminary_visit_images,
        # which, if images fail/aren't attempted to be produced, would result
        # in no update at all.
        #
        # This solution is fine as long as there is an end-of-night
        # finalization step to catch everything in the end, and this is
        # easily achieved as we need to reinstantiate a report as each day
        # rolls over anyway.

        self.dayObs = dayObs if dayObs else getCurrentDayObsInt()

        # always attempt to resume on init
        saveFile = self.getSaveFile()
        if os.path.isfile(saveFile):
            self.log.info(f"Resuming from {saveFile}")
            self.report = NightReport(self.butler, self.dayObs, saveFile)
            self.report.rebuild()
        else:  # otherwise start a new report from scratch
            self.report = NightReport(self.butler, self.dayObs)

    def finalizeDay(self) -> None:
        """Perform the end of day actions and roll the day over.

        Creates a final version of the plots at the end of the day, starts a
        new NightReport object, and rolls ``self.dayObs`` over.
        """
        self.log.info(f"Creating final plots for {self.dayObs}")
        self.createPlotsAndUpload()
        # TODO: add final plotting of plots which live in the night reporter
        # class here somehow, perhaps by moving them to their own plot classes.

        self.dayObs = getCurrentDayObsInt()
        self.saveFile = self.getSaveFile()
        self.log.info(f"Starting new report for dayObs {self.dayObs}")
        self.report = NightReport(self.butler, self.dayObs)
        return

    def getSaveFile(self) -> str:
        return os.path.join(self.locationConfig.nightReportPath, f"report_{self.dayObs}.pickle")

    def getMetadataTableContents(self) -> pd.DataFrame | None:
        """Get the measured data for the current night.

        Returns
        -------
        mdTable : `pandas.DataFrame`
            The contents of the metdata table from the front end.
        """
        # TODO: need to find a better way of getting this path ideally,
        # but perhaps is OK?
        sidecarFilename = os.path.join(self.locationConfig.auxTelMetadataPath, f"dayObs_{self.dayObs}.json")

        try:
            mdTable = pd.read_json(sidecarFilename).T
            mdTable = mdTable.sort_index()
        except Exception as e:
            self.log.warning(f"Failed to load metadata table from {sidecarFilename}: {e}")
            return None

        if mdTable.empty:
            return None

        return mdTable

    def createCcdVisitTable(self, dayObs: int) -> pd.DataFrame | None:
        """Make the consolidated visit summary table for the given dayObs.

        Parameters
        ----------
        dayObs : `int`
            The dayObs.

        Returns
        -------
        visitSummaryTableOutputCatalog : `pandas.DataFrame` or `None`
            The visit summary table for the dayObs.
        """
        visitSummariesQuery = self.butler.registry.queryDatasets(
            "visitSummary",
            where="visit.day_obs=dayObs",
            bind={"dayObs": dayObs},
            collections=["LATISS/runs/quickLook/1"],
        ).expanded()
        visitSummaries = list(visitSummariesQuery)
        if len(visitSummaries) == 0:
            self.log.warning(f"Found no visitSummaries for dayObs {dayObs}")
            return None
        self.log.info(f"Found {len(visitSummaries)} visitSummaries for dayObs {dayObs}")
        ddRefs = [self.butler.getDeferred(vs) for vs in visitSummaries]
        task = MakeCcdVisitTableTask()
        table = task.run(ddRefs)
        return table.outputCatalog

    def createPlotsAndUpload(self) -> None:
        """Create and upload all plots defined in nightReportPlots.

        All plots defined in __all__ in nightReportPlots are discovered,
        created and uploaded. If any fail, the exception is logged and the next
        plot is created and uploaded.
        """
        md = self.getMetadataTableContents()
        report = self.report
        ccdVisitTable = self.createCcdVisitTable(self.dayObs)
        self.log.info(
            f"Creating plots for dayObs {self.dayObs} with: "
            f"{len(report.data)} items in the night report, "
            f"{0 if md is None else len(md)} items in the metadata table, and "
            f"{0 if ccdVisitTable is None else len(ccdVisitTable)} items in the ccdVisitTable."
        )

        for plotName in latissNightReportPlots.PLOT_FACTORIES:
            try:
                self.log.info(f"Creating plot {plotName}")
                plotFactory = getattr(latissNightReportPlots, plotName)
                plot = plotFactory(
                    dayObs=self.dayObs,
                    locationConfig=self.locationConfig,
                    s3Uploader=self.s3Uploader,
                )
                plot.createAndUpload(report, md, ccdVisitTable)
            except Exception:
                self.log.exception(f"Failed to create plot {plotName}")
                continue

    def callback(self, expRecord: DimensionRecord, doCheckDay: bool = True) -> None:
        """Method called on each new expRecord as it is found in the repo.

        Parameters
        ----------
        expRecord : `lsst.daf.butler.DimensionRecord`
            The exposure record for the latest data.
        doCheckDay : `bool`, optional
            Whether to check if the day has rolled over. This should be left as
            True for normal operation, but set to False when manually running
            on past exposures to save triggering on the fact it is no longer
            that day, e.g. during testing or doing catch-up/backfilling.
        """
        dataId = expRecord.dataId
        md = {}
        try:
            if doCheckDay and hasDayRolledOver(self.dayObs):
                self.log.info(f"Day has rolled over, finalizing report for dayObs {self.dayObs}")
                self.finalizeDay()

            else:
                self.report.rebuild()
                self.report.save(self.getSaveFile())  # save on each call, it's quick and allows resuming

                # make plots here, uploading one by one
                # make all the automagic plots from nightReportPlots.py
                self.createPlotsAndUpload()

                # plots which come from the night report object itself:
                # the per-object airmass plot
                airMassPlotFile = os.path.join(self.locationConfig.nightReportPath, "airmass.png")
                self.report.plotPerObjectAirMass(saveFig=airMassPlotFile)
                self.s3Uploader.uploadNightReportData(
                    instrument="auxtel",
                    dayObs=self.dayObs,
                    filename=airMassPlotFile,
                    plotGroup="Coverage",
                    uploadAs="airmass.png",
                )

                # the alt/az coverage polar plot
                altAzCoveragePlotFile = os.path.join(self.locationConfig.nightReportPath, "alt-az.png")
                self.report.makeAltAzCoveragePlot(saveFig=altAzCoveragePlotFile)
                self.s3Uploader.uploadNightReportData(
                    instrument="auxtel",
                    dayObs=self.dayObs,
                    filename=altAzCoveragePlotFile,
                    plotGroup="Coverage",
                    uploadAs="alt-az.png",
                )

                # Add text items here
                shutterTimes = _catchPrintOutput(self.report.printShutterTimes)
                md["text_010"] = shutterTimes

                obsGaps = _catchPrintOutput(self.report.printObsGaps)
                md["text_020"] = obsGaps

                # Upload the text here
                # Note this file must be called md.json because this filename
                # is used for the upload, and that's what the frontend expects
                jsonFilename = os.path.join(self.locationConfig.nightReportPath, "md.json")
                with open(jsonFilename, "w") as f:
                    json.dump(md, f, cls=NumpyEncoder)
                self.s3Uploader.uploadNightReportData(
                    instrument="auxtel",
                    dayObs=self.dayObs,
                    filename=jsonFilename,
                    isMetadataFile=True,
                )

                self.log.info(f"Finished updating plots and table for {dataId}")

        except Exception as e:
            msg = f"Skipped updating the night report for {dataId}:"
            raiseIf(self.doRaise, e, self.log, msg=msg)
