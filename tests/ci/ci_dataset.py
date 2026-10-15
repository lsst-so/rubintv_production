"""The end-of-run expectations of the integration suite, derived from the
exposures it feeds.

The exposures themselves are defined once, in ``tests/fixtureExposures.py``,
shared with the unit tests. This module sets the order in which
``drip_feed_data.py`` dispatches the LSSTCam ones, and derives from all of
them every plot file and Redis data product which ``test_rapid_analysis.py``
checks for at the end of the run. Nothing else should hard-code expected
outputs: change the inputs and the feeding and checking ends cannot drift
apart.

Plot expectations are expressed as `PlotSpec`s - a plot type plus the
kinds of exposure that produce it - so the expected plot set is a pure
function of the input exposures. When the suite grows the ability to run
different pipeline files, selecting which `PlotSpec` groups apply should
become a function of the chosen pipeline file; the per-exposure derivation
in `getExpectedPlots` will not need to change.
"""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from fixtureExposures import (
    LATISS_EXPOSURES,
    LSSTCAM_BIAS,
    LSSTCAM_DARK,
    LSSTCAM_EXPOSURES,
    LSSTCAM_FAM_EXTRA,
    LSSTCAM_FAM_INTRA,
    LSSTCAM_FLAT,
    LSSTCAM_IN_FOCUS,
    ExposureKind,
    FixtureExposure,
)

__all__ = [
    "ALL_EXPOSURES",
    "LSSTCAM_DISPATCH_ORDER",
    "PLOT_SPECS",
    "PlotSpec",
    "getAosVisits",
    "getExpectedPlots",
    "getExpectedZernikeCounts",
    "getSfmVisits",
]


ALL_EXPOSURES = LSSTCAM_EXPOSURES + LATISS_EXPOSURES

# The order in which the drip-feeder dispatches the LSSTCam exposures. This
# relies on the drip-feeder putting the items in the queue *before* the head
# node is online, so that it starts by dispatching from the intra-focal FAM
# image as soon as it lands, followed by the others (most likely in reverse
# order, but that shouldn't matter). This ensures the first FAM image of the
# pair is processed before the 2nd image in the pair. If/when the potential
# single-pod-set-deadlock issue is resolved, try inverting this to test. The
# most likely order here for dispatch *by the head node* is: intra, extra,
# in-focus, calibs, but the only part that should matter is intra before
# extra.
#
# We are dispatching the intra-focal image first specifically to make sure it
# beats the extra-focal one. Recall though, that this only works correctly
# because the first payload is landing on empty pods. We dispatch by the
# headnode as intra, bias, dark, flat, in-focus, extra, and intra is picked up
# first. These pods are then busy. The rest get fanned out by the head node
# much quicker than the processing succeeds, building up queues for each pod.
# These are then processed last-in, first-out, so the last one to be
# dispatched (extra) is the next one to be processed after intra. If the pods
# were not empty at the start, then intra and extra would both land in the
# queue before either gets picked up, thus being processed in reverse order.
#
# NB: Do not add something before the intra-focal image without carefully
# reading the above
LSSTCAM_DISPATCH_ORDER = (
    LSSTCAM_FAM_INTRA,
    LSSTCAM_BIAS,
    LSSTCAM_DARK,
    LSSTCAM_FLAT,
    LSSTCAM_IN_FOCUS,
    LSSTCAM_FAM_EXTRA,
)


@dataclass(frozen=True)
class PlotSpec:
    """A plot the pipeline writes, and which exposure kinds produce it."""

    plotType: str  # the plot type as it appears in the on-disk filename, e.g. "mount"
    suffix: str  # file extension, without the leading dot
    kinds: frozenset[ExposureKind]
    minSizeBytes: int = 5_000


# The recurring sets of exposure kinds that produce plots:
ALL_KINDS = frozenset(ExposureKind)
# everything on-sky, i.e. all but calibs
ON_SKY = frozenset({ExposureKind.SCIENCE, ExposureKind.FAM_INTRA, ExposureKind.FAM_EXTRA})
# in-focus on-sky science images only
SCIENCE_ONLY = frozenset({ExposureKind.SCIENCE})
# images that AOS results land on: pair results go on the extra-focal
# image's id, and CWFS (corner sensor) results on the science image itself
AOS_RESULT_IMAGES = frozenset({ExposureKind.SCIENCE, ExposureKind.FAM_EXTRA})
# the calibration images
CALIBS = frozenset({ExposureKind.BIAS, ExposureKind.DARK, ExposureKind.FLAT})
# images with postISR that aren't full-frame defocused (i.e. not FAM CWFS)
NOT_DEFOCUSED = frozenset({ExposureKind.SCIENCE}) | CALIBS

# Plots produced for LSSTCam regardless of which science pipelines run:
# per-exposure infrastructure plots and ISR-level mosaics.
LSSTCAM_BASE_PLOTS = (
    PlotSpec("event_timeline", "png", ALL_KINDS),  # event timelines for all images
    PlotSpec("focal_plane_mosaic", "jpg", ALL_KINDS),  # post ISR mosaics for all images
    PlotSpec("witness_detector", "jpg", NOT_DEFOCUSED),  # for all with postISR that aren't CWFS
    PlotSpec("mount", "png", ON_SKY),  # mount plots for the on-sky images
)

# Plots from SFM (single frame measurement) processing.
LSSTCAM_SFM_PLOTS = (
    PlotSpec("calexp_mosaic", "jpg", SCIENCE_ONLY),  # calexp mosaic for the in-focus image
    # all the other plots for the on-sky image: fwhm, imexam
    # TODO: DM-51391 add psfAzEl plot
    PlotSpec("fwhm_focal_plane", "png", SCIENCE_ONLY),
    PlotSpec("imexam", "png", SCIENCE_ONLY),
    PlotSpec("psf_shape_azel", "png", SCIENCE_ONLY),
)

# Plots from AOS processing.
LSSTCAM_AOS_PLOTS = (
    PlotSpec("fp_donut_gallery", "png", ON_SKY),  # donut galleries for the FAM pair and the CWFS image
    PlotSpec("zk_measurement_pyramid", "png", AOS_RESULT_IMAGES),
    PlotSpec("zk_residual_pyramid", "png", AOS_RESULT_IMAGES),
    PlotSpec("psf_zk_panel", "png", AOS_RESULT_IMAGES),
    PlotSpec("fp_pairing_plot", "png", SCIENCE_ONLY),  # donut pairing plot for the regular image
    PlotSpec("donut_fits", "png", SCIENCE_ONLY),
    # Zernike and DOF FWHM prediction plots
    PlotSpec("zernike_predicted_fwhm", "png", SCIENCE_ONLY),
    PlotSpec("dof_predicted_fwhm", "png", SCIENCE_ONLY),
)

# Guider plots and movies.
LSSTCAM_GUIDER_PLOTS = (
    PlotSpec("full_movie", "mp4", ON_SKY, minSizeBytes=200_000),
    PlotSpec("star_movie", "mp4", ON_SKY, minSizeBytes=100_000),
    PlotSpec("centroid_alt_az", "jpg", SCIENCE_ONLY),
    PlotSpec("flux_trend", "jpg", SCIENCE_ONLY),
    PlotSpec("psf_trend", "jpg", SCIENCE_ONLY),
)

# Performance analysis plots.
LSSTCAM_PERFORMANCE_PLOTS = (
    PlotSpec("timing_diagram", "jpg", ALL_KINDS),  # timing diagrams for all images
    PlotSpec("aos_timing", "jpg", AOS_RESULT_IMAGES),  # AOS performance plots
)

# images that get per-exposure ISR on LATISS: the science image and both
# images of the CWFS pair
LATISS_ISR_KINDS = frozenset(
    {ExposureKind.SCIENCE, ExposureKind.CWFS_PAIR_INTRA, ExposureKind.CWFS_PAIR_EXTRA}
)

LATISS_PLOTS = (
    PlotSpec("mount", "png", LATISS_ISR_KINDS),
    PlotSpec("monitor", "jpg", LATISS_ISR_KINDS),
    PlotSpec("imexam", "png", LATISS_ISR_KINDS),
    PlotSpec("specexam", "png", SCIENCE_ONLY),  # the CWFS pair are not spectra
)

PLOT_SPECS: dict[str, tuple[PlotSpec, ...]] = {
    "LSSTCam": (
        LSSTCAM_BASE_PLOTS
        + LSSTCAM_SFM_PLOTS
        + LSSTCAM_AOS_PLOTS
        + LSSTCAM_GUIDER_PLOTS
        + LSSTCAM_PERFORMANCE_PLOTS
    ),
    "LATISS": LATISS_PLOTS,
}


def getExpectedPlots(
    exposures: Sequence[FixtureExposure] = ALL_EXPOSURES,
    plotSpecs: Mapping[str, Sequence[PlotSpec]] = PLOT_SPECS,
) -> list[tuple[str, int]]:
    """Derive the plot files expected on disk from the input exposures.

    Parameters
    ----------
    exposures : `Sequence` [`FixtureExposure`], optional
        The exposures fed into the pipeline.
    plotSpecs : `Mapping` [`str`, `Sequence` [`PlotSpec`]], optional
        The plots each instrument's processing produces, keyed by
        instrument name.

    Returns
    -------
    expected : `list` [`tuple` [`str`, `int`]]
        The expected plots as ``(path, minSizeBytes)`` tuples, where the
        path is relative to ``locationConfig.plotPath``.
    """
    expected: list[tuple[str, int]] = []
    for exposure in exposures:
        for spec in plotSpecs[exposure.instrument]:
            if exposure.kind in spec.kinds:
                expected.append((exposure.getPlotPath(spec.plotType, spec.suffix), spec.minSizeBytes))
    return expected


def getSfmVisits(instrument: str) -> list[int]:
    """Get the visits expected to complete SFM step1b for an instrument.

    Parameters
    ----------
    instrument : `str`
        The instrument name.

    Returns
    -------
    visits : `list` [`int`]
        The visit IDs.
    """
    return [
        exposure.id
        for exposure in ALL_EXPOSURES
        if exposure.instrument == instrument and exposure.kind is ExposureKind.SCIENCE
    ]


def getAosVisits(instrument: str) -> list[int]:
    """Get the visits expected to complete AOS step1b for an instrument.

    The science image is processed through the corner wavefront sensors
    and the FAM images as a pair, so all on-sky images complete AOS
    step1b. Note that only LSSTCam completes AOS step1b at the visit
    level: the LATISS CWFS pair is tracked per-exposure, against
    ``LATISS_CWFS_EXTRA``, and is checked separately.

    Parameters
    ----------
    instrument : `str`
        The instrument name.

    Returns
    -------
    visits : `list` [`int`]
        The visit IDs.
    """
    return [
        exposure.id
        for exposure in ALL_EXPOSURES
        if exposure.instrument == instrument and exposure.kind in ON_SKY
    ]


# One Zernike announcement to MTAOS is expected per sensor (pair) processed:
# the in-focus image is processed through the 8 corner wavefront sensors, and
# each FAM image through the 18 science detectors the suite runs SFM workers
# for (see the detectorNumbers list in test_rapid_analysis.py).
# TODO: will need to double these for unpaired pipelines
ZERNIKE_COUNT_CWFS = 8
ZERNIKE_COUNT_FAM = 18


def getExpectedZernikeCounts() -> dict[FixtureExposure, int]:
    """Get the expected MTAOS Zernike announcement count per exposure.

    Returns
    -------
    counts : `dict` [`FixtureExposure`, `int`]
        The expected Zernike count for each LSSTCam exposure which gets
        AOS processing.
    """
    counts: dict[FixtureExposure, int] = {}
    for exposure in LSSTCAM_EXPOSURES:
        if exposure.kind is ExposureKind.SCIENCE:
            counts[exposure] = ZERNIKE_COUNT_CWFS
        elif exposure.isFam:
            counts[exposure] = ZERNIKE_COUNT_FAM
    return counts
