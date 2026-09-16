import sys
import time
from pathlib import Path

t0 = time.time()

# the exposures to feed are defined once, alongside the unit tests which share
# them; the CI runner puts that directory on sys.path too, but the scripts it
# exec's should be runnable on their own
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fixtureExposures import (  # noqa: E402
    LATISS_ON_SKY,
    LSSTCAM_BIAS,
    LSSTCAM_DARK,
    LSSTCAM_EXPOSURES,
    LSSTCAM_FAM_EXTRA,
    LSSTCAM_FAM_INTRA,
    LSSTCAM_FLAT,
    LSSTCAM_IN_FOCUS,
)

from lsst.daf.butler import Butler, DimensionRecord  # noqa: E402
from lsst.rubintv.production.locationConfig import getAutomaticLocationConfig  # noqa: E402
from lsst.rubintv.production.payloads import Payload  # noqa: E402
from lsst.rubintv.production.podDefinition import PodDetails, PodFlavor  # noqa: E402
from lsst.rubintv.production.redisUtils import RedisHelper  # noqa: E402

print(f"Imports took {(time.time() - t0):.2f} seconds")
t0 = time.time()


instrument = "LSSTCam"

locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.lsstCamButlerPath,
    instrument=instrument,
    collections=[
        f"{instrument}/defaults",
    ],
)

redisHelper = RedisHelper(butler, locationConfig)

# See fixtureExposures.py for what each exposure is and is for. In short: an
# in-focus on-sky image which goes to SFM (expect a preliminary_visit_image
# mosaic etc.), a FAM CWFS pair which goes to the SFM pods (CWFS goes to the
# AOS pods), and a bias, a dark and a flat for the three cp_verify calib
# pipelines (step1a + step1b) and mosaicing. Nothing here assumes they share a
# night: the calibs are from a much newer one, as older raw headers lack
# information cp_verify needs.
ids = ",".join(str(exposure.id) for exposure in LSSTCAM_EXPOSURES)
where = f"exposure in ({ids}) AND instrument='{instrument}'"
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
nExpected = len(LSSTCAM_EXPOSURES)
assert len(records) == nExpected, f"Expected {nExpected} records, got {len(records)}"
recordDict = {r.id: r for r in records}  # so we can dispatch in a specific order
for exposure in LSSTCAM_EXPOSURES:  # so the log shows what each exposure really is
    print(f"CI exposure {exposure}: observation_type={recordDict[exposure.id].observation_type}")

performancePod = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.PERFORMANCE_MONITOR, detectorNumber=None, depth=None
)

podsOffline = True
while podsOffline:
    workers = redisHelper.getAllWorkers(instrument=instrument, podFlavor=PodFlavor.SFM_WORKER)
    podsOffline = len(workers) < 8
    if not podsOffline:
        print("Waiting for SFM pods to come online...")
        time.sleep(1)

headNodeOffline = True
headNodePod = PodDetails(
    instrument=instrument, podFlavor=PodFlavor.HEAD_NODE, detectorNumber=None, depth=None
)
while headNodeOffline:
    headNodeOffline = redisHelper.confirmRunning(headNodePod) is False
    if headNodeOffline:
        print("Waiting for head node to come online...")
        time.sleep(1)
time.sleep(3)  # make sure it's fully online

# this relies on the drip-feeder putting the items in the queue *before* the
# head node is online, so that it starts by dispatching from the intra-focal
# FAM image as soon as it lands, followed by the others (most likely in reverse
# order, but that shouldn't matter). This ensures the first FAM image of the
# pair is processed before the 2nd image in the pair. If/when the potential
# single-pod-set-deadlock issue is resolved, try inverting this to test. The
# most likely order here for dispatch *by the head node* is: intra, extra,
# in-focus, calibs, but the only part that should matter is intra before extra.

# NB: Do not add something before the intra-focal image without carefully
# reading all comments
for exposure in (
    LSSTCAM_FAM_INTRA,
    LSSTCAM_BIAS,
    LSSTCAM_DARK,
    LSSTCAM_FLAT,
    LSSTCAM_IN_FOCUS,
    LSSTCAM_FAM_EXTRA,
):
    record = recordDict[exposure.id]
    assert isinstance(record, DimensionRecord)
    redisHelper.pushNewExposureToHeadNode(record)
    redisHelper.pushToButlerWatcherList(instrument, record)

    # We are dispatching the intra-focal image first specifically to make sure
    # it beats the extra-focal one. Recall though, that this only works
    # correctly because the first payload is landing on empty pods. We
    # dispatch by the headnode as intra, bias, dark, flat, in-focus, extra, and
    # intra is picked up first. These pods are then busy. The rest get fanned
    # out by the head node much quicker than the processing succeeds, building
    # up queues for each pod. These are then processed last-in, first-out, so
    # the last one to be dispatched (extra) is the next one to be processed
    # after intra. If the pods were not empty at the start, then intra and
    # extra would both land in the queue before either gets picked up, thus
    # being processed in reverse order.

    # the 2s sleep time is picked to be >> than the loop speed and << any
    # processing time. Other than that, it doesn't really matter.
    time.sleep(2)

    # queue everything up for performance monitoring once that spins up
    # that comes as a 2nd round, so only starts once everything else is over
    # so it's fine to just enqueue it all right now
    payload = Payload(record.dataId, b"", "", who="")
    redisHelper.enqueuePayload(payload, performancePod)

t1 = time.time()
print(f"Butler init and query took {(time.time() - t0):.2f} seconds")

time.sleep(2)  # make sure the head node has done the dispatch of the SFM image

print("Pushing pair announcement signal to redis (simulating OCS signal)")
redisHelper.redis.rpush("LSSTCam-FROM-OCS_DONUTPAIR", f"{LSSTCAM_FAM_INTRA.id},{LSSTCAM_FAM_EXTRA.id}")

# do LATISS with the same drip-feeder
instrument = "LATISS"
locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.auxtelButlerPath,
    collections=[
        f"{instrument}/defaults",
    ],
)

where = f"exposure = {LATISS_ON_SKY.id} AND instrument='{instrument}'"  # on sky!
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 1, f"Expected 1 LATISS record, got {len(records)}"
redisHelper.pushNewExposureToHeadNode(records[0])
redisHelper.pushToButlerWatcherList(instrument, records[0])
