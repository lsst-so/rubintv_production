# import sys
import time

t0 = time.time()

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

# 226 - in focus, goes to SFM, expect a preliminary_visit_image mosaic etc.
# 227 - FAM CWFS image, goes as a FAM pair, but to the SFM pods
# 228 - FAM CWFS image, goes as a FAM pair, but to the SFM pods
# CWFS goes to AOS pods
# 437 - a bias, to test cpVerify pipelines and mosaicing

where = (
    "exposure.day_obs=20251115 AND exposure.seq_num in (226..228,436)"
    f" AND instrument='{instrument}'"  # on sky!
)
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 4, f"Expected 4 records, got {len(records)}"
records = sorted(records, key=lambda x: (x.day_obs, x.seq_num))  # always dispatch in order
assert len(set(r.day_obs for r in records)) == 1, "Expected all records to have the same day_obs"
recordDict = {r.seq_num: r for r in records}  # so we can dispatch in specific order

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
# head node is online, so that it starts by dispatching from 227 as soon as it
# lands, followed by the others (most likely in reverse order, but that
# shouldn't matter). This ensures the first FAM image of the pair is processed
# before the 2nd image in the pair. If/when the potential
# single-pod-set-deadlock issue is resolved, try inverting this to test. The
# most likely order here for dispatch *by the head node* is: 227, 228, 226,
# 436, but the only part that should matter is 227 before 228.

# NB: Do not add something before 227 without carefully reading all comments
for record in (recordDict[227], recordDict[436], recordDict[226], recordDict[228]):
    assert isinstance(record, DimensionRecord)
    redisHelper.pushNewExposureToHeadNode(record)
    redisHelper.pushToButlerWatcherList(instrument, record)

    # We are dispatching 227 first specifically to make sure it beats 228.
    # Recall though, that this only works correctly because the first payload
    # is landing on empty pods. We dispatch by the headnode as 227, 436, 226,
    # 228, and 227 is picked up first. These pods are then busy. The rest get
    # fanned out by the head node much quicker than the processing succeeds,
    # building up queues for each pod. These are then processed last-in,
    # first-out, so the last one to be dispatched (228) is the next one to be
    # processed after 227. If the pods were not empty at the start, then 227
    # and 228 would both land in the queue before either gets picked up, thus
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
redisHelper.redis.rpush("LSSTCam-FROM-OCS_DONUTPAIR", "2025111500227,2025111500228")

# do LATISS with the same drip-feeder
instrument = "LATISS"
locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.auxtelButlerPath,
    collections=[
        f"{instrument}/defaults",
    ],
)

where = f"exposure.day_obs=20240813 AND exposure.seq_num=632 AND instrument='{instrument}'"  # on sky!
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
assert len(records) == 1, f"Expected 1 LATISS record, got {len(records)}"
redisHelper.pushNewExposureToHeadNode(records[0])
redisHelper.pushToButlerWatcherList(instrument, records[0])
