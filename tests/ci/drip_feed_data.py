# import sys
import time

t0 = time.time()

from ci_dataset import (  # type: ignore[import-not-found] # noqa: E402
    LATISS_EXPOSURES,
    LSSTCAM_DISPATCH_ORDER,
    LSSTCAM_EXPOSURES,
    getFamPairSignal,
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

# the exposures, their roles, and the order they're dispatched in are all
# defined in ci_dataset.py - this file is just the delivery mechanism
dayObses = {exposure.dayObs for exposure in LSSTCAM_EXPOSURES}
assert len(dayObses) == 1, "Expected all LSSTCam exposures to be on the same dayObs"
seqNumCsv = ",".join(str(exposure.seqNum) for exposure in LSSTCAM_EXPOSURES)

where = (
    f"exposure.day_obs={dayObses.pop()} AND exposure.seq_num in ({seqNumCsv})"
    f" AND instrument='{instrument}'"  # on sky!
)
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
nExpected = len(LSSTCAM_EXPOSURES)
assert len(records) == nExpected, f"Expected {nExpected} records, got {len(records)}"
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

# the dispatch order matters - see the comments on LSSTCAM_DISPATCH_ORDER in
# ci_dataset.py before changing anything about this loop
for exposure in LSSTCAM_DISPATCH_ORDER:
    record = recordDict[exposure.seqNum]
    assert isinstance(record, DimensionRecord)
    redisHelper.pushNewExposureToHeadNode(record)
    redisHelper.pushToButlerWatcherList(instrument, record)

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
redisHelper.redis.rpush(f"{instrument}-FROM-OCS_DONUTPAIR", getFamPairSignal())

# do LATISS with the same drip-feeder
instrument = "LATISS"
locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.auxtelButlerPath,
    collections=[
        f"{instrument}/defaults",
    ],
)

for exposure in LATISS_EXPOSURES:
    where = (
        f"exposure.day_obs={exposure.dayObs} AND exposure.seq_num={exposure.seqNum}"
        f" AND instrument='{instrument}'"  # on sky!
    )
    records = list(butler.registry.queryDimensionRecords("exposure", where=where))
    assert len(records) == 1, f"Expected 1 LATISS record, got {len(records)}"
    redisHelper.pushNewExposureToHeadNode(records[0])
    redisHelper.pushToButlerWatcherList(instrument, records[0])
