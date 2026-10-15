import sys
import time
from pathlib import Path

t0 = time.time()

# for fixtureExposures; the CI runner adds it too, but this must run standalone
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ci_dataset import LSSTCAM_DISPATCH_ORDER  # type: ignore[import-not-found] # noqa: E402
from fixtureExposures import LATISS_EXPOSURES, LSSTCAM_EXPOSURES  # noqa: E402

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

# see fixtureExposures.py for what each exposure is for
ids = ",".join(str(exposure.id) for exposure in LSSTCAM_EXPOSURES)
where = f"exposure in ({ids}) AND instrument='{instrument}'"
records = list(butler.registry.queryDimensionRecords("exposure", where=where))
nExpected = len(LSSTCAM_EXPOSURES)
assert len(records) == nExpected, f"Expected {nExpected} records, got {len(records)}"
recordDict = {r.id: r for r in records}  # so we can dispatch in a specific order
for exposure in LSSTCAM_EXPOSURES:  # so the log shows what each exposure is
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

# the dispatch order matters - see the comments on LSSTCAM_DISPATCH_ORDER in
# ci_dataset.py before changing anything about this loop
for exposure in LSSTCAM_DISPATCH_ORDER:
    record = recordDict[exposure.id]
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

# do LATISS with the same drip-feeder
instrument = "LATISS"
locationConfig = getAutomaticLocationConfig()
butler = Butler.from_config(
    locationConfig.auxtelButlerPath,
    collections=[
        f"{instrument}/defaults",
    ],
)

# LATISS_EXPOSURES is also the dispatch order - see the comments on it in
# fixtureExposures.py before changing anything about this loop. The sleep is
# so the intra-focal image of the CWFS pair is fanned out before the
# extra-focal one lands and completes the pair.
ids = ",".join(str(exposure.id) for exposure in LATISS_EXPOSURES)
where = f"exposure in ({ids}) AND instrument='{instrument}'"
latissRecords = {r.id: r for r in butler.registry.queryDimensionRecords("exposure", where=where)}
nExpected = len(LATISS_EXPOSURES)
assert len(latissRecords) == nExpected, f"Expected {nExpected} LATISS records, got {len(latissRecords)}"
for exposure in LATISS_EXPOSURES:
    record = latissRecords[exposure.id]
    redisHelper.pushNewExposureToHeadNode(record)
    redisHelper.pushToButlerWatcherList(instrument, record)
    time.sleep(2)
