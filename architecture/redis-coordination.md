# Redis Coordination

Redis is the central nervous system for distributed work coordination in
the rapid analysis backend. All inter-pod communication flows through Redis
- there is no direct pod-to-pod communication.

## Connection

Configured via environment variables:
- `REDIS_HOST` (required)
- `REDIS_PORT` (default: 6379)
- `REDIS_PASSWORD` (required)

All Redis operations go through `RedisHelper` (in `redisUtils.py`), which
wraps a `redis.Redis` client and provides domain-specific methods.

## Key Categories

### 1. Work Queues (LIST)

**Incoming exposure queue:**
```
INCOMING-{instrument}-raw
```
- Direction: LIFO (`lpush` by ButlerWatcher, `lpop` by head node)
- Content: JSON-serialized `ExposureRecord` (Butler dimension record)
- Purpose: New raw exposures waiting for the head node to process

**Per-worker payload queues:**
```
{PodFlavor}-{instrument}-{depth}-{detector}   (PER_DETECTOR)
{PodFlavor}-{instrument}-{depth}              (PER_INSTRUMENT)
{PodFlavor}-{instrument}                      (SINGLETON)
```
- Direction: `lpush` by head node, `blpop` by worker (blocking, 5 s timeout)
- Content: JSON-serialized `Payload` (dataId + base64 pipeline graph bytes)
- The head node picks a free worker's queue for each payload

**Butler watcher history:**
```
{instrument}-fromButlerWacher
```
- Type: LIST (append-only)
- Content: JSON-serialized ExposureRecords that have been seen
- Purpose: Prevent reprocessing on restart (note: typo in key is intentional)

### 2. Queue Length Tracking (HASH)

```
_QUEUE-LENGTHS
```
- Fields: `{queue_name}` -> integer count
- Updated on every `enqueuePayload()` (increment) and `dequeuePayload()` (decrement)
- Reset to 0 when queue empties to prevent drift
- Used by head node to check backlog depth

### 3. Pod Health (STRING with TTL)

**Existence heartbeat:**
```
{queue_name}+EXISTS
```
- Value: `1`
- TTL: **30 seconds** (`POD_EXISTENCE_TIMEOUT`)
- Reasserted on every successful work cycle via `announceExistence()`
- If key expires, pod is considered dead
- Removed explicitly via `announceExistence(remove=True)` on shutdown

**Busy flag:**
```
{queue_name}+IS_BUSY
```
- Value: `1`
- TTL: **900 seconds** (15 min, `BUSY_EXPIRY`) - safety net for hung workers
- Set by `announceBusy()` when worker starts processing
- Deleted by `announceFree()` when worker finishes
- Head node checks this to avoid sending work to busy pods

**Running affirmation:**
```
{queue_name}+IS_RUNNING
```
- Value: `1`
- TTL: dynamic, set via `affirmRunning(timePeriod)`
- General-purpose liveness signal

**Secondary status:**
```
{queue_name}+SECONDARY_STATUS
```
- Value: string status ("RESTARTING", "GUEST_PAYLOAD", etc.)
- No TTL
- Read/written via `getPodSecondaryStatus()` / `setPodSecondaryStatus()`

### 4. Unified Exposure Tracking (HASH with TTL + SET)

All per-exposure tracking state lives in a single Redis hash per exposure,
with a unified TTL. This prevents lifecycle mismatches where some keys
expire while others persist.

**Per-exposure tracking hash:**
```
{instrument}-TRACKING-{expId}
```
- TTL: **2.5 days** (216,000 seconds), set on creation
- Contains all detector-level tracking, dispatch flags, and pipeline config
- Field naming convention:
  - `_initialized` -> "1" (sentinel, forces hash creation)
  - `{who}:expected` -> comma-separated detector IDs ("10,12,...,188")
  - `{who}:finished:{det}` -> "1" (one field per finished detector)
  - `{who}:failed:{det}` -> "1" (one field per failed detector)
  - `{who}:step1aDispatched` -> "1" (gather triggered for this who)
  - `{who}:step1bDispatched` -> "1" (step1b sent to worker)
  - `{who}:step1bFinished` -> "1" (step1b worker completed)
  - `pipeline_config` -> AOS pipeline name ("AOS_DANISH", etc.)
- Workers write `{who}:finished:{det}` via atomic `HSET` (no races)
- Head node writes expected detectors and dispatch flags
- Completion check: `finished_detectors >= expected_detectors` (set ops)
- Hash is NOT deleted on completion — only removed from active set.
  TTL handles cleanup so async consumers (mosaic plotter) can still read it.

**Active exposures set:**
```
{instrument}-ACTIVE-EXPOSURES
```
- Type: SET (no TTL, self-heals via stale entry cleanup)
- Members: exposure IDs currently being tracked
- `SADD` by head node during fanout
- `SREM` by head node when all gathers for an exposure are dispatched
- Head node iterates this in `dispatchGatherSteps()` to find work
- If a member's tracking hash has expired, the head node removes the
  stale entry automatically

**Visit-level finished counter (STRING):**
```
{instrument}-{step}-{who}-VISIT_FINISIHED_COUNTER
{instrument}-{step}-{who}-VISIT_FAILED_COUNTER
```
- Integer counter (note: typo "FINISIHED" is intentional in the code)
- Incremented when step1b completes for a visit (global counter)

**Night-level rollup counter (STRING):**
```
{instrument}-{who}-NIGHTLYROLLUP-FINISHEDCOUNTER
```
- Integer counter for nightly aggregation completion

**Per-task counter (HASH):**
```
{instrument}-{taskName}-FINISHEDCOUNTER
{instrument}-{taskName}-FAILEDCOUNTER
```
- Fields: `{dataId_json}` -> count
- Used for per-task tracking within a pipeline (e.g. `binnedIsrCreation`)
- Separate from the tracking hash — different abstraction layer

### 7. Visit Summary Stats (HASH with TTL)

```
{instrument}-VISIT_SUMMARY_STATS-{visit}
```
- Fields: `{detector}` -> JSON-serialized summary statistics dict
- TTL: **1.5 days** (129,600 seconds)
- Written by SFM workers after calibration
- Read by step1b to compute visit-level aggregates (median FWHM, etc.)

### 8. WEP Processing Results (HASH)

```
{INSTRUMENT_UPPER}_WEP_PROCESSING_RESULT
```
- Fields: `{visitId}` -> zernike count
- Reports wavefront processing completion to MTAOS system

### 9. ConsDB Announcements (HASH with TTL)

```
consdb-announcements-{dayObs}
```
- dayObs derived as `obsId // 100_000`
- Fields: `{instrument}-{table}-{obsId}` -> `1`
- TTL: **2 days** (172,800 seconds)
- Written when a ConsDB insert completes
- Other pods can poll/wait for results via `waitForResultInConsdDb()`
- Enables cross-pod coordination without direct communication

### 10. RubinTV Control Interface

**Control commands (STRING, consumed via `getdel()`):**
```
RUBINTV_CONTROL_RESET_HEAD_NODE     -> trigger head node restart
RUBINTV_CONTROL_AOS_PIPELINE        -> set AOS pipeline config
RUBINTV_CONTROL_AOS_FAM_PIPELINE    -> set AOS FAM pipeline
RUBINTV_CONTROL_VISIT_PROCESSING_MODE -> set visit mode (not yet implemented)
RUBINTV_CONTROL_CHIP_SELECTION      -> set focal plane pattern
RUBINTV_CONTROL_WITNESS_DETECTOR    -> set reference detector for AOS
```

**Readback keys (STRING):**
```
{control_key}_READBACK
```
- Set to command value on success
- Set to `"REJECTED_BETWEEN_PAIR!"` if rejected (e.g., mid-FAM-pair)
- RubinTV frontend polls these to confirm commands were processed

### 11. Head Node State

**Ignored detectors:**
```
{instrument}-HEADNODE-IGNORED_DETECTORS
```
- Value: CSV list of detector numbers not being processed
- Published by head node for frontend display

**Donut pair announcements (from OCS):**
```
{instrument}-FROM-OCS_DONUTPAIR
```
- Value: comma-separated exposure IDs (e.g., "2025111500227,2025111500228")
- Written by OCS to announce intra/extra focal pairs

## Work Distribution Flow

### Dispatch (Head Node -> Workers)

When a new exposure arrives, the head node fans it out to workers. For
LSSTCam, this involves up to 189 imaging detectors (SFM) plus 8 corner
wavefront sensors (AOS), each dispatched to a dedicated worker pod.

**AOS fanout** (`doAosFanout`, always 8 CWFS detectors):
```
1. Initialize tracking (idempotent):
     SADD LSSTCam-ACTIVE-EXPOSURES {expId}
     HSET LSSTCam-TRACKING-{expId} _initialized 1
     EXPIRE LSSTCam-TRACKING-{expId} 216000
2. Write expected detectors for AOS and ISR:
     HSET LSSTCam-TRACKING-{expId} AOS:expected "191,192,195,196,199,200,203,204"
     HGET + merge + HSET LSSTCam-TRACKING-{expId} ISR:expected (append CWFS detectors)
3. Record which AOS pipeline is active:
     HSET LSSTCam-TRACKING-{expId} pipeline_config "AOS_DANISH"
4. For each CWFS detector:
     LPUSH AOS_WORKER-LSSTCam-001-{det} <payload JSON>
```

**SFM fanout** (`doDetectorFanout`, enabled imaging detectors):
```
1. Initialize tracking (idempotent, may already exist from doAosFanout):
     SADD LSSTCam-ACTIVE-EXPOSURES {expId}
     HSET LSSTCam-TRACKING-{expId} _initialized 1
     EXPIRE LSSTCam-TRACKING-{expId} 216000
2. Get enabled detectors from CameraControlConfig (up to 189)
3. Write expected detectors for ISR (append) and SFM:
     HGET + merge + HSET LSSTCam-TRACKING-{expId} ISR:expected (append imaging dets)
     HSET LSSTCam-TRACKING-{expId} SFM:expected "10,12,...,188"
4. For each detector, match to its dedicated worker via detector affinity:
     LPUSH SFM_WORKER-LSSTCam-001-{det} <payload JSON>
     HINCRBY _QUEUE-LENGTHS SFM_WORKER-LSSTCam-001-{det} 1
```

**Detector affinity**: each PER_DETECTOR worker handles exactly one detector.
The head node finds the right worker by matching `detectorNumber` in the
`PodDetails`. It prefers a free worker, falls back to a busy one (the
payload queues), and errors if no worker exists for that detector at all.
On dispatch failure, the detector is removed from expected-detectors so it
won't block the gather step.

### Execution (Worker)

```
1. Worker calls announceFree():
     SET {queue}+EXISTS 1 EX 30
     DEL {queue}+IS_BUSY
2. BLPOP on queue (5 s timeout)
3. On payload received, calls announceBusy():
     SET {queue}+IS_BUSY 1 EX 900
4. Deserialize Payload, execute pipeline quanta
5. For each completed quantum (e.g. ISR, then calibrateImage):
     HINCRBY {instrument}-{taskLabel}-FINISHEDCOUNTER {dataIdNoDetector} 1
6. After all quanta done, report detector-level completion:
     HSET {instrument}-TRACKING-{expId} {who}:finished:{det} 1
7. Back to step 1
```

Note: step 5 uses the per-task counter (for things like `binnedIsrCreation`
which triggers post-ISR mosaic assembly). Step 6 writes to the tracking
hash, marking this specific detector as finished for the given pipeline.

### Gather Trigger (Head Node)

The head node calls `dispatchGatherSteps()` **three times per loop iteration**
at 5 Hz - once each for "SFM", "AOS", and "ISR". Each operates independently
on the same per-exposure tracking hash.

```
1. SMEMBERS {instrument}-ACTIVE-EXPOSURES
   -> returns set of expIds currently being tracked

2. For each expId:
   a. HGETALL {instrument}-TRACKING-{expId}
      -> parse into ExposureProcessingInfo (expected/finished/failed sets,
         dispatch flags, pipeline config)
      -> if hash expired (None), SREM from active set and skip
   b. Check: has expected detectors for this who? Already dispatched?
   c. Compare: finished_detectors >= expected_detectors (set comparison)
      -> If True: this exposure is COMPLETE for this pipeline

3. For each complete exposure:
   a. Create step1b Payload with visit-level dataId (no detector dimension)
   b. For AOS: read pipeline config from tracking hash:
      HGET {instrument}-TRACKING-{expId} pipeline_config
      -> e.g. "AOS_DANISH", determines which step1b graph to use
   c. Enqueue to STEP1B_WORKER or STEP1B_AOS_WORKER
   d. Mark dispatched:
      HSET {instrument}-TRACKING-{expId} {who}:step1aDispatched 1
      HSET {instrument}-TRACKING-{expId} {who}:step1bDispatched 1
   e. If all whos with expected detectors are dispatched:
      SREM {instrument}-ACTIVE-EXPOSURES {expId}
   f. Dispatch downstream:
      - ONE_OFF_POSTISR_WORKER (always for SFM/ISR/AOS)
      - ONE_OFF_VISITIMAGE_WORKER (SFM, non-LATISS)
      - MOSAIC_WORKER for visit_image mosaic (SFM, non-LATISS)
      - RADIAL_PLOTTER (SFM, non-LATISS)
```

**Key subtlety**: the ISR expected detectors in the tracking hash are
written with `append=True` by both `doAosFanout` (CWFS detectors) and
`doDetectorFanout` (imaging detectors), because ISR runs as the first
step of every pipeline. The SFM expected field covers only imaging
detectors, the AOS expected field covers only CWFS detectors.

### Post-step1b Worker-Initiated Dispatch

After the step1b worker finishes, it pushes directly to downstream pod
queues (no head node involvement). Each plotter dispatch picks one
free pod of the chosen flavor via `RedisHelper.getSingleWorker()` and
enqueues a minimal `Payload` carrying the visit-level dataId on that
pod's queue:

```
SFM step1b completion:
  HSET {instrument}-TRACKING-{expId} SFM:step1bFinished 1
  enqueuePayload(visit dataId) -> PSF_PLOTTER pod
  enqueuePayload(visit dataId) -> FWHM_PLOTTER pod
  enqueuePayload(visit dataId) -> ZERNIKE_PREDICTED_FWHM_PLOTTER pod
  INCR {instrument}-step1b-SFM-VISIT_FINISIHED_COUNTER

AOS step1b completion:
  HSET {instrument}-TRACKING-{expId} AOS:step1bFinished 1
  HSET {instrument_upper}_WEP_PROCESSING_RESULT {visitId} {zernikeCount}
  INCR {instrument}-step1b-AOS-VISIT_FINISIHED_COUNTER
```

### PostISR Mosaic (Task-Level Gather)

A second gather pattern uses per-task counters instead of detector-level
counters. This tracks the `binnedIsrCreation` task specifically:

```
1. Each worker, after writing a binned ISR image:
     HINCRBY {instrument}-binnedIsrCreation-FINISHEDCOUNTER {dataIdNoDetector} 1

2. Head node, in dispatchPostIsrMosaic():
     HGETALL {instrument}-binnedIsrCreation-FINISHEDCOUNTER
     For each dataId: compare count vs ISR:expected in TRACKING hash
     If complete: enqueue to MOSAIC_WORKER, then HDEL the counter
```

## Timeout Summary

| Key Pattern | TTL | Purpose |
|------------|-----|---------|
| `+EXISTS` | 30 s | Worker heartbeat |
| `+IS_BUSY` | 900 s (15 min) | Safety net for hung workers |
| `TRACKING-{expId}` | 216,000 s (2.5 days) | Unified per-exposure tracking |
| `ACTIVE-EXPOSURES` | None (self-heals) | Active exposure enumeration |
| `VISIT_SUMMARY_STATS` | 129,600 s (1.5 days) | Per-visit stat retention |
| `consdb-announcements` | 172,800 s (2 days) | Cross-pod result signals |
| `DEQUE_TIMEOUT` (blpop) | 5 s | Worker queue poll interval |

## Design Patterns

1. **Heartbeat + Busy flag**: Two-key health system. `+EXISTS` (30 s TTL,
   continuously reasserted) proves the pod is alive. `+IS_BUSY` (15 min
   safety TTL) prevents double-dispatch. Together they let the head node
   distinguish "free", "busy", and "dead" workers.

2. **Unified tracking hash**: All per-exposure state (expected detectors,
   finished detectors, failed detectors, dispatch flags, pipeline config)
   lives in a single Redis hash with a single TTL. This prevents lifecycle
   mismatches where some keys expire while others persist. Workers report
   per-detector completion via atomic `HSET` (one field per detector).
   The head node checks completion via set comparison
   (`finished >= expected`). The `dispatched` flags prevent re-triggering
   even if the hash hasn't been cleaned up yet.

3. **Active set + TTL self-healing**: The `ACTIVE-EXPOSURES` set tracks
   which exposures need checking. When the tracking hash expires (TTL),
   the head node detects it (`HGETALL` returns empty) and removes the
   stale entry from the active set. The tracking hash is NOT deleted on
   completion — only removed from the active set — so async consumers
   (mosaic plotter) can still read it until TTL expiry.

4. **Atomic control consumption**: RubinTV control commands use `getdel()`
   for atomic read-and-delete, ensuring each command is processed exactly once.
   Readback keys confirm processing to the frontend.

5. **ConsDB announcements**: Instead of polling the database, pods announce
   results in Redis. Other pods that need those results can wait on the
   announcement key. This is much faster than polling ConsDB directly.

6. **Append-only history**: The butler watcher list is append-only and
   checked on startup to prevent reprocessing exposures that were already
   seen in a previous pod lifecycle.

7. **Per-dayObs grouping**: ConsDB announcement keys are grouped by dayObs
   so old announcements naturally expire together.
