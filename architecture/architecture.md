# Architecture & Data Flow

## System Overview

RubinTV Production is a distributed real-time data processing system. Raw
telescope exposures arrive in a Butler repository; the system detects them,
fans work out to parallel workers via Redis queues, runs LSST Science Pipeline
tasks, and publishes results (binned images, metadata JSON, plots) to S3 for
the RubinTV web frontend.

```
Telescope
  |
  v
Butler Repository  <--- ButlerWatcher polls every 1 s
  |
  v
Redis queue: INCOMING-{instrument}-raw
  |
  v
HeadProcessController (head node, 5 Hz loop)
  |--- fans out Payloads to per-detector worker queues
  |--- tracks expected detectors per exposure
  |--- dispatches gather (step1b) when all detectors finish step1a
  |--- dispatches one-off processors (expRecord, postISR, visitImage, guiders)
  |
  v
Worker pods (SingleCorePipelineRunner) via RedisWatcher
  |--- execute pipeline quantum graphs
  |--- write metadata shards + binned images
  |--- populate ConsDB
  |--- report task completion back to Redis
  |
  v
TimedMetadataServer (1.5 s cadence)
  |--- merges JSON shards
  |--- uploads to S3
  |
  v
S3 Buckets ---> RubinTV Web Frontend
```

## Pod Types and Flavors

Each Kubernetes pod runs one Python script from `scripts/{instrument}/`.
Pods are identified by a `PodDetails` object containing instrument, flavor,
type, optional depth, and optional detector number.

### Pod Types (`PodType` enum)

| Type | Has Depth | Has Detector | Example |
|------|-----------|-------------|---------|
| `PER_DETECTOR` | Yes | Yes | SFM_WORKER, AOS_WORKER |
| `PER_INSTRUMENT` | Yes | No | STEP1B_WORKER, MOSAIC_WORKER |
| `PER_INSTRUMENT_SINGLETON` | No | No | HEAD_NODE, PERFORMANCE_MONITOR |

### Pod Flavors (`PodFlavor` enum)

**Head node:** `HEAD_NODE`

**Per-detector workers (step1a):**
- `SFM_WORKER` - Source Finding & Measurement (one per detector)
- `AOS_WORKER` - Adaptive Optics (corner wavefront sensors only)
- `BACKLOG_WORKER` - processes backlog exposures

**Per-instrument workers (step1b / aggregation):**
- `STEP1B_WORKER` - visit-level SFM gather
- `STEP1B_AOS_WORKER` - visit-level AOS gather
- `NIGHTLYROLLUP_WORKER` - nightly aggregation
- `MOSAIC_WORKER` - focal plane mosaics
- `GUIDER_WORKER` - guider camera analysis

**One-off / plotting:**
- `ONE_OFF_EXPRECORD_WORKER` - per-exposure metadata
- `ONE_OFF_POSTISR_WORKER` - post-ISR mosaics
- `ONE_OFF_VISITIMAGE_WORKER` - visit image mosaics
- `PSF_PLOTTER` - PSF shape plots
- `PERFORMANCE_MONITOR` - timing metrics

### Queue Naming

Redis queue names follow the pattern:

```
{PodFlavor}-{instrument}[-{depth}[-{detector}]]
```

Examples:
- `HEAD_NODE-LSSTCam`
- `SFM_WORKER-LSSTCam-001-094`   (depth 001, detector 94)
- `STEP1B_WORKER-LSSTCam-001`    (depth 001, no detector)
- `PERFORMANCE_MONITOR-LSSTCam`  (no depth, no detector)

## Pipeline Stages

Processing happens in two main phases:

### Step1a (per-detector, parallel)

Each detector is processed independently on its own worker pod.

**ISR (Instrument Signature Removal):**
- Bias/dark/flat correction
- Produces `post_isr_image`
- Always runs first in any pipeline

**SFM (Source Finding & Measurement):**
- Source detection, astrometry, photometry
- Produces `preliminary_visit_image` at end of step1a
- Runs on all science imaging detectors (up to 189 for LSSTCam)

**AOS (Adaptive Optics System):**
- Donut detection and Zernike wavefront estimation
- Runs only on 8 corner wavefront sensors (detectors 191-204)
- Handles paired (intra/extra focal) and FAM (full array mode) observations
- Special quantum graph builder handles donut pair merging

### Step1b (per-visit, sequential)

Triggered by the head node when all expected detectors finish step1a.

**SFM Step1b:**
- `ConsolidateVisitSummaryTask` - aggregates per-detector stats
- Produces `preliminary_visit_summary`
- Computes PSF FWHM, ellipticity, sky background, astrometry metrics

**AOS Step1b:**
- `AggregateZernikeTables` - combines Zernike measurements
- Applies OFC Y2 correction per detector
- Computes residual AOS FWHM prediction

### Pipeline Selection Logic

The head node's `getPipelineConfig()` routes exposures:

| Observation Type | Pipeline | Workers |
|-----------------|----------|---------|
| BIAS, DARK, FLAT | ISR-only | SFM_WORKER |
| CWFS (FAM) | AOS FAM | AOS_WORKER |
| Science images | SFM | SFM_WORKER + AOS_WORKER (corner chips) |

## Head Node Event Loop

`HeadProcessController.run()` at 5 Hz:

1. **Check control messages** - `updateConfigsFromRubinTV()` reads Redis for
   pipeline switches, focal plane config changes, reset signals
2. **Get new exposure** - `getNewExposureAndDefineVisit()` pops from incoming
   queue, calls `defineVisit()` to register in Butler
3. **Dispatch one-off** - send expRecord to `ONE_OFF_EXPRECORD_WORKER`
4. **Write metadata shard** - ISR config info for the frontend
5. **Detector fanout** - `doDetectorFanout()` creates a Payload per enabled
   detector and enqueues to the matching `SFM_WORKER` queue
6. **AOS fanout** - `doAosFanout()` for LSSTCam corner chips (8 detectors)
7. **Guider dispatch** - for on-sky LSSTCam observations
8. **Check gather readiness** - `dispatchGatherSteps()` for SFM, AOS, ISR:
   compares finished detector count vs expected; dispatches step1b when ready
9. **PostISR mosaic** - dispatch when ISR complete across detectors
10. **Nightly rollup** - currently disabled
11. **Repattern** - apply focal plane detector pattern if configured

## Worker Event Loop

`SingleCorePipelineRunner` extends `BaseButlerChannel` with a `RedisWatcher`:

1. **Announce free** - sets Redis existence key, clears busy flag
2. **Blocking dequeue** - `blpop()` with 5 s timeout on assigned queue
3. **Check restart signal** - exit gracefully if RestartPayload received
4. **Announce busy** - sets busy flag with 15 min safety expiry
5. **Deserialize payload** - reconstruct PipelineGraph from base64 bytes
6. **Wait for raw data** - poll Butler until raw exposure is available
7. **Build quantum graph** - `TrivialQuantumGraphBuilder` (step1a) or
   `AllDimensionsQuantumGraphBuilder` (step1b)
8. **Execute quanta** - iterate through quantum graph nodes:
   - Run quantum via `SingleQuantumExecutor`
   - Post-process: write binned images, metadata shards, ConsDB rows
   - Report task finished to Redis
9. **Report completion** - detector-level and visit-level finish signals
10. **Loop** - back to step 1

## Payload Serialization

Payloads are frozen dataclasses serialized to JSON for Redis transport:

```json
{
  "dataId": {"detector": 94, "exposure": 2025040800123, ...},
  "pipelineGraphBytes": "<base64-encoded serialized PipelineGraph>",
  "run": "LSSTCam/runs/rapidAnalysis/2026-03-15T12:00:00Z",
  "who": "SFM",
  "specialMessage": ""
}
```

Workers deserialize with `Payload.from_json(json_str, butler)` which expands
the minimal dataId back to a full `DataCoordinate` via the Butler registry.

## Metadata Shards

Workers write small JSON files ("shards") to per-instrument shard directories.
The `TimedMetadataServer` (running as its own pod) merges these every 1.5 s
into a per-dayObs sidecar file and uploads to S3.

Shard path: `{shardsDirectory}/{dayObs}/{seqNum}/{taskName}.json`

The merge uses `deep_update()` to recursively combine nested dicts. NaN values
are sanitized to null. Numeric strings are converted to numbers.

## Upload Architecture

`MultiUploader` wraps two `S3Uploader` instances:
- **Local**: immediate upload (blocks until complete)
- **Remote**: background thread upload (non-blocking)

At the summit, the local bucket is on-site S3 and the remote is USDF (via
squid proxy). This ensures the frontend gets data immediately from local S3
while remote backup happens asynchronously.

## Configuration

`LocationConfig` (in `utils.py`) is the central configuration object. It reads
a YAML config file selected by the `RAPID_ANALYSIS_LOCATION` environment
variable and provides ~100 cached properties for all paths:

- Butler repository paths per instrument
- Metadata and shard directories
- Pipeline file locations
- Output collection chain names
- ConsDB connection URL
- S3 bucket configuration

Config files: `config/config_summit.yaml`, `config/config_usdf.yaml`, etc.

## The Focal Plane and Detector Distribution

### LSSTCam Focal Plane Layout

LSSTCam has **205 detectors** arranged in a 5x5 grid of "rafts", with each
raft containing up to 9 sensors. They break down into three physical types:

- **189 imaging detectors** (science CCDs) - mix of E2V (117) and ITL (72)
- **8 wavefront sensors** (corner chips, type `ITL_WF`) - used for AOS
- **8 guider sensors** (type `ITL_G`)

The wavefront sensors sit in the four corners of the focal plane as
intra/extra focal pairs:
- Bottom-left: 191 (extra), 192 (intra)
- Bottom-right: 195 (extra), 196 (intra)
- Top-left: 199 (extra), 200 (intra)
- Top-right: 203 (extra), 204 (intra)

LATISS (AuxTel) has a single detector, so none of the focal plane fanout
logic applies to it.

### CameraControlConfig

`CameraControlConfig` manages which detectors are active. It maintains a
`_detectorStates` dict mapping every detector ID to a bool. The head node
calls `getEnabledDetIds(excludeCwfs=True)` to get the list of imaging
detectors to fan out to (CWFS detectors are always dispatched separately via
`doAosFanout()`).

Named patterns can be applied via `applyNamedPattern()`:
- `all` - all 189 imaging detectors
- `raft_checkerboard` - alternating rafts (108 or 81 detectors)
- `ccd_checkerboard` - alternating CCDs (96 or 93 detectors)
- `5-on-a-die` - 5 rafts in a die pattern (R11, R13, R22, R31, R33)
- `minimal` - diagonal + cardinal cross patterns
- `ultra-minimal` - just the two diagonals

The pattern can be changed at runtime via the RubinTV control interface.
`VisitProcessingMode` can also alternate the pattern between visits:
- `CONSTANT` - same pattern every visit
- `ALTERNATING` - invert the detector selection each visit
- `ALTERNATING_BY_TWOS` - invert every 2 visits

### Detector Fanout

When a new exposure arrives, `doDetectorFanout()` in the head node:

1. **AOS first** (non-FAM images only): calls `doAosFanout()` which always
   sends all 8 CWFS detectors to `AOS_WORKER` pods. Writes expected
   detectors into the tracking hash for both "AOS" and "ISR" who-tags.

2. **Imaging detectors**: gets the enabled detector list from
   `CameraControlConfig`, creates a `Payload` per detector, and writes
   expected detectors into the tracking hash for both "ISR" and the
   pipeline's who-tag (e.g. "SFM"). Dispatches all payloads to
   `SFM_WORKER` pods.

The dispatch in `_dispatchPayloads()` uses **detector affinity**: each
`PER_DETECTOR` worker pod is permanently assigned to one detector (the
detector number is part of the queue name). The head node matches each
payload to the worker for that specific detector:

```
Payload for det=94  -->  SFM_WORKER-LSSTCam-001-094
Payload for det=10  -->  SFM_WORKER-LSSTCam-001-010
```

If a worker is busy (still processing the previous image), the payload is
queued anyway - it will be processed when the worker finishes. If no worker
exists at all for a detector (cluster misconfiguration), the payload fails
and that detector is removed from the expected list so it doesn't block
the gather step.

On startup, if workers haven't registered yet, the head node waits 30 s and
retries once (only within the first 60 s of head node lifetime).

### The Gather Mechanism (step1a -> step1b)

This is the core coordination pattern. Each worker processes one detector
independently. The system needs to know when *all* detectors for an exposure
have finished so it can trigger the visit-level step1b.

**Setup (head node, at fanout time):**
```
initExposureTracking("LSSTCam", expId=2025040800123)
setExpectedDetectors("LSSTCam", expId=2025040800123, [10,12,...,188], "SFM")
```
This creates a per-exposure tracking hash in Redis and writes the
expected detector IDs as a comma-separated field.

**Reporting (each worker, after finishing step1a):**
```
reportDetectorFinished("LSSTCam", expId=2025040800123, who="SFM", detector=94)
```
This does `HSET` on the tracking hash, writing a single field
`SFM:finished:94`. Failed tasks also call this with `failed=True`
(writing both a `finished` and `failed` field for that detector).

**Checking (head node, every loop iteration at 5 Hz):**
`dispatchGatherSteps("SFM")` runs each iteration:
1. `getActiveExposures()` - `SMEMBERS` on the active exposures set
   to get all exposure IDs currently being tracked
2. For each ID, `getExposureProcessingInfo()` - `HGETALL` on the
   tracking hash, parsed into an `ExposureProcessingInfo` object
3. Compare `finishedDetectors >= expectedDetectors` (set comparison)

**Triggering step1b:**
When an exposure is complete:
1. Create a visit-level `Payload` with the step1b pipeline graph
2. Enqueue to `STEP1B_WORKER` (SFM) or `STEP1B_AOS_WORKER` (AOS)
3. Mark dispatched: `markStep1aDispatched()` + `markStep1bDispatched()`
4. If all whos are dispatched: `completeExposure()` (SREM from active set)
5. Dispatch downstream one-off workers (postISR mosaic, visit image, etc.)
6. For SFM: also dispatch `ONE_OFF_POSTISR_WORKER`, `ONE_OFF_VISITIMAGE_WORKER`,
   `MOSAIC_WORKER`, and radial plotter

**The same pattern runs independently for three who-tags**: "SFM", "AOS",
and "ISR". All share the same per-exposure tracking hash. The head node
calls `dispatchGatherSteps()` for each one every loop iteration.

### Handling Failures and Edge Cases

- **Dispatch failures**: if no worker exists for a detector, that detector
  is removed from the expected set via `removeExpectedDetectors()`. This
  prevents the gather step from waiting forever.

- **Worker failures**: if a worker crashes mid-processing, it still calls
  `reportDetectorFinished(failed=True)`. The detector is marked as both
  finished and failed in the tracking hash, so the gather step still
  triggers. The failed set tracks which detectors failed for diagnostics.

- **Tracking hash TTL**: the 2.5-day TTL on the per-exposure tracking hash
  means that if something goes completely wrong and workers never report
  back, the hash eventually expires. Because expected and finished
  detectors share the same hash and TTL, they always expire together —
  there is no state where finished data persists after expected data
  disappears. The head node detects expired hashes and removes stale
  entries from the active exposures set.

- **FAM pair safety**: `isBetweenFamPair()` prevents the RubinTV control
  interface from switching AOS pipelines between the intra and extra focal
  images of a CWFS pair. Control commands received between a pair are
  rejected with `"REJECTED_BETWEEN_PAIR!"` read-back.

### Post-step1b Cascade

After step1b completes on a worker, the worker itself triggers further
downstream processing via Redis:
- SFM step1b completion picks one free pod of each plotter flavor
  (`PSF_PLOTTER`, `FWHM_PLOTTER`, `ZERNIKE_PREDICTED_FWHM_PLOTTER`) via
  `RedisHelper.getSingleWorker()` and enqueues a visit-level `Payload`
  on that pod's queue
- AOS step1b completion reports the Zernike count to MTAOS

### PostISR Mosaic Dispatch

The `dispatchPostIsrMosaic()` method uses the same expected-vs-finished
pattern but with the `binnedIsrCreation` task counter (a legacy/task-level
counter rather than the detector-level counter). When all detectors have
written their binned ISR images, it dispatches the `MOSAIC_WORKER` to
assemble them into a full focal plane mosaic.

## External Dependencies

- **Butler**: LSST data access framework (read/write datasets)
- **Redis**: Work distribution and coordination (see [redis-coordination.md](redis-coordination.md))
- **S3**: Object storage for frontend consumption
- **ConsDB**: Consolidated database for engineering metrics
- **EFD**: Engineering Facilities Database (telescope telemetry)
- **Sentry**: Error tracking and monitoring
- **Google Cloud Storage**: Legacy upload path (being replaced by S3)
