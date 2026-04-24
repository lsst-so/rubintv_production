# Unit-test coverage report — `rubintv_production`

The package is ~20k lines across ~30 modules but only has ~8 unit-test files,
almost all of them very thin. The integration suite covers the actual pipeline
runs, so we should focus unit tests on **pure logic, dataclasses, parsers,
formatters, key/queue construction, validators, and small state machines** —
and refactor a few hot spots to make more of that surface reachable without
contrived mocking.

## Easy wins — no refactoring, little or no mocking

These are direct analogues of the existing `test_podDefinition.py` /
`test_payloads.py` style.

| Module | What to test |
|---|---|
| `timing.py` | `BoxCarTimer` — pure state machine: init, lap, pause/resume, buffer wraparound, empty/edge cases. Already has `test_timing.py` — verify it covers all the transitions. |
| `payloads.py` | Beyond the existing dataclass tests: `pipelineGraphToBytes`/`pipelineGraphFromBytes` round-trip, `isRestartPayload` predicate, dataId-based detector extraction. |
| `podDefinition.py` | Already well covered — only gap is the `podFlavorToPodType` lookup table for *every* enum value (catches future additions that forget to register). |
| `utils.py` | `isDayObsContiguous`, `hasDayRolledOver`, `sanitizeNans`, `raiseIf`, `isCalibration`, `isWepImage`, `getRubinTvInstrumentName`, `getFilterColorName`, `runningCI`/`runningPyTest`/`runningScons`, `mapAosWorkerNumber`. All pure or env-only. |
| `channels.py` | `getCameraAndPlotName` — pure lookup; cover every channel key plus error path on unknown. |
| `consdbUtils.py` | The `*_MAPPING` dicts — schema-shape assertions that all required `ExposureSummaryStats` columns are present (catches drift). |
| `processingControl.py` | `WorkerProcessingMode` / `VisitProcessingMode` enum coverage; expand `CameraControlConfig` tests around currently-untested rafts/sensors and `__post_init__` validation. |
| `aosUtils.py` | `parseDofStr` (string→struct), `makeDataframeFromZernikes` (table→dataframe shape), small numpy transforms — feed synthetic arrays. |
| `clusterManagement.py` | `QueueItem`, `WorkerStatus`, `FlavorStatus` dataclasses — construction + aggregation properties. |
| `workerSets.py` | `WorkerSet.__post_init__` validation; `allFree`, `allBusy`, `maxQueueLength` against synthetic statuses. |

That alone is probably 50–80 new tests with no infrastructure beyond what the
existing tests use.

## Medium — modest mocking (`fakeredis`, stub Butler, tmpdir)

| Module | What to test |
|---|---|
| `redisUtils.py` | `ExposureProcessingInfo` round-trip (`fromRedisHash` ↔ field accessors), all the set-arithmetic helpers (`getMissingDetectors`, `isComplete`, etc.). With [fakeredis](https://pypi.org/project/fakeredis/) most of `RedisHelper`'s queue operations become testable. |
| `uploaders.py` | `Bucket`/`EndPoint` enums, `createLocalS3UploaderForSite` — pure factory selection; one test per site branch. |
| `utils.py` | `LocationConfig` parsing/validation against a fixture YAML; `writeMetadataShard` with `tmp_path`; `getShardPath` (pure). |
| `resources.py` | `getBasePath` site-branch selection with monkeypatched env. |
| `exposureLogUtils.py` | URL/query construction — already tested; double-check `getLogsForDayObs` request building with a `requests` mock. |
| `timedServices.py` | `deep_update` recursive dict merge (pure). |
| `clusterManagement.py` | `ClusterManager` query methods against fakeredis-backed cluster state. |

## Refactoring opportunities (worth doing before adding tests)

These are the highest leverage. None require behaviour changes — just
extracting pure logic out of I/O wrappers.

1. **Extract Redis key construction from `redisUtils.py`**
   Key strings like `f"{instrument}-{who}-{queue}"` are scattered through
   `RedisHelper.enqueuePayload`, `dequeuePayload`, and friends. Lifting them to
   module-level pure helpers (`_makeQueueKey`, `_makeBusyKey`, `_parseBusyKey`,
   …) makes them trivially unit-testable and protects the format from
   accidental drift. Probably the single biggest win.

2. **Split `utils.py` by responsibility**
   At 1,889 lines it is doing far too much. Suggest grouping into:
   - predicates (`isDayObsContiguous`, `isCalibration`, `isWepImage`, …)
   - formatters (`getFilterColorName`, `getRubinTvInstrumentName`, title/path builders)
   - shard I/O (`writeMetadataShard`, `getShardedData`, …)
   - parsers (`expRecordFromJson`, dimension-universe helpers)

   The refactor is mechanical and each piece becomes obviously testable.

3. **Pull `CameraControlConfig` validation out of `processingControl.py`**
   `__post_init__` has a stack of validation rules that would each be a
   one-line unit test if they were standalone functions. Same goes for any
   pure pipeline-graph analysis sitting beside the Butler-bound dispatch code.

4. **Extract performance computation from plotting in `performance.py`**
   2,156 lines mixing data fetch, aggregation, and matplotlib. A
   `PerformanceComputation` class taking already-fetched dicts/dataframes and
   producing aggregated structures would be unit-testable; the existing
   `PerformanceBrowser` keeps the plotting.

5. **Separate quantum-graph construction from execution in `pipelineRunning.py`**
   The trivial-vs-all-dimensions strategy selection and small graph
   manipulations are pure-ish but currently entangled with the runner. A
   factory module would let you test the strategy logic without a Butler.

## Skip — leave to integration tests

`aos.py`, `allSky.py`, `guiders.py`, `starTracker.py`, `mountTorques.py`,
`oneOffProcessing.py`, `cleanup.py`, most of `plotting/`, and the long-running
orchestrators in `baseChannels.py` / `catchupService.py` / `rubinTv.py`. These
are either thin wrappers around DM Stack code or end-to-end loops where mocks
would teach us nothing the CI doesn't.

---

## Suggested order of attack

1. **Bag the easy wins first** — `timing`, `utils` predicates, `channels`,
   `consdbUtils` mappings, `aosUtils` parsers, `clusterManagement` dataclasses,
   expanded `CameraControlConfig`. This is one or two evenings and roughly
   doubles unit-test count with zero refactoring.
2. **Add `fakeredis` to the test deps** and write `ExposureProcessingInfo` +
   `RedisHelper` queue tests against it. (Even before any refactor — the
   current `RedisHelper` does work against a real `redis.Redis`.)
3. **Do the redisUtils key-extraction refactor** — small, low-risk, unblocks a
   lot.
4. **Split `utils.py`** — also mostly mechanical.
5. **Extract `CameraControlConfig` validators**, then add the validator tests.
6. Decide whether the `performance.py` and `pipelineRunning.py` splits are
   worth it based on how much that code is changing.
