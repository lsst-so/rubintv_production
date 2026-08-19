# Testing Guide

## Type Checking (mypy)

`mypy` is the type checker for this repo. Neither pre-commit nor CI runs it
automatically, so it must be run by hand on any Python change before a task
is considered done.

```bash
source ~/stack.sh && . ~/setup_packages.sh && mypy
```

Run from the repo root with no arguments — the `mypy.ini` config sets
`files = scripts/, tests/` and `mypy_path = python`, which together cover the
whole package plus scripts and tests. Passing a specific path (e.g.
`mypy python/...`) will miss errors in the paths you didn't name.

The stack must be sourced (see the `rapid-analysis-lsst-stack` skill) —
without it, mypy cannot resolve sibling `lsst.*` imports and reports a flood
of spurious missing-import errors.

If a new third-party package lands in `lsst.*` that mypy can't find stubs
for, add an `[mypy-<package>]` block with `ignore_missing_imports = True` to
`mypy.ini` rather than suppressing errors at call sites.

## Unit Tests (`tests/`)

Run with pytest. Some tests require a live Butler connection (only available on
`staff-rsp` or `rubin-devl` hosts) and are skipped otherwise.

Run the full suite in parallel via pytest-xdist (one worker per logical CPU);
serial is only worth it when you're running a single targeted file:

```bash
source ~/stack.sh && . ~/setup_packages.sh && pytest tests/ -q -n logical
```

### Test Files

| File | What it tests | Butler needed? |
|------|---------------|----------------|
| `test_utils.py` | Environment/instrument/filter/AOS helpers across `predicates`, `parsers`, `formatters`, and middleware `utils` | No |
| `test_timing.py` | `BoxCarTimer` (lap timing, statistics, pause/resume) — uses a fake clock for determinism | No |
| `test_processingControl.py` | `CameraControlConfig` (focal plane detector patterns) and surrounding head-node helpers | No |
| `test_podDefinition.py` | `PodDetails` construction, queue name round-trips | No |
| `test_payloads.py` | `Payload` construction, equality, JSON round-trip | Partially |
| `test_metadataService.py` | `TimedMetadataServer` shard merging, NaN sanitization | No |
| `test_s3_uploader.py` | `S3Uploader` using moto (mock AWS) | No |
| `test_exposureLogUtils.py` | `getLogsForDayObs` with mocked HTTP responses | No |
| `test_redisKeys.py` | Pure Redis key-construction helpers in `redisKeys.py` | No |
| `test_aosUtils.py` | `parseDofStr` and other AOS helper functions | No |
| `test_consdbUtils.py` | `consdbUtils` mappings, helpers, and the async background-write path | No |
| `test_clusterManagement.py` | Dataclasses in `clusterManagement.py` | No |
| `test_workerSets.py` | `WorkerSet` registry helpers | No |
| `test_pipelines.py` | Full pipeline graph generation and validation | Yes |
| `test_locationConfig.py` | `LocationConfig` accessors and dispatch helpers, against a fixture YAML dict | No |
| `test_resources.py` | `getBasePath` per-site URI / endpoint selection | No |
| `test_shardIo.py` | Shard write / read / delete helpers (uses `tmp_path`) | No |
| `test_timedServices.py` | `deep_update` recursive dict merge | No |
| `test_exposureProcessingInfo.py` | `ExposureProcessingInfo.fromRedisHash` parsing and predicates | No |
| `test_redisUtils.py` | `RedisHelper` lifecycle / queueing / tracking, against `fakeredis` | No |

### Test Data

- `tests/data/sampleExpRecord.json` - sample Butler exposure record
- `tests/data/butlerDimensionUniverse.json` - Butler dimension configuration
- `tests/data/LATISS_raw_2023101100291.json` - LATISS raw exposure metadata
- `tests/files/test_file_0001.txt` - test file for S3 uploader

### Mocking Patterns

- **S3**: `moto` library (`mock_aws()` context manager) for full S3 simulation
- **HTTP**: `responses` library (`@responses.activate`) for REST API mocking
- **Butler**: conditional skip with `@unittest.skipIf(NO_BUTLER, ...)` when
  Butler is not available
- **Redis**: `fakeredis` (`fakeredis.FakeStrictRedis()`) for in-memory Redis
  in unit tests. Patch `lsst.rubintv.production.redisUtils.redis.Redis` with
  a side-effect that returns a fakeredis client, then construct
  `RedisHelper(butler=None, locationConfig=None)` — see
  `tests/test_redisUtils.py` for the fixture pattern. The CI suite still
  uses a real Redis server for end-to-end coverage.

### Import Style

Put **all** imports at the top of the test module — do **not** defer them
into function or method bodies unless it is absolutely unavoidable (e.g. a
genuine import cycle that has no other fix). This applies even to imports
that only a couple of tests use, and even to ones that pull in heavy
machinery (a Butler, the dimension universe, sample-data fixtures) and so
add to import time.

The reasoning is deliberate: if an import can ever fail at runtime — a
missing data fixture, a moved symbol, a broken transitive dependency — we
want that failure surfaced **upfront, at collection time**, where it is
loud and unambiguous, rather than hidden inside the one test that happens
to exercise it. A module-level import that breaks fails the whole file
immediately and obviously; a deferred import that breaks looks like a test
failure in a single unrelated-looking test. The increased import time is a
price worth paying for that early, honest failure.

(For example, `getSampleExpRecord` in `test_redisUtils.py` is imported at
module scope even though only two tests use it — see the third-party
import group at the top of that file.)

## CI Integration Suite (`tests/ci/`)

The CI suite is a custom test framework (not pytest) that spins up a real Redis
server and runs actual processing scripts as subprocesses. It validates the
full distributed system and pipelines end-to-end, including all work
distribution, payload handling, and S3 uploads (mocked).

### Entry Point

```bash
python tests/ci/test_rapid_analysis.py -l <label_name>
```

### Architecture

The CI suite has its own mini-framework:

- **`TestConfig`** - centralized config (timeouts, Redis port, test scripts)
- **`RedisManager`** - starts/stops a local Redis server on port 6111
- **`LogManager`** - creates timestamped log directories under `ci_logs/`
- **`ProcessManager`** - launches test scripts as `multiprocessing.Process`
- **`ResultCollector`** - aggregates pass/fail results

### Test Phases

**Phase 1: Meta Tests** (30 s timeout)
Small scripts validating the test framework itself:
- `meta_test_runs_ok.py` - verifies process management works
- `meta_test_raise.py` - verifies exception capture
- `meta_test_sys_exit_non_zero.py` - verifies exit code handling
- `meta_test_patching.py` - verifies lsstDebug patching
- `meta_test_logging_capture.py` - verifies log capture
- `meta_test_s3_upload.py` - verifies uploader mock
- `meta_test_debug_config.py` - verifies debug config
- `meta_test_env.py` - verifies env vars and Redis lifecycle

**Phase 2: Round 1** (900 s / 15 min timeout)
Full pipeline execution:
- Head node + SFM workers + step1b workers for LATISS and LSSTCam
- 18 SFM detectors for LSSTCam (90-98, 144-152)
- Real Butler queries against test data (dayObs=20251115)
- Test exposures: 226 (SFM), 227+228 (FAM CWFS pair), 436 (bias)

**Phase 3: Round 2** (200 s timeout)
Post-processing and visualization:
- Plotting scripts (PSF, FWHM, Zernike, radial)
- Step1b gather processing
- Nightly rollup workers

### Data Feeding

`drip_feed_data.py` pre-loads test exposures into Redis:
1. Initializes Butler and RedisHelper
2. Waits for SFM workers and head node to come online
3. Pushes exposures to Redis with specific ordering and delays:
   - 227 first (intra-focal, must arrive before 228)
   - Then 436 (bias), 226 (SFM), 228 (extra-focal)
   - 2 s delays between pushes
4. Announces FAM pair via `LSSTCam-FROM-OCS_DONUTPAIR`
5. Also tests LATISS:
   - exposure 20240813/632 (on-sky science, exercises SFM)
   - exposures 20260625/12+13 (a CWFS intra/extra pair, pushed intra
     first; the extra-focal image landing triggers the `AOS_LATISS`
     WEP monolith processing of the pair). The final checks assert the
     AOS detector finished in the tracking hash and that `zernikes`,
     `donutStampsExtra` and `donutStampsIntra` landed in the butler for
     the extra-focal visit

### Redis in CI

- Real Redis server started on `127.0.0.1:6111` with password `redis_password`
- `FLUSHALL` between test phases for isolation
- All S3 uploaders are mocked via `MockUploader` (tracks uploads without I/O)

### Log Analysis

After a CI run, use the interactive log viewer:

```bash
python tests/ci/view_ci_logs.py
```

Features:
- Browse test runs chronologically
- View individual pod logs
- Search across all logs
- Extract tracebacks with context
- Filter by PID

### Test Collection Setup

`tests/createUnitTestCollections.py` builds Butler collections for CI:
- Sets `RAPID_ANALYSIS_LOCATION=usdf_testing`
- Runs pipelines in parallel via `ThreadPoolExecutor`
- Creates collections for: FAM, AOS, SFM, calibration pipelines (LSSTCam),
  plus `AOS_LATISS` (the LATISS WEP monolith, run on the CWFS pair
  20260625/12+13)
- Used to create the underlying collections for `test_pipelines.py` unit tests
- Only needs to be rerun when outputs change

## Key Testing Notes

- The CI suite uses real Butler and real (local) Redis, but mocked S3 uploaders
- Pipeline tests (`test_pipelines.py`) are the most comprehensive unit tests
  but require a Butler connection (1600+ lines, 14 pipeline variants)
- `CameraControlConfig` tests validate all named focal plane patterns and
  detector count arithmetic (no external dependencies needed)
- The CI suite validates the full head-node-to-worker flow including Redis
  queue dispatch, payload serialization, and task completion tracking
