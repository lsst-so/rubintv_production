# Testing Guide

## Terminology

This repo has two layers of testing that the on-disk naming
unfortunately conflates with conventional CI:

- **Unit tests** — `pytest tests/test_*.py` and `mypy`. Run on every
  Python change, on any machine with the LSST stack sourced. This is
  the routine validation gate. Nothing automated runs them; manual
  invocation is required and the `rapid-analysis-testing` skill is
  the checklist.
- **Integration suite** — `tests/ci/test_rapid_analysis.py`. Spins up
  a real Redis server, runs the distributed pipeline scripts as
  subprocesses against a real Butler. Requires a SLAC dev node; run
  manually as part of pre-deployment validation.

There is **no automated CI in the conventional sense** for this repo.
`.github/workflows/build_and_push.yaml` only builds and publishes the
Docker image. Despite that, the integration suite is named "CI" on
disk (`tests/ci/`, `RA_CI_*` env vars, `runningCI()` predicate, etc.)
— that's a misnomer kept for now to avoid churn. See
[claudePlans/backlog.md](../claudePlans/backlog.md). Whenever this
doc says "integration suite", that's the thing that lives in
`tests/ci/`.

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
| `test_consdbUtils.py` | `consdbUtils` mappings and helper functions | No |
| `test_clusterManagement.py` | Dataclasses in `clusterManagement.py` | No |
| `test_workerSets.py` | `WorkerSet` registry helpers | No |
| `test_pipelines.py` | Full pipeline graph generation and validation | Yes |

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
- **No Redis mocking in unit tests**: Redis-dependent code is tested in the
  CI suite instead

## Integration Suite (`tests/ci/`)

> **Naming reminder:** the on-disk directory is `tests/ci/` and many
> related artefacts use "CI" in their names, but this is the
> *integration suite*, **not** conventional CI. Nothing about it runs
> in GitHub Actions or k8s. The name is kept on disk for now to avoid
> churn — see [the rename backlog item](../claudePlans/backlog.md).

The integration suite is a custom test framework (not pytest) that spins up
a real Redis server and runs actual processing scripts as subprocesses. It
validates the full distributed system and pipelines end-to-end, including
all work distribution, payload handling, and S3 uploads (mocked).

### Per-user setup (one-time)

The CI suite is designed to be runnable by anyone, not just the original
author. Two scripts in `tests/ci/` handle the per-user setup:

- **`tests/ci/preinstall_ci_deps.sh`** — installs CI dependencies that are
  not in rubinenv (`sentry-sdk`, `redis` python client, `batoid`, `danish`,
  `timm`, `peft`, `google-cloud-storage`, `lsst-efd-client`,
  `pytorch_lightning`) via `pip install --user`, plus builds the
  `redis-server` binary from source into `${HOME}/local/bin`. Run once per
  user; idempotent on re-run. Mirrors the production Dockerfile's
  conda+pip lists, minus `rubin-libradtran` (conda-only; only matters for
  the LATISS spectral pipeline).
- **`tests/ci/setup_ci_env.sh`** — exports the per-user environment
  variables the suite needs and prepends `${HOME}/local/bin` to `PATH` so
  the source-built redis-server is found. Edit the values at the top of
  the file for your account, then `source` it in any shell session before
  running CI.

The required env vars (listed in `_REQUIRED_USER_ENV_VARS` at the top of
both `tests/ci/test_rapid_analysis.py` and `tests/createUnitTestCollections.py`):

| Env var | Purpose |
|---|---|
| `RA_CI_DATA_ROOT` | Root for plots, sidecar metadata, shards, AOS data, dimension universe file. Substituted as `${RA_CI_DATA_ROOT}` in `config/config_usdf_testing.yaml`. |
| `RA_CI_STAR_TRACKER_DATA_PATH` | Star-tracker raw data root. |
| `RA_CI_ASTROMETRY_NET_REF_CAT_PATH` | astrometry.net reference-catalogue base. |
| `TARTS_DATA_DIR` | TARTS pipeline data dir (read by the AOS worker). |
| `AI_DONUT_DATA_DIR` | AI-donut model data dir. |
| `RA_CI_REDIS_PORT` | Port for the CI's private redis-server (default 6111; bump if a colleague is using it on the same node). |

Both scripts (`test_rapid_analysis.py` and `createUnitTestCollections.py`)
hard-fail at startup if any of these are unset, with a message pointing
the user at `setup_ci_env.sh`. There are **no** hard-coded user paths
left in either script.

The YAML config supports `${VAR}` substitution in any string value because
`locationConfig._loadConfigFile` runs `os.path.expandvars` recursively
over the loaded YAML before returning it.

### Entry Point

```bash
source tests/ci/setup_ci_env.sh           # required, per shell session
python tests/ci/test_rapid_analysis.py -l <label_name>
```

### Concurrent-run isolation

The CI suite is *partially* safe to run concurrently with another user on
the same dev node:

- **S3 scratch is per-user** — `config/config_usdf_testing.yaml` sets
  `scratchPath: rapidAnalysisScratchCi-${USER}`, so `getBasePath` resolves
  to a user-specific S3 prefix.
- **Redis is per-user** — port comes from `RA_CI_REDIS_PORT`, and
  `RedisManager.is_redis_running` uses `pgrep -u $USER` so a colleague's
  redis on the same node doesn't trip the "already running" guard.
- **Butler output chains are NOT per-user** — `outputChains` in the YAML
  (`LSSTCam/runs/quickLookTesting`, etc.) are still shared. Concurrent
  runs that hit step1b will fight over these collections. Coordinate with
  collaborators if you both need to run at the same time.

### Architecture

The CI suite has its own mini-framework:

- **`TestConfig`** - centralized config (timeouts, redis port from
  `RA_CI_REDIS_PORT`, test scripts)
- **`RedisManager`** - starts/stops a local Redis server on the user's
  configured port; `is_redis_running` is scoped to `$USER`
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
5. Also tests LATISS with exposure 20240813/632

### Redis in CI

- Real Redis server started on `127.0.0.1:${RA_CI_REDIS_PORT}` with password
  `redis_password`
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
- Requires the same `RA_CI_*` / `TARTS_DATA_DIR` / `AI_DONUT_DATA_DIR`
  env vars as the main suite — `source tests/ci/setup_ci_env.sh` first.
- Sets `RAPID_ANALYSIS_LOCATION=usdf_testing`
- Runs pipelines in parallel via `ThreadPoolExecutor`
- Creates collections for: FAM, AOS, SFM, calibration pipelines
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
