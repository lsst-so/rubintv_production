# Rapid Analysis - Agent Guide

This is the backend processing system commonly referred to across the project
as **rapid analysis** — the Vera Rubin Observatory's real-time observation
processing pipeline. It runs as a distributed set of Kubernetes pods at the
summit (and at USDF/SLAC) that ingest raw telescope exposures, run LSST
Science Pipeline tasks on them, and publish results (images, plots, metadata)
to S3 buckets consumed by the (separate) RubinTV web frontend.

## ⚠️ A note on names — read this first

The python package on disk is called `rubintv_production`, and the git repo
is `rubintv_production`, but **this is a misleading historical name** and
nobody actually calls the system that. Across the project, in docs, in
Slack, in tickets, and in conversation, this codebase is called **rapid
analysis**. A future rename of the package to `rapid_analysis` is on the
backlog ([claudePlans/backlog.md](claudePlans/backlog.md)).

There is also a **completely separate** repository called `rubintv` — that
one is the web frontend that displays some of the plots this backend
produces. Do not confuse the two. In particular:

- Don't call this codebase "RubinTV" or "RubinTV Production" in docstrings,
  comments, commit messages, PR descriptions, new module names, new skill
  names, or new doc titles. Call it **rapid analysis** (or "the rapid
  analysis backend" when disambiguation is needed).
- The existing `rubintv_production` / `lsst.rubintv.production` names on
  disk have to stay until the rename ticket lands — don't go on a search-
  and-replace spree to "fix" them. But do not *introduce* new uses of
  "rubintv" to describe this system.
- When you see "RubinTV" referenced externally (e.g. `consumed by RubinTV`,
  `the RubinTV frontend`), that really does mean the separate frontend
  repo, not this one.

## This is an application, not a library

`rubintv_production` is the *end consumer* of everything it imports. Nothing
external imports from this package — there are no downstream library users,
no API contract to preserve across releases, no `__all__` exposed for
third-party reuse. The package is shipped as a set of pod images that run
the scripts in `scripts/`, and that is the only consumer.

What this means in practice for refactors:
- API changes inside the package are completely fine. Renaming a function,
  splitting a module, moving a symbol from one file to another, deleting
  unused code — none of these break anyone. The only requirement is that
  the package remains *self-consistent*: every internal call site must be
  updated in the same change so the package still imports cleanly and the
  CI integration tests still pass.
- Do **not** add backwards-compatibility shims, deprecated re-exports,
  alias wrappers, or "compatibility" modules when refactoring. They are
  pure deadweight here. If a symbol moves, move every call site at the
  same time and delete the old name outright.
- Versioning the public surface, deprecation warnings, and "leave the old
  name in place for one release" are all anti-patterns in this codebase —
  there are no downstream releases.

## Terminology — "tests", "CI", and what runs when

Two different things in this repo are sometimes both called "tests",
and the on-disk naming conflates them with conventional CI in a way
that has actively caused confusion. Get these straight:

- **Unit tests** = `pytest tests/test_*.py` and `mypy`. They run on
  any machine with the LSST stack sourced (including the user's Mac).
  These are the routine validation gate: **every Python change**
  under `python/lsst/rubintv/production/`, `scripts/`, or `tests/`
  must pass them before being declared done. Nothing automated runs
  them — manual `mypy` + `pytest` is the gate.
- **Integration suite** = `tests/ci/test_rapid_analysis.py`. This
  spins up a real Redis server and runs the distributed pipeline
  scripts as subprocesses against real Butler data. It requires a
  SLAC dev node and is run manually as part of pre-deployment
  validation. **It does not run in GitHub Actions or k8s.**

There is **no automated CI in the conventional sense**. The only
GitHub Action is `build_and_push.yaml`, which builds the Docker image
and nothing more — "CI is green" tells you the image built, not that
anything was validated. Pre-commit runs trailing-whitespace,
check-yaml, isort, black, and flake8 — no mypy, no pytest.

Despite that, on disk the integration suite is named "CI" everywhere:
the directory is `tests/ci/`, the env vars are `RA_CI_*` /
`RAPID_ANALYSIS_CI`, the predicate is `runningCI()`. **This is a
misnomer**, retained for now to avoid churn (see
[claudePlans/backlog.md](claudePlans/backlog.md) for the rename
ticket). When you read "CI" in this repo, translate it to
"integration suite" mentally and you will not be misled.

The `rapid-analysis-testing` skill is the canonical guide to running
the unit tests; `architecture/testing.md` covers both.

## Architecture Documentation

Detailed architecture docs live in `architecture/`:

- [Architecture & Data Flow](architecture/architecture.md) - overall system
  design, pod types, pipeline stages, focal plane layout, detector fanout,
  the gather mechanism, and how exposures flow through the system
- [Redis Coordination](architecture/redis-coordination.md) - how Redis is used
  for work distribution, pod health, task tracking, and control signals
- [Testing Guide](architecture/testing.md) - unit tests, the integration suite
  (the thing in `tests/ci/`), and how to run them

These docs must stay in sync with the code; the `rubintv-architecture-sync`
skill describes when and how to update them alongside a code change.

## Quick Orientation

```
python/lsst/rubintv/production/   # Main Python package
  processingControl.py             # HeadProcessController - the orchestrator
  pipelineRunning.py               # SingleCorePipelineRunner - the worker
  redisUtils.py                    # RedisHelper - all Redis operations
  redisKeys.py                     # Pure helpers for Redis key construction
  podDefinition.py                 # PodDetails, PodFlavor - pod identity/queues
  payloads.py                      # Payload - serializable work unit
  watchers.py                      # ButlerWatcher, RedisWatcher - event sources
  baseChannels.py                  # BaseChannel, BaseButlerChannel - worker bases
  locationConfig.py                # LocationConfig - central path/config manager
  startupChecks.py                 # Pre-flight checks run by pod scripts at boot
  butlerQueries.py                 # Helpers that touch a Butler (kept isolated)
  utils.py                         # Small middleware helpers (no Butler needed)
  predicates.py                    # Boolean helpers: isCalibration, isWepImage, ...
  parsers.py                       # JSON parsers, NumpyEncoder, sanitizeNans
  formatters.py                    # Filename, title and lookup formatters
  shardIo.py                       # Read/write/merge metadata JSON "shards"
  timing.py                        # logDuration, timeFunction, BoxCarTimer
  uploaders.py                     # S3Uploader, MultiUploader - cloud uploads
  aos.py                           # AOS donut/zernike processing & plotting
  aosUtils.py                      # AOS helper functions (DOF parsing, etc.)
  aosRecipes.py                    # Zernike post-processing recipes
  timedServices.py                 # TimedMetadataServer, TMA telemetry
  consdbUtils.py                   # ConsDBPopulator - consolidated DB writes
  channels.py                      # Channel definitions for RubinTV frontend
  performance.py                   # PerformanceMonitor - pipeline timing metrics
  cleanup.py                       # TempFileCleaner - daily housekeeping
  clusterManagement.py             # Worker-set and cluster-layout dataclasses
  workerSets.py                    # WorkerSet registry helpers
  plotting/                        # Night reports, mosaics, focal plane plots

scripts/                           # Entry points per instrument
  LSSTCam/                         # ~26 scripts (head node, workers, plotters)
  LATISS/                          # AuxTel scripts
  LSSTComCam/, LSSTComCamSim/      # ComCam scripts
  summit/                          # Summit-only entry points (e.g. auxTel/, misc/)

config/                            # Per-environment YAML configs
  config_summit.yaml               # Production (summit)
  config_usdf.yaml                 # US Data Facility (SLAC)

tests/                             # Unit tests (pytest + mypy gate)
tests/ci/                          # Integration suite (misleadingly named "ci")
  test_rapid_analysis.py           # Main integration-suite orchestrator
  drip_feed_data.py                # Feeds test exposures into Redis
```

## Key Concepts

- **Instrument**: Camera being processed (LSSTCam, LATISS, LSSTComCam, etc.)
- **Butler**: LSST's data access layer for reading/writing datasets
- **Exposure / Visit**: A single shutter opening / logical observation grouping
- **dayObs**: Integer date (YYYYMMDD) identifying an observing night
- **seqNum**: Sequence number within a night
- **Pipeline / Step**: A graph of processing tasks (ISR, SFM, AOS, step1b)
- **Payload**: A frozen dataclass sent via Redis containing a dataId +
  serialized pipeline graph for a worker to execute
- **Shard**: A small JSON file written by workers, periodically merged by
  TimedMetadataServer and uploaded to S3 for the frontend

## Writing unit tests alongside code changes

Add unit tests in the same commit as the code change **whenever it is
feasible to do so**. Feasible here means the change touches a unit that
can be exercised in isolation without standing up a Butler, Redis, S3,
or the wider distributed machinery — in other words, the kind of change
that `tests/test_*.py` already covers (pure dataclasses, serialisers,
parsers, predicates, small helpers, key construction, etc.).

Examples of changes that should always ship with tests:

- Adding or removing a field on a serialisable dataclass (e.g. `Payload`,
  `PodDetails`). Test the default value, equality, and JSON round-trip.
  If the wire format needs to stay backward-compatible with older
  messages in flight, test that explicitly by decoding a legacy-shape
  JSON blob.
- Adding or changing a pure helper in `utils.py`, `predicates.py`,
  `parsers.py`, `formatters.py`, `redisKeys.py`, `timing.py`, etc.
- Fixing a bug in any of the above — add a regression test that fails
  without the fix.

When tests are genuinely hard (the change is deep inside an event loop,
requires a live Butler/Redis, or is a plumbing change whose only
observable effect is end-to-end in the integration suite), it is
fine to skip them — but state in the commit message *why* the change
isn't unit-tested, so a reviewer doesn't have to guess whether it was
an oversight or a deliberate call.

The reminder: nothing in pre-commit or in any automated workflow
forces this — the `rapid-analysis-testing` skill is the checklist for
validating what you *did* write, but it won't tell you that you
*should* have written a test in the first place. That judgement is on
you at the moment of editing the code, and must be made explicit in
the commit message.

## Skills

Project-scoped skills live under `.claude/skills/` and load automatically
when their triggering context matches:

- **rapid-analysis-lsst-stack** — sourcing the DM stack before running,
  testing, or importing package code.
- **rapid-analysis-code-style** — naming, formatting, type annotation, and
  docstring conventions when writing or editing Python here.
- **rapid-analysis-testing** — the manual validation loop (mypy + pytest)
  to run after editing Python, since neither pre-commit nor any
  automated workflow runs them.
- **rapid-analysis-architecture-sync** — keeping `architecture/*.md` in
  step with code changes that touch the system's shape.
