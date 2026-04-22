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

## Architecture Documentation

Detailed architecture docs live in `architecture/`:

- [Architecture & Data Flow](architecture/architecture.md) - overall system
  design, pod types, pipeline stages, focal plane layout, detector fanout,
  the gather mechanism, and how exposures flow through the system
- [Redis Coordination](architecture/redis-coordination.md) - how Redis is used
  for work distribution, pod health, task tracking, and control signals
- [Testing Guide](architecture/testing.md) - unit tests, CI integration suite,
  and how to run them

These docs must stay in sync with the code; the `rubintv-architecture-sync`
skill describes when and how to update them alongside a code change.

## Quick Orientation

```
python/lsst/rubintv/production/   # Main Python package
  processingControl.py             # HeadProcessController - the orchestrator
  pipelineRunning.py               # SingleCorePipelineRunner - the worker
  redisUtils.py                    # RedisHelper - all Redis operations
  podDefinition.py                 # PodDetails, PodFlavor - pod identity/queues
  payloads.py                      # Payload - serializable work unit
  watchers.py                      # ButlerWatcher, RedisWatcher - event sources
  baseChannels.py                  # BaseChannel, BaseButlerChannel - worker bases
  utils.py                         # LocationConfig - central path/config manager
  uploaders.py                     # S3Uploader, MultiUploader - cloud uploads
  aos.py                           # AOS donut/zernike processing & plotting
  timedServices.py                 # TimedMetadataServer, TMA telemetry
  consdbUtils.py                   # ConsDBPopulator - consolidated DB writes
  channels.py                      # Channel definitions for RubinTV frontend
  performance.py                   # PerformanceMonitor - pipeline timing metrics
  cleanup.py                       # TempFileCleaner - daily housekeeping
  plotting/                        # Night reports, mosaics, focal plane plots

scripts/                           # Entry points per instrument
  LSSTCam/                         # ~26 scripts (head node, workers, plotters)
  LATISS/                          # AuxTel scripts
  LSSTComCam/, LSSTComCamSim/      # ComCam scripts
  summit/, slac/, tts/             # Site-specific scripts

config/                            # Per-environment YAML configs
  config_summit.yaml               # Production (summit)
  config_usdf.yaml                 # US Data Facility (SLAC)

tests/                             # Unit tests
tests/ci/                          # CI integration suite
  test_rapid_analysis.py           # Main CI orchestrator
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

## Skills

Project-scoped skills live under `.claude/skills/` and load automatically
when their triggering context matches:

- **rapid-analysis-lsst-stack** — sourcing the DM stack before running,
  testing, or importing package code.
- **rapid-analysis-code-style** — naming, formatting, type annotation, and
  docstring conventions when writing or editing Python here.
- **rapid-analysis-testing** — the manual validation loop (mypy + pytest)
  to run after editing Python, since neither pre-commit nor CI runs them.
- **rapid-analysis-architecture-sync** — keeping `architecture/*.md` in
  step with code changes that touch the system's shape.
