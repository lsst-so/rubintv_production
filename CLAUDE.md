# RubinTV Production - Agent Guide

This is the backend processing system for **RubinTV**, the Vera Rubin
Observatory's real-time observation visualization platform. It runs as a
distributed set of Kubernetes pods at the summit (and at USDF/SLAC) that
ingest raw telescope exposures, run LSST Science Pipeline tasks on them,
and publish results (images, plots, metadata) to S3 buckets consumed by the
RubinTV web frontend.

## Architecture Documentation

Detailed architecture docs live in `architecture/` and MUST be kept up to date
whenever architectural changes are made:

- [Architecture & Data Flow](architecture/architecture.md) - overall system
  design, pod types, pipeline stages, focal plane layout, detector fanout,
  the gather mechanism, and how exposures flow through the system
- [Redis Coordination](architecture/redis-coordination.md) - how Redis is used
  for work distribution, pod health, task tracking, and control signals
- [Testing Guide](architecture/testing.md) - unit tests, CI integration suite,
  and how to run them

**When modifying code that changes any of the following, update the
corresponding architecture doc(s) in the same change:**
- Redis key names, formats, or TTLs (`redis-coordination.md`)
- Pod flavors, types, or queue naming (`architecture.md`, `redis-coordination.md`)
- Pipeline stages, task chaining, or the gather mechanism (`architecture.md`)
- Head node or worker event loop logic (`architecture.md`)
- The focal plane control or detector fanout logic (`architecture.md`)
- Payload serialization format (`architecture.md`)
- Test infrastructure or CI phases (`testing.md`)

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

## Development

- **Formatting**: black (line-length 110), isort (black profile)
- **Linting**: flake8
- **Type checking**: mypy (Python 3.13 target, use builtins and | instead of Union)
- **Pre-commit hooks**: trailing whitespace, YAML, isort, black, flake8
- **Build system**: LSST SCons + pyproject.toml
- **License**: GPLv3
