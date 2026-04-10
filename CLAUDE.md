# RubinTV Production - Agent Guide

This is the backend processing system for **RubinTV**, the Vera Rubin
Observatory's real-time observation visualization platform. It runs as a
distributed set of Kubernetes pods at the summit (and at USDF/SLAC) that
ingest raw telescope exposures, run LSST Science Pipeline tasks on them,
and publish results (images, plots, metadata) to S3 buckets consumed by the
RubinTV web frontend.

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

### Naming

- camelCase for all variables, functions, methods, and attributes.
- PascalCase for classes.
- No snake_case except when required by external APIs.
- All function/method names must contain a verb (including private ones),
  e.g. ``getTrackingKey`` not ``trackingKey``. Exception: ``fromX``
  class methods for constructors do not need a verb.
- Prefer longer, descriptive variable names over short, abbreviated ones. For
  example, ``step1aDispatched = isStep1aDispatched()`` rather than ``s1aD =
  self.isStep1aDispatched()``. This is not a hard rule though, and as long as
  it's clearly human-readable its fine to abbreviate a bit, so saying ``dets``
  for ``detectors`` is fine. It is also important to stick to established
  abbreviations already in use, as some of these are "terms of art" e.g.
  ``expId`` vs ``exposureIdentifier``.

### Formatting

- black (line-length 110), isort (black profile)

### Type Annotations

- Use built-in types (``int``, ``str``, ``float``, ``dict``, ``list``,
  ``tuple``, etc.).
- Never import ``Dict``, ``List``, ``Tuple``, ``Optional``, or ``Union``
  from ``typing``.
- Use ``| None`` instead of ``Optional[...]``.

### Docstrings

- Use numpydoc format.
- Include types for every parameter and for the return value (if not
  ``None``).
- Always name the return value unless the return type is ``None`` (omit
  the Returns section in that case).
- If a parameter is ``| None``, describe its type as ``<type>, optional``.
- Argument order in docstrings must match the function signature, and
  types must be correct.
- No docstrings for class ``__init__``; document the class instead.

Example:

```python
def myFunction(param1: int, param2: str | None = None) -> bool:
    """This function does something.

    Parameters
    ----------
    param1 : `int`
        The first parameter.
    param2 : `str`, optional
        The second parameter.

    Returns
    -------
    result : `bool`
        The result of the function.
    """
    return param1 > 0 and param2 != "hello"
```

### Environment Setup

To access the full DM Stack (everything in the ``lsst.*`` namespace),
source the following in order:

```bash
source ~/stack.sh
. ~/setup_packages.sh
```

This is needed when importing code from other ``lsst.*`` packages or
locating dependencies outside this repo.

### Linting & Tooling

- **Linting**: flake8
- **Type checking**: mypy (Python 3.13 target)
- **Pre-commit hooks**: trailing whitespace, YAML, isort, black, flake8
- **Build system**: LSST SCons + pyproject.toml
- **License**: GPLv3
