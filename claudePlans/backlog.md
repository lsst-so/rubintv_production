# Rapid Analysis — Backlog & Tech Debt

The forward-looking list of open work on this package: strategic items, unit
test coverage gaps, refactor opportunities, and the remaining code-level tech
debt surfaced by the DM-54577 audit. This file supersedes the earlier
`unit_test_coverage_plan.md`, the old `backlog.md`, and
`DM-54577/tech-debt-audit.md` — completed items have been removed; what
remains is what's still on the table.

Active per-ticket design docs (anything mid-flight) live alongside this file
in `claudePlans/DM-XXXXX/` directories — for example
[`DM-54577/redis-tracking-unification.md`](DM-54577/redis-tracking-unification.md).
This file is the catch-all "someday" list; once an item is ready to be
worked, move its content into a new `claudePlans/DM-XXXXX/` directory and
link the ticket here.

> **Note on line numbers.** The audit was a snapshot of the tree at a
> specific point in time. Code has moved since (especially in
> `highLevelTools.py`, where ~200 lines were removed when
> `createChannelByName` / `remakeDay` / `remakePlotByDataId` were deleted).
> Treat any line-number reference here as a hint, not a promise — re-grep
> for the TODO/XXX marker text if you need a precise location.

---

## 1. Strategic / cross-cutting

These are the long-running, multi-area items that don't fit in a single PR.
Each needs human coordination before an agent picks it up.

### Rename "CI" → "integration suite" on disk

**Why it matters.** The integration suite (`tests/ci/test_rapid_analysis.py`)
is named "CI" everywhere on disk: the directory `tests/ci/`, the env vars
`RA_CI_*` and `RAPID_ANALYSIS_CI`, the predicate `runningCI()`, the
scripts `setup_ci_env.sh` / `preinstall_ci_deps.sh`, the log directory
`ci_logs/`, and many docstrings and prose references.

This is a misnomer. "CI" in software-engineering usage means
*continuous integration* — automated, runs on every push, gates merges.
This repo has none of that: the only GitHub Action is
`build_and_push.yaml` (Docker image build), pre-commit doesn't run mypy
or pytest, and the "CI suite" runs manually on a SLAC dev node as a
pre-deployment check. The name has actively caused confusion (model
agents and human readers alike interpret "CI" with its conventional
meaning and reach the wrong conclusions about what runs when).

**Scope of the rename (rough).**
- Directory: `tests/ci/` → `tests/integration/` (or `tests/integration_suite/`).
- Scripts: `setup_ci_env.sh` → `setup_integration_env.sh`,
  `preinstall_ci_deps.sh` → `preinstall_integration_deps.sh`.
- Log directory: `ci_logs/` → `integration_logs/`.
- Env vars: `RA_CI_DATA_ROOT` → `RA_INTEGRATION_DATA_ROOT` (and likewise
  for the other `RA_CI_*` vars and `RA_CI_REDIS_PORT`).
- `RAPID_ANALYSIS_CI` env var → `RAPID_ANALYSIS_INTEGRATION` (or similar).
- `runningCI()` predicate in `predicates.py` → `runningIntegrationSuite()`.
- All references in docstrings, comments, prose, the testing skill,
  CLAUDE.md, and `architecture/testing.md`.
- `tests/ci/view_ci_logs.py` and `tests/test_view_ci_logs.py`.

**Coordination.** None expected externally; this is internal naming
only. But the env vars are user-facing (every CI runner will need to
update their shell rc / sourced setup file), and the directory name
is referenced from many scripts, so it must be done atomically in one
PR.

**Status.** Not started. Until this lands, the
`rapid-analysis-testing` skill, CLAUDE.md, and `architecture/testing.md`
all carry "CI ≠ conventional CI" terminology callouts so that readers
translate the on-disk naming correctly.

### `config_usdf.yaml` has empty-string paths that now break eager init

**Why it matters.** `LocationConfig.__post_init__` was changed (in the
same work that introduced the per-user CI env vars) to eagerly validate
every path declared in the YAML by touching every `cached_property`.
That's an intentional fail-fast contract — but `config_usdf.yaml`
currently has several keys whose value is the empty string, e.g.:

```yaml
auxTelMetadataPath: ''
auxTelMetadataShardPath: ''
auxtelButlerPath: ''
comCamAosMetadataPath: ''
comCamAosMetadataShardPath: ''
comCamSimAosMetadataPath: ''
comCamSimAosMetadataShardPath: ''
```

Under the old (lazy) validation, these were only a problem if something
actually read those properties. Under the new eager validation, calling
`LocationConfig("usdf")` will now try to `_checkDir("")`, which calls
`os.makedirs("", exist_ok=True)` and raises `FileNotFoundError`. That
means **anyone constructing the `usdf` config in production today will
hit a hard failure at LocationConfig init**.

**Scope of the fix.**
- For each empty-string path, decide: is the property genuinely unused
  at USDF (in which case the right answer is to delete the
  cached_property and the YAML key from every config — the YAML
  uniformity check requires removing it everywhere at once), or is it
  used but should point at a real directory at USDF (in which case set
  it).
- The four `aos*` ones above smell like dead-at-USDF; the `auxTel*` /
  `auxtelButlerPath` ones may still be needed by some plotter or
  watcher even at USDF — verify before deleting.

**Status.** Not started. This is a strict regression risk for any pod
or test that constructs the `usdf` LocationConfig — pick it up before
the next `usdf` deployment. Workaround in the meantime: those pods
currently aren't running into it because nothing has picked up the new
LocationConfig changes yet, but the moment the branch lands, every
USDF startup will fail.

### Remove all remaining TS8 references

**Why it matters.** The TS8 test stand is no longer supported by the rapid
analysis backend, but TS8-related code, identifiers, and channels still
linger across the package. This is dead code that confuses new readers
about what instruments the system actually targets, and it can mislead
edits ("does this need a TS8 case?") into preserving plumbing that has
no live consumers.

**Scope of the cleanup (rough).** Greppable starting points (current as
of writing — re-grep before working):
- `python/lsst/rubintv/production/channels.py` — the `ts8_*` channel
  names and their `("slac_ts8", ...)` mappings.
- `python/lsst/rubintv/production/plotting/mosaicPlotting.py` — the
  `case "LSST-TS8":` branch in the rubin-tv-instrument-name mapping.
- `python/lsst/rubintv/production/redisUtils.py` — `LSST-TS8` in
  instrument lists alongside `LSSTComCam`.
- `python/lsst/rubintv/production/watchers.py` — outdated TS8 reference
  in a comment about exposure-set sizes.
- `tests/test_utils.py` and `tests/test_s3_uploader.py` — tests that
  exercise the TS8 paths above.
- `config/config_*.yaml` — comments referencing TS8 (e.g.
  `# paths for serving TS8 metadata at SLAC`,
  `# pretending that ComCam is TS8`).

The dead `LocationConfig` cached_properties (`ts8ButlerPath`,
`ts8MetadataPath`, `ts8MetadataShardPath`, `botButlerPath`,
`botMetadataPath`, `botMetadataShardPath`, `metadataPath`) have already
been removed as part of the eager-init-validation work — those had no
callers and no YAML keys, so they were trivially dead.

**Coordination.** None expected; this is internal cleanup with no
on-disk artefacts to migrate. Verify no live RubinTV-frontend page is
still asking for `ts8_*` channel slugs before deleting them from
`channels.py`.

**Status.** Not started.

### Rename the python package: `rubintv_production` → `rapid_analysis`

**Why it matters.** The on-disk names (`rubintv_production` as the git
repo, `lsst.rubintv.production` as the python namespace) are a historical
accident from when this backend was conceived as the "production side" of
the RubinTV web frontend. Nobody actually calls the system that — across
the project, in Slack, in DM tickets, and in conversation it is "rapid
analysis". Worse, there is an unrelated repo literally called `rubintv`
(the web frontend that consumes some of our S3 output), so the current
name actively misleads new contributors into thinking the two are part of
the same codebase.

**Scope of the rename (rough).** This is non-trivial because the name
appears in many coupled places:
- Git repo name on GitHub.
- Top-level directory name (`rubintv_production/`).
- Python namespace (`lsst.rubintv.production` → `lsst.rapid.analysis` or
  similar — decide the exact target namespace before starting).
- Every `import` statement across `python/`, `scripts/`, and `tests/`.
- EUPS table file and `ups/` configuration.
- `pyproject.toml` / `setup.cfg` package declarations.
- SConstruct / SConscript references.
- Kubernetes manifests and pod image tags (coordinate with the deploy repo).
- CI configuration referencing the package path.
- External references: anything in `lsst_distrib` or other stack-level
  metapackages that pulls this in; downstream jobs at USDF.
- Internal docstrings and doc titles (the ones that still say "RubinTV
  Production" or similar).

**Coordination.** Needs a heads-up to summit + USDF operators because pod
images will need re-tagging and k8s manifests will need to be updated in
lockstep. Can't be done as a quiet refactor.

**Blockers / dependencies.** None known; this is a pure coordination cost.
The rename has been "on the list" for years precisely because finding a
quiet window across summit + USDF + CI is the hard part.

**Status.** Not started. Claude agents: do **not** attempt this as a
drive-by — flag it to the human and wait for them to kick it off as its
own planned ticket.

### LSSTComCam / LSSTComCamSim head-node guard regression (c900bbf9)

`HeadProcessController.getPipelineConfig` (in
[processingControl.py](../python/lsst/rubintv/production/processingControl.py))
unconditionally raises `ValueError` for any instrument other than `LATISS`
or `LSSTCam`. But
[scripts/LSSTComCam/runHeadNode.py](../scripts/LSSTComCam/runHeadNode.py)
and
[scripts/LSSTComCamSim/runHeadNode.py](../scripts/LSSTComCamSim/runHeadNode.py)
still exist and instantiate `HeadProcessController`. If either pod is
ever deployed it will crash on the first exposure.

Needs a deployment-aware decision:
- If ComCam / ComCamSim are dead in production, delete the scripts.
- If not, move the guard inside the `case "cwfs"` branch (the only place
  the per-instrument dispatch actually differs) and let the calib +
  generic SFM paths handle ComCam as before.

### `raiseIf` always captures to Sentry

`raiseIf` in
[predicates.py](../python/lsst/rubintv/production/predicates.py) calls
`sentry_sdk.capture_exception(error)` unconditionally — every swallowed
exception (`doRaise=False`) still fires a Sentry event. There are many
callsites including routine expected-failure paths (`starTracker`,
`timedServices`, `oneOffProcessing`), so the noise floor on Sentry may
be much higher than intended. Either add an explicit `captureSentry:
bool` arg or only capture when `doRaise=True`.

### `setupSentry()` runs before `setupLogging()` in every pod script

Pod scripts call `setupSentry()` before `setupLogging()`. Once 980b4e7a
added a `logger.warning(...)` inside `setupSentry` (for failed init), the
warning now fires before the LSST logger is configured and may be silently
dropped or formatted unexpectedly. Two ways to fix:
- Swap the order in ~30 scripts so `setupLogging()` runs first.
- Or defer the warning inside `setupSentry` until after logging is up.

### Move `getPodWorkerNumber` out of `formatters.py`

[`getPodWorkerNumber`](../python/lsst/rubintv/production/formatters.py)
isn't a formatter — it's env-var + `sys.argv` plumbing that reads
`WORKER_NAME` / `WORKER_NUMBER` and prints to stdout. Imported by ~20 pod
scripts. Better fit:
[`startupChecks.py`](../python/lsst/rubintv/production/startupChecks.py),
which is already the home for pod-boot helpers.

### Docstring + typo sweep on the extracted modules

The utils.py split (a5ea1306 etc.) left some documentation gaps that
weren't caught by the follow-up cleanup passes:
- [`formatters.py`](../python/lsst/rubintv/production/formatters.py):
  `makePlotFile` and `makePlotFileFromRecord` have no docstrings;
  `expRecordToUploadFilename` doesn't document `extension` / `zeroPad`.
- [`parsers.py`](../python/lsst/rubintv/production/parsers.py):
  `getDimensionUniverse` has no docstring; `writeDimensionUniverseFile`
  has a one-line placeholder and untyped `butler` arg; `NumpyEncoder` has
  no class docstring.
- [`shardIo.py`](../python/lsst/rubintv/production/shardIo.py):
  `getShardPath` documents only one of three parameters.
- [`timing.py`](../python/lsst/rubintv/production/timing.py): no module
  docstring; `DurationResult` dataclass has no class docstring.
- Tests `test_metadataService.py`, `test_processingControl.py`, and
  `test_timing.py` still have `"""Test cases for utils."""` as the
  module docstring.
- Pre-existing typo: `CamearaControlConfigTestCase` →
  `CameraControlConfigTestCase` in `tests/test_processingControl.py`.

None of these are bugs; they're worth a single mechanical pass.

---

## 2. Unit-test coverage gaps

The original plan (`unit_test_coverage_plan.md`) inventoried ~20 modules.
Most of the easy + medium wins have landed since: test files now exist for
`timing`, `payloads`, `podDefinition`, `utils`, `consdbUtils`,
`processingControl`, `aosUtils`, `clusterManagement`, `workerSets`,
`redisUtils` (with fakeredis), `exposureProcessingInfo`, `uploaders`
(`test_s3_uploader.py`), `locationConfig`, `shardIo`, `resources`,
`exposureLogUtils`, `timedServices`, plus `allSky`, `metadataService`,
`pipelines`, and `view_ci_logs`. What remains:

### Targeted gaps

- **`channels.getCameraAndPlotName`** — no test file targets this. Pure
  lookup; cover every channel key plus the error path on unknown.
- **`ClusterManager` query methods** —
  [`test_clusterManagement.py`](../tests/test_clusterManagement.py)
  covers the `QueueItem` / `WorkerStatus` / `FlavorStatus` /
  `ClusterStatus` dataclasses and the
  `DescribeActiveExposuresChange` helper, but does not exercise
  `ClusterManager` itself against a fakeredis-backed cluster state.

### Skip — leave to integration tests

`aos.py`, `allSky.py` (beyond the existing tests), `guiders.py`,
`starTracker.py`, `mountTorques.py`, `oneOffProcessing.py`, `cleanup.py`,
most of `plotting/`, and the long-running orchestrators in
`baseChannels.py` / `rubinTv.py`. These are either thin wrappers around
DM Stack code or end-to-end loops where mocks would teach us nothing the
CI doesn't.

---

## 3. Refactor opportunities

These don't change behaviour — they extract pure logic out of I/O wrappers
so the pure parts become testable. Listed roughly in order of leverage.
**None of them have been done yet.**

1. **Extract Redis key construction from `redisUtils.py`**
   Key strings like `f"{instrument}-{who}-{queue}"` are scattered through
   `RedisHelper.enqueuePayload`, `dequeuePayload`, and friends. Lifting them
   to module-level pure helpers (`_makeQueueKey`, `_makeBusyKey`,
   `_parseBusyKey`, …) makes them trivially unit-testable and protects the
   format from accidental drift. Probably the single biggest win.

2. **Split `utils.py` by responsibility**
   At ~1,900 lines it's doing too much. Suggested groupings:
   - predicates (`isDayObsContiguous`, `isCalibration`, `isWepImage`, …)
   - formatters (`getFilterColorName`, `getRubinTvInstrumentName`,
     title/path builders)
   - shard I/O (`writeMetadataShard`, `getShardedData`, …)
   - parsers (`expRecordFromJson`, dimension-universe helpers)

   The refactor is mechanical and each piece becomes obviously testable.

3. **Pull `CameraControlConfig` validation out of `processingControl.py`**
   `__post_init__` has a stack of validation rules that would each be a
   one-line unit test if they were standalone functions. Same goes for any
   pure pipeline-graph analysis sitting beside the Butler-bound dispatch
   code.

4. **Extract performance computation from plotting in `performance.py`**
   ~2,150 lines mixing data fetch, aggregation, and matplotlib. A
   `PerformanceComputation` class taking already-fetched dicts/dataframes
   and producing aggregated structures would be unit-testable; the
   existing `PerformanceBrowser` keeps the plotting.

5. **Separate quantum-graph construction from execution in
   `pipelineRunning.py`**
   The trivial-vs-all-dimensions strategy selection and small graph
   manipulations are pure-ish but currently entangled with the runner. A
   factory module would let you test the strategy logic without a Butler.

---

## 4. Code-level tech debt (DM-54577 audit — remaining items)

Package-wide sweep of explicit `TODO`, `XXX`, and other in-code tech-debt
markers, categorised by *how* you'd fix it. The "Cluster patterns" and
"Single-area" sections below follow the original audit taxonomy.

Most of the original audit's items have landed already — listed here are
only what's still open.

### 4.1 Cluster patterns (one fix → many TODOs gone)

#### C4 — "Generalise this to all bands" (LATISS night reports)

**Where:** [`latissNightReportPlots.py:106,177,253,343,417,723`](../python/lsst/rubintv/production/plotting/latissNightReportPlots.py)
(6 TODO sites) plus the related
[`latissNightReportPlots.py:50`](../python/lsst/rubintv/production/plotting/latissNightReportPlots.py#L50)
DM-38287 TODO.

**Status:** deliberately deferred. Each hardcodes bands as
`"SDSSr_65mm"`, `"SDSSg_65mm"`, `"SDSSi_65mm"`. LATISS has a fixed
filter wheel — unless new bands realistically appear, replacing the
hardcodes with a `BAND_TO_COLOR` map is cosmetic churn. Revisit if
either (a) LATISS gains a new filter or (b) the same plots are reused
for another single-camera instrument.

#### C6 — DM-43413 "S3 move" cleanup

**Where:** [`predicates.py`](../python/lsst/rubintv/production/predicates.py)
(`isWorldWritable`), [`allSky.py`](../python/lsst/rubintv/production/allSky.py)
(`SEQNUM_MAX` removable),
[`mosaicPlotting.py`](../python/lsst/rubintv/production/plotting/mosaicPlotting.py)
(`getInstrumentChannelName`),
[`uploaders.py`](../python/lsst/rubintv/production/uploaders.py),
[`highLevelTools.py`](../python/lsst/rubintv/production/highLevelTools.py),
[`starTracker.py`](../python/lsst/rubintv/production/starTracker.py).

**Status:** deferred. Vestiges from before the GCS→S3 cutover. Each one
is independently small but they all hinge on the frontend team having
fully switched off the v1 channel names — worth confirming first, then
doing as one focused PR labeled DM-43413. Treat as a blocking question
on the frontend side, not an internal cleanup.

### 4.2 Single-area / single-ticket work

#### S1 — DM-44102 Redis monitoring & blocking `.keys()`

**Where:** [`redisUtils.py`](../python/lsst/rubintv/production/redisUtils.py)
— four TODOs, all on `redis.keys("*PATTERN*")` calls.

**Status:** deferred. Replace `redis.keys` with a `SCAN`-backed helper.
The broader monitoring overhaul (`displayRedisDb` — the big one) is a
bigger separate piece; split it into its own ticket. Not blocking.

#### S3 — DM-45438 NV writing to ConsDB at USDF

**Where:** [`pipelineRunning.py`](../python/lsst/rubintv/production/pipelineRunning.py).

**Status:** deferred. The actual fix is upstream — NV needs to write to
a different table or know its location. The existing code already gates
via a location check inside `ConsDBPopulator`; the noise in the warn
logs could be quieted by converting the broad `except Exception` into a
silent-skip-if-non-summit path. Low priority.

#### S4 — DM-49609 Unify mountAnalysis with summit_utils

**Where:** [`mountTorques.py`](../python/lsst/rubintv/production/mountTorques.py),
[`oneOffProcessing.py`](../python/lsst/rubintv/production/oneOffProcessing.py).

**Status:** deferred. Cross-package work — needs a coordinated PR with
`summit_utils` to move `calculateMountErrors` logic there. Not a quick
win.

#### S5 — DM-52351 LATISS rotator angle handling

**Where:** [`oneOffProcessing.py`](../python/lsst/rubintv/production/oneOffProcessing.py).

**Status:** deferred. Two sub-parts: (a) figure out which EFD topic
LATISS uses for rotator data (currently hardcoded to the LSSTCam topic);
(b) once the LSSTCam path is proven stable on off-sky images, drop the
broad `try/except`. Part (a) requires domain knowledge about LATISS EFD
topic names.

#### S9 — LATISS WEP move

**Where:** [`processingControl.py`](../python/lsst/rubintv/production/processingControl.py).

**Status:** deferred. Blocked on LATISS WEP being moved into RA.

#### S10 — Remove `runCollection` from `SingleCorePipelineRunner` class state

**Where:** [`pipelineRunning.py`](../python/lsst/rubintv/production/pipelineRunning.py).

**Status:** deferred. `self.runCollection` is set per-callback from the
payload but is read by `getCollections()`. Could be threaded through as
an arg. Small refactor.

### 4.3 Smaller one-shots — not yet landed

#### O3 — `cleanup.deletePixelProducts` fold-in

**Where:** [`cleanup.py`](../python/lsst/rubintv/production/cleanup.py).

**Status:** deferred. Comment says "remove this function entirely once
the cleanup code is actually managing to finish before sunset". Requires
measuring the current main-pass runtime in production; blocked on
operational observation rather than code work.

#### O4 — Move `cleanup.py` dirs to yaml

**Where:** [`cleanup.py`](../python/lsst/rubintv/production/cleanup.py).

**Status:** deferred until the broader LocationConfig refactor. The
TODO explicitly waits for that larger work.

#### O6 — Investigate dead `visit` branch in `pipelineRunning.callback`

**Where:** [`pipelineRunning.py`](../python/lsst/rubintv/production/pipelineRunning.py)
(`# XXX is this ever true? Do we need this?`).

**Status:** deferred. Needs a production-log check to see if the branch
ever fires in real data. Add a warning log inside the branch, deploy for
one obs night, then delete if it never fires.

#### O7 — `watchers.py` dead `self.payload` attribute

**Where:** [`watchers.py`](../python/lsst/rubintv/production/watchers.py)
(`# XXX that is this for?` / `# XXX why is this being saved on the class?`).

**Status:** deferred. Needs a grep confirming nothing reads
`self.payload` from outside `RedisWatcher.run`. If nothing does, delete
both lines. Low risk, trivial — just haven't done it yet.

#### O8 — `watchers.py` RESTART_SIGNAL key cleanup

**Where:** [`watchers.py`](../python/lsst/rubintv/production/watchers.py).

**Status:** deferred. When a worker exits via restart signal, its
`+EXISTS`/`+IS_BUSY` keys are left dangling until TTL. Adding
`redisHelper.deletePodKeys(podDetails)` before `sys.exit(0)` would clean
them up. Low priority, low risk.

#### O9 — `clusterManagement.py` dead-branch probe

**Where:** [`clusterManagement.py`](../python/lsst/rubintv/production/clusterManagement.py).

**Status:** deferred. Same pattern as O6 — grep production logs for
"how did an empty set get passed here?", delete the branch if it never
fires.

#### O11 — Reduce `DonutLauncher` 10s sleep

**Where:** [`aos.py`](../python/lsst/rubintv/production/aos.py).

**Status:** moot — `DonutLauncher` is dead since ComCam decommissioning
(documented in the C8 commit). Only worth addressing if the launcher is
resurrected, in which case the broader blocking `WaitForExpRecord`
helper the TODO proposes is the right fix.

#### O12 — `locationConfig._checkDir` world-writable check

**Where:** [`locationConfig.py`](../python/lsst/rubintv/production/locationConfig.py).

**Status:** deferred. Needs confirmation that production filesystems
actually need 777 on RA-created dirs (the comment implies they do).
~5 lines of code once that's confirmed.

#### O13 — DM-33859 `mountTorques` azimuth from expRecord

**Where:** [`mountTorques.py`](../python/lsst/rubintv/production/mountTorques.py).

**Status:** deferred. Worth checking if `expRecord.azimuth_begin` exists
in the current stack — if it does, drop the `butler.get("raw.metadata",
...)` query and the `ObservationInfo` overhead. Significant runtime
saving on a hot path.

#### O14 — DM-45436 split `pipetask run` command

**Where:** [`aos.py`](../python/lsst/rubintv/production/aos.py).

**Status:** deferred / likely moot. Same reason as O11 (DonutLauncher is
dead). The TODO itself already says "may well be moot".

#### O16 — `resources.py` `S3_ENDPOINT_URL` env var side effect

**Where:** [`resources.py`](../python/lsst/rubintv/production/resources.py)
(`# XXX this almost certainly isn't good enough / won't work in many places`).

**Status:** deferred. Setting an env var as a side effect of a function
is ugly but probably load-bearing somewhere downstream. Investigate
which `lsst.resources` code path actually consumes it; if it can be set
once at process start instead, do that.

#### O18 — DM-49948 export mosaic path pattern

**Where:** [`timedServices.py`](../python/lsst/rubintv/production/timedServices.py).

**Status:** deferred. Pattern is currently hardcoded twice — once in
`mosaicPlotting.py` and once in `timedServices.py`. Export from
`mosaicPlotting`, import here. Tiny, ticketed, just haven't done it.

#### O19 — `createUnitTestCollections.py` `#isr` substep hardcode

**Where:** [`tests/createUnitTestCollections.py`](../tests/createUnitTestCollections.py).

**Status:** deferred. Waits for pipeline labels to be unified upstream.
No action needed from us.

#### O20 — Add step1b tests for all pipelines

**Where:** [`tests/test_pipelines.py`](../tests/test_pipelines.py).

**Status:** deferred. Legitimate test-coverage gap — add a parametric
`test_pipelineGenerationForStep1bForAllPipelines` that builds every
entry in `PIPELINE_NAMES` and checks the step1b graph is non-empty.

#### O21 — DM-54468 overhaul `view_ci_logs.py`

**Where:** [`tests/ci/view_ci_logs.py`](../tests/ci/view_ci_logs.py).

**Status:** deferred per the TODO itself ("needs decent ROI, not clear
it will").

### 4.4 Misc / deferred

Items that are either open questions, one-line defers, or that overlap
enough with deferred tickets above that rolling them into a separate
pass isn't worth it. Listed for completeness — the one-line summary is
the whole story.

| Location | Note |
|---|---|
| `processingControl.py` — LSSTCam-only control-key consumption | Real fix needs frontend changes; defer. |
| `processingControl.py` — "Consider whether this should move…" | Open question, not actionable. Close. |
| `processingControl.py` — "should be visitId but OK for now" | Trivial, 5-min check. |
| `processingControl.py` — `CameraControlConfig` camera-agnostic | Defer until 4th camera. |
| `pipelineRunning.py` — Post-OR3 TASK_ENDPOINTS_TO_TRACK from pipeline graph | Defer. |
| `pipelineRunning.py` — `awaitsDataProduct` inconsistency | Small API nit, defer. |
| `pipelineRunning.py` — Don't drop `calczernikes` quanta for unpaired runs | Real ticket if unpaired pipelines active. |
| `pipelineRunning.py` — FAM mode timeout | Defer. |
| `pipelineRunning.py` — BTS hardware upgrade timeout | YAML config when S2 lands. |
| `pipelineRunning.py` — Silence "no work found" warning for paired step1b AOS | Trivially silenceable. |
| `pipelineRunning.py` — Split `postProcessQuanta` into its own module | Genuine ergonomic win, ~700-line extraction. |
| `pipelineRunning.py` — Dict-style PSF/source-count metadata | Bigger, touches `mergeShardsAndUpload`. |
| `redisUtils.py` — Queue-length tracking removal | Measure first. |
| `oneOffProcessing.py` — AuxTel imexam threading | Defer unless we observe a backlog. |
| `oneOffProcessing.py` — DM-41764 `isDispersedDataId` dataId rework | Quick win if the helper now accepts real dataIds. |
| `highLevelTools.py:211` — Make `bucket` mandatory in `getPlotSeqNumsForDayObs` | Trivial. |
| `highLevelTools.py` — `remakeStarTrackerDay` post-refactor | Paired with broader StarTracker work. |
| `allSky.py` — "Add wait time message" | Two-line nice-to-have. |
| `guiders.py` — Replace `waitForIngest` with `CachingLimitedButler` | Blocked on butler upgrade. |
| `tests/ci/test_rapid_analysis.py` — Double zernike count for unpaired pipelines | Pair with unpaired-pipeline rollout. |
| `tests/ci/test_rapid_analysis.py` — DM-51391 psfAzEl plot | One-line add once the plot exists. |
| `scripts/LATISS/runSfmRunner.py` — "needs changing to defaults and the quicklook collection creating" | Investigate, likely 5-min fix. |
| `tests/test_workerSets.py` — `WorkerSet.minQueueLength` sentinel `9_999_999` documented as bug-in-waiting | Worth a real fix that returns `None` when set is empty and updates callers; the existing test pin protects against accidental drift. |
