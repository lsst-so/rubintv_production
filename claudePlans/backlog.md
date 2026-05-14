# Rapid Analysis — Claude Project Backlog

Long-running, cross-cutting project items that aren't tied to a single DM
ticket. Individual per-ticket plans live in sibling directories like
[DM-54577/](DM-54577/); this file tracks the bigger "someday" work that
spans many tickets.

When an item is ready to be picked up, move its content into a new
`claudePlans/DM-XXXXX/` directory (or equivalent) and link the ticket here.
Mark it struck-through when complete.

## Open

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
exception (`doRaise=False`) still fires a Sentry event. There are ~33
callsites including routine expected-failure paths (`catchupService`,
`starTracker`, `timedServices`, `oneOffProcessing`), so the noise floor
on Sentry may be much higher than intended. Either add an explicit
`captureSentry: bool` arg or only capture when `doRaise=True`.

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

## Done

*(nothing yet)*
