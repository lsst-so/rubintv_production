# rubintv_production Tech-Debt Audit

Package-wide sweep of explicit `TODO`, `XXX`, and other in-code tech-debt
markers. Generated during the DM-54577 refactor branch. Each item is
categorised by *how* you'd fix it (cluster pattern, single ticket,
judgment call, etc.) rather than alphabetically — the goal is to let
future-you work through a batch of closely-related items in one sitting.

The "Cluster patterns" and "Single-area / single-ticket work" sections
below follow the original audit taxonomy. Items prefixed with a ✅
landed on this branch as of commit [`5b7a5253`](/) ("Stop abusing
Payload.run for mosaic command dispatch") — fourteen separate commits
between [`e236252d`](/) and [`5b7a5253`](/), one per audit item, each
verified against the 204-test unit suite plus mypy and flake8 on the
full 44-file package.

**Items landed (14)**: C1, C2, C3, C5, C7, C8, S2, S6, S8/O5, O1,
O2, O10, O17, S11.

**Items intentionally deferred**: the rest — see the sections below
for the "why not now" rationale on each.

---

## Cluster patterns (one fix → many TODOs gone)

### ✅ C1 — Channel subclasses own their `MultiUploader` directly

**Landed:** [`44f528f7`](/) *Have channel subclasses own their MultiUploader directly*

**Was:** Eleven `assert self.s3Uploader is not None  # XXX why is this
necessary? Fix mypy better!` hacks across `guiders.py`, `performance.py`,
`oneOffProcessing.py` and `starTracker.py`.

**Fix:** Dropped the `addUploader` flag from `BaseChannel.__init__` /
`BaseButlerChannel.__init__` entirely. Subclasses that actually want an
uploader now set `self.s3Uploader: MultiUploader = MultiUploader()`
themselves after the super call. `SingleCorePipelineRunner` (which
explicitly opted out with `addUploader=False`) just doesn't declare the
attribute. As a side effect, caught and fixed a dormant
`StarTrackerNightReportChannel` bug — it never actually passed
`addUploader=True` to its base, so `self.s3Uploader` would have been
`None` at the plot-factory call site in production.

### ✅ C2 — DM-43764 `BaseButlerChannel` constructor cleanup

**Landed:** [`e236252d`](/) *DM-43764: drop unused detectors/channelName/dataProduct from BaseButlerChannel*

**Was:** Twelve TODOs across the four live `BaseButlerChannel`
subclasses, all variants of the same three-line block:

```python
# TODO: DM-43764 this shouldn't be necessary on the base class ...
detectors=None,
dataProduct=None,
# TODO: DM-43764 should also be able to fix needing channelName ...
channelName="",
```

**Fix:** Removed `detectors`, `channelName`, and `dataProduct` from
`BaseButlerChannel.__init__`. The two subclasses that actually pre-wait
on a dataProduct (`SingleCorePipelineRunner`, `OneOffProcessor`) now set
`self.dataProduct` themselves after the super call. Per-instance logger
names are derived from `type(self).__name__` instead of the empty
`channelName`, fixing a secondary bug where every Redis-driven worker's
log name was silently `lsst.rubintv.production.` (trailing dot).

### ✅ C3 — Matplotlib figure-leak in night report plotters

**Landed:** [`40fe6e6c`](/) *Defensively close night-report plot figures on exception*

**Was:** Seventeen `# TODO: get a figure you can reuse to avoid
matplotlib memory leak` markers across `latissNightReportPlots.py`
(13 sites) and `starTrackerNightReportPlots.py` (4 sites).

**Fix:** The happy-path leak was already handled by the base class's
`plt.close()` after `savefig`. The remaining exposure was the *exception*
path — if a subclass's `plot()` method raised between `plt.figure(...)`
and the base's `plt.close()`, the figure leaked. Wrapped the
plot/savefig sequence in both `LatissPlot.createAndUpload` and
`StarTrackerPlot.createAndUpload` in `try/finally` with
`plt.close("all")`, and deleted all seventeen stale markers.

### ✅ C5 — Deduplicate Zernike post-processing recipes

**Landed:** [`5b37a586`](/) *Extract Zernike post-processing recipes into aosRecipes module*

**Was:** Two pairs of recipes (Zernike → ConsDB dict for
`ccdvisit1_quicklook`, and aggregated Zernikes → residual FWHM for
`visit1_quicklook`) copied between `pipelineRunning.py` (live workers)
and `highLevelTools.py` (backfill helpers), each pair flagged with
matching `# NOTE: this recipe is copied and pasted to ...` comments and
a `# TODO: refactor this for proper reuse and remove this note` marker.

**Fix:** New `aosRecipes.py` module with two pure functions —
`computeRotatedZernikesForConsDB` and `computeAosResidualFwhm` — used by
both call sites. The `lsst.ts.wep` imports stay lazy inside the helper
bodies so importing `aosRecipes` from the hot path in `pipelineRunning`
doesn't pull in the full T&S software stack at module load (preserving
the original protection). A subtle behaviour-preserving detail: the old
walrus `if donutBlurFwhm := ... .get("fwhm")` truthiness check is
preserved by the helper returning `NaN` rather than `None`, and the
caller uses `not np.isnan(...)` to decide whether to emit the value.

### C4 — "Generalise this to all bands" (LATISS night reports)

**Where:** [`latissNightReportPlots.py:106,177,253,343,417,723`](python/lsst/rubintv/production/plotting/latissNightReportPlots.py)
(6 TODO sites) plus the related [`latissNightReportPlots.py:50`](python/lsst/rubintv/production/plotting/latissNightReportPlots.py#L50)
DM-38287 TODO.

**Status:** deliberately deferred. Each hardcodes bands as
`"SDSSr_65mm"`, `"SDSSg_65mm"`, `"SDSSi_65mm"`. LATISS has a fixed
filter wheel — unless new bands realistically appear, replacing the
hardcodes with a `BAND_TO_COLOR` map is cosmetic churn. Revisit if
either (a) LATISS gains a new filter or (b) the same plots are reused
for another single-camera instrument.

### C6 — DM-43413 "S3 move" cleanup

**Where:** [`predicates.py:245`](python/lsst/rubintv/production/predicates.py#L245)
(`isWorldWritable`), [`allSky.py:523`](python/lsst/rubintv/production/allSky.py#L523)
(`SEQNUM_MAX` removable), [`mosaicPlotting.py:179`](python/lsst/rubintv/production/plotting/mosaicPlotting.py#L179)
(`getInstrumentChannelName`), [`uploaders.py:761,785`](python/lsst/rubintv/production/uploaders.py#L761),
[`highLevelTools.py:433`](python/lsst/rubintv/production/highLevelTools.py#L433),
[`starTracker.py:273,274`](python/lsst/rubintv/production/starTracker.py#L273).

**Status:** deferred. Vestiges from before the GCS→S3 cutover. Each one
is independently small but they all hinge on the frontend team having
fully switched off the v1 channel names — worth confirming first, then
doing as one focused PR labeled DM-43413. Treat as a blocking question
on the frontend side, not an internal cleanup.

### ✅ C7 — Stop abusing `Payload.run` for mosaic command dispatch

**Landed:** [`5b7a5253`](/) *Stop abusing Payload.run for mosaic command dispatch*

**Was:** The two focal-plane mosaic dispatchers were shoehorning the
dataset-type string into `Payload.run` (e.g.
`Payload(dataCoord, b"", "preliminary_visit_image", who="SFM")`), and
the receiver in `mosaicPlotting.py` read it back via
`dataProduct = payload.run  # TODO: this really needs improving`.

**Fix:** Added an explicit `taskName: str | None = None` field to
`Payload` with full round-trip support in `to_json` / `from_json`. The
wire format stays backward-compatible: old payloads deserialise with
`taskName=None`, so no rolling-rollout surgery is needed. Mosaic
dispatchers now build payloads with `run=""` and `taskName=dataProduct`;
the receiver reads `payload.taskName` and logs + drops if it's missing.
Both TODO markers are gone.

### ✅ C8 — `FocusSweepAnalysis` / `DonutLauncher` to `PodDetails`

**Landed:** [`ba955224`](/) *Migrate FocusSweepAnalysis and DonutLauncher to PodDetails and type them*

**Was:** Four `# XXX still needs type annotations and to move to using
podDetails` markers across the LSSTCam / LSSTComCam / LSSTComCamSim
launcher scripts.

**Fix:** Added new `PodFlavor.FOCUS_SWEEP_ANALYZER` and
`PodFlavor.DONUT_LAUNCHER` singletons. Both classes now take
`podDetails: PodDetails`, derive the OCS queue name from
`podDetails.instrument` (so the wire contract lives in one place), and
have full type annotations. The OCS-facing queue name
(`{instrument}-FROM-OCS_FOCUSSWEEP` etc.) is a wire contract with the
OCS team, so the PodDetails' own computed queue name is unused — the
PodFlavor exists purely for identity, logging and operational monitoring.
A comment on the enum entries documents this. As a side effect, surfaced
a latent `DonutLauncher.getAosPipelineFile` AttributeError bug (the
class has been dead since ComCam decommissioning); documented with a
class-level NOTE and a scoped type-ignore so the dead code stays
visibly dead.

---

## Single-area / single-ticket work

### S1 — DM-44102 Redis monitoring & blocking `.keys()`

**Where:** [`redisUtils.py:1112,1652,1680,1778`](python/lsst/rubintv/production/redisUtils.py#L1112)
(four TODOs, all on `redis.keys("*PATTERN*")` calls).

**Status:** deferred. Replace `redis.keys` with a `SCAN`-backed helper.
The broader monitoring overhaul (`displayRedisDb` — the big one at
`:1680`) is a bigger separate piece; split it into its own ticket. Not
blocking.

### ✅ S2 — DM-50003 Data-driven ISR config + dispatch

**Landed:** [`54ebac4f`](/) *DM-50003: make ISR config dumping and image-type dispatch data-driven*

**Was:** Eighteen-line hand-unrolled `isrDict["doX"] = f"{config.doX}"`
block in `getIsrConfigDict`, and a `match imageType:` block in
`getPipelineConfig` whose four simple arms were copy-paste of
`pipelineKey = ...; who = ...` pairs.

**Fix:** Extracted a module-level `ISR_CONFIG_KEYS` tuple and replaced
the 18 lines with a one-line comprehension using `operator.attrgetter`
(which handles the dotted-path `ampOffset.doApplyAmpOffset` case natively).
Extracted the four simple dispatch arms into `_SIMPLE_IMAGE_TYPE_DISPATCH`;
the two genuinely special cases (`cwfs`, which depends on instrument and
head-node AOS state, and the default non-calib branch) stay as explicit
`elif`/`else`. The `graphBytes`/`graphs` lookup now happens once at the
bottom of the method instead of being repeated inside each arm.

### S3 — DM-45438 NV writing to ConsDB at USDF

**Where:** [`pipelineRunning.py:781,883`](python/lsst/rubintv/production/pipelineRunning.py#L781).

**Status:** deferred. The actual fix is upstream — NV needs to write to
a different table or know its location. The existing code already gates
via a location check inside `ConsDBPopulator`; the noise in the warn
logs could be quieted by converting the broad `except Exception` into a
silent-skip-if-non-summit path. Low priority.

### S4 — DM-49609 Unify mountAnalysis with summit_utils

**Where:** [`mountTorques.py:111`](python/lsst/rubintv/production/mountTorques.py#L111),
[`oneOffProcessing.py:744`](python/lsst/rubintv/production/oneOffProcessing.py#L744).

**Status:** deferred. Cross-package work — needs a coordinated PR with
`summit_utils` to move `calculateMountErrors` logic there. Not a quick
win.

### S5 — DM-52351 LATISS rotator angle handling

**Where:** [`oneOffProcessing.py:193,211`](python/lsst/rubintv/production/oneOffProcessing.py#L193).

**Status:** deferred. Two sub-parts: (a) figure out which EFD topic
LATISS uses for rotator data (currently hardcoded to the LSSTCam topic);
(b) once the LSSTCam path is proven stable on off-sky images, drop the
broad `try/except`. Part (a) requires domain knowledge about LATISS EFD
topic names.

### ✅ S6 — Dedupe `HeadProcessController.getSingleWorker`

**Landed:** [`e5f4af70`](/) *Drop duplicate HeadProcessController.getSingleWorker*

**Was:** Two near-identical `getSingleWorker(instrument, podFlavor)`
methods — one on `RedisHelper` (added in the earlier
[`7f4b2c3e`](/) plotter migration commit as the canonical helper) and
one still on `HeadProcessController` with a stale
`# TODO: until we have a real backlog queue` comment and a dead
`except IndexError` branch that was unreachable after a preceding
`if len(busyWorkers) == 0` guard.

**Fix:** Deleted the duplicate outright and switched all six call sites
in `processingControl.py` (detector fanout, mosaic dispatch, radial plot,
step1b gather, focal-plane mosaic, and the now-removed nightly rollup)
to call `self.redisHelper.getSingleWorker` directly. Removed the
orphaned `raiseIf` import that was only used by the deleted defensive
`except`.

### S7 — Covered by C2 (DM-43764)

### ✅ S8 + O5 — Remove dead `nightlyRollup` code path

**Landed:** [`de8b2faf`](/) *Remove dead nightlyRollup dispatch and trigger-task helper*

**Was:** `HeadProcessController.dispatchRollupIfNecessary` began with
an unconditional `return False  # stop running rollups until we have
some plots attached etc` — everything below was unreachable dead code.
Plus the `# TODO: remove nightlyrollup` marker in the SFM
`PipelineComponents` build, plus a zero-non-test-caller
`getNightlyRollupTriggerTask` helper carrying a
`# TODO: See if this can be removed entirely now we have finished counters`.

**Fix:** Deleted `dispatchRollupIfNecessary` and its `self.nNightlyRollups`
counter and event-loop call site. Removed `step1d-single-visit-global`
from the SFM `PipelineComponents` (collapsing the LATISS and LSSTCam SFM
branches into one common builder above the `if instrument != "LATISS":`
AOS-pipeline block). Deleted `getNightlyRollupTriggerTask` entirely
along with the six-test `GetNightlyRollupTriggerTaskTestCase` that was
its only consumer. **Deliberately left alone** so the production
deployment keeps working: `PodFlavor.NIGHTLYROLLUP_WORKER`, the three
`runNightlyWorker.py` scripts, `SingleCorePipelineRunner`'s
`step == "nightlyRollup"` handling, `RedisHelper.reportNightLevelFinished`,
and `getNightlyRollupFinishedKey`. The pods still start and sit idle on
their Redis queue (the behaviour they already had). Removing the pod
flavor is a deployment concern and belongs in a separate k8s-coordinated
change.

### S9 — LATISS WEP move

**Where:** [`processingControl.py:856`](python/lsst/rubintv/production/processingControl.py#L856).

**Status:** deferred. Blocked on LATISS WEP being moved into RA.

### S10 — Remove `runCollection` from `SingleCorePipelineRunner` class state

**Where:** [`pipelineRunning.py:492`](python/lsst/rubintv/production/pipelineRunning.py#L492).

**Status:** deferred. `self.runCollection` is set per-callback from the
payload but is read by `getCollections()`. Could be threaded through as
an arg. Small refactor.

### ✅ S11 — `displayRedisDb.getPayloadDataId` plural rename

**Landed:** [`89e944a9`](/) *Drop stale "dataIds plural" XXX from displayRedisDb.getPayloadDataId*

**Was:** `# XXX pretty sure this now crashes due to it being dataIds
plural` in `displayRedisDb`.

**Fix:** Verified by JSON round-trip that `Payload.to_json` uses the
singular `"dataId"` key and always has — the function was never broken,
the XXX was stale. Deleted the comment. Zero code change.

### S12 — `CalibrateCcdRunner` / `NightReportChannel` dead-code removal

**Where:** [`rubinTv.py:82`](python/lsst/rubintv/production/rubinTv.py#L82)
(`CalibrateCcdRunner`), [`rubinTv.py:422`](python/lsst/rubintv/production/rubinTv.py#L422)
(`NightReportChannel`).

**Status:** deferred but confirmed dead. Both classes pass `instrument=`
and `watcherType=` to `BaseButlerChannel.__init__`, which doesn't accept
those arguments — anyone who actually instantiated either would crash at
`__init__`. Their launcher scripts
(`scripts/summit/auxTel/runCalibrateCcdRunner.py`,
`scripts/summit/auxTel/runNightReporter.py`) also exist but are
equivalently dead. Partial deadness was already surfaced in C8 for
`DonutLauncher` (documented but not removed); this is the larger
related cleanup. Worth doing as one commit that deletes both classes,
their launcher scripts, and the three DM-37272/37426/37427 TODOs that
live inside `CalibrateCcdRunner`. **Requires confirming with the
auxTel team that no one is still running either script out of band.**

---

## Smaller one-shots — landed

### ✅ O1 — Drop stale `DM-XXXXX` marker on `DatabaseConflictError` import

**Landed:** [`b5f7dbf0`](/) *Drop stale "DM-XXXXX fix this import" marker from DatabaseConflictError*

Verified against `daf_butler gf51ac0d9f8+4b4a01d4e6`: `DatabaseConflictError`
is still not exported from `lsst.daf.butler` or `lsst.daf.butler.registry`,
so the deep `registry.interfaces` import is still the only path. The TODO's
placeholder ticket number was never filled in — just removed the comment.

### ✅ O2 — Use `locationConfig.getOutputChain` in `TempFileCleaner`

**Landed:** [`3311e344`](/) *Use locationConfig.getOutputChain in TempFileCleaner*

Dropped the hardcoded `COLLECTION_CHAIN = "LSSTCam/runs/quickLook/"`
module-level constant. The trailing slash was intentional (it filters
`queryCollections("*<chain>/*")` to children of the chain rather than
the bare chain root itself); preserved exactly by appending `"/"` to the
result of `getOutputChain("LSSTCam")`, with an inline comment explaining
the intent.

### ✅ O5 — Covered by S8 (above)

### ✅ O10 — Narrow broad `except` in `performance.py`

**Landed:** [`82b69345`](/) *Narrow broad except in performance.py expRecord/visitRecord lookups*

Two `except Exception` with `# XXX make this a little less broad`
comments in `getExpRecord` and `getVisitRecord`. Both functions's only
real failure mode is the `(x,) = iterable` unpacking raising
`ValueError` when the iterable is empty or has more than one element.
Narrowed to `except ValueError` with an explanatory comment.

### ✅ O17 — Modern matplotlib colormap API in `mosaicing.py`

**Landed:** [`3e4a8f30`](/) *Use modern mpl.colormaps['gray'] API in mosaicing*

Legacy `from matplotlib import cm; cmap = cm.gray  # type: ignore`
(deprecated since matplotlib 3.7) replaced with
`import matplotlib as mpl; cmap = mpl.colormaps["gray"]`. Same colormap
object, no type ignore, XXX comment removed.

---

## Smaller one-shots — not yet landed

### O3 — `cleanup.deletePixelProducts` fold-in

**Where:** [`cleanup.py:170`](python/lsst/rubintv/production/cleanup.py#L170).

**Status:** deferred. Comment says "remove this function entirely once
the cleanup code is actually managing to finish before sunset". Requires
measuring the current main-pass runtime in production; blocked on
operational observation rather than code work.

### O4 — Move `cleanup.py` dirs to yaml

**Where:** [`cleanup.py:142`](python/lsst/rubintv/production/cleanup.py#L142).

**Status:** deferred until the broader LocationConfig refactor. The
TODO explicitly waits for that larger work.

### O6 — Investigate dead `visit` branch in `pipelineRunning.callback`

**Where:** [`pipelineRunning.py:527`](python/lsst/rubintv/production/pipelineRunning.py#L527)
(`# XXX is this ever true? Do we need this?`).

**Status:** deferred. Needs a production-log check to see if the branch
ever fires in real data. Add a warning log inside the branch, deploy for
one obs night, then delete if it never fires.

### O7 — `watchers.py` dead `self.payload` attribute

**Where:** [`watchers.py:67,88`](python/lsst/rubintv/production/watchers.py#L67)
(`# XXX that is this for?` / `# XXX why is this being saved on the class?`).

**Status:** deferred. Needs a grep confirming nothing reads
`self.payload` from outside `RedisWatcher.run`. If nothing does, delete
both lines. Low risk, trivial — just haven't done it yet.

### O8 — `watchers.py` RESTART_SIGNAL key cleanup

**Where:** [`watchers.py:83`](python/lsst/rubintv/production/watchers.py#L83).

**Status:** deferred. When a worker exits via restart signal, its
`+EXISTS`/`+IS_BUSY` keys are left dangling until TTL. Adding
`redisHelper.deletePodKeys(podDetails)` before `sys.exit(0)` would clean
them up. Low priority, low risk.

### O9 — `clusterManagement.py:633` dead-branch probe

**Where:** [`clusterManagement.py:633`](python/lsst/rubintv/production/clusterManagement.py#L633).

**Status:** deferred. Same pattern as O6 — grep production logs for
"how did an empty set get passed here?", delete the branch if it never
fires.

### O11 — Reduce `DonutLauncher` 10s sleep

**Where:** [`aos.py:238`](python/lsst/rubintv/production/aos.py#L238).

**Status:** moot — `DonutLauncher` is dead since ComCam decommissioning
(documented in C8's commit). Only worth addressing if the launcher is
resurrected, in which case the broader blocking `WaitForExpRecord`
helper the TODO proposes is the right fix.

### O12 — `locationConfig._checkDir` world-writable check

**Where:** [`locationConfig.py:91`](python/lsst/rubintv/production/locationConfig.py#L91).

**Status:** deferred. Needs confirmation that production filesystems
actually need 777 on RA-created dirs (the comment implies they do).
~5 lines of code once that's confirmed.

### O13 — DM-33859 `mountTorques` azimuth from expRecord

**Where:** [`mountTorques.py:150`](python/lsst/rubintv/production/mountTorques.py#L150).

**Status:** deferred. Worth checking if `expRecord.azimuth_begin` exists
in the current stack — if it does, drop the `butler.get("raw.metadata",
...)` query and the `ObservationInfo` overhead. Significant runtime
saving on a hot path.

### O14 — DM-45436 split `pipetask run` command

**Where:** [`aos.py:252`](python/lsst/rubintv/production/aos.py#L252).

**Status:** deferred / likely moot. Same reason as O11 (DonutLauncher is
dead). The TODO itself already says "may well be moot".

### O15 — Covered by C6 (DM-43413 S3 cleanup)

### O16 — `resources.py:79` `S3_ENDPOINT_URL` env var side effect

**Where:** [`resources.py:79`](python/lsst/rubintv/production/resources.py#L79)
(`# XXX this almost certainly isn't good enough / won't work in many places`).

**Status:** deferred. Setting an env var as a side effect of a function
is ugly but probably load-bearing somewhere downstream. Investigate
which `lsst.resources` code path actually consumes it; if it can be set
once at process start instead, do that.

### O18 — DM-49948 export mosaic path pattern

**Where:** [`timedServices.py:686`](python/lsst/rubintv/production/timedServices.py#L686).

**Status:** deferred. Pattern is currently hardcoded twice — once in
`mosaicPlotting.py` and once in `timedServices.py`. Export from
`mosaicPlotting`, import here. Tiny, ticketed, just haven't done it.

### O19 — `createUnitTestCollections.py` `#isr` substep hardcode

**Where:** [`tests/createUnitTestCollections.py:189`](tests/createUnitTestCollections.py#L189).

**Status:** deferred. Waits for pipeline labels to be unified upstream.
No action needed from us.

### O20 — Add step1b tests for all pipelines

**Where:** [`tests/test_pipelines.py:96`](tests/test_pipelines.py#L96).

**Status:** deferred. Legitimate test-coverage gap — add a parametric
`test_pipelineGenerationForStep1bForAllPipelines` that builds every
entry in `PIPELINE_NAMES` and checks the step1b graph is non-empty.

### O21 — DM-54468 overhaul `view_ci_logs.py`

**Where:** [`tests/ci/view_ci_logs.py:20`](tests/ci/view_ci_logs.py#L20).

**Status:** deferred per the TODO itself ("needs decent ROI, not clear
it will").

---

## Misc / deferred

Items that are either open questions, one-line defers, or that overlap
enough with deferred tickets above that rolling them into a separate
pass isn't worth it. Listed here for completeness — the one-line
summary is the whole story.

| Location | Note |
|---|---|
| [`processingControl.py:791`](python/lsst/rubintv/production/processingControl.py#L791) | LSSTCam-only control-key consumption. Real fix needs frontend changes; defer. |
| [`processingControl.py:921`](python/lsst/rubintv/production/processingControl.py#L921) | "Consider whether this should move…" — open question, not actionable. Close. |
| [`processingControl.py:1309`](python/lsst/rubintv/production/processingControl.py#L1309) | "should be visitId but OK for now" — trivial, 5-min check. |
| [`processingControl.py:1477`](python/lsst/rubintv/production/processingControl.py#L1477) | `CameraControlConfig` camera-agnostic — defer until 4th camera. |
| [`pipelineRunning.py:89,90`](python/lsst/rubintv/production/pipelineRunning.py#L89) | Post-OR3 TASK_ENDPOINTS_TO_TRACK from pipeline graph. Defer. |
| [`pipelineRunning.py:147`](python/lsst/rubintv/production/pipelineRunning.py#L147) | `awaitsDataProduct` inconsistency — small API nit, defer. |
| [`pipelineRunning.py:235`](python/lsst/rubintv/production/pipelineRunning.py#L235) | Don't drop `calczernikes` quanta for unpaired runs — bug for unpaired AOS, real ticket if unpaired pipelines active. |
| [`pipelineRunning.py:335`](python/lsst/rubintv/production/pipelineRunning.py#L335) | FAM mode timeout — defer. |
| [`pipelineRunning.py:514`](python/lsst/rubintv/production/pipelineRunning.py#L514) | BTS hardware upgrade timeout — yaml config when S2 lands. |
| [`pipelineRunning.py:561`](python/lsst/rubintv/production/pipelineRunning.py#L561) | Silence "no work found" warning for paired step1b AOS — trivially silenceable. |
| [`pipelineRunning.py:653`](python/lsst/rubintv/production/pipelineRunning.py#L653) | Split `postProcessQuanta` into its own module — genuine ergonomic win, ~700-line extraction. |
| [`pipelineRunning.py:665`](python/lsst/rubintv/production/pipelineRunning.py#L665) | Dict-style PSF/source-count metadata — bigger, touches `mergeShardsAndUpload`. |
| [`redisUtils.py:1105`](python/lsst/rubintv/production/redisUtils.py#L1105) | Queue-length tracking removal — measure first. |
| [`oneOffProcessing.py:821`](python/lsst/rubintv/production/oneOffProcessing.py#L821) | AuxTel imexam threading — defer unless we observe a backlog. |
| [`oneOffProcessing.py:854`](python/lsst/rubintv/production/oneOffProcessing.py#L854) | DM-41764 `isDispersedDataId` dataId rework — quick win if the helper now accepts real dataIds. |
| [`highLevelTools.py:211`](python/lsst/rubintv/production/highLevelTools.py#L211) | Make `bucket` mandatory — trivial. |
| [`highLevelTools.py:501`](python/lsst/rubintv/production/highLevelTools.py#L501) | `remakeStarTrackerDay` post-refactor — paired with broader StarTracker work. |
| [`allSky.py:638`](python/lsst/rubintv/production/allSky.py#L638) | "Add wait time message" — two-line nice-to-have. |
| [`catchupService.py:49`](python/lsst/rubintv/production/catchupService.py#L49) | Catchup for imExam/specExam/metadata — big speculative work, defer. |
| [`catchupService.py:269`](python/lsst/rubintv/production/catchupService.py#L269) | Move auxtel movie to its own channel — defer. |
| [`guiders.py:127`](python/lsst/rubintv/production/guiders.py#L127) | Replace `waitForIngest` with `CachingLimitedButler` — blocked on butler upgrade. |
| [`Dockerfile:106`](Dockerfile#L106) | DM-43475 resync RA images — out-of-scope DevOps. |
| [`tests/ci/test_rapid_analysis.py:577`](tests/ci/test_rapid_analysis.py#L577) | Double zernike count for unpaired pipelines — pair with unpaired-pipeline rollout. |
| [`tests/ci/test_rapid_analysis.py:1190`](tests/ci/test_rapid_analysis.py#L1190) | DM-51391 psfAzEl plot — one-line add once the plot exists. |
| [`scripts/LATISS/runSfmRunner.py:53`](scripts/LATISS/runSfmRunner.py#L53) | "needs changing to defaults and the quicklook collection creating" — investigate, likely 5-min fix. |
| [`tests/test_workerSets.py`](tests/test_workerSets.py) | `WorkerSet.minQueueLength` sentinel `9_999_999` documented as bug-in-waiting — worth a real fix that returns `None` when set is empty and updates callers; the existing test pin protects against accidental drift. |

---

## Recovery notes

This file was written after auto-compact kicked in on the original audit
conversation, so it is rebuilt from two sources: (a) the fourteen landed
commit messages, which embed the rationale for each landed item, and (b)
a fresh `grep TODO|XXX|FIXME|HACK` sweep of the package that confirms
the "not yet landed" items still exist in the current tree. Line
numbers on unlanded items are valid as of this file's creation time;
re-grep if the tree has moved on significantly.

The commits this audit cross-references live on the
`claude_refactor/DM-54577` branch:

```
e236252d  DM-43764: drop unused detectors/channelName/dataProduct from BaseButlerChannel
44f528f7  Have channel subclasses own their MultiUploader directly
5b37a586  Extract Zernike post-processing recipes into aosRecipes module
e5f4af70  Drop duplicate HeadProcessController.getSingleWorker
de8b2faf  Remove dead nightlyRollup dispatch and trigger-task helper
54ebac4f  DM-50003: make ISR config dumping and image-type dispatch data-driven
b5f7dbf0  Drop stale "DM-XXXXX fix this import" marker from DatabaseConflictError
3311e344  Use locationConfig.getOutputChain in TempFileCleaner
82b69345  Narrow broad except in performance.py expRecord/visitRecord lookups
3e4a8f30  Use modern mpl.colormaps['gray'] API in mosaicing
89e944a9  Drop stale "dataIds plural" XXX from displayRedisDb.getPayloadDataId
40fe6e6c  Defensively close night-report plot figures on exception
ba955224  Migrate FocusSweepAnalysis and DonutLauncher to PodDetails and type them
5b7a5253  Stop abusing Payload.run for mosaic command dispatch
```
