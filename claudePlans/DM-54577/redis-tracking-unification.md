# Unified Exposure Tracking in Redis

## Context

Per-exposure tracking state is spread across independent Redis keys with
independent lifecycles. When `EXPECTED_DETECTORS` (2.5-day TTL) expires
while `DETECTOR_FINISHED_COUNTER` (no TTL) persists, `nFinished >= 0`
evaluates true, causing false gather triggers and reprocessing.

The fix unifies all per-exposure state into a single Redis hash with a
single TTL, making it impossible for expected and finished counts to have
different lifetimes. We also take this opportunity to track per-detector
completion (not just counts) and add step1b lifecycle tracking.

## Design

### New Redis keys

**Per-exposure hash** (replaces EXPECTED_DETECTORS, DETECTOR_FINISHED_COUNTER,
AOS_PIPELINE_CONFIG):
```
Key:    {instrument}-TRACKING-{expId}
TTL:    2.5 days (set once on creation)
Fields:
  _initialized                  "1" sentinel (forces hash creation + TTL)
  {who}:expected                comma-separated detector IDs (head node writes)
  {who}:finished:{det}          "1" per detector (workers HSET - atomic)
  {who}:failed:{det}            "1" per detector (workers HSET - atomic)
  {who}:step1a_dispatched       "1" flag (head node, after gather triggers)
  {who}:step1b_dispatched       "1" flag (head node, after step1b dispatch)
  {who}:step1b_finished         "1" flag (worker, after step1b completes)
  pipeline_config               AOS pipeline name string
```

Workers report per-detector completion via `HSET key {who}:finished:{det} 1`.
This is atomic (single field write) and gives full per-detector visibility.
No `HINCRBY` needed — completion is checked via set membership:
`finished_detectors >= expected_detectors`.

**Active exposures set** (replaces HGETALL enumeration on finished counter):
```
Key:    {instrument}-ACTIVE-EXPOSURES
Type:   SET (no TTL, self-heals via stale entry cleanup)
```

### Data structure: `ExposureProcessingInfo`

Dataclass in `redisUtils.py`, parsed from HGETALL of the tracking hash.
Uses `set[int]` for detectors throughout:

```python
@dataclass
class ExposureProcessingInfo:
    expId: int
    expected: dict[str, set[int]]     # who -> set of expected detector IDs
    finished: dict[str, set[int]]     # who -> set of finished detector IDs
    failed: dict[str, set[int]]       # who -> set of failed detector IDs
    step1a_dispatched: dict[str, bool]  # who -> gather dispatched?
    step1b_dispatched: dict[str, bool]  # who -> step1b dispatched?
    step1b_finished: dict[str, bool]    # who -> step1b finished?
    pipeline_config: str | None

    @classmethod
    def fromRedisHash(cls, expId: int, fields: dict[str, str]) -> Self:
        """Parse HGETALL result into structured object.

        Field naming convention:
          {who}:expected         -> comma-separated detector list
          {who}:finished:{det}   -> "1" per completed detector
          {who}:failed:{det}     -> "1" per failed detector
          {who}:step1a_dispatched -> "1"
          {who}:step1b_dispatched -> "1"
          {who}:step1b_finished  -> "1"
          pipeline_config        -> string
        """

    def getExpectedDetectors(self, who: str) -> set[int]: ...
    def getFinishedDetectors(self, who: str) -> set[int]: ...
    def getFailedDetectors(self, who: str) -> set[int]: ...
    def getMissingDetectors(self, who: str) -> set[int]:
        """expected - finished — useful for debugging."""
    def isComplete(self, who: str) -> bool:
        """finished >= expected (set superset-or-equal)."""
    def isStep1aDispatched(self, who: str) -> bool: ...
    def isStep1bDispatched(self, who: str) -> bool: ...
    def isStep1bFinished(self, who: str) -> bool: ...
    def markStep1aDispatched(self, who: str) -> None:  # mutate Python-side
    def allGathersDispatched(self) -> bool:
        """All whos with non-empty expected sets have step1a_dispatched."""
```

### Parsing HGETALL into ExposureProcessingInfo

The `fromRedisHash` classmethod iterates over decoded hash fields:
- Fields matching `{who}:expected` → parse comma-separated string into
  `set[int]`, add to `expected[who]`
- Fields matching `{who}:finished:{det}` → extract who and det,
  add det to `finished[who]`
- Fields matching `{who}:failed:{det}` → same pattern for `failed[who]`
- Fields matching `{who}:step1a_dispatched` → `step1a_dispatched[who] = True`
- Fields matching `{who}:step1b_dispatched` → `step1b_dispatched[who] = True`
- Fields matching `{who}:step1b_finished` → `step1b_finished[who] = True`
- `pipeline_config` → string
- `_initialized` → skip

## Files to modify

### 1. redisUtils.py

**Add** `ExposureProcessingInfo` dataclass before `RedisHelper` class.
Add it to `__all__`.

**Add new `RedisHelper` methods:**

| Method | Redis ops | Called by |
|--------|-----------|-----------|
| `initExposureTracking(instrument, expId)` | SADD active set + HSET `_initialized` + EXPIRE | Head node fanout |
| `setExpectedDetectors(instrument, expId, detectors, who, append=False)` | HGET (if append) + HSET comma-sep string | Head node fanout |
| `removeExpectedDetectors(instrument, expId, detectors, who)` | HGET + parse + remove + HSET (head node only) | Head node failure handler |
| `getExpectedDetectors(instrument, expId, who)` | HGET, parse → `list[int]` | Mosaic plotter, postISR mosaic |
| `reportDetectorFinished(instrument, expId, who, detector, failed=False)` | HSET `{who}:finished:{det}` (+ `{who}:failed:{det}` if failed) | Workers |
| `getExposureProcessingInfo(instrument, expId)` | HGETALL → ExposureProcessingInfo | Head node gather |
| `getActiveExposures(instrument)` | SMEMBERS → `set[int]` | Head node gather |
| `markStep1aDispatched(instrument, expId, who)` | HSET `{who}:step1a_dispatched` | Head node gather |
| `markStep1bDispatched(instrument, expId, who)` | HSET `{who}:step1b_dispatched` | Head node gather |
| `markStep1bFinished(instrument, expId, who)` | HSET `{who}:step1b_finished` | Step1b worker |
| `completeExposure(instrument, expId)` | SREM (does NOT del hash — TTL handles it) | Head node gather |
| `setAosPipelineConfig(instrument, expId, name)` | HSET `pipeline_config` | Head node fanout |
| `getAosPipelineConfig(instrument, expId)` | HGET `pipeline_config` | Head node gather |

Note: `getExpectedDetectors` keeps a compatible signature returning
`list[int]` for existing consumers (mosaicPlotting, dispatchPostIsrMosaic).
Internally it reads from the tracking hash.

**Remove old methods:**
- `writeDetectorsToExpect`, `removeDetectorsToExpect`
- `reportDetectorLevelFinished`, `getAllIdsForDetectorLevel`,
  `getNumDetectorLevelFinished`, `removeFinishedIdDetectorLevel`
- Old `recordAosPipelineConfig`, old `getAosPipelineConfig`

**Update** `clearTaskCounters` to clear `*TRACKING*` and
`*ACTIVE-EXPOSURES*` patterns (replace old `*EXPECTED_DETECTORS*` and
`*DETECTOR_FINISHED_COUNTER*` patterns).

**Keep unchanged:** per-task counters (`reportTaskFinished` etc.),
pod health methods, visit-level counters.

Note: the `step` parameter is always `"step1a"` at every call site, so it
is dropped from the new API entirely.

### 2. processingControl.py

**`doAosFanout` (~line 960):**
```python
# Before:
self.redisHelper.writeDetectorsToExpect(..., "AOS")
self.redisHelper.writeDetectorsToExpect(..., "ISR")
self.redisHelper.recordAosPipelineConfig(...)

# After:
self.redisHelper.initExposureTracking(self.instrument, expRecord.id)
self.redisHelper.setExpectedDetectors(..., detectorIds, "AOS")
self.redisHelper.setExpectedDetectors(..., detectorIds, "ISR", append=True)
self.redisHelper.setAosPipelineConfig(...)
```

**`doDetectorFanout` (~line 993):**
Add `initExposureTracking` at the top of the method (before the
LATISS/FAM branching) so it runs for all paths including FAM.
Replace `writeDetectorsToExpect` calls. Replace
`recordAosPipelineConfig` (FAM path, line 1015) with
`setAosPipelineConfig`.

```python
# ISR always appends (AOS may have already written CWFS detectors)
self.redisHelper.setExpectedDetectors(instrument, expRecord.id, detectorIds, "ISR", append=True)
# The who-specific key does NOT append (first write for this who)
self.redisHelper.setExpectedDetectors(instrument, expRecord.id, detectorIds, who)
```

**`_dispatchPayloads` failure handler (~line 1136):**
Replace `removeDetectorsToExpect` with `removeExpectedDetectors`.

**`dispatchGatherSteps` (~line 1215) - main rewrite:**
```python
def dispatchGatherSteps(self, who: str) -> bool:
    activeIds = self.redisHelper.getActiveExposures(self.instrument)
    if not activeIds:
        return False

    completeIds: list[int] = []
    infoMap: dict[int, ExposureProcessingInfo] = {}

    for expId in activeIds:
        info = self.redisHelper.getExposureProcessingInfo(self.instrument, expId)
        if info is None:
            self.redisHelper.completeExposure(self.instrument, expId)  # stale cleanup
            continue

        expected = info.getExpectedDetectors(who)
        if not expected or info.isStep1aDispatched(who):
            continue  # no work for this who, or already dispatched

        if info.isComplete(who):  # finished >= expected (set comparison)
            completeIds.append(expId)
            infoMap[expId] = info

        if len(info.getFinishedDetectors(who)) > len(expected):
            self.log.warning(...)

    if not completeIds:
        return False

    for expId in completeIds:
        info = infoMap[expId]

        # ... existing dispatch logic for step1b, one-off, etc. ...
        # Use info.getFinishedDetectors(who) where count was needed
        # Use self.redisHelper.getAosPipelineConfig() as before

        # After dispatching step1b:
        self.redisHelper.markStep1bDispatched(self.instrument, expId, who)

        # Mark step1a gather as dispatched
        self.redisHelper.markStep1aDispatched(self.instrument, expId, who)
        info.markStep1aDispatched(who)  # update Python-side

        if info.allGathersDispatched():
            self.redisHelper.completeExposure(self.instrument, expId)

    return True
```

**`dispatchPostIsrMosaic` (~line 1390):**
Add `nExpected > 0` guard (per-task counters kept separate):
```python
if nExpected > 0 and nFinished >= nExpected:
```
`getExpectedDetectors` keeps same signature, reads from tracking hash.

### 3. pipelineRunning.py

**`callback` (~line 596-622):**

For step1a completion, pass the detector ID:
```python
# Before:
self.redisHelper.reportDetectorLevelFinished(
    self.instrument, "step1a", who=who, processingId=expId
)

# After:
detector = int(payload.dataId["detector"])
self.redisHelper.reportDetectorFinished(
    self.instrument, expId, who=who, detector=detector
)
```
Same for failure path (~line 621), adding `failed=True`.

For step1b completion, add tracking hash update:
```python
# After existing reportVisitLevelFinished (keep that too):
if self.step == "step1b":
    self.redisHelper.reportVisitLevelFinished(...)  # keep existing
    self.redisHelper.markStep1bFinished(self.instrument, expId, who=who)
```

### 4. plotting/mosaicPlotting.py (line 118)

**No change needed.** `getExpectedDetectors` keeps compatible signature
returning `list[int]`. Reads from tracking hash internally.

### 5. architecture/redis-coordination.md

Update sections 4 (Task Completion Tracking), 5 (Expected Detectors),
6 (AOS Pipeline Configuration), Work Distribution Flow, Timeout Summary,
and Design Patterns to reflect the new unified tracking hash design.

## Edge cases handled

1. **FAM images**: `doAosFanout` is skipped but `initExposureTracking` at
   the top of `doDetectorFanout` ensures the hash exists before
   `setAosPipelineConfig` writes to it.

2. **LATISS / LSSTComCam**: `doAosFanout` returns early.
   `initExposureTracking` from `doDetectorFanout` is sufficient.

3. **Partial fanout crash**: `initExposureTracking` sets TTL proactively
   via `_initialized` sentinel, so hash always expires even if subsequent
   writes crash.

4. **Stale active set entries**: `dispatchGatherSteps` cleans up entries
   whose tracking hash has expired (`info is None -> completeExposure`).

5. **Mosaic plotter timing**: Tracking hash is NOT deleted on completion
   (only SREM from active set). TTL cleanup preserves it for async
   consumers like the mosaic plotter.

6. **`completeExposure` idempotency**: `SREM` is naturally idempotent.

7. **Worker atomicity**: Each worker writes a single `HSET` for its
   detector. No read-modify-write, no race conditions between workers.

8. **Hash size**: ~400 fields max for LSSTCam (189 SFM finished + 8 AOS
   finished + expected strings + flags). HGETALL is still microseconds.

## Implementation order

1. Add `ExposureProcessingInfo` dataclass to `redisUtils.py`
2. Add all new `RedisHelper` methods (additive, no impact on existing code)
3. Update `pipelineRunning.py` worker reporting (step1a + step1b)
4. Update `processingControl.py` head node (fanout + gather + postISR)
5. Remove old `RedisHelper` methods
6. Update `clearTaskCounters`
7. Update `architecture/redis-coordination.md`

Steps 3-4 must deploy together (workers and head node must agree on
key format). No backwards compatibility shim needed -- clear Redis and
redeploy during daytime downtime.

## Verification

- Run existing CI integration test (`tests/ci/test_rapid_analysis.py`) --
  it checks step1b completion and MTAOS zernike counts, which are
  downstream of the tracking changes
- Check CI test's `_check_failure_keys()` still works (key patterns changed)
- Manual verification: `redis-cli HGETALL {instrument}-TRACKING-{expId}`
  after a fanout to inspect per-detector fields, and
  `SMEMBERS {instrument}-ACTIVE-EXPOSURES` to see active set
- Verify that after all gathers dispatch, the expId is removed from the
  active set but the tracking hash persists until TTL
