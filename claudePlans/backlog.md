# Rapid Analysis — Claude Project Backlog

Long-running, cross-cutting project items that aren't tied to a single DM
ticket. Individual per-ticket plans live in sibling directories like
[DM-54577/](DM-54577/); this file tracks the bigger "someday" work that
spans many tickets.

When an item is ready to be picked up, move its content into a new
`claudePlans/DM-XXXXX/` directory (or equivalent) and link the ticket here.
Mark it struck-through when complete.

## Open

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

## Done

*(nothing yet)*
