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

## Done

*(nothing yet)*
