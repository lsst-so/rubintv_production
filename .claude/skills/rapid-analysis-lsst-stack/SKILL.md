---
name: rapid-analysis-lsst-stack
description: Source the LSST DM Stack before running any code from the rapid analysis backend (the repo whose on-disk python package is still called rubintv_production for historical reasons — see CLAUDE.md for the naming explanation). Use this skill whenever you need to run Python, pytest, scripts, or imports from this repo — anything under python/lsst/rubintv/production/ or scripts/ or tests/. Without sourcing the stack, every invocation fails with ModuleNotFoundError because the package lives in the lsst.* namespace and depends on sibling stack packages (lsst.daf.butler, lsst.pipe.base, lsst.summit.utils, etc.). Also apply when the user asks to "run the tests", "try importing X", "execute the CI suite", or any similar request that ultimately shells out to Python in this repo.
---

# Rapid Analysis: Sourcing the LSST DM Stack

The package lives in the `lsst.*` namespace and imports many sibling packages
from the LSST stack. Running `python`, `pytest`, or `python -c "import
lsst.rubintv..."` without the stack sourced fails with `ModuleNotFoundError`
every single time. Do not waste a turn discovering this — source the stack
first.

## The rule

**Every** Bash invocation that runs, tests, or imports package code must be
prefixed with both source commands, in this order:

```bash
source ~/stack.sh && . ~/setup_packages.sh && <your command>
```

The shell state from previous Bash tool calls does not persist, so you must
re-source on every invocation — there is no caching between calls.

## Examples

```bash
source ~/stack.sh && . ~/setup_packages.sh && pytest tests/test_podDefinition.py
source ~/stack.sh && . ~/setup_packages.sh && python -c "from lsst.rubintv.production.podDefinition import PodFlavor; print(list(PodFlavor))"
# CI suite also needs the per-user CI env vars exported:
source tests/ci/setup_ci_env.sh && source ~/stack.sh && . ~/setup_packages.sh && python tests/ci/test_rapid_analysis.py -l ci_smoke
```

Running `tests/ci/test_rapid_analysis.py` or
`tests/createUnitTestCollections.py` without first sourcing
`tests/ci/setup_ci_env.sh` exits immediately with a clear error
listing the missing `RA_CI_*` / `TARTS_DATA_DIR` / `AI_DONUT_DATA_DIR`
vars — see [architecture/testing.md](../../../architecture/testing.md)
for what each one does.

## What does not work

Do **not** try `PYTHONPATH=python python ...` or any other workaround. The
sibling stack packages are not installable via pip in this environment —
sourcing the stack is the only supported path, and `setup_packages.sh` does
the EUPS setup the stack relies on.

## Why two files?

- `~/stack.sh` sets up the base LSST Science Pipelines environment (compilers,
  Python, EUPS).
- `~/setup_packages.sh` runs `setup -k -r <pkg>` for the locally checked-out
  development packages (including this one), overlaying them on top of the
  released stack.

Both are required; sourcing only one leaves imports broken.