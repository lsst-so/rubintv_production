---
name: rapid-analysis-testing
description: Validate Python changes in the rapid analysis backend before declaring a task done. Neither pre-commit nor CI runs mypy or the unit tests in this repo, so validation is manual and easy to skip — this skill is the checklist. Use this skill whenever you have finished editing Python under python/lsst/rubintv/production/, scripts/, or tests/ and are about to hand the task back to the user; when the user asks to "run the tests", "type check", "run mypy", "validate", or "check my changes"; or when you are about to stage a commit of Python changes. Does NOT cover the CI integration suite as a routine step — that is a heavier separate command, only run when explicitly asked.
---

# Rapid Analysis: Validating Python Changes

Neither pre-commit nor CI runs `mypy` or the unit tests for this repo. That
means type errors and broken tests can land silently on main unless someone
runs them by hand. **You** are that someone whenever you edit Python here.

Reference material (what test files exist, mocking patterns, CI phases) lives
in [architecture/testing.md](../../../architecture/testing.md). This skill
is just the validation loop.

## The validation loop

After editing any Python under `python/lsst/rubintv/production/`, `scripts/`,
or `tests/`, run these two commands before declaring the task done. Both
need the LSST stack sourced — see the `rapid-analysis-lsst-stack` skill.

### 1. Type check (always)

```bash
source ~/stack.sh && . ~/setup_packages.sh && mypy
```

Run from the repo root with no arguments. The `mypy.ini` config sets
`files = scripts/, tests/` and `mypy_path = python`, which together cover
the whole package plus scripts and tests. **Passing a specific path (e.g.
`mypy python/...`) will miss errors in the paths you didn't name** — don't
do that, use the config.

Expected clean output: `Success: no issues found in N source files`.

### 2. Unit tests (targeted, then broad if needed)

Start with the tests most likely to exercise what you changed:

```bash
source ~/stack.sh && . ~/setup_packages.sh && pytest tests/test_<module>.py -q
```

If your change is cross-cutting (touched `redisUtils`, `payloads`,
`podDefinition`, `processingControl`, or anything imported widely), run the
full unit suite:

```bash
source ~/stack.sh && . ~/setup_packages.sh && pytest tests/ -q
```

Tests that need a live Butler (see the table in
[architecture/testing.md](../../../architecture/testing.md)) are skipped
automatically off-summit — that's expected, not a failure.

## When to run the CI integration suite

The CI suite (`tests/ci/test_rapid_analysis.py`) spins up a real Redis
server and runs the full distributed pipeline end-to-end. It takes many
minutes and is heavier than the unit suite. Only run it when:

- The user explicitly asks ("run the CI suite", "validate with CI", etc.).
- The change touches the head-node / worker event loop, payload
  serialization, Redis queue dispatch, or pipeline graph generation — areas
  the unit tests cannot fully cover.

For routine edits, the mypy + pytest loop above is the right gate. Do not
default to running the CI suite.

## Reporting results

When you report back to the user:

- If mypy or tests failed, say so explicitly and either fix or flag.
- If you edited UI or a running system where type/unit checks don't capture
  correctness, say so — type checking verifies shape, not behavior.
- Do not claim "tests pass" if you only ran mypy, or vice versa.