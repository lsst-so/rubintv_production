---
name: rapid-analysis-testing
description: Validate Python changes in the rapid analysis backend before declaring a task done, and write new tests with their intent visible in the source. Neither pre-commit nor CI runs mypy or the unit tests in this repo, so validation is manual and easy to skip — this skill is the checklist. Use this skill whenever you have finished editing Python under python/lsst/rubintv/production/, scripts/, or tests/ and are about to hand the task back to the user; when the user asks to "run the tests", "type check", "run mypy", "validate", or "check my changes"; when you are about to stage a commit of Python changes; or when you are adding a new test or test case. Does NOT cover the CI integration suite as a routine step — that is a heavier separate command, only run when explicitly asked.
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

Start with the tests most likely to exercise what you changed. For a
single file or single test, serial is fine:

```bash
source ~/stack.sh && . ~/setup_packages.sh && pytest tests/test_<module>.py -q
```

If your change is cross-cutting (touched `redisUtils`, `payloads`,
`podDefinition`, `processingControl`, or anything imported widely), run
the full unit suite — and always parallelise it with `-n logical` (one
worker per logical CPU, via pytest-xdist, which is baked into the
image):

```bash
source ~/stack.sh && . ~/setup_packages.sh && pytest tests/ -q -n logical
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

## Writing tests: make the regression visible in the source

In this repo, the *why* of a test belongs in the test file, not in the
commit message. Commit messages are read once; the test is read every
time it fails. A reader who hits a failing test should be able to
answer "what kind of bug did I just introduce?" from the test source
alone, without `git log` or `git blame`.

This is the deliberate house style, and it overrides the more general
default of "don't comment code." For tests, lean toward more comments
than you would in production code.

### What to put where

- **Module docstring** — name the kinds of regression this whole file
  catches. "These tests catch (1) accessor key drift between code and
  YAML, (2) new accessors added without validation, (3) validation
  rule drift." A reader scanning the file should learn the threat
  model before reading any specific assertion.
- **Class docstring** — for grouped tests, summarise the contract
  being pinned (e.g. "argv[1] wins, env var is fallback, missing both
  raises") rather than just "tests for X".
- **Test-level comment** — one short comment at the top of each test
  saying what regression it catches and, if non-obvious, *why* the
  current behaviour matters. "Production guard against a silently-
  misconfigured bucket name — empty bucketName previously meant the
  YAML key was set but blank" is far more useful than "test empty
  bucket name raises".
- **Explicit lists (`_FOO_KEYS = (...)`)** — when a test iterates over
  a hand-maintained list, say in a nearby comment that the list is
  the bug-catching mechanism: a new accessor that bypasses validation
  should surface as a missing entry rather than a silent pass.

### What this looks like in practice

Don't write tests whose only documentation is the test name:

```python
# bad — reader has to reverse-engineer the intent from the assertion
def test_emptyBucketNameRaises(self) -> None:
    cfgDict["bucketName"] = ""
    with self.assertRaises(RuntimeError):
        cfg.bucketName
```

Write tests that explain themselves:

```python
# good — reader knows what bug this pins and why it matters
def test_emptyBucketNameRaises(self) -> None:
    # Production guard: an empty bucketName has previously meant the
    # YAML key was added but never set for this site. The accessor
    # must raise rather than hand back "", which would later show up
    # as silently-failing S3 uploads.
    cfgDict["bucketName"] = ""
    with self.assertRaises(RuntimeError):
        cfg.bucketName
```

When in doubt, ask: "if this test fails six months from now and someone
who didn't write it has to fix it, do they have what they need from
the file alone?" If the answer is "they'd have to read the commit that
added the test," the comment is missing.

## Reporting results

When you report back to the user:

- If mypy or tests failed, say so explicitly and either fix or flag.
- If you edited UI or a running system where type/unit checks don't capture
  correctness, say so — type checking verifies shape, not behavior.
- Do not claim "tests pass" if you only ran mypy, or vice versa.