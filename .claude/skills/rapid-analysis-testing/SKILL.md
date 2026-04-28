---
name: rapid-analysis-testing
description: Validate Python changes in the rapid analysis backend before declaring a task done. Neither pre-commit nor any automated CI runs mypy or the unit tests in this repo, so validation is manual and easy to skip — this skill is the checklist. Use this skill whenever you have finished editing Python under python/lsst/rubintv/production/, scripts/, or tests/ and are about to hand the task back to the user; when the user asks to "run the tests", "type check", "run mypy", "validate", or "check my changes"; or when you are about to stage a commit of Python changes. Does NOT cover the integration suite (the heavyweight thing in tests/ci/) as a routine step — that is run only when explicitly asked.
---

# Rapid Analysis: Validating Python Changes

## Terminology — read this once

This repo has two very different things people sometimes both call "tests",
and the names on disk make it worse. Get them straight before reading on:

| Term used in this skill | What it actually is | When it runs | Where it runs |
|---|---|---|---|
| **Unit tests** | `pytest tests/test_*.py` + `mypy` | **Always**, on every Python change | Any machine with the LSST stack sourced (including the user's Mac) |
| **Integration suite** | `tests/ci/test_rapid_analysis.py` — spins up a real Redis, runs distributed pipeline scripts as subprocesses | Manually, as part of pre-deployment validation | Requires a SLAC dev node with Butler access; **does not run locally** |

Notes on the misleading on-disk naming:
- `tests/ci/`, `RA_CI_*` env vars, `setup_ci_env.sh`, `RAPID_ANALYSIS_CI`,
  `runningCI()` etc. all use "CI" to mean **the integration suite**,
  *not* CI in the GitHub-Actions / Jenkins sense. There is no automated
  CI in the conventional sense for this repo.
- `.github/workflows/build_and_push.yaml` only builds the Docker image.
  It does **not** run mypy or pytest. "CI is green" tells you the image
  built, nothing else.
- A future rename of the on-disk "CI" naming to "integration suite" is
  on the backlog. Until that lands, internalise the terminology in the
  table above and translate as you read code.

## You are the validation gate

Because nothing automated runs mypy or pytest, type errors and broken
tests will land on main unless **you** run them by hand whenever you
edit Python under `python/lsst/rubintv/production/`, `scripts/`, or
`tests/`. The rest of this skill is just the checklist for doing that.

## Don't preemptively decline to test

Past failure mode: writing "I haven't run mypy/pytest because the LSST
stack isn't available in this environment" without ever attempting to
source it. The stack is sourceable on the user's Mac (it lives at
`~/stack.sh`) and on every dev node. Verify, don't assume:

- If you are about to write "I haven't run X" or "X isn't available
  here" in your handoff, **stop and run X first**. The cost is one
  bash call.
- Only after `source ~/stack.sh && . ~/setup_packages.sh` actually
  fails are you allowed to claim the stack is unavailable, and even
  then you must report the error you saw — not a guess.
- Do not cite the `no automated mypy or pytest in this repo` project
  memory as a reason to skip. That memory says manual validation is
  *required*, not optional. Treating it as licence to skip reads it
  exactly backwards.

## The validation loop

After editing any Python under `python/lsst/rubintv/production/`,
`scripts/`, or `tests/`, run these two commands before declaring the
task done. Both need the LSST stack sourced — see the
`rapid-analysis-lsst-stack` skill.

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

**Exception:** `tests/test_pipelines.py` calls `getAutomaticLocationConfig()`,
which loads `config_usdf_testing.yaml` — and that YAML now contains
`${RA_CI_DATA_ROOT}` placeholders. `LocationConfig.__post_init__`
eagerly validates *every* path in the YAML at construction time, so
without the env vars set, init fails on the first path it can't create.
Source `tests/ci/setup_ci_env.sh` before running it (or running the
full suite that includes it):

```bash
source tests/ci/setup_ci_env.sh && source ~/stack.sh && . ~/setup_packages.sh && pytest tests/test_pipelines.py -q
```

Skipping this gives a `Directory ${RA_CI_DATA_ROOT}/plots does not
exist and could not be created` error rather than a clean
missing-env-var message — see `architecture/testing.md` for the
eager-fail contract. The other unit tests don't load LocationConfig
and don't need the CI env vars.

`tests/test_locationConfig.py` itself only needs the standard stack
sourcing — it builds its own tmp-dir-rooted env vars internally.

## Before-handoff checklist

Before saying any version of "the task is done" / "ready for review" /
"this should be good to commit" on a Python change, confirm — by
checking your own scrollback, not by guessing:

1. mypy ran with the stack sourced and reported `Success: no issues found`.
2. The relevant pytest target ran and showed all green (or only Butler
   skips, which are expected off-summit).
3. If either ran with failures, you either fixed them or are explicitly
   flagging the failing items in your handoff message.

If you cannot tick all three, the right answer is to run the missing
step now, **not** to hand back with a disclaimer about why you didn't.
"I haven't run X" in a handoff is the failure mode this skill exists
to prevent.

## When to run the integration suite

The integration suite (`tests/ci/test_rapid_analysis.py`) spins up a
real Redis server and runs the full distributed pipeline end-to-end.
It takes many minutes, requires a SLAC dev node with Butler access,
and is heavier than the unit suite. Only run it when:

- The user explicitly asks ("run the integration suite", "run the CI
  suite", "validate end-to-end", etc.).
- The change touches the head-node / worker event loop, payload
  serialization, Redis queue dispatch, or pipeline graph generation —
  areas the unit tests cannot fully cover.

For routine edits, the mypy + pytest loop above is the right gate. Do
not default to running the integration suite.

### Running the integration suite (per-user setup)

The integration suite requires per-user paths and a per-user redis
port. Two scripts in `tests/ci/` handle setup (still named "ci" on
disk pending the backlogged rename):

1. **`tests/ci/preinstall_ci_deps.sh`** — once per user, installs deps
   missing from rubinenv via `pip --user` and builds `redis-server`
   from source into `${HOME}/local/bin`.
2. **`tests/ci/setup_ci_env.sh`** — every shell session, exports the
   `RA_CI_*` env vars and adds `${HOME}/local/bin` to `PATH`. Edit the
   values at the top for the running user (the file ships with
   `mfl`'s defaults).

Then:

```bash
source tests/ci/setup_ci_env.sh
source ~/stack.sh && . ~/setup_packages.sh && python tests/ci/test_rapid_analysis.py -l <label>
```

Both `test_rapid_analysis.py` and `tests/createUnitTestCollections.py`
hard-fail at startup if any required env var is unset, naming the
missing ones — if you see that, source `setup_ci_env.sh` first.

See [architecture/testing.md](../../../architecture/testing.md) for
the full env-var list, the YAML `${VAR}` substitution mechanism, and
the concurrent-run isolation story (S3 scratch and redis are per-user;
butler output chains are still shared).

## Reporting results

When you report back to the user:

- If mypy or tests failed, say so explicitly and either fix or flag.
- If you edited UI or a running system where type/unit checks don't capture
  correctness, say so — type checking verifies shape, not behavior.
- Do not claim "tests pass" if you only ran mypy, or vice versa.
- Do not bury an unrun-validation admission inside a longer handoff —
  if you didn't run it, fix that before sending the handoff.
