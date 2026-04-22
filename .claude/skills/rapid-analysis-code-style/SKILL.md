---
name: rapid-analysis-code-style
description: Apply the rapid analysis Python code style — camelCase identifiers, PascalCase classes, built-in type hints with "| None" (never typing.Optional/List/Dict), numpydoc docstrings with types, and black/isort formatting at line-length 110. Use this skill whenever writing or editing any Python code in this repo (adding a function, method, class, refactoring, renaming, writing a test). The conventions here diverge deliberately from PEP 8 (camelCase is the house style, inherited from the wider LSST stack) and from typing conventions common elsewhere in Python — do not "fix" them to PEP 8 even if they look unusual.
---

# Rapid Analysis: Python Code Style

These conventions are deliberate and diverge from PEP 8. They match the
wider LSST stack, which this package is part of. Apply them whenever you
write or edit Python in this repo.

## Naming

- **camelCase** for all variables, functions, methods, and attributes.
- **PascalCase** for classes.
- **No snake_case** except when required by external APIs (e.g. a library
  forces it on you).
- **All function/method names must contain a verb**, including private ones:
  `getTrackingKey` not `trackingKey`. The one exception is `fromX`
  classmethod constructors (`fromDict`, `fromYaml`), which don't need a verb.
- **Prefer descriptive names over cryptic abbreviations.** Write
  `step1aDispatched = isStep1aDispatched()` rather than `s1aD =
  self.isStep1aDispatched()`. This isn't an absolute rule — `dets` for
  `detectors` is fine if the context is clear, and established "terms of
  art" (`expId`, `dayObs`, `seqNum`) should be preserved rather than
  expanded.

## Formatting

- **black** with line-length 110.
- **isort** with the black profile.
- Run via the pre-commit hooks; don't reformat manually and end up fighting
  the tools.

## Type Annotations

- Use **built-in types**: `int`, `str`, `float`, `dict`, `list`, `tuple`,
  `set`.
- **Never** import `Dict`, `List`, `Tuple`, `Optional`, or `Union` from
  `typing`. (Other things from `typing` — `Callable`, `Iterable`,
  `Protocol`, `TYPE_CHECKING`, etc. — are fine.)
- Use `X | None` instead of `Optional[X]`.
- Use `X | Y` instead of `Union[X, Y]`.

```python
# good
def foo(things: list[int], name: str | None = None) -> dict[str, int]: ...

# bad — do not write code like this in this repo
from typing import Optional, List, Dict
def foo(things: List[int], name: Optional[str] = None) -> Dict[str, int]: ...
```

## Docstrings

- **numpydoc format.**
- Include a type for **every parameter** and for the return value (unless
  the return type is `None`, in which case omit the `Returns` section
  entirely — don't write "returns None").
- Always **name the return value** in the `Returns` section.
- If a parameter is typed `X | None`, describe its docstring type as
  `` `X`, optional ``.
- **Argument order in the docstring must match the function signature**,
  and the types must be correct — stale docstrings that disagree with the
  signature are a bug.
- **No docstrings on `__init__`** — document the class itself instead.

### Canonical example

```python
def myFunction(param1: int, param2: str | None = None) -> bool:
    """This function does something.

    Parameters
    ----------
    param1 : `int`
        The first parameter.
    param2 : `str`, optional
        The second parameter.

    Returns
    -------
    result : `bool`
        The result of the function.
    """
    return param1 > 0 and param2 != "hello"
```

Note the backticks around types, the `, optional` suffix for `| None`
parameters, and the named return value (`result`).

## Tooling

- **flake8** — linter; runs automatically via pre-commit.
- **black** and **isort** — formatters; run automatically via pre-commit.
- **mypy** — type checker; **not** run by pre-commit or CI, so you must run
  it by hand on any Python change before declaring a task done. See the
  `rapid-analysis-testing` skill for the command and the validation loop.
- Build: LSST SCons + pyproject.toml.

Pre-commit catches most style slips on commit, but apply these conventions
while writing rather than relying on the hook to flag them — the hook will
fail your commit, and then you have to re-edit and re-stage anyway.