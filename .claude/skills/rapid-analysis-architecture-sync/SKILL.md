---
name: rapid-analysis-architecture-sync
description: Keep the architecture/ docs in sync with code changes in the rapid analysis backend. Use this skill whenever a change touches the distributed system's shape — Redis keys/formats/TTLs, pod flavors or queue naming, pipeline stages or task chaining, the gather/fanout mechanism, head-node or worker event loop logic, focal plane control or detector fanout, payload serialization, or CI test phases. The architecture/*.md files are the canonical source of truth for the system's design; letting them drift from the code is the single biggest way this project accumulates onboarding debt, so update the relevant doc in the same commit as the code change — not "later". Also apply when the user asks to "document this change", "update the architecture", or reviews a PR that obviously needs doc updates.
---

# Rapid Analysis: Architecture Doc Sync

The `architecture/` directory holds the canonical design docs for the
distributed system. They must stay in sync with the code — a stale design
doc is worse than no doc, because a reader will trust it.

When a change touches the shape of the system, update the relevant doc
**in the same change** as the code, not as a follow-up.

## The three docs

| File | Covers |
|------|--------|
| [architecture/architecture.md](../../../architecture/architecture.md) | Overall system design, pod types/flavors, pipeline stages, focal plane layout, detector fanout, gather mechanism, exposure flow |
| [architecture/redis-coordination.md](../../../architecture/redis-coordination.md) | Redis key names/formats/TTLs, work distribution, pod health, task tracking, control signals |
| [architecture/testing.md](../../../architecture/testing.md) | Unit tests, CI integration suite, CI phases, test data, mocking patterns |

## Change → doc mapping

Use this table to decide which doc(s) need updating. If a change spans
multiple rows, update **all** the named docs.

| If your change touches… | Update |
|--------------------------|--------|
| Redis key names, formats, or TTLs | `redis-coordination.md` |
| Pod flavors, types, or queue naming | `architecture.md` **and** `redis-coordination.md` |
| Pipeline stages, task chaining, or the gather mechanism | `architecture.md` |
| Head node or worker event loop logic | `architecture.md` |
| Focal plane control or detector fanout logic | `architecture.md` |
| Payload serialization format | `architecture.md` |
| Test infrastructure or CI phases | `testing.md` |

If the change doesn't fit any of these rows, the architecture docs
probably don't need updating — but skim the headings of the relevant doc
as a sanity check before skipping.

## How to update

1. Make the code change.
2. Open the matching doc(s) from the table.
3. Find the section that describes the thing you changed — tables of pod
   flavors, lists of Redis keys, diagrams of the event loop, etc.
4. Update the doc so it describes the **new** state, not a diff from the
   old state. The doc is read by new contributors who have no memory of
   the old behavior; they don't need "formerly known as X" annotations.
5. Include the doc edits in the same commit as the code change.

## Don't

- Don't leave a "TODO: update architecture doc" comment and move on — the
  TODO will outlive you.
- Don't write a changelog entry in the architecture doc. Changelogs belong
  in git history; the arch doc describes the current state.
- Don't duplicate content across docs. If something belongs in
  `redis-coordination.md`, link to it from `architecture.md` rather than
  copying.