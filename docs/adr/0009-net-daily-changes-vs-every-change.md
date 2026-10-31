---
status: accepted
date: 2026-10-31
---

# 0009 — INCREMENTAL publishes net daily changes, not every change

## Context and problem statement

Each business day a consumer receives `<entity>_incremental`. A key can change
many times in a day (status flaps, create-then-delete, delete-then-re-insert).
Do we publish every change event, or one net change per key?

## Decision drivers

* Contract: `FULL(N-1) + INCREMENTAL(N) = FULL(N)`, provable every run
* Consumers apply a day in one simple pass (upsert I/U, delete D)
* FULL and INCREMENTAL must agree by construction (same cut-off)
* Full event history must still exist somewhere (audit)

## Considered options

1. **Net change per key**: hash diff of the EOD(N) tag vs the EOD(N-1) tag
2. Every change event of the day (a changelog)
3. Every key with any event in the window

## Decision outcome

Chosen: **option 1**, implemented once in `macros/gold_publish.sql`. Op is
defined on *alive* rows (present and not soft-deleted):

| EOD(N-1) | EOD(N) | op |
|---|---|---|
| absent or deleted | alive | `I` (new or re-inserted) |
| alive | alive, different `row_hash` | `U` |
| alive | deleted | `D` (last known values carried) |
| anything else | | nothing (e.g. created and deleted the same day, a flap) |

### Consequences

* Good: the invariant is exact on active rows and `checksum(row_hash)`:
  verified for 5 days (milestone d), enforced as a test in (e).
* Good: a day's INCREMENTAL size is proportional to net change, not activity.
* Good: rerun-safe. Both sides are immutable tags, so a rerun reproduces it.
* Bad: intraday history isn't in gold. It stays in bronze (the append-only
  log, ADR 0004), and Phase 2 can add a changelog view for auditors.
* Bad: a missed day is published as one INCREMENTAL covering the gap
  (diff against the last COMMITTED EOD). That's still correct for consumers,
  because `prev_batch_id` is recorded in `control.batches`.

Option 2 rejected: consumers would need ordering logic, and the result
can't be checked against FULL in one equation. Option 3 rejected: publishes
noise (flaps) as changes.
