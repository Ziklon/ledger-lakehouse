---
status: accepted
date: 2026-11-01
---

# 0016 — Back-dated entries: closed days are immutable, adjustments go into the next day

## Context and problem statement

A ledger entry can belong economically to an earlier day than the day it is
booked (`effective_date < posting_date`): a late deposit confirmation, a
correction. Day N-2 is already published (FULL, INCREMENTAL, EOD tag). Do we
reopen it?

## Considered options

1. **Closed days are immutable**: the entry is published in the batch of the
   day it was COMMITTED in the source (its window), carrying both dates
2. Restate: rebuild the affected past days and republish them
3. Hold back-dated entries for manual approval

## Decision outcome

Chosen: **option 1**.

* Windows are defined on **source commit time** (ADR 0005/0010), so a back-dated
  entry falls naturally into the current window. No special code path.
* `posting_date` (booked) and `effective_date` (economic) are both kept on the
  entry (`ledger_entries_current`), so consumers who report by economic date can
  do so, and an auditor can see the adjustment explicitly.
* The planner refuses to rerun a closed day (`windows.plan_batch`), and the EOD
  tag of a COMMITTED day is never moved (ADR 0011), so published history can't
  change silently.
* Rebuilding history as of today (restatement) is a separate, explicit operation:
  `make backfill` (Phase 2), never a side effect of a daily run.

### Consequences

* Good: what a consumer received for day N stays true forever (reproducible
  audits, `close_YYYY_MM` tags keep 7 years).
* Good: FULL(N-1) + INCREMENTAL(N) = FULL(N) still holds; the adjustment is just
  another change of day N.
* Bad: "balance as of the end of day N-2 by economic date" isn't FULL(N-2); it
  has to be computed from entries by `effective_date`.

## Confirmation

Generator scenario `sc_back_dated`: a deposit with `effective_date` 2026-01-07,
posted 2026-01-09 (entries 900209/900210), is published in batch 2026-01-09. Both
accounts appear as `U` in the 2026-01-09 `account_balances_incremental`, and the
2026-01-07 FULL partition is unchanged.
