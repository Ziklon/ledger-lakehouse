# Daily windows, FULL and INCREMENTAL

Status: **explainer.** Decisions: [ADR 0009](../adr/0009-net-daily-changes-vs-every-change.md)
(net changes), [0010](../adr/0010-cutoff-timezone-and-completeness-gate.md) (cut-off),
[0011](../adr/0011-daily-full-as-tags-and-partitions.md) (tags + partitions),
[0016](../adr/0016-back-dated-entries-policy.md) (back-dated entries),
[0008](../adr/0008-hash-diff-change-detection.md) (hashes).

## Windows tile time exactly

A batch covers source commits in `[window_start, window_end)`. Each window starts
exactly where the previous one ended; `control.batches` on the local stack:

```
 batch_id   | window_start                   | window_end
 2026-01-01 | 1970-01-01 00:00:00.000 UTC    | 2026-11-03 12:26:52.648 UTC
 2026-01-02 | 2026-11-03 12:26:52.648 UTC    | 2026-11-03 14:18:56.440 UTC
 2026-01-03 | 2026-11-03 14:18:56.440 UTC    | 2026-11-03 14:30:32.435 UTC
```

(Demo cut-offs are `now`; production uses 00:00 UTC after the business date.)

Traps in the boundaries:
* **Precision = millisecond**, the precision of `_source_commit_ts`. A first
  version rounded to seconds and pushed the last instant's commits into the next
  day; sub-ms boundaries made adjacent windows overlap after the round trip
  through `control.batches`.
* **`now` rounds UP** to the ms: rounding down would exclude commits from the
  current millisecond that are already in bronze.
* **Half-open** `[start, end)`: an event exactly at the boundary belongs to one
  day only.

## Event time, not arrival time

The filter is on `_source_commit_ts`, so an event delayed by an outage still
lands in its own day: the gate decides *when* the day may close, the commit time
decides *which* day. Bronze partitions use the same column (ADR 0005).

## Closed days are immutable

* The planner refuses to run a closed day; rerunning the **latest** COMMITTED day
  reuses its window and its EOD tag (identical output, proven by checksums).
* A COMMITTED day's tag is never moved: silver may already contain later changes.
* A back-dated entry (`effective_date` in the past) is published in the day it
  was *committed*, with both dates; past days are not restated.

## FULL vs INCREMENTAL

* `FULL(N)` = gold state built from silver `FOR VERSION AS OF 'eod_N'`.
* `INCREMENTAL(N)` = hash diff of EOD(N) vs **the previous COMMITTED** EOD, not
  "events in the window".
* Contract, checked as an invariant before COMMITTED:
  `FULL(N-1) + INCREMENTAL(N) = FULL(N)` on active rows, counts, and
  `checksum(row_hash)`.

| EOD(N-1) | EOD(N) | op |
|---|---|---|
| absent or deleted | alive | `I` |
| alive | alive, different hash | `U` |
| alive | deleted | `D` |
| otherwise | | nothing (created + deleted same day, a flap) |

Consequences that surprised us:
* The first day publishes everything as `I`, because the previous FULL is empty.
* A **flap** (ACTIVE → FROZEN → ACTIVE) publishes nothing in gold, though silver
  changed (`updated_at` moved).
* **Gold computes its own hash over exactly the published columns.** Reusing
  silver's hash (which includes `updated_at`) published flaps as `U`. That was
  found by `make demo`, not by unit tests, because the fixtures had an unchanged
  `updated_at`. Rule: every published column is in the gold hash, and nothing else.
* A missed day is covered by one INCREMENTAL against the last COMMITTED day
  (`prev_batch_id` is recorded), still correct for consumers.

## Balances: recompute, don't add

`account_balances_current` recomputes the full balance of every account touched
in the window from all its entries (ADR 0007). Adding today's delta to
yesterday's balance would carry any past error forever; recomputing heals it the
next time the account moves.

## Questions to test yourself

1. Day N-1 failed and was never committed. What does `INCREMENTAL(N)` diff against?
2. Why would hashing `updated_at` in gold break the flap case but not the invariant?
3. An entry commits at `window_end` exactly. Which batch publishes it?
