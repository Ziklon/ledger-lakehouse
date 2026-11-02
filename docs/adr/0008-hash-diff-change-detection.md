---
status: accepted
date: 2026-10-31
---

# 0008 — Hash-diff change detection, with `is_deleted` in the hash

## Context and problem statement

Silver mirrors and gold INCREMENTALs need to know which keys *actually*
changed. A key can have events in a window without a net change (a status
flap ACTIVE→FROZEN→ACTIVE), and a delete changes nothing in the carried-forward
business columns. How do we detect real changes?

## Decision drivers

* Detect net changes, not event activity
* Detect deletes even when business columns are unchanged
* Idempotent reruns: same window twice → zero writes
* Cheap comparison, one column, usable by order-independent checksums

## Considered options

1. **`row_hash = dbt_utils.generate_surrogate_key(business columns + is_deleted)`**,
   compare with the stored hash, merge only differences
2. Column-by-column comparison in SQL (`a.x is distinct from b.x or ...`)
3. Treat every key with an event in the window as changed

## Decision outcome

Chosen: **option 1**, at two levels with **different column sets**:

| | Hashed columns | Why |
|---|---|---|
| silver `*_current` (`macros/cdc_current.sql`) | every mirrored source column incl. `updated_at`, + `is_deleted` | silver is an exact mirror (reconciliation compares it with the source) |
| gold FULL/INCREMENTAL (`macros/gold_publish.sql`) | exactly the **published** columns, + `is_deleted` | INCREMENTAL = hash diff of EOD(N) vs EOD(N-1); the invariant uses `checksum(row_hash)` |

*Revised in milestone (g).* The first version reused silver's hash in gold.
The clean `make demo` run showed a same-day flap (ACTIVE→FROZEN→ACTIVE) being
published as `U`: the source sets `updated_at = now()` on every update, so the
net row *did* change, but only in a bookkeeping column. Gold now publishes
business columns only (`updated_at` is not published) and hashes exactly what
it publishes. Rule: **every published column is in the gold hash**, otherwise
`FULL(N-1) + INCREMENTAL(N) = FULL(N)` would hold on hashes but not on values.
For the same reason `last_changed_batch_id` (silver `_batch_id`, moved by a flap)
is no longer published; lineage stays in silver.

`is_deleted` is in the hash because deletes carry business columns forward
unchanged (ADR 0006): without it, the old and new hashes are equal and a
delete would never reach silver or the INCREMENTAL.

Technical metadata (`_last_lsn`, `_last_commit_ts`, `_batch_id`, `deleted_at`)
is deliberately excluded: it changes without a business change.

### Consequences

* Good: net-change semantics for consumers (a flap publishes nothing); deletes detected.
* Good: silver stays an exact mirror (a flap updates `updated_at` there).
* Good: idempotent — verified: rerunning a window merges 0 rows and leaves
  `checksum(row_hash)` identical.
* Good: one column powers merge filtering, INCREMENTAL diffs and invariants.
* Bad: adding a business column changes every hash → a one-off "everything
  changed" diff. Schema changes must be released with a planned re-baseline
  (Phase 2 schema-evolution scenarios).
* Bad: MD5-based; collisions are irrelevant at this scale but the hash is not
  a security control.

Option 2 rejected: verbose, easy to forget a column, NULL-unsafe by default.
Option 3 rejected: publishes noise (flaps) as changes.

## Confirmation

Milestone (c): a later-window delete (key 43) is merged; reruns of W2 and of
the older W1 merge 0 rows (fingerprint unchanged). *Correction:* (c) also
reported "flap keys end ACTIVE with no extra write". That held only for fixtures
with an unchanged `updated_at`, which real source updates never produce.

Milestone (g), `make demo` prints every key updated several times in one
window, its status path, and what gold published. Before the fix: flap
accounts 7 and 27 (`FROZEN -> ACTIVE`) were published as `U`. After it, flaps publish
nothing, and `FROZEN -> ACTIVE -> FROZEN` publishes `U` (status really changed).
