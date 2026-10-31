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

Chosen: **option 1**. Implemented in `macros/cdc_current.sql`; the same
`row_hash` is reused by gold (INCREMENTAL = hash diff of EOD(N) vs EOD(N-1))
and by the invariant `checksum(row_hash)`.

`is_deleted` is in the hash because deletes carry business columns forward
unchanged (ADR 0006): without it, the old and new hashes are equal and a
delete would never reach silver or the INCREMENTAL.

Technical metadata (`_last_lsn`, `_last_commit_ts`, `_batch_id`, `deleted_at`)
is deliberately excluded: it changes without a business change.

### Consequences

* Good: net-change semantics (a flap produces no write); deletes detected.
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

Milestone (c): the flap keys (7, 12) end ACTIVE with no extra write; a
later-window delete (key 43) is merged; reruns of W2 and of the older W1
merge 0 rows (fingerprint unchanged).
