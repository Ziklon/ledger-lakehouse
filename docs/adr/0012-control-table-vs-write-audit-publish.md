---
status: accepted
date: 2026-10-31
---

# 0012 — Control table (`control.batches`) instead of Write-Audit-Publish

## Context and problem statement

Consumers must only ever see complete, verified batches, and each batch's
window must be derived from what was actually published. Do we gate visibility
with a control table or with Iceberg Write-Audit-Publish (branches)?

## Considered options

1. **`control.batches`**: one Iceberg row per batch (window, watermarks, counts,
   checksums, status RUNNING → COMMITTED). Consumer views read only COMMITTED.
2. WAP on Iceberg branches: write to an audit branch, fast-forward `main` on success

## Decision outcome

Chosen: **option 1** for Phase 1. WAP is on the Phase 3 roadmap as an alternative.

* Writer: `ledger_pipeline` (`open_batch`, `commit_batch`). dbt only receives
  the batch as vars and never writes this table.
* `COMMITTED` is the last write, one UPDATE (= one atomic Iceberg commit).
  `open_batch` is one MERGE.
* Window rules (`windows.plan_batch`, unit-tested): `window_start` = latest
  COMMITTED `window_end`, so missed days are caught up. Closed days are refused.
  A rerun of the latest COMMITTED day reuses its exact window. At most one batch is
  in flight.
* Consumer views (`gold.accounts`, `gold.account_balances`) use one macro,
  `latest_committed_batch_date()`. During a rerun the batch is RUNNING, so they
  fall back to the previous day instead of showing a half-rewritten partition.

### Consequences

* Good: one place answers "what's published, up to which LSN, with which
  counts and checksums". It's the audit trail and the input to reconciliation (Phase 2).
* Good: engine-agnostic. Any SQL engine can apply the COMMITTED filter, and
  publication spans several tables, which a single-table branch can't cover atomically.
* Bad: visibility depends on consumers using the gold views. Reading
  `*_full_daily` partitions directly bypasses the gate (mitigated by access control, Phase 2).
* Bad: a crashed batch stays RUNNING until it's rerun (by design: it blocks new
  batches, so nobody skips it unknowingly).
