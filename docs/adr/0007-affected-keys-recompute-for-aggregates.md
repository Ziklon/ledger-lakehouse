---
status: accepted
date: 2026-10-31
---

# 0007 — Affected-keys recompute for aggregates (`account_balances_current`)

## Context and problem statement

Balances are an aggregate over ledger entries. Each batch window brings new
entries for some accounts. How do we keep `account_balances_current` correct
without rebuilding every balance every batch?

## Decision drivers

* Correctness first: a balance must equal the sum of its entries, always
* Cost proportional to change volume, not to ledger size
* Self-healing after a bug fix or a rerun
* Output changes must be detected precisely (they feed the daily INCREMENTAL)

## Considered options

1. **Affected-keys recompute**: for accounts with entries in the window,
   recompute the full balance from all their entries; merge if the hash changed
2. Delta arithmetic: `balance = previous balance + window delta`
3. Full rebuild every batch

## Decision outcome

Chosen: **option 1**.

### Consequences

* Good: self-correcting — any account touched again is rebuilt from facts;
  a delta approach would carry a past error forever.
* Good: cost ∝ history of *touched* accounts only.
* Good: output changes ≠ input changes, explicitly: an affected account whose
  window entries net to zero (e.g. a deposit and its reversal) is recomputed
  but not merged, so it does not appear in the day's INCREMENTAL.
* Bad: a very active account with a huge history is re-summed each time it is
  touched. Mitigation if needed (Phase 2): checkpoint balances per closed day
  and recompute from the last checkpoint.
* Note: the independent recompute test (milestone e) compares this model with
  balances computed directly from bronze, catching errors in either path.

Option 2 rejected: compounding errors and order sensitivity. Option 3
rejected: cost grows with the whole ledger every batch.

## Confirmation

Milestone (c): after two incremental windows, `account_balances_current`
equals a from-scratch `GROUP BY` over the source (51/51 rows, 0 differences
either way via EXCEPT).
