---
status: accepted
date: 2026-10-31
---

# 0013 — Where each kind of test runs

## Context and problem statement

The brief requires invariants, standard tests and edge-case unit tests that
"must pass on every run". These tests check different things: some check
**today's data**, some check **the code's logic**. Where should each run?

## Decision outcome

| Kind | Checks | Runs | Fails → |
|---|---|---|---|
| Invariants (`tag:invariant`): FULL+INC=FULL, double entry, recompute from bronze | the batch's **data**, across models | own pipeline step after gold, **before COMMITTED** (`dbt test --select tag:invariant`) | batch stays RUNNING, consumers keep the previous day |
| Standard tests (unique, not_null, accepted values) | a model's data | inside each `dbt build`, right after the model (Cosmos: task per model); gold scoped to the batch partition | the batch stops at that model |
| dbt unit tests (edge cases) | the macros' **logic** on fixtures | `make test` and CI on every code change; excluded from batches (`--exclude-resource-type unit_test`) | the change isn't merged |
| Source freshness | pipeline health | heartbeat strict, business tables soft (ADR 0006 / milestone c) | gate / alert |

**Deviation from the brief:** unit tests run on every *code change*, not on every
*batch*. Their result can't change between batches (fixtures, not data), so in a
batch they'd only add runtime. dbt unit tests also introspect the input relations,
which don't exist yet on the first build of a fresh stack.

### Consequences

* Good: a batch is blocked only by data problems. Logic problems are blocked before merge.
* Good: invariant cost doesn't grow with history (gold tests scoped by
  partition). Double entry is a full-ledger aggregation on purpose, so it also
  catches a lost leg (see the test's header for the large-scale variant).
* Confirmation: every invariant was shown to FAIL on deliberately corrupted data
  and to PASS after repair. Mutation tests on `cdc_current` (drop `is_deleted`
  from the hash; drop the LSN guard) turn the matching unit tests red.
