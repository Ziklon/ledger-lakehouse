---
status: accepted
date: 2026-10-31
---

# 0010 — Cut-off at 00:00 UTC and the completeness gate

## Context and problem statement

A daily batch covers source commits in `[window_start, window_end)`. Ingestion
is continuous and asynchronous (WAL → Debezium → Kafka → sink → Iceberg commit
every few minutes). How do we know, at publish time, that bronze holds **every**
commit before the cut-off?

## Decision drivers

* Never publish an incomplete day silently
* Work on idle days (no business events)
* No dependency on wall-clock agreement between machines
* Precision: windows must tile time exactly (no gap, no overlap)

## Decision outcome

**Cut-off:** 00:00 UTC after the business date (`windows.calendar_cutoff`).
UTC because the source commits in UTC and finance closes need one
unambiguous instant. Demo mode passes an explicit cut-off (`now`).
Boundaries use **millisecond** precision, the precision of `_source_commit_ts`
(Debezium `source.ts_ms`) and of Trino's `from_iso8601_timestamp`. A `now`
cut-off is rounded **up** to the millisecond. Rounding down (to seconds, as
a first version did) pushed commits from the last instant into the next day,
and sub-ms boundaries made adjacent windows overlap after the round trip
through `control.batches`. The planner refuses non-aligned cut-offs.

**Gate:** a run proceeds only when all three hold, checked in this order:

1. **Heartbeat past the cut-off is in bronze**:
   `max(_source_commit_ts)` of `bronze.cdc_heartbeat_cdc` ≥ cut-off.
   Debezium writes the heartbeat table every `CDC_HEARTBEAT_INTERVAL_MS`, so the
   evidence exists on idle days. Logical decoding emits in commit order, so
   every earlier commit has already left Postgres.
2. **The sink has read everything that existed at that moment**: snapshot
   the end offset of every partition of every CDC topic (`ledger\.app\..+`),
   then wait until the sink's consumer group (`connect-bronze-sink`) has
   committed at least that far on each one.
   Needed because the heartbeat travels on its *own* topic: check (1) alone
   proves Debezium *sent* everything, not that the sink *read* every topic.
   A partition with no committed offset (a topic the sink never subscribed to)
   blocks too.
3. **What the sink read is committed to Iceberg**: read the source's current
   WAL position `L` (`pg_current_wal_lsn()` through the read-only `ledger_pg`
   federation catalog, on the same numeric scale as bronze `_lsn`), then wait
   until bronze holds a heartbeat with `_lsn > L`. That heartbeat was written
   after (2) was observed, so it sits in a later sink commit cycle than every
   offset (2) checked. The coordinator commits cycles one at a time and carries
   a failed cycle's files into the next commit, so the later heartbeat being
   visible implies those offsets' rows are visible too.

   *Revised in Phase 2 (milestone h, backlog `gate-iceberg-commit-race`): the
   original text said "the Iceberg sink commits Kafka offsets only after the data
   is committed to Iceberg, so (2) means in bronze". Reading the 1.12.0 source
   proved the opposite order: the worker commits the source offsets in its Kafka
   transaction together with `DATA_WRITTEN` (`channel/Channel.java`, `send`),
   before the coordinator's Iceberg commit (`channel/Coordinator.java`,
   `doCommit`). So (2) proves *read*, not *visible*. Failure scenario: after an
   outage the worker reads a backlog, a new heartbeat is fetched in an earlier
   cycle than older business events on another partition (Kafka has no order
   across partitions), and the gate opens between that later cycle's worker
   transaction and its Iceberg commit. Silver misses those events, and the next
   window starts after their commit time, so they are lost from silver and gold.
   Reproduced deterministically against a model of the sink's protocol in
   `pipeline/tests/test_gate.py` (red before (3), green after).*

   **Residual risk (open, backlog `bronze-tx-id`):** the coordinator commits each
   table separately, in parallel (`Tasks.foreach(...).stopOnFailure()` in
   `doCommit`). If one table's commit fails (e.g. a conflict with `optimize`) while
   the heartbeat table's succeeds, the heartbeat table runs ahead, and (3) can pass
   while that table is a cycle behind. Closing it needs per-table evidence: with
   `_kafka_partition`/`_kafka_offset` in bronze, the last check becomes "for every
   partition, bronze holds offset `end - 1`", which is exact and independent of
   the sink's internals. Until then the independent recompute invariant
   (`balances_match_bronze_recompute`) is the backstop for ledger entries.

   Considered: compare the heartbeat's `_source_commit_ts` with the gate host's
   clock instead of an LSN. Simpler, but it reintroduces a cross-machine clock
   comparison (driver 3). Considered: read the coordinator's
   `kafka.connect.offsets.*` snapshot property. It records *control-topic*
   offsets, not source offsets, so it can't be compared with (2).

   *Revised during milestone (f): the brief suggests "consumer lag ≈ 0". Measured,
   the heartbeat topic **always** has lag ≥ 1 (a new heartbeat every 10 s), and in
   production business traffic continues past midnight. "Lag == 0" would wait
   forever. The snapshot formulation asks only for what existed when (1) proved
   the cut-off had passed.*

The gate compares only source-side values with source-side values (commit
timestamps in (1), WAL positions in (3)), so a host/VM clock skew can't open
the gate early.

### Consequences

* Good: provable completeness, and it works on idle days.
* Good: a down connector shows up as a gate timeout with a clear message, not
  as a short day (runbook: connector down).
* Bad: publication latency ≥ heartbeat interval + sink commit interval for (1),
  and up to the same again for (3): ~10 min worst case in production settings
  (5 min commits), ~20 s locally (10 s).
* Bad: the gate now also depends on the source being reachable through the
  `ledger_pg` federation catalog (read-only role). If Postgres is down, Debezium
  emits no heartbeats either, so the gate would block anyway.
* Good: a catalog outage (sink reads, Iceberg commits fail) now blocks the gate
  with its own message instead of opening it on "offsets caught up".
* Note: back-dated entries are about `effective_date`, not commit time. They
  land in the window of the day they were committed (see the back-dated
  entries ADR).
