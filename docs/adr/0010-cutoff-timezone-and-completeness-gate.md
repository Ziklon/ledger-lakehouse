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
3. **Every partition's last record is in its bronze table**: for every partition
   of (2), `max(_kafka_offset)` among that partition's rows in its bronze table is
   ≥ `end - 1`. The sink stamps each row with its Kafka coordinates
   (`InsertField` `partition.field` / `offset.field` → `_kafka_partition`,
   `_kafka_offset`). This is exact, per table, and needs no reasoning about the
   sink's internals. Assumes CDC topics are written without Kafka transactions
   (Connect has no exactly-once source support configured, verified: last record
   = end offset − 1): a transaction marker takes an offset and would never arrive.

   **Migration fallback.** Rows written before the columns existed have NULL
   offsets. `ledger-pipeline record-offset-baseline` (bootstrap step 5) records,
   per partition of a table holding such rows, the first offset guaranteed to
   carry one (`control.bronze_offset_baseline`): it waits until a bronze row
   with an offset is visible (the single sink task runs the new config), then
   reads the end offsets. A partition whose last record is below that boundary
   (no event since the migration) can't be checked by offset, so it falls back
   to: read the source's current WAL position `L` (`pg_current_wal_lsn()` through
   the read-only `ledger_pg` catalog, same scale as `_lsn`) after (2), then wait
   for a bronze heartbeat with `_lsn > L`. That heartbeat sits in a later sink
   commit cycle than every offset (2) checked, which covers the cross-partition
   race but not a per-table commit failure (below). The fallback ends at the
   partition's next event, and the gate logs a warning naming the partitions on it.
   A fresh stack never uses it. "No offsets in bronze yet" is deliberately *not*
   the fallback criterion: it is also what a table whose commits fail looks like.

   *Revised in Phase 2 (milestone h), twice.* (a) The original text said "the
   Iceberg sink commits Kafka offsets only after the data is committed to
   Iceberg, so (2) means in bronze". Reading the 1.12.0 source proved the
   opposite order: the worker commits the source offsets in its Kafka
   transaction together with `DATA_WRITTEN` (`channel/Channel.java`, `send`),
   before the coordinator's Iceberg commit (`channel/Coordinator.java`,
   `doCommit`). So (2) proves *read*, not *visible*. Failure scenario: after an
   outage the worker reads a backlog, a new heartbeat is fetched in an earlier
   cycle than older business events on another partition (Kafka has no order
   across partitions), and the gate opens between that later cycle's worker
   transaction and its Iceberg commit. Silver misses those events, and the next
   window starts after their commit time, so they are lost from silver and gold.
   The first fix was the heartbeat-LSN check (now the fallback). (b) It still had
   a gap: the coordinator commits each table separately, in parallel
   (`Tasks.foreach(...).stopOnFailure()` in `doCommit`). If one table's commit
   fails (e.g. a conflict with `optimize`) while the heartbeat table's succeeds,
   the heartbeat table runs ahead and the LSN check passes while that table is
   a cycle behind. Hence the per-partition offset check. Both scenarios are
   reproduced deterministically against a model of the sink's protocol in
   `pipeline/tests/test_gate.py` (each red before its fix, green after; one test
   pins the fallback's remaining limit).*

   Considered: compare the heartbeat's `_source_commit_ts` with the gate host's
   clock instead of an LSN. Simpler, but it reintroduces a cross-machine clock
   comparison (driver 3). Considered: read the coordinator's
   `kafka.connect.offsets.*` snapshot property. It records *control-topic*
   offsets, not source offsets, so it can't be compared with (2). Considered:
   backfilling `_kafka_offset` for old rows by re-reading Kafka. Impossible past
   the 7-day retention, and only the boundary matters for the gate.

   *Revised during milestone (f): the brief suggests "consumer lag ≈ 0". Measured,
   the heartbeat topic **always** has lag ≥ 1 (a new heartbeat every 10 s), and in
   production business traffic continues past midnight. "Lag == 0" would wait
   forever. The snapshot formulation asks only for what existed when (1) proved
   the cut-off had passed.*

The gate compares only source-side values with source-side values (commit
timestamps in (1), Kafka offsets in (2) and (3), WAL positions in the
fallback), so a host/VM clock skew can't open the gate early.

### Consequences

* Good: provable completeness, and it works on idle days.
* Good: a down connector shows up as a gate timeout with a clear message, not
  as a short day (runbook: connector down).
* Bad: publication latency ≥ heartbeat interval + sink commit interval for (1),
  and up to one more sink commit interval for (3) (the fallback: up to one more
  heartbeat + commit): ~10 min worst case in production settings (5 min
  commits), ~20-40 s locally (10 s).
* Bad: while a partition is on the migration fallback, the gate also depends on
  the source being reachable through the `ledger_pg` catalog (read-only role).
  If Postgres is down, Debezium emits no heartbeats either, so it would block anyway.
* Bad: (3) scans `_kafka_offset` of each bronze table every poll, pruned to rows
  ≥ the lowest required offset by Parquet min/max stats. Cheap after `optimize`;
  watch it once bronze holds years of data.
* Good: a catalog outage (sink reads, Iceberg commits fail) now blocks the gate
  with its own message instead of opening it on "offsets caught up".
* Note: back-dated entries are about `effective_date`, not commit time. They
  land in the window of the day they were committed (see the back-dated
  entries ADR).
