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

**Gate:** a run proceeds only when both hold:

1. **Heartbeat past the cut-off is in bronze**:
   `max(_source_commit_ts)` of `bronze.cdc_heartbeat_cdc` ≥ cut-off.
   Debezium writes the heartbeat table every `CDC_HEARTBEAT_INTERVAL_MS`, so the
   evidence exists on idle days. Logical decoding emits in commit order, so
   every earlier commit has already left Postgres.
2. **The sink has committed everything that existed at that moment**: snapshot
   the end offset of every partition of every CDC topic (`ledger\.app\..+`),
   then wait until the sink's consumer group (`connect-bronze-sink`) has
   committed at least that far on each one. The Iceberg sink commits Kafka
   offsets only after the data is committed to Iceberg, so this means "in bronze".
   Needed because the heartbeat travels on its *own* topic: check (1) alone
   proves Debezium *sent* everything, not that the sink *committed* every topic.
   A partition with no committed offset (a topic the sink never subscribed to)
   blocks too.

   *Revised during milestone (f): the brief suggests "consumer lag ≈ 0". Measured,
   the heartbeat topic **always** has lag ≥ 1 (a new heartbeat every 10 s), and in
   production business traffic continues past midnight. "Lag == 0" would wait
   forever. The snapshot formulation asks only for what existed when (1) proved
   the cut-off had passed.*

The gate uses only source-side timestamps compared with source-side timestamps,
so a host/VM clock skew can't open the gate early.

### Consequences

* Good: provable completeness, and it works on idle days.
* Good: a down connector shows up as a gate timeout with a clear message, not
  as a short day (runbook: connector down).
* Bad: publication latency ≥ heartbeat interval + sink commit interval
  (~5 min in production settings).
* Note: back-dated entries are about `effective_date`, not commit time. They
  land in the window of the day they were committed (see the back-dated
  entries ADR).
