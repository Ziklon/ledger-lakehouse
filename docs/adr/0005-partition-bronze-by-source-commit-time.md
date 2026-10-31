---
status: accepted
date: 2026-10-31
---

# 0005 — Partition bronze by `day(_source_commit_ts)`, not ingestion time

## Context and problem statement

Bronze tables need a partition spec. Candidates are the time the change was
committed in the source, or the time it arrived in the lake. Daily batches
select events by commit-time window `[window_start, window_end)`.

## Decision drivers

* Batch windows are defined in source commit time (watermarks in control.batches)
* Late or replayed events (connector down for hours, re-snapshot, backfill)
  must land in the business day they belong to
* Partition pruning for the daily window query
* Idempotent reruns of a day must read the same files

## Considered options

1. **`day(_source_commit_ts)`** — Iceberg hidden partitioning on commit time
2. `day(_ingested_at)` — arrival time
3. No partitioning (rely on file-level min/max stats)

## Decision outcome

Chosen: **option 1**. `_source_commit_ts` is Debezium's `source.ts_ms`: the
commit time of the source transaction, identical for all events of one
transaction.

### Consequences

* Good: a connector outage delays events but does not move them to the wrong
  day; the completeness gate (ADR 0010) decides *when* a day may close.
* Good: the window query prunes to 1–2 partitions; hidden partitioning means
  queries filter on the column itself, no derived `dt` column to forget.
* Bad: late events append to an *older* partition, which can be small-file
  heavy; handled by compaction (DAG `optimize` task).
* Note: `source.ts_ms` has millisecond precision and ties are common (a whole
  demo day can commit within the same millisecond). **Ordering always uses
  `_lsn`**, never the timestamp; the timestamp is only for windowing.
* `_ingested_at` is kept as a column for latency monitoring
  (`_ingested_at - _source_commit_ts`, ~1.5 s measured locally).

## Confirmation

`"ledger_entries_cdc$partitions"` shows `_source_commit_ts_day` partitions;
window queries in milestone (c) show pruning in `EXPLAIN ANALYZE`.
