---
status: accepted
date: 2026-10-31
---

# 0004 — Bronze is an append-only change log, not a mirror

## Context and problem statement

CDC delivers a stream of row-level events. Bronze can either *apply* them
(upsert/delete — a mirror of the source) or *append* them (one row per event).
Bronze is the permanent system of record and the replay source for every
downstream layer.

## Decision drivers

* Auditability: reconstruct any key's state at any point in time
* Replay: rebuild silver/gold (or a new model) from bronze alone, after Kafka's
  7-day retention has expired
* Simplicity and cost of the write path (continuous, every few minutes)
* Recoverability from downstream bugs (a wrong merge must not destroy history)

## Considered options

1. **Append-only log**: one row per event, unique key `(pk, _lsn)`
2. Mirror via the sink's upsert mode (equality deletes on `pk`)
3. Both: log + mirror written by the sink

## Decision outcome

Chosen: **option 1**. The Iceberg sink appends; Debezium's envelope is
flattened on the sink side (Kafka keeps the full envelope for other consumers)
and each row carries `_op`, `_lsn`, `_source_commit_ts`, `_ingested_at`.
The mirror is built in silver by dbt (`*_current`), where it is testable.

Naming: single-level namespaces (`lakehouse.bronze.accounts_cdc`) instead of
the brief's `bronze.app.accounts_cdc`, because AWS Glue Data Catalog (Phase 3)
does not support nested namespaces. The source schema stays `app` in Postgres.

### Consequences

* Good: full history per key (verified: a key with c,u,u,u events; a key with
  c,d in one day) — the "bronze is a log, silver is a mirror" property.
* Good: appends are the cheapest Iceberg write (no equality deletes, no
  merge-on-read debt in bronze); downstream bugs never destroy history.
* Good: exactly-once is checkable: `count(*) = count(DISTINCT (pk, _lsn))`.
* Bad: "current state" needs computation (`row_number() over (partition by pk
  order by _lsn desc)`); paid in silver, incrementally per window.
* Bad: bronze grows with change volume, not entity count; mitigated by
  partitioning (ADR 0005) and compaction, and it is the price of auditability.
* Rule: additive schema evolution only — bronze is a superset of every schema
  the source has had; columns are never dropped.

## Confirmation

Milestone (b) evidence: per-key event histories; `(pk, _lsn)` uniqueness
(56/56, 144/144); federated reconciliation source vs bronze (row counts and
18-decimal amount sums equal); no duplicates after a SIGKILL of Connect mid-traffic.
