---
status: accepted
date: 2026-11-01
---

# 0015 — Transformations in dbt-trino, not Spark

## Context and problem statement

Silver and gold are incremental MERGEs, aggregations and diffs over Iceberg.
Which engine runs them? The brief excludes Spark from Phase 1, but the choice
should hold for production too.

## Considered options

1. **dbt-trino** (SQL models + macros, Trino executes)
2. Spark (PySpark jobs, or dbt-spark) on a cluster
3. Python (pyiceberg + DuckDB/Polars) jobs

## Decision outcome

Chosen: **option 1**.

* The work is set-based SQL: latest event per key, MERGE, GROUP BY, hash diff.
  One reusable macro per pattern (`cdc_latest_event`, `cdc_current`, `gold_publish`)
  makes adding a table about 5 lines of configuration.
* Trino is needed anyway: federation with the Postgres source (reconciliation,
  Phase 2) and the consumer query engine. One engine fewer to operate.
* Iceberg features used: MERGE (v2 row-level), time travel to tags
  (`FOR VERSION AS OF 'eod_…'`, verified on Trino 484), `optimize`, metadata tables.
* dbt brings tests, unit tests, docs and lineage, and Cosmos maps it to Airflow tasks.

### Consequences

* Good: runs on a laptop (Trino 3 GB) and identically on EKS (Phase 3).
* Good: incremental cost tracks daily change volume (window filters prune
  bronze partitions; MERGE only touches changed keys).
* Bad: Trino isn't a fault-tolerant batch engine by default; a worker loss
  fails the query. Mitigated by small idempotent steps (Airflow retries) and, at
  scale, Trino fault-tolerant execution (exchange spooling to S3).
* Bad: maintenance that Trino lacks (e.g. rewrite with sort orders at scale,
  the changelog view) goes to occasional Spark (EMR Serverless) in Phase 3, not
  to the daily path.
* Option 2 rejected: a cluster to operate for MB-to-GB daily deltas. Option 3
  rejected: no MERGE/transaction story across tables at the same level, and it duplicates
  what the SQL engine already does.
