---
status: accepted
date: 2026-10-31
---

# 0002 — Iceberg catalog: `apache/iceberg-rest-fixture` locally, AWS Glue in Phase 3

## Context and problem statement

An Iceberg catalog stores, per table, the pointer to the current `metadata.json`
and swaps it atomically on commit — this swap is what makes Iceberg commits ACID.
Trino, the Kafka Connect sink and pyiceberg must all share one catalog.
Which catalog do we run locally?

## Decision drivers

* Simplest and most reliable on a laptop (few moving parts, fast boot)
* Speaks the Iceberg REST spec, so every client is configured the same way
* Supports what we need: namespaces, tables, snapshot refs (tags) via commits
* Local is not the production target: AWS uses Glue Data Catalog

## Considered options

1. **`apache/iceberg-rest-fixture:1.10.1`** (Apache Iceberg's reference REST server)
2. Apache Polaris 1.8.0
3. Lakekeeper (not in the brief's component list)

## Decision outcome

Chosen: **option 1**, backed by SQLite on a Docker volume so tables survive
restarts. It is a thin REST adapter over Iceberg's `JdbcCatalog`; the image
bundles `S3FileIO` and the SQLite driver (verified by inspecting the jar).

### Consequences

* Good: one container, no bootstrap; healthy in ~5 s; ~300 MB RAM.
* Good: it *is* the reference implementation of the REST spec — client
  behaviour against it is the spec's behaviour.
* Bad: named a "fixture": no authn/authz, no credential vending, single node,
  SQLite. Acceptable for local; never deployed. Production uses Glue.
* Bad: no Postgres JDBC driver in the image, so SQLite (not the Airflow DB).
* Neutral: catalog type is environment config. Trino rejects unused properties,
  so the Trino catalog *file* is the environment overlay
  (`infra/trino/catalog/lakehouse.properties` locally, a Glue variant on AWS).

Polaris rejected for Phase 1: realm/principal/catalog bootstrap and S3
credential-vending configuration against a local S3 store add real complexity
with no Phase 1 payoff, since AWS uses Glue (or Glue's Iceberg REST endpoint,
evaluated in Phase 3 for config parity).

## Confirmation

`make smoke`: Trino creates a schema and a partitioned table, inserts, reads
back, lists snapshots and data files under `s3://warehouse/`.
