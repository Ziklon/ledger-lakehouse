---
status: accepted
date: 2026-10-31
---

# 0001 — Bronze writer: Apache Iceberg Kafka Connect sink, built from source (1.12.0)

## Context and problem statement

Debezium CDC events land in Kafka topics. Something must append them to Iceberg
bronze tables continuously, with exactly-once semantics, schema evolution and
one table per source table. Which component writes bronze?

## Decision drivers

* Exactly-once into Iceberg (no duplicate events in the system of record)
* Additive schema evolution following the source (bronze is a superset of every schema)
* Runs in the existing Kafka Connect worker (no extra JVM on a 16 GB laptop)
* Same component on AWS (Strimzi KafkaConnect on EKS) — config-only differences
* Pinned, verifiable artifact (supply chain)

## Considered options

1. **Apache Iceberg Kafka Connect sink 1.12.0, built from the ASF source release**
2. Apache Iceberg Kafka Connect sink 1.9.2, prebuilt ASF bundle from Confluent Hub
3. Small Python consumer using pyiceberg
4. Redpanda Iceberg Topics (broker-native)

## Decision outcome

Chosen: **option 1**. The runtime bundle is not published to Maven Central (only
the thin `iceberg-kafka-connect` jar is), so `infra/connect/Dockerfile` builds
`:iceberg-kafka-connect:iceberg-kafka-connect-runtime:distZip` in a multi-stage
build from the ASF tarball, verified against the published SHA-512.

The sink gives us, by configuration only: exactly-once via a coordinator and a
control topic (commits Kafka offsets *inside* the Iceberg snapshot), table
auto-create, `evolve-schema-enabled`, dynamic routing by a record field, and a
default partition spec for auto-created tables.

### Consequences

* Good: latest Iceberg; same Iceberg version family as Trino/pyiceberg clients.
* Good: exactly-once is built in; we don't hand-roll offset/snapshot atomicity.
* Good: one Connect worker hosts source + sink → ~1 GB RAM saved.
* Bad: a cold build costs ~5–10 min and a JDK/Gradle toolchain. Mitigated by
  Docker layer cache + BuildKit `--mount=type=cache` for `~/.gradle`; CI uses
  the buildx GHA cache or pulls a prebuilt image from GHCR.
* Bad: sink built against Kafka 3.x/4.x client APIs runs in a Kafka 4.3 worker;
  relies on Connect plugin isolation. Verified in milestone (b).
* Neutral: option 2 remains a one-line fallback (download instead of build).

Option 3 rejected: we would re-implement exactly-once (offsets + snapshot
atomicity), schema evolution and retries — the core of the problem.
Option 4 rejected: couples bronze to the broker vendor; MSK (Phase 3) has no
equivalent, breaking local/AWS parity.

## Confirmation

`GET /connector-plugins` lists `org.apache.iceberg.connect.IcebergSinkConnector`;
milestone (b) shows events in `bronze` tables with no duplicates after a Connect
restart (count of `(pk, _lsn)` = distinct count).
