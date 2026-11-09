# The Iceberg sink's commit protocol, and what the gate can prove

Status: **explainer.** The decision lives in [ADR 0010](../adr/0010-cutoff-timezone-and-completeness-gate.md);
this note explains the mechanics behind it and the traps found while building it.
Source read: Apache Iceberg Kafka Connect sink 1.12.0 (`kafka-connect/.../channel/`).

## The question

At publish time: *does bronze hold every source commit before the cut-off?*
Ingestion is asynchronous (WAL → Debezium → Kafka → sink → Iceberg), so "the
connector is RUNNING" or "lag is small" proves nothing.

## Who does what inside the sink

One connector task runs two roles:

| Role | Consumer group | Does |
|---|---|---|
| **Worker** (every task) | `connect-bronze-sink` (the source topics) | reads CDC records, writes Parquet into open files |
| **Coordinator** (one task) | `connect-bronze-sink-coord` (the `_iceberg_control` topic) | every `commit.interval-ms` starts a commit, collects files, commits to Iceberg |

One commit cycle, in order:

```
Coordinator                    _iceberg_control                 Worker
    | START_COMMIT  ------------------>|------------------------->|
    |                                  |   closes its files (uploads to S3)
    |                                  |   ONE Kafka transaction:
    |                                  |     DATA_WRITTEN (file list)
    |                                  |     DATA_COMPLETE
    |                                  |     commit SOURCE offsets   <-- (A) Channel.java:97-106
    |<---------------------------------|<-------------------------|
    | Iceberg commit, ONE PER TABLE, in parallel                  <-- (B) Coordinator.java:200-211
    |   (Tasks.foreach(...).stopOnFailure())
    | commit control-topic offsets (group -coord), snapshot summary
    |   kafka.connect.offsets.<control>.<group>
```

**(A) happens before (B).** Source offsets prove the sink has *read* a record,
not that the record is *visible* in bronze. That single fact drives the whole
gate design.

## Three traps, in the order we hit them

### 1. "Lag == 0" never happens

The heartbeat topic always has lag ≥ 1: Debezium writes a new heartbeat every
`CDC_HEARTBEAT_INTERVAL_MS` (10 s). In production, business traffic also
continues past midnight. Fix: **snapshot** the end offsets at the moment check 1
passes and wait only for those (ADR 0010, revised in milestone f).

### 2. Offsets move before rows are visible (the cross-partition race)

Kafka has no order across partitions. After an outage the worker reads a
backlog; a *new* heartbeat (after the cut-off) can be fetched in an earlier
cycle than an *older* ledger entry on another topic:

```
cycle k   : heartbeat H (after cut-off)      -> committed, visible     => check 1 passes
cycle k+1 : ledger entry E (before cut-off)  -> (A) done, (B) not yet  => check 2 passes
```

Gate open, E not in bronze. Silver misses E; day N+1's window starts after E's
commit time, so E is lost from silver and gold. Reproduced deterministically in
`pipeline/tests/test_gate.py::test_offsets_committed_before_the_iceberg_commit_do_not_pass_the_gate`.

### 3. Tables commit separately

(B) is one commit per table. If `ledger_entries` fails (e.g. a conflict with
`optimize`) while `cdc_heartbeat_cdc` succeeds, the heartbeat table runs ahead.
Any check based only on heartbeats passes while `ledger_entries` is a cycle
behind (`test_a_table_whose_iceberg_commits_fail_blocks_the_gate`).

## What the gate checks now

1. A heartbeat with `_source_commit_ts ≥ cut-off` is **in bronze**.
2. The sink's committed source offsets ≥ the end offsets snapshotted then (*read*).
3. For every partition, `max(_kafka_offset)` in **its own bronze table** ≥
   `end − 1` (*visible*, per table, exact).

Check 3 is possible because the sink stamps every row with its Kafka coordinates
(`InsertField` → `_kafka_partition`, `_kafka_offset`). Assumes CDC topics are
not transactional (verified: last record = end offset − 1); `_iceberg_control`
is transactional but is not a CDC topic.

**Migration fallback.** Rows written before those columns existed have NULL
offsets. `control.bronze_offset_baseline` records, per partition, the first
offset guaranteed to carry one. A partition whose last record is older than that
falls back to "a bronze heartbeat with `_lsn` > the source's
`pg_current_wal_lsn()`", logged as a warning. A fresh stack never uses it.

Trap avoided: the first fallback rule was "no offsets in bronze yet". That is
*also* what a table whose commits fail looks like, so it would have disabled
the protection exactly when needed. **Don't infer migration state from the data
the migration changes; record it.**

## Incident: the sink does not survive a long pause (2026-11-04)

Observed on the local stack while the laptop slept:

```
23:33:54  ... session timed out without receiving a heartbeat response     (VM resumed)
23:33:54  connect-bronze-sink-coord: consumer poll timeout has expired, LeaveGroup
23:52:13  Coordinator completed commit to table bronze.cdc_heartbeat_cdc, snapshot ...5459
23:52:13  ERROR Coordinator error during process, exiting thread
          CommitFailedException: ... the consumer was kicked out of the group
23:52:21  ERROR WorkerSinkTask{id=bronze-sink-0} ... will not recover until manually restarted
```

`make cdc-status` 2.6 days later: `bronze-sink tasks=['FAILED']`, heartbeat lag
11,374, bronze 224,123 s behind; `ledger-source` RUNNING (Debezium retries
retriable errors itself, the sink's coordinator failure is not retriable).

### Recovery drill (2026-11-07)

State left by the failure: the last snapshot recorded control offset **73681**,
the coordinator group was still committed at **73668**: the 23:52 commit was in
Iceberg, but the coordinator had not recorded it.

`POST /connectors/bronze-sink/tasks/0/restart` at 13:56:58, then:

```
13:57:43  Coordinator ... found nothing to commit to table bronze.cdc_heartbeat_cdc, skipping
13:57:58  completed commit to table bronze.cdc_heartbeat_cdc   (added 11,420 = 20,264 - 8,844)
13:58:00  heartbeat lag 0;  bronze freshness 1 s
```

The first cycle re-read the 23:52 `DATA_WRITTEN` and **skipped it**: the table's
last snapshot already carried control offset 73681, beyond that event. That is
how a commit that landed in Iceberg but not in the coordinator's group is never
applied twice. Evidence:

```
offsets 8842, 8843 (the 23:52 batch):        1 row each
rows with _kafka_offset: 15,118 = distinct offsets 15,118 = max - min + 1   (none twice, none missing)
```

### Bronze is not exactly-once: Debezium is at-least-once

The same check found **duplicate `(id, _lsn)` in the heartbeat table** (40
before the restart, 124 after), each with **two different Kafka offsets**: the
same WAL change was *produced to Kafka twice*. Every host wake-up hit a Debezium
"Producer failure"; on restart it resumes from the last **flushed** LSN and
re-sends what was in flight. Kafka Connect source tasks run without
exactly-once source support here, so the source side is at-least-once.

| Hop | Guarantee | Evidence |
|---|---|---|
| Postgres → Kafka (Debezium) | **at-least-once** | same `_lsn`, different offsets |
| Kafka → Iceberg (sink) | exactly-once per Kafka record | offsets distinct and contiguous |
| bronze → silver | idempotent: latest event per key by `_lsn`; duplicates are identical | business tables: 0 duplicates so far, only because no business traffic hit a restart |

Consequences:
* The right bronze check is **"no Kafka offset twice"**, not
  `count(*) = count(distinct (pk, _lsn))` (ADR 0004 states the latter: to correct).
* Anything that counts or sums bronze directly (reconciliation, the
  `bronze-tx-completeness` invariant) must dedup by `(pk, _lsn)` first.

Lessons:
* The Iceberg commit (B) succeeded, then the coordinator's *own* offset commit
  failed. On restart the coordinator re-reads `_iceberg_control` from its last
  committed offset and sees the same `DATA_WRITTEN` again; the snapshot summary
  offsets let it skip what is already committed (drill above).
* Kafka Connect never restarts a FAILED task on its own. Production needs a
  supervisor: an alert on task state plus an automated
  `POST /connectors/bronze-sink/tasks/0/restart` (with a retry limit), or an
  operator in the runbook. Locally: `docs/runbooks/connector-down.md`.
* The gate did its job by design: with the sink down, check 1 can never pass,
  so no day could publish short. A *down* sink is safe; a *silently wrong* one
  is what the gate exists for.
* The same failure in production: a node freeze, a long GC pause, a network
  partition longer than `max.poll.interval.ms`.

## Questions to test yourself

1. Why can check 2 never be replaced by "the connector is RUNNING"?
2. Why does check 3 compare against `end − 1` and not `end`?
3. After restarting the failed sink, which query proves no event was written twice?
