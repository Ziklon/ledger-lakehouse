# Runbook: connector down

**Symptom.** In the `ledger_daily` run, `check_connectors` stays in
`up_for_reschedule` (it pokes the Kafka Connect REST API every 15 s and times
out after 10 min), or `completeness_gate` times out with
`latest heartbeat in bronze … < cut-off` / `sink behind: …`.
A third message, `no heartbeat beyond source lsn … (sink read but not committed to
Iceberg)`, means the connectors run but Iceberg commits fail: check the REST catalog
and MinIO (`make logs S=iceberg-rest`, `make logs S=connect | grep -i commit`).

**Impact.** Nothing is published, and nothing is opened: no `control.batches` row
exists for the day, and consumers keep the previous COMMITTED day. Source changes
keep accumulating in Postgres WAL (bounded by `max_slot_wal_keep_size`, see the
replication-slot runbook) and in Kafka (7-day retention), so nothing is lost while
you fix it.

## 1. Which connector, which state?

```bash
make cdc-status                          # connectors, slot lag, sink lag, bronze freshness
curl -s localhost:8083/connectors?expand=status | python3 -m json.tool
```

| State | Meaning | Action |
|---|---|---|
| `PAUSED` | someone paused it | `curl -X PUT localhost:8083/connectors/<name>/resume` |
| `FAILED` (task) | exception: read `trace` in the status | fix the cause, then `curl -X POST localhost:8083/connectors/<name>/tasks/0/restart` |
| connector missing | config lost (e.g. Connect volume wiped) | `make bootstrap` (idempotent PUT of both connectors) |
| REST API unreachable | Connect container down | `make ps`, `make logs S=connect`, `docker compose up -d connect` |

## 2. Let the run continue

Nothing to rerun: the sensor picks the connector up again at its next poke, and
the DAG continues (gate → … → COMMITTED). If the run already failed (sensor
timeout), trigger the same date again: `make airflow-run DATE=YYYY-MM-DD`
(demo: `CUTOFF=now`).

## 3. Verify

`make batches` shows the day COMMITTED, and `make cdc-status` shows sink lag back to ~0.

## Rehearsed (milestone f, 2026-11-01)

`bronze-sink` paused (HTTP 202 → `PAUSED`), traffic for 2026-01-14 written,
DAG triggered: `check_connectors` went to `up_for_reschedule`, and no
`control.batches` row was created. After `resume`, the same run continued to COMMITTED.
