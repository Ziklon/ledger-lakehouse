# Runbook: replication-slot lag growing

**Why it matters.** Postgres keeps every WAL segment the logical replication
slot `dbz_ledger` hasn't confirmed. If Debezium stops consuming, the source disk
fills. `max_slot_wal_keep_size = 1GB` (`infra/postgres/postgresql.conf`) caps
that: beyond it Postgres **invalidates the slot** (`wal_status = lost`), which
protects the database but breaks CDC (a new snapshot is needed, see step 4).

**Symptom.** `make cdc-status` shows `slot_lag` growing and `safe_wal_size_left`
shrinking. Downstream, the completeness gate times out (no new heartbeat reaches bronze).

**Alert on** `slot_lag` and `safe_wal_size`, **not** on `active`: a paused
Debezium task keeps its replication connection, so the slot stays `active = t`
while lag grows (observed during the rehearsal below).

## 1. Measure

```bash
make cdc-status
```
```sql
-- in Postgres
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS slot_lag,
       pg_size_pretty(safe_wal_size) AS safe_wal_size_left
FROM pg_replication_slots WHERE slot_name = 'dbz_ledger';
```

| wal_status | Meaning |
|---|---|
| `reserved` | fine: within `max_wal_size` |
| `extended` | retaining extra WAL for the slot: act now |
| `unreserved` | past the limit, about to be removed: urgent |
| `lost` | slot invalidated: go to step 4 |

## 2. Find why Debezium isn't consuming

`ledger-source` connector PAUSED/FAILED → see `connector-down.md`. Connect
running but slow → `make logs S=connect`. A long-running source transaction
also holds the slot back: check `pg_stat_activity` for old `xact_start`.

## 3. Resume and confirm it drains

Resume or restart the connector. `slot_lag` should fall back to bytes within
seconds. Nothing is lost: every change is still in WAL and is streamed in commit order.

## 4. If the slot was lost (`wal_status = lost`)

CDC can't resume from where it stopped. Recreate the slot and take a new
initial snapshot (Debezium `snapshot.mode=initial` on a new slot/connector
name). Bronze stays append-only: the snapshot arrives as `_op = 'r'` events.
Silver converges because the latest event per key wins. Then reconcile with
the source (Phase 2: `control.reconciliation`). Record the gap in the incident
report: changes between the loss and the snapshot are visible only as net state.

## Rehearsed (milestone g, 2026-11-01)

`ledger-source` paused, two business days of traffic written: `slot_lag`
1.3 kB → 160 kB, `wal_status` reserved, `safe_wal_size_left` 1029 MB, slot still
`active = t`. After resume, `slot_lag` back to 192 bytes.
