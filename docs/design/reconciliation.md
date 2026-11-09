# Reconciliation — design notes (input to milestone j)

Status: **notes, not a decision.** The choices marked *open* become an ADR in
milestone j. Every result below was run on the local stack against business day
2026-01-05 (cut-off `2026-11-03 19:37:21.549 UTC`, EOD tag `eod_2026_01_05`).

## Why

Every check so far compares the pipeline **with itself**:

| Check | Proves | Blind to |
|---|---|---|
| Completeness gate (ADR 0010) | everything Debezium *sent* is in bronze | events Debezium *never sent* |
| FULL(N-1) + INC(N) = FULL(N) | gold is consistent day to day | gold being consistently wrong |
| Invariants (ADR 0013) | data is internally coherent | a missing account is still coherent |

Reconciliation compares the published data with **the source of truth (Postgres)**
over a path that does not go through CDC. It catches failures that are not in
the code: a manual `UPDATE` with replication off, a recreated replication slot,
a table missing from the publication, a restored backup.

Trino reads the source through the read-only `ledger_pg` catalog
(`trino_reader` role), the lake through time travel on the EOD tag.

## The three levels

| Level | Finds | Cost | Names the row? |
|---|---|---|---|
| 1. Counts | missing / extra rows | low | no |
| 2. Gross totals | wrong amounts, missing money | low | no (only the asset) |
| 3. Hash diff | any change in the hashed columns | high | **yes** |

### 1. Counts

```sql
select count(*) from ledger_pg.app.ledger_entries where created_at < :cutoff;   -- 460
select count(*) from lakehouse.silver.ledger_entries_current
  for version as of 'eod_2026_01_05' where not is_deleted;                       -- 460
```

Misses value corruption, and a missing row plus a duplicate cancel out.

### 2. Totals — gross, never net

The **net** (credits − debits) per asset is **0 on both sides, always**:
double-entry makes every transaction balance. A check that cannot fail proves
nothing. Use gross credits and debits separately:

```
 asset | credits                  | debits
 ETH   | 38.624739972991937195    | 38.624739972991937195
 USD   | 111486.22                | 111486.22
```

Fault injection (entry 460 hidden from the lake side):

```
 ETH | source | 144 rows | net  0.000000 | gross 77.2494…
 ETH | lake   | 143 rows | net -0.929115 | gross 76.3203…   ← caught
```

Note: losing a *whole transaction* (both legs) keeps the net at 0; only gross
totals, counts or the hash diff see it.

### 3. Hash diff

Hash the same columns on both sides per key, `FULL OUTER JOIN`, classify:
`MISSING_IN_LAKE` / `EXTRA_IN_LAKE` / `VALUE_DIFF` / `MATCH`.

```
real data:      MATCH 73                          (balances per account + asset)
with the fault: entry_id 460 | MISSING_IN_LAKE    ← names the row
```

Pitfalls:
* **Canonical serialization**: both sides must format values identically
  (`decimal(38,18)` as `1.5` vs `1.500000000000000000` is a false diff).
* `concat_ws('|', …)` skips NULLs and is ambiguous if a value contains the
  separator: `('a|b', 'c')` and `('a', 'b|c')` hash the same. Use a NULL
  marker and a separator that cannot occur, or hash each column and combine.

## Measuring the source at the right instant

### Immutable data (`ledger_entries`): the commit-time trap

`created_at` is `now()` = transaction **start**; windows use **commit** time
(`_source_commit_ts`). Measured in bronze:

```
max gap 141 ms, median 4 ms
```

An entry started at 23:59:59.950 and committed at 00:00:00.050 passes
`created_at < cutoff` but belongs to the next window → false `MISSING_IN_LAKE`.
In production traffic continues past midnight, so this happens daily.

*Open* — options:
1. `track_commit_timestamp = on` + `pg_xact_commit_timestamp(xmin)`: exact,
   needs a server setting (restart), xmin is reset by freezing on old rows.
2. Filter by LSN / a watermark recorded by the gate.
3. Re-check boundary diffs against the next day's window before failing.

### Mutable data (`accounts`): the source only knows *now*

Skip rows with `updated_at >= cutoff`, compare the rest. First result:
`EXTRA_IN_GOLD 2, MATCH 78`. Investigation: accounts 28 and 43 were deleted
before the cut-off; `gold.accounts_full_daily` keeps soft-deleted rows
(`is_deleted = true`) by design. Corrected query: **MATCH 78**.

Lessons:
* Reconcile against the **published contract** (soft deletes included), not
  your memory of the table.
* Holes in the `updated_at` filter:
  * a row deleted *after* the cut-off is gone from the source → false
    `EXTRA_IN_GOLD`;
  * a bulk `UPDATE` that does not touch `updated_at` slips past the filter.

*Open*: how to tell these apart from real errors (cross-check with bronze
events after the cut-off for the same key?).

## Dev / CI vs production

| | Dev / CI | Production |
|---|---|---|
| Question | can this check fail? | is today's data right? |
| How | fault injection (delete / alter / duplicate a row, expect red) | step before COMMITTED, every batch |
| On a diff | expected (test passes) | batch stays RUNNING, alert, runbook |

Production specifics:
* **Zero tolerance for money.** A timing effect may be tolerated only if it is
  explained and recorded, never silently absorbed.
* **Results are persisted** (e.g. `control.reconciliation`: batch, level, both
  sides, verdict): the auditor's evidence.
* **Frequency by cost**: counts + gross totals every batch (blocking); hash diff
  of the new day's `ledger_entries` every batch; full-history hash diff weekly
  or at month-end close (milestone m) or after an incident.
* Phase 3: Trino on EKS → RDS is one more network path to secure and to fail;
  the step needs a clear "source unreachable" message.

## Load on the production database

Trino `EXPLAIN` on today's queries:

```
Level 2:  Aggregate[PARTIAL]                 ← the sum runs in TRINO
          └─ TableScan ledger_entries  constraint on [created_at]   ← only the filter is pushed down
Level 3:  ScanProject ledger_entries         ← every row, 5 columns, over JDBC
Indexes:  pkey(entry_id), account_idx, txn_idx    ← none on created_at
```

The connector does not push `sum`/`count` over `numeric` down, and the filter is
a full scan. Harmless at 460 rows, a nightly full scan plus GBs over JDBC at
500 M.

| Mechanism | Effect | Who notices |
|---|---|---|
| Disk I/O and CPU | full scan competes with business queries | app latency during the run |
| Cache | large seq scans use a small ring buffer: cache mostly safe, I/O still competes | mostly fine |
| **Long-running snapshot** | holds back the xmin horizon: `VACUUM` cannot remove dead rows, hot tables bloat | slow degradation over weeks — the sneaky one |
| Locks | `SELECT` only conflicts with DDL, but a queued `ALTER TABLE` blocks every later query | sudden stall during a migration |
| Network | rows streamed to Trino | DB network, connection slots |

Mitigations, cheapest first:
1. **Aggregate inside Postgres** via the `ledger_pg.system.query(...)`
   passthrough (as the gate does for `pg_current_wal_lsn()`): 8 rows back
   instead of every entry.
2. **Reconcile only the new day** of immutable data, by an **indexed** range
   (`entry_id` from the last reconciled id on), not `created_at`.
3. **Read replica**, not the primary. Trade-off: long queries on a replica are
   cancelled by default, or with `hot_standby_feedback = on` they bloat the
   primary again. A replica lags, which adds a variable to the timing problem.
4. **Limit `trino_reader`**: `statement_timeout`,
   `idle_in_transaction_session_timeout`, a small connection limit.
5. **Full-history scans off-peak** (weekly / month-end), never in the daily
   critical path.

Not measured: at local volume every query takes milliseconds. Milestone j
should include a load test (millions of rows, app latency with and without
reconciliation running) to replace this argument with a number.

## Open questions for the milestone j plan

1. Commit-time boundary: `track_commit_timestamp`, LSN watermark, or re-check?
2. Mutable tables: how to classify rows changed or deleted after the cut-off.
3. Which levels block COMMITTED, which only warn.
4. Schema of `control.reconciliation` and how long results are kept.
5. Read replica locally (a second Postgres) or only documented for Phase 3?
6. Load test size and the acceptable latency budget.
