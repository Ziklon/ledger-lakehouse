# Runbook: invariant failure

**Symptom.** `make run-day` (or the Airflow `invariant_checks` task) ends with:

```
ERROR step failed (test --select): batch YYYY-MM-DD NOT committed, it stays RUNNING; ...
```

**Impact.** None for consumers yet. The batch is `RUNNING`, so `gold.accounts` /
`gold.account_balances` keep serving the previous COMMITTED day. New batches
are refused until this one is finished ("batch … is RUNNING; rerun it to
completion first"), so no day can be skipped silently.

## 1. Which invariant, which rows?

```bash
cd dbt && DBT_PROFILES_DIR=. uv run --env-file ../.env dbt test --select tag:invariant \
  --vars "$(docker compose exec -T trino trino --output-format=TSV --execute "
    select '{\"batch_id\":\"'||batch_id||'\",\"window_start\":\"'||to_iso8601(window_start)||
           '\",\"window_end\":\"'||to_iso8601(window_end)||'\",\"prev_batch_id\":\"'||prev_batch_id||'\"}'
    from lakehouse.control.batches where status = 'RUNNING'")" --store-failures
```

`--store-failures` writes the failing rows to `lakehouse.dbt_test__audit.<test name>`,
e.g. `select * from lakehouse.dbt_test__audit.double_entry_balances`.

| Invariant | Meaning | Usual cause |
|---|---|---|
| `double_entry_balances` | a transaction/asset doesn't net to zero | **source** wrote an unbalanced transaction, or a leg was lost on the way |
| `balances_match_bronze_recompute` | published balances ≠ recompute from bronze | bug in silver/affected-keys logic, or wrong EOD tag |
| `full_plus_incremental_equals_full` | FULL(prev) + INC ≠ FULL | bug in the gold macros, or a gold partition edited by hand |

## 2. Fix the cause

* **Source data wrong** (e.g. one-legged transaction): the owning team posts the
  correcting entry in the source. Never UPDATE/DELETE ledger entries; entries
  are immutable. Example: complete transaction 900001 by posting its missing
  CREDIT leg.
* **Code bug**: fix it, run `make test` (unit tests) and merge.
* **Gold edited by hand**: nothing to fix upstream; the rerun overwrites the partition.

## 3. Rerun the SAME date

```bash
make run-day DATE=YYYY-MM-DD CUTOFF=now     # demo; production: omit CUTOFF
```

An uncommitted batch isn't frozen. With `CUTOFF=now` its window extends to
include the correction. A COMMITTED day (latest only) reruns on its stored window.

## 4. Verify

```bash
make batches        # the day is COMMITTED
```

and `select max(snapshot_date) from lakehouse.gold.accounts` shows the day.

## Rehearsed (milestone e, 2026-10-31)

A one-legged DEBIT (transaction 900001) inserted into `app.ledger_entries`:
`double_entry_balances` FAIL 1, batch 2026-01-06 RUNNING, consumers on
2026-01-05, 2026-01-07 refused. After the CREDIT leg was posted, the rerun of
2026-01-06 passed all invariants and was COMMITTED. The INCREMENTAL shows both
accounts as `U`.
