#!/usr/bin/env bash
# Evidence for one published business day, queried through Trino:
# control.batches row, INCREMENTAL by op + samples, FULL sizes + consumer view,
# and the invariants re-checked independently in SQL.
#   scripts/demo-report.sh 2026-01-03
set -euo pipefail
cd "$(dirname "$0")/.."
D=${1:?usage: demo-report.sh YYYY-MM-DD}
T() { docker compose exec -T trino trino --output-format=ALIGNED --execute "$1" 2>/dev/null; }
h() { printf '\n  -- %s\n' "$1"; }

P=$(docker compose exec -T trino trino --output-format=TSV --execute \
  "select coalesce(prev_batch_id, '') from lakehouse.control.batches where batch_id = '$D'" 2>/dev/null)
PREV_FILTER=$([ -n "$P" ] && echo "snapshot_date = date '$P'" || echo "false")

h "control.batches"
T "select batch_id, status, window_start, window_end, watermark_lsn, prev_batch_id, committed_at
   from lakehouse.control.batches where batch_id = '$D'"

h "INCREMENTAL: net changes of the day"
T "select 'accounts' as dataset, op, count(*) as n from lakehouse.gold.accounts_incremental where batch_date = date '$D' group by op
   union all
   select 'account_balances', op, count(*) from lakehouse.gold.account_balances_incremental where batch_date = date '$D' group by op
   order by 1, 2"
T "select op, account_id, customer_id, asset, status, is_deleted
   from (select *, row_number() over (partition by op order by account_id) as rn
         from lakehouse.gold.accounts_incremental where batch_date = date '$D')
   where rn <= 2 order by op, account_id"

h "FULL: end-of-day state"
T "select (select count(*) from lakehouse.gold.accounts_full_daily where snapshot_date = date '$D') as accounts_rows,
          (select count_if(not is_deleted) from lakehouse.gold.accounts_full_daily where snapshot_date = date '$D') as accounts_active,
          (select count(*) from lakehouse.gold.account_balances_full_daily where snapshot_date = date '$D') as balances_rows,
          (select max(snapshot_date) from lakehouse.gold.accounts) as consumer_view_as_of"

h "invariants (re-checked here in SQL; Airflow's 'invariants' task gated the commit)"
T "with
 fa as (select snapshot_date d, account_id k1, '' k2, row_hash from lakehouse.gold.accounts_full_daily where not is_deleted),
 fb as (select snapshot_date d, cast(account_id as varchar) k1, asset k2, row_hash from lakehouse.gold.account_balances_full_daily),
 ia as (select account_id k1, '' k2, op, row_hash from lakehouse.gold.accounts_incremental where batch_date = date '$D'),
 ib as (select cast(account_id as varchar) k1, asset k2, op, row_hash from lakehouse.gold.account_balances_incremental where batch_date = date '$D'),
 aa as (select row_hash from fa where $(echo "$PREV_FILTER" | sed 's/snapshot_date/d/') and (k1, k2) not in (select cast(k1 as bigint), k2 from ia)
        union all select row_hash from ia where op in ('I','U')),
 ab as (select row_hash from fb where $(echo "$PREV_FILTER" | sed 's/snapshot_date/d/') and (k1, k2) not in (select k1, k2 from ib)
        union all select row_hash from ib where op in ('I','U')),
 unbalanced as (select transaction_id, asset from lakehouse.silver.ledger_entries_current where not is_deleted
        group by 1, 2 having sum(case when direction='DEBIT' then amount else 0 end) <> sum(case when direction='CREDIT' then amount else 0 end)),
 bronze_bal as (select account_id, asset, sum(case when direction='CREDIT' then amount else -amount end) balance from (
        select account_id, asset, direction, amount, _op, row_number() over (partition by entry_id order by _lsn desc) rn
        from lakehouse.bronze.ledger_entries_cdc
        where _source_commit_ts < (select window_end from lakehouse.control.batches where batch_id = '$D'))
        where rn = 1 and _op <> 'd' group by 1, 2),
 pub as (select account_id, asset, balance from lakehouse.gold.account_balances_full_daily where snapshot_date = date '$D')
select '1. FULL(prev)+INC = FULL, accounts' as invariant,
       format('%d = %d rows, checksum %s', (select count(*) from aa), (select count(*) from fa where d = date '$D'),
              if((select checksum(row_hash) from aa) is not distinct from (select checksum(row_hash) from fa where d = date '$D'), 'equal', 'DIFFERENT')) as result
union all
select '2. FULL(prev)+INC = FULL, balances',
       format('%d = %d rows, checksum %s', (select count(*) from ab), (select count(*) from fb where d = date '$D'),
              if((select checksum(row_hash) from ab) is not distinct from (select checksum(row_hash) from fb where d = date '$D'), 'equal', 'DIFFERENT'))
union all
select '3. double entry: unbalanced txn/asset', cast((select count(*) from unbalanced) as varchar)
union all
select '4. balances vs recompute from bronze: diffs',
       cast((select count(*) from (select * from bronze_bal except select * from pub)) + (select count(*) from (select * from pub except select * from bronze_bal)) as varchar)
order by 1"
