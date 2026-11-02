#!/usr/bin/env bash
# make demo: the Phase 1 Definition of Done, end to end.
#   clean stack -> seed (day 1) -> DAYS more business days of traffic
#   (inserts, updates, deletes, reversals, back-dated entries, create+delete
#   same day, re-inserts) -> each day published THROUGH AIRFLOW -> evidence.
#
# RESETS the local stack (all volumes): the demo is deterministic (SEED) and
# needs an empty control.batches. Knobs: DAYS=3 PACE=30 (seconds per business
# day of traffic) SEED=42 START_DATE=2026-01-01.
set -euo pipefail
cd "$(dirname "$0")/.."

DAYS=${DAYS:-3}
PACE=${PACE:-30}
SEED=${SEED:-42}
START_DATE=${START_DATE:-2026-01-01}
# Short sink commits so each day's gate passes in seconds (production: ~5 min).
# Exported for this `make up` only; a later plain `make up` restores .env's value.
export BRONZE_COMMIT_INTERVAL_MS=${DEMO_COMMIT_INTERVAL_MS:-10000}

t0=$(date +%s)
banner() { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
day_after() { python3 -c "import datetime,sys; print(datetime.date.fromisoformat(sys.argv[1]) + datetime.timedelta(days=int(sys.argv[2])))" "$1" "$2"; }

banner "1/4  clean stack (volumes deleted) and start everything"
make clean >/dev/null 2>&1 || true
make up 2>&1 | grep -E 'BOOTSTRAP|RUNNING|ERROR' || true

banner "2/4  business day $START_DATE: seed, then publish through Airflow"
make seed SEED="$SEED" START_DATE="$START_DATE" 2>&1 | grep -E '^\s+[a-z_]+\s+[0-9]+$' | paste -sd' ' - | sed 's/^/  generator: /'
./scripts/airflow-run.sh "$START_DATE" now 2>&1 | grep -vE '\[(warning|info) ' | tail -1
./scripts/demo-report.sh "$START_DATE"

banner "3/4  $DAYS more business days (traffic paced over ${PACE}s each)"
for i in $(seq 1 "$DAYS"); do
  d=$(day_after "$START_DATE" "$i")
  printf '\n\033[1m--- business day %s ---\033[0m\n' "$d"
  make traffic DATE="$d" PACE="$PACE" SEED="$SEED" 2>&1 | grep -E '^\s+[a-z_]+\s+[0-9]+$' | paste -sd' ' - | sed 's/^/  generator: /'
  ./scripts/airflow-run.sh "$d" now 2>&1 | grep -vE '\[(warning|info) ' | tail -1
  ./scripts/demo-report.sh "$d"
done

banner "4/4  summary"
T() { docker compose exec -T trino trino --output-format=ALIGNED --execute "$1" 2>/dev/null; }
echo "  -- every batch"
T "select batch_id, status, window_end, watermark_lsn, prev_batch_id,
          element_at(row_counts, 'accounts_full_daily.active') as accounts_active,
          element_at(row_counts, 'accounts_incremental') as accounts_changed,
          element_at(row_counts, 'account_balances_incremental') as balances_changed
   from lakehouse.control.batches order by batch_id"
echo "  -- bronze is a LOG, silver is a MIRROR: the account with the most history"
K=$(docker compose exec -T trino trino --output-format=TSV --execute "
  select account_id from lakehouse.bronze.accounts_cdc group by 1
  order by count_if(_op = 'd') desc, count(*) desc, 1 limit 1" 2>/dev/null)
T "select _op, _lsn, _source_commit_ts, customer_id, status from lakehouse.bronze.accounts_cdc
   where account_id = $K order by _lsn"
T "select account_id, customer_id, status, is_deleted, deleted_at, _batch_id as last_changed_in
   from lakehouse.silver.accounts_current where account_id = $K"
echo "  -- several updates to one key in one day: gold publishes the NET change only"
T "with ev as (
     select b.batch_id, a.account_id, a._lsn, a.status, a._op
     from lakehouse.bronze.accounts_cdc as a
     join lakehouse.control.batches as b
       on a._source_commit_ts >= b.window_start and a._source_commit_ts < b.window_end),
   multi as (
     select batch_id, account_id, count(*) as updates,
            array_join(array_agg(status order by _lsn), ' -> ') as status_path
     from ev group by 1, 2 having count(*) >= 2 and count_if(_op <> 'u') = 0)
   select m.batch_id, m.account_id, p.status as status_before, m.status_path, m.updates,
          coalesce(i.op, '(nothing)') as published
   from multi as m
   left join lakehouse.control.batches as cb on cb.batch_id = m.batch_id
   left join lakehouse.gold.accounts_full_daily as p
     on p.account_id = m.account_id and p.snapshot_date = cast(cb.prev_batch_id as date)
   left join lakehouse.gold.accounts_incremental as i
     on i.account_id = m.account_id and i.batch_date = cast(m.batch_id as date)
   order by 1, 2"
echo "  -- tests"
uv run pytest -q 2>&1 | tail -1 | sed 's/^/  pytest: /'
(cd dbt && DBT_PROFILES_DIR=. uv run --env-file ../.env dbt test --select test_type:unit 2>&1) \
  | grep -oE 'PASS=[0-9]+ WARN=[0-9]+ ERROR=[0-9]+' | sed 's/^/  dbt unit tests: /'
banner "done in $(( $(date +%s) - t0 ))s  |  Airflow UI http://localhost:${HOST_AIRFLOW_PORT:-18090}  |  make trino"
