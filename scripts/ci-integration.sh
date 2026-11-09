#!/usr/bin/env bash
# CI job "integration" (also runnable locally: `make ci-integration`, on a FRESH stack).
# Unlike demo.sh, which prints evidence, this script ASSERTS and exits non-zero.
#   1. fresh stack: up + bootstrap, smoke round trip
#   2. day 1: seed, publish with `make run-day` (host code); the gate must use the exact
#      offset check everywhere (fresh stack: no migration fallback)
#   3. day 2: traffic, publish through the Airflow DAG (baked image)
#   4. assertions on control and bronze
# On failure: every service log, `ps` and the sink status go to ci-logs/.
# CI_NO_BUILD=1: images are already built/pulled (CI), so `compose up` without --build.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
./scripts/ci-guard.sh
LOGS=ci-logs
TMP=$(mktemp -d)

dump_logs() {
  mkdir -p "$LOGS"
  docker compose ps -a > "$LOGS/ps.txt" 2>&1 || true
  for s in $(docker compose config --services); do
    docker compose logs --no-color "$s" > "$LOGS/$s.log" 2>&1 || true
  done
  curl -s "localhost:${HOST_CONNECT_PORT}/connectors/bronze-sink/status" > "$LOGS/bronze-sink-status.json" || true
  cp "$TMP"/*.log "$LOGS/" 2>/dev/null || true
  echo "logs written to $LOGS/"
}
trap 'rc=$?; if [[ $rc -ne 0 ]]; then dump_logs; fi; exit $rc' EXIT

step() { printf '\n\033[1m=== %s ===\033[0m\n' "$*"; }
fail() { echo "ASSERTION FAILED: $*" >&2; exit 1; }
T() { docker compose exec -T trino trino --output-format=TSV --execute "$1"; }

step "1/4 fresh stack"
if [[ "${CI_NO_BUILD:-0}" == 1 ]]; then
  docker compose up -d --wait
  ./scripts/bootstrap.sh
else
  make up
fi
make smoke

step "2/4 day 2026-01-01: seed, publish with make run-day"
make seed SEED=42 START_DATE=2026-01-01
make run-day DATE=2026-01-01 CUTOFF=now 2>&1 | tee "$TMP/run-day-2026-01-01.log"
grep -qE 'gate 3/3: in bronze: [1-9][0-9]* partition\(s\) by offset, 0 by heartbeat lsn' \
  "$TMP/run-day-2026-01-01.log" || fail "gate did not use the exact offset check on every partition"

step "3/4 day 2026-01-02: traffic, publish through Airflow"
make traffic DATE=2026-01-02 PACE=20 SEED=42
./scripts/airflow-run.sh 2026-01-02 now

step "4/4 assertions"
batches=$(T "select batch_id || ':' || status from lakehouse.control.batches order by batch_id" | tr -d '"' | paste -sd' ' -)
echo "batches: $batches"
[[ "$batches" == "2026-01-01:COMMITTED 2026-01-02:COMMITTED" ]] || fail "expected both days COMMITTED, got: $batches"

baseline=$(T "select count(*) from lakehouse.control.bronze_offset_baseline" | tr -d '"')
echo "offset baseline rows: $baseline (fresh stack: the migration has nothing to record)"
[[ "$baseline" == 0 ]] || fail "offset baseline should be empty on a fresh stack, has $baseline rows"

for t in accounts_cdc assets_cdc ledger_entries_cdc cdc_heartbeat_cdc; do
  r=$(T "select count(*), count(_kafka_offset), count(distinct (_kafka_partition, _kafka_offset))
         from lakehouse.bronze.$t" | tr -d '"')
  read -r rows with_offset distinct <<< "$r"
  echo "bronze.$t: rows=$rows with_offset=$with_offset distinct_offsets=$distinct"
  [[ "$rows" -gt 0 ]] || fail "bronze.$t is empty"
  [[ "$with_offset" == "$rows" ]] || fail "bronze.$t has rows without _kafka_offset on a fresh stack"
  [[ "$distinct" == "$rows" ]] || fail "bronze.$t: a Kafka record was written twice (sink not exactly-once)"
done

T "select batch_id, element_at(row_counts, 'accounts_full_daily.active') as accounts_active,
          element_at(row_counts, 'accounts_incremental') as accounts_changed,
          element_at(row_counts, 'account_balances_incremental') as balances_changed
   from lakehouse.control.batches order by batch_id"
echo "CI-INTEGRATION OK"
