#!/usr/bin/env bash
# Trigger the ledger_daily DAG for one business day and wait for the result.
#   scripts/airflow-run.sh 2026-01-09 [now|ISO-cutoff]
# Exit code 0 only if the run succeeded. Prints every task's final state.
set -euo pipefail
cd "$(dirname "$0")/.."

DATE=${1:?usage: airflow-run.sh YYYY-MM-DD [cutoff]}
CUTOFF=${2:-}
AF=(docker compose exec -T airflow-scheduler airflow)
RUN_ID="manual__${DATE}__$(date -u +%Y%m%dT%H%M%S)"
CONF=$(printf '{"batch_id": "%s", "cutoff": %s}' "$DATE" "$( [ -n "$CUTOFF" ] && printf '"%s"' "$CUTOFF" || echo null)")

"${AF[@]}" dags unpause ledger_daily >/dev/null
"${AF[@]}" dags trigger ledger_daily --run-id "$RUN_ID" --conf "$CONF" >/dev/null
echo "triggered $RUN_ID conf=$CONF"

state=queued
for _ in $(seq 1 180); do
  state=$("${AF[@]}" dags list-runs ledger_daily -o json 2>/dev/null \
    | python3 -c "import json,sys; r=[x for x in json.load(sys.stdin) if x['run_id']==sys.argv[1]]; print(r[0]['state'] if r else 'unknown')" "$RUN_ID")
  case "$state" in success|failed) break ;; esac
  sleep 5
done

"${AF[@]}" tasks states-for-dag-run ledger_daily "$RUN_ID" -o json 2>/dev/null \
  | python3 -c "
import json, sys
rows = json.loads(sys.stdin.read() or '[]')
for r in sorted(rows, key=lambda r: (r.get('start_date') or '9', r['task_id'])):
    print(f\"  {r['state'] or 'none':16} {r['task_id']}\")"
echo "run $RUN_ID: $state"
[ "$state" = success ]
