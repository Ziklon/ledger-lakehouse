#!/usr/bin/env bash
# Idempotent platform bootstrap. Safe to run any number of times.
#   1. Iceberg sink control topic (broker auto-create is disabled)
#   2. Bronze DDL (typed contract) -- BEFORE the sink starts, otherwise the
#      sink would auto-create tables with inferred (varchar) timestamp types
#   3. Connectors via PUT /connectors/<name>/config (upsert): source, then sink
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

CONNECT_URL="http://localhost:${HOST_CONNECT_PORT}"
rpk() { docker compose exec -T redpanda rpk "$@"; }

echo "== 1. Iceberg control topic =="
if rpk topic describe _iceberg_control >/dev/null 2>&1; then
  echo "_iceberg_control exists"
else
  rpk topic create _iceberg_control -p 1 -r 1 \
    -c cleanup.policy=delete -c retention.ms=604800000
fi

echo "== 2. Bronze DDL =="
docker compose exec -T trino trino < infra/lakehouse/bronze.sql
docker compose exec -T trino trino --execute "SHOW TABLES FROM lakehouse.bronze"

echo "== 3. Connectors =="
for name in ledger-source bronze-sink; do
  code=$(curl -s -o /tmp/connect-resp.json -w '%{http_code}' -X PUT \
    -H 'Content-Type: application/json' \
    --data @"infra/connect/connectors/${name}.json" \
    "${CONNECT_URL}/connectors/${name}/config")
  if [[ "$code" != 200 && "$code" != 201 ]]; then
    echo "FAILED to apply ${name} (HTTP ${code}):"; cat /tmp/connect-resp.json; echo; exit 1
  fi
  echo "applied ${name} (HTTP ${code})"
done

echo "== 4. Wait for connectors + tasks RUNNING =="
for name in ledger-source bronze-sink; do
  for _ in $(seq 1 30); do
    state=$(curl -s "${CONNECT_URL}/connectors/${name}/status" | python3 -c \
      "import sys,json;s=json.load(sys.stdin);t=[x['state'] for x in s.get('tasks',[])];print(s['connector']['state'] if t and all(x=='RUNNING' for x in t) else 'PENDING:'+','.join(t) or 'PENDING')" 2>/dev/null || echo PENDING)
    [[ "$state" == RUNNING ]] && break
    [[ "$state" == *FAILED* ]] && { curl -s "${CONNECT_URL}/connectors/${name}/status"; echo; exit 1; }
    sleep 2
  done
  echo "${name}: ${state}"
  [[ "$state" == RUNNING ]] || exit 1
done
echo "BOOTSTRAP OK"
