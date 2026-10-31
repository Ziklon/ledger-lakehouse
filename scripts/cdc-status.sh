#!/usr/bin/env bash
# One-screen CDC health: connector state, replication-slot lag (source disk
# risk), sink consumer lag (lake freshness), and bronze freshness.
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a

echo "== connectors =="
for name in ledger-source bronze-sink; do
  curl -s "http://localhost:${HOST_CONNECT_PORT}/connectors/${name}/status" | python3 -c \
    "import sys,json;s=json.load(sys.stdin);print(f\"{s['name']:<14} connector={s['connector']['state']:<8} tasks={[t['state'] for t in s['tasks']]}\")"
done

echo "== replication slot (source-side WAL retained for Debezium) =="
docker compose exec -T postgres psql -U "$PG_USER" -d "$PG_DATABASE" -P pager=off -c "
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS slot_lag,
       wal_status,                              -- reserved | extended | unreserved | lost
       pg_size_pretty(safe_wal_size)            AS safe_wal_size_left
FROM pg_replication_slots WHERE slot_name = 'dbz_ledger';"

echo "== bronze sink consumer lag (Kafka -> Iceberg) =="
docker compose exec -T redpanda rpk group describe connect-bronze-sink \
  | awk '/^TOTAL-LAG/{print} /^TOPIC/{p=1} p{print $1, $2, $3, $5, $6}'

echo "== bronze freshness =="
docker compose exec -T trino trino --output-format=ALIGNED --execute "
SELECT max(_source_commit_ts) AS latest_heartbeat_in_bronze,
       date_diff('second', max(_source_commit_ts), current_timestamp) AS seconds_behind
FROM lakehouse.bronze.cdc_heartbeat_cdc"
