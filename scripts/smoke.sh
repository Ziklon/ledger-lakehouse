#!/usr/bin/env bash
# Milestone (a) smoke test: proves the full storage path works, not just that
# containers are running.  Trino -> Iceberg REST catalog -> S3FileIO -> MinIO.
set -euo pipefail
cd "$(dirname "$0")/.."

trino() { docker compose exec -T trino trino --output-format=ALIGNED --execute "$1"; }

echo "== catalogs =="
trino "SHOW CATALOGS"

echo "== Iceberg write/read round trip =="
trino "CREATE SCHEMA IF NOT EXISTS lakehouse.smoke"
trino "DROP TABLE IF EXISTS lakehouse.smoke.ping"
trino "CREATE TABLE lakehouse.smoke.ping (id bigint, note varchar, at timestamp(6) with time zone)
       WITH (partitioning = ARRAY['day(at)'])"
trino "INSERT INTO lakehouse.smoke.ping VALUES
       (1, 'hello', current_timestamp), (2, 'lakehouse', current_timestamp)"
trino "SELECT * FROM lakehouse.smoke.ping ORDER BY id"

echo "== Iceberg metadata tables (snapshots / files on S3) =="
# element_at() (not summary['k']) because the CREATE TABLE snapshot has no added-* keys.
trino "SELECT committed_at, operation, element_at(summary, 'added-records') AS added,
              element_at(summary, 'total-records') AS total
       FROM lakehouse.smoke.\"ping\$snapshots\" ORDER BY committed_at"
trino "SELECT file_path, record_count FROM lakehouse.smoke.\"ping\$files\""

echo "== source Postgres reachable via federation =="
trino "SHOW SCHEMAS FROM ledger_pg"

echo "== cleanup =="
trino "DROP TABLE lakehouse.smoke.ping"
trino "DROP SCHEMA lakehouse.smoke"
echo "SMOKE OK"
