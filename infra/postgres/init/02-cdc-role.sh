#!/usr/bin/env bash
# Least-privilege role for Debezium. A shell script (not .sql) so the password
# comes from the environment instead of being committed.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v db="$POSTGRES_DB" -v cdc_user="$PG_CDC_USER" -v cdc_password="$PG_CDC_PASSWORD" <<'SQL'
-- REPLICATION: may open a logical replication stream / create its slot.
-- No ownership of tables => cannot create publications (we created it in 01).
CREATE ROLE :"cdc_user" WITH LOGIN REPLICATION PASSWORD :'cdc_password';

GRANT CONNECT ON DATABASE :"db" TO :"cdc_user";
GRANT USAGE ON SCHEMA app TO :"cdc_user";
-- SELECT is needed for the initial snapshot only.
GRANT SELECT ON ALL TABLES IN SCHEMA app TO :"cdc_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO :"cdc_user";
-- The only write: Debezium's heartbeat.action.query.
GRANT INSERT, UPDATE ON app.cdc_heartbeat TO :"cdc_user";
SQL
