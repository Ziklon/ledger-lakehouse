#!/usr/bin/env bash
# Read-only role for Trino's ledger_pg catalog (reconciliation, ad-hoc queries).
# Idempotent: runs automatically on a fresh volume (docker-entrypoint-initdb.d)
# and can be re-applied to an existing database:
#   docker compose exec postgres bash /docker-entrypoint-initdb.d/03-reader-role.sh
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "${POSTGRES_USER}" --dbname "${POSTGRES_DB}" \
     -v db="${POSTGRES_DB}" -v reader="${PG_READER_USER}" -v reader_password="${PG_READER_PASSWORD}" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN', :'reader')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'reader') \gexec
ALTER ROLE :"reader" WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
    PASSWORD :'reader_password';

GRANT CONNECT ON DATABASE :"db" TO :"reader";
GRANT USAGE ON SCHEMA app TO :"reader";
GRANT SELECT ON ALL TABLES IN SCHEMA app TO :"reader";
-- tables created later by the owner are readable too
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO :"reader";
-- belt and braces: never writable, even if someone grants PUBLIC later
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA app FROM :"reader";
SQL
