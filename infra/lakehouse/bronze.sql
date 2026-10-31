-- =============================================================================
-- Bronze: append-only CDC change log (one row per EVENT, not per entity).
-- Applied through Trino by scripts/bootstrap.sh (idempotent: IF NOT EXISTS).
--
-- * Explicit types: Debezium encodes timestamptz as ISO strings, so sink
--   auto-create would type them varchar. The sink parses strings into these
--   typed columns. Money is decimal(38,18), never double.
-- * All columns nullable: bronze accepts whatever the source emits (e.g.
--   key-only delete events). Validation happens downstream.
-- * Partitioned by day(_source_commit_ts) = business time, not ingestion time,
--   so late-arriving events land in the right day (docs/adr/0005).
-- * Additive evolution only: the sink adds new source columns; never drop.
-- * Single-level namespace (bronze.*): AWS Glue has no nested namespaces.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS lakehouse.bronze;

CREATE TABLE IF NOT EXISTS lakehouse.bronze.assets_cdc (
    asset               varchar,
    name                varchar,
    precision           integer,
    updated_at          timestamp(6) with time zone,
    _op                 varchar,
    _lsn                bigint,
    _source_commit_ts   timestamp(6) with time zone,
    _ingested_at        timestamp(6) with time zone,
    _cdc_table          varchar
)
WITH (format_version = 2, partitioning = ARRAY['day(_source_commit_ts)']);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.accounts_cdc (
    account_id          bigint,
    customer_id         bigint,
    asset               varchar,
    status              varchar,
    opened_at           timestamp(6) with time zone,
    updated_at          timestamp(6) with time zone,
    _op                 varchar,
    _lsn                bigint,
    _source_commit_ts   timestamp(6) with time zone,
    _ingested_at        timestamp(6) with time zone,
    _cdc_table          varchar
)
WITH (format_version = 2, partitioning = ARRAY['day(_source_commit_ts)']);

CREATE TABLE IF NOT EXISTS lakehouse.bronze.ledger_entries_cdc (
    entry_id            bigint,
    transaction_id      bigint,
    account_id          bigint,
    direction           varchar,
    amount              decimal(38, 18),
    asset               varchar,
    posting_date        date,
    effective_date      date,
    reverses_entry_id   bigint,
    created_at          timestamp(6) with time zone,
    _op                 varchar,
    _lsn                bigint,
    _source_commit_ts   timestamp(6) with time zone,
    _ingested_at        timestamp(6) with time zone,
    _cdc_table          varchar
)
WITH (format_version = 2, partitioning = ARRAY['day(_source_commit_ts)']);

-- Completeness-gate evidence: max(_source_commit_ts) here past the cut-off
-- proves every earlier commit is in bronze (commit-order delivery).
CREATE TABLE IF NOT EXISTS lakehouse.bronze.cdc_heartbeat_cdc (
    id                  integer,
    ts                  timestamp(6) with time zone,
    _op                 varchar,
    _lsn                bigint,
    _source_commit_ts   timestamp(6) with time zone,
    _ingested_at        timestamp(6) with time zone,
    _cdc_table          varchar
)
WITH (format_version = 2, partitioning = ARRAY['day(_source_commit_ts)']);
