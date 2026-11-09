-- sqlfluff:exclude_rules:LT01,CP01
-- (hand-aligned DDL: statements UPPER, types lower; sqlfluff fix would destroy the alignment)
-- =============================================================================
-- Control plane: one row per publication batch (docs/adr/0012).
-- Applied through Trino by scripts/bootstrap.sh (idempotent: IF NOT EXISTS).
--
-- Writer: ledger_pipeline (make run-day; Airflow tasks in milestone f).
-- Readers: gold consumer views (latest COMMITTED snapshot), the next batch
--          (window_start = previous COMMITTED window_end), invariant checks.
--
-- Lifecycle: open_batch -> status RUNNING ... commit_batch -> COMMITTED.
-- COMMITTED is written LAST, in one statement, after every dataset and check;
-- consumers read only COMMITTED batches.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS lakehouse.control;

CREATE TABLE IF NOT EXISTS lakehouse.control.batches (
    batch_id            varchar,                   -- business date YYYY-MM-DD
    batch_type          varchar,                   -- daily | intraday (Phase 2)
    window_start        timestamp(6) with time zone,   -- inclusive, source commit time
    window_end          timestamp(6) with time zone,   -- exclusive = the cut-off
    watermark_lsn       bigint,                    -- max _lsn seen in the window
    watermark_commit_ts timestamp(6) with time zone,   -- max _source_commit_ts in the window
    prev_batch_id       varchar,                   -- batch the INCREMENTAL diffs against
    status              varchar,                   -- RUNNING | COMMITTED
    row_counts          map(varchar, bigint),      -- per published dataset
    checksums           map(varchar, varchar),     -- checksum(row_hash), active rows
    started_at          timestamp(6) with time zone,
    committed_at        timestamp(6) with time zone
)
WITH (format_version = 2);

-- Migration boundary of bronze _kafka_partition/_kafka_offset (docs/adr/0010).
-- One row per CDC partition whose bronze table already held rows when the columns
-- were added: from first_offset on, every bronze row carries its Kafka offset.
-- Writer: `ledger-pipeline record-offset-baseline` (scripts/bootstrap.sh), insert-only.
-- Reader: the completeness gate. A partition whose last record is below
-- first_offset (no event since the migration) can't be checked by offset and
-- uses the heartbeat-LSN fallback; every other partition is checked exactly.
CREATE TABLE IF NOT EXISTS lakehouse.control.bronze_offset_baseline (
    topic               varchar,
    kafka_partition     integer,
    first_offset        bigint,
    recorded_at         timestamp(6) with time zone
)
WITH (format_version = 2);
