-- =============================================================================
-- Source ledger schema. Runs once, on first boot with an empty data volume.
-- Postgres holds CURRENT state only; history is captured from WAL by Debezium.
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS app;

-- Reference data: tradable assets and their display precision.
CREATE TABLE app.assets (
    asset       text        PRIMARY KEY,                       -- 'BTC', 'USDT', ...
    name        text        NOT NULL,
    precision   smallint    NOT NULL CHECK (precision BETWEEN 0 AND 18),
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Mutable entity: status changes, closures, (rare) deletes, re-inserts.
-- account_id is assigned by the caller (not an identity) so a deleted key can
-- legitimately come back -- one of the CDC edge cases we must handle.
CREATE TABLE app.accounts (
    account_id  bigint      PRIMARY KEY,
    customer_id bigint      NOT NULL,
    asset       text        NOT NULL REFERENCES app.assets (asset),
    status      text        NOT NULL CHECK (status IN ('ACTIVE', 'FROZEN', 'CLOSED')),
    opened_at   timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

-- Immutable facts: a ledger is append-only. Corrections are NEW entries
-- (reversals), never UPDATE/DELETE. Double entry: per transaction and asset,
-- sum(DEBIT) = sum(CREDIT).
CREATE TABLE app.ledger_entries (
    entry_id          bigint         PRIMARY KEY,
    transaction_id    bigint         NOT NULL,
    account_id        bigint         NOT NULL REFERENCES app.accounts (account_id),
    direction         text           NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount            numeric(38, 18) NOT NULL CHECK (amount > 0),
    asset             text           NOT NULL REFERENCES app.assets (asset),
    posting_date      date           NOT NULL,   -- business day it was booked
    effective_date    date           NOT NULL,   -- business day it economically belongs to
    reverses_entry_id bigint         NULL REFERENCES app.ledger_entries (entry_id),
    created_at        timestamptz    NOT NULL DEFAULT now(),
    CHECK (effective_date <= posting_date)       -- back-dating allowed, future-dating not
);
CREATE INDEX ledger_entries_account_idx ON app.ledger_entries (account_id);
CREATE INDEX ledger_entries_txn_idx     ON app.ledger_entries (transaction_id);

-- Pipeline artifact: Debezium upserts this row every heartbeat.interval.ms.
-- Keeps the replication slot advancing on an idle DB and gives bronze a
-- monotonic "we have everything committed up to here" marker.
CREATE TABLE app.cdc_heartbeat (
    id  smallint    PRIMARY KEY,
    ts  timestamptz NOT NULL
);

-- DEFAULT = WAL carries only the primary key as the old row on UPDATE/DELETE.
-- Set explicitly to document the decision (see docs/adr/0006).
ALTER TABLE app.assets          REPLICA IDENTITY DEFAULT;
ALTER TABLE app.accounts        REPLICA IDENTITY DEFAULT;
ALTER TABLE app.ledger_entries  REPLICA IDENTITY DEFAULT;
ALTER TABLE app.cdc_heartbeat   REPLICA IDENTITY DEFAULT;

-- Dedicated publication with an EXPLICIT table list: a new table never starts
-- streaming (possibly with PII) by accident.
CREATE PUBLICATION dbz_ledger
    FOR TABLE app.assets, app.accounts, app.ledger_entries, app.cdc_heartbeat;
