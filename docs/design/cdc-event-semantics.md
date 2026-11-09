# What a CDC event really contains

Status: **explainer.** Decisions: [ADR 0004](../adr/0004-append-only-bronze-change-log.md)
(bronze is a log), [0005](../adr/0005-partition-bronze-by-source-commit-time.md)
(commit-time partitions), [0006](../adr/0006-soft-deletes-delete-events-replica-identity.md)
(deletes). Evidence from the local stack, bronze as of 2026-11-07.

Debezium reads Postgres logical decoding (`pgoutput`, slot `dbz_ledger`); the
sink flattens the envelope (`ExtractNewRecordState`) and adds metadata columns.
Every rule below exists because the naive reading of an event is wrong.

## Inserts and updates: the full row, after the change

An update carries the **whole `after` image**, not only the changed columns.
Updating one column of `accounts` still produces every column. `before` is NULL
under `REPLICA IDENTITY DEFAULT` (Postgres logs only the key as old row), so an
event says *what the row is now*, not *what changed*. Change detection is
silver's job (hash diff, ADR 0008).

Exception, not hit in this schema: a TOASTed value (large text/jsonb, stored
out of line) that the update did not touch arrives as the placeholder
`__debezium_unavailable_value`. Taking it literally would overwrite real data
with that string; it must be carried forward like delete columns. Checked:

```
TOAST placeholders in bronze.accounts_cdc: 0      (no large columns today)
```

Becomes relevant the day a large column is added (schema evolution, milestone k).

## Deletes: the key, and lies for everything else

```
 account_id | _op |   _lsn   | status | customer_id | asset | opened_at
         28 | c   | 26779800 | ACTIVE |          24 | ETH   | 2026-11-03 04:48:35.829169 UTC
         28 | d   | 30552256 |        |           0 |       | 1970-01-01 00:00:00.000000 UTC
```

NOT NULL columns are *required* in the event schema, so Debezium fills **type
defaults**: `0`, `''`, the epoch. `customer_id = 0` is the house customer: a
"latest event wins" silver would hand the deleted account to the exchange.
Rule: **from `_op = 'd'` take only the key and the metadata**; business columns
are carried forward from the last non-delete event. `coalesce` does not help:
the defaults are not NULL.

## Order by `_lsn`, never by time

```
ledger_entries events: 100   distinct _source_commit_ts: 44   distinct _lsn: 100   transactions: 44
```

`_source_commit_ts` is the **commit** time with millisecond precision, the same
for every event of a transaction, and ties across transactions are common. Only
`_lsn` (WAL position) is unique and ordered. Use the timestamp for windows
(which day), the LSN for order (which event wins).

## One transaction, several events

```
 _tx_id | legs | lsns | ts
   6376 |    4 |    4 |  1
```

A ledger transaction is several rows (debit and credit legs) committed
atomically in Postgres, but each row is a separate Kafka record. Nothing in
Kafka or the sink guarantees they become visible together. `_tx_id` lets us
check it: backlog item `bronze-tx-completeness` (every transaction complete and
balanced in bronze).

## Snapshot reads

`_op = 'r'` rows come from Debezium's initial snapshot, not from the WAL. They
look like inserts and are treated as inserts, but their commit time is the
snapshot time, not when the row was originally created. None on this stack
(`count(*) where _op = 'r'` = 0): the slot existed before the seed, so every row
arrived as a `c`. A re-snapshot (new slot, new table in the publication) would
produce them.

## Time columns, and what each means

| Column | Set by | Meaning |
|---|---|---|
| `created_at`, `updated_at` | the application (`now()`) | **transaction start**, app bookkeeping |
| `_source_commit_ts` | Debezium `source.ts_ms` | source **commit** (windows use this) |
| `_ingested_at` | the sink (Kafka record timestamp) | when the record entered Kafka, not the lake (backlog `bronze-ingested-at-naming`) |

`created_at` vs commit: up to 141 ms apart locally (see
[reconciliation](reconciliation.md#immutable-data-ledger_entries-the-commit-time-trap)).

## Questions to test yourself

1. A status flaps ACTIVE → FROZEN → ACTIVE within one millisecond. How does
   silver know the final value?
2. Why is "skip events whose business columns are NULL" not a valid delete rule?
3. What breaks if `REPLICA IDENTITY FULL` is enabled on `accounts` and nothing else changes?
