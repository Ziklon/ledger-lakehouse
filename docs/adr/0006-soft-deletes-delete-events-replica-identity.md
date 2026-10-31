---
status: accepted
date: 2026-10-31
---

# 0006 — Soft deletes, delete-event handling, and `REPLICA IDENTITY DEFAULT`

## Context and problem statement

With `REPLICA IDENTITY DEFAULT`, Postgres writes only the primary key as the
"old" row for UPDATE/DELETE. Debezium's delete event therefore has no real
values for non-key columns. How do we represent deletes, and should we pay for
`REPLICA IDENTITY FULL` to get complete before-images?

## Observed behaviour (evidence, milestone b)

Delete events are **not null-filled for NOT NULL columns**. Those columns are
*required* in the Avro schema, so Debezium fills type defaults:

```
account_id=11  customer_id=0  asset=''  status=''  opened_at=1970-01-01  _op='d'
```

`customer_id = 0` is the house (exchange) customer: a naive "latest event wins"
silver would silently re-assign a deleted customer account to the exchange.
Nullable columns arrive as NULL. Both shapes must be handled.

## Decision drivers

* Correctness: a delete must never overwrite business attributes with defaults
* Auditability: consumers see that a key was deleted and when
* Source cost: WAL volume on the OLTP database
* Downstream simplicity: consumers must not filter deletes themselves

## Considered options

1. **DEFAULT identity + soft deletes in silver; trust only key + metadata on `_op='d'`**
2. FULL identity: complete before-image on every UPDATE/DELETE
3. Hard deletes in silver (remove the row)

## Decision outcome

Chosen: **option 1**.

* Silver `*_current` keeps the row: `is_deleted = true`, `deleted_at =
  _source_commit_ts` of the delete. **All business columns are carried forward
  from the last non-delete event** — never taken from the delete event, and
  never "coalesce nulls" (that would accept the `0` / `''` / epoch defaults).
* `is_deleted` is part of `row_hash`, so a delete is detected as a change even
  though carried-forward business columns are unchanged.
* Keys created and deleted inside one window are skipped (never existed at EOD).
* A re-inserted key comes back (`is_deleted = false`): the latest event wins.
* Consumer views (`gold.accounts`) filter `not is_deleted`.

### Consequences

* Good: no extra WAL; zero source-side configuration beyond the default.
* Good: delete semantics are explicit and unit-tested (both NULL and
  default-filled delete payloads).
* Bad: bronze delete events alone don't show what was deleted; history does
  (previous events of the key are in bronze). Auditors query the key's log.
* Revisit: if an auditor needs before-images *in the event itself* (e.g. for a
  regulator export without joins), switch the specific tables to FULL; cost is
  roughly one extra row image per UPDATE/DELETE in WAL. Per-table, not global.

Option 3 rejected: loses the information that a key existed and was deleted,
and makes FULL(N-1) + INCREMENTAL(N) = FULL(N) checks unable to show `op = D`.
