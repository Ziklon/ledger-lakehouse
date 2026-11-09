# The Iceberg write path, small files, and maintenance

Status: **explainer + open gaps.** Retention and expiry are Phase 2 (milestone n).
Evidence from the local stack, 2026-11-07.

## Where the "buffer" is

Not in Kafka, not in a WAL inside Iceberg:

1. Records sit in Kafka until the worker reads them.
2. The worker writes them into **open Parquet files**, one per table/partition.
   Small files are uploaded when closed; large ones stream as multipart uploads.
3. At `START_COMMIT` (every `BRONZE_COMMIT_INTERVAL_MS`: 10 s locally in `.env`,
   300 s in `.env.example`/production) the files close.
4. The coordinator **commits metadata only**: new manifest, manifest list,
   `metadata.json`, then an atomic pointer swap in the catalog. Data files are
   written once and never rewritten by a commit.

So every interval adds, per table that received data, **one data file and one
snapshot**. That is cheap per commit and expensive in aggregate.

## The small-files problem, measured

```
 table           | files | rows  | avg bytes
 ledger_entries  |     1 |   460 | 11,783      <- compacted by the last DAG run
 accounts        |     1 |   120 |  5,270
 cdc_heartbeat   | 2,116 | 8,844 |  3,237      <- ~1 file per heartbeat

 heartbeat partitions:  2026-11-03: 311 files   2026-11-04: 1,805 files
 heartbeat snapshots:   5,459 (5,454 appends, 5 replaces)
 metadata.json kept:    101
```

The heartbeat table writes every 10 s whether or not the business is busy, so
it is the first to drown. A query planning over 2,000 files of 3 KB spends its
time opening files, not reading data.

## What cleans up today, and what doesn't

| Debt | Cleaned by | Status |
|---|---|---|
| Small data files | `optimize` (DAG task after `commit_batch`): bronze partitions the window touched + silver | ✅ but **only when the DAG runs** |
| Old `metadata.json` | Iceberg `write.metadata.previous-versions-max` (default 100) | ✅ automatic (101 seen) |
| Old snapshots (and the manifests and files only they reference) | `expire_snapshots` | ❌ Phase 2: 5,459 snapshots and growing |
| Files no snapshot references (failed writes) | `remove_orphan_files` | ❌ Phase 2 |
| Delete files (merge-on-read in silver MERGEs) | `optimize` rewrites them | ✅ silver, whole tables |

Two consequences found on this stack:
* **Idle stacks accumulate debt.** No DAG run since 2026-11-03 → 1,805 files in
  one heartbeat partition. In production with `daily` it is bounded to one day,
  but an outage or a paused schedule grows it.
* **`optimize` replaces files, it does not delete them.** The old small files
  stay referenced by older snapshots until `expire_snapshots` runs. Compaction
  without expiry makes storage *grow*.

## Maintenance can fight the sink

`optimize` commits a `replace` snapshot on the same table the sink appends to.
Two commits at once: one wins, the other retries (Iceberg optimistic
concurrency). If the sink's retries run out, that table's commit fails while
others succeed: the per-table case the completeness gate checks for
([sink protocol](sink-commit-protocol-and-gate.md#3-tables-commit-separately)).
That is why `optimize` runs **after** `commit_batch`: it can never delay or
change a publication.

## Expiry must respect time travel

Gold reads silver `FOR VERSION AS OF 'eod_YYYY_MM_DD'`. A tag keeps its snapshot
alive (`eod_*` 90 days, `close_YYYY_MM` 7 years, ADR 0011), so expiry is safe
for tagged snapshots. Untagged ones are fair game after a retention window,
which also bounds how far back a **rerun** or an investigation can time-travel.

Open for milestone n:
1. Expiry age for bronze (no tags; the log itself is the history, snapshots aren't needed).
2. Expiry for silver beyond tags.
3. Orphan cleanup grace period (must exceed the longest possible in-flight write).
4. A maintenance run independent of the business DAG, so idle days don't pile up.

## Questions to test yourself

1. Why does doubling `BRONZE_COMMIT_INTERVAL_MS` roughly halve the file count but
   also lengthen the gate?
2. After `optimize`, `$files` shows 1 file but the old small files are still in
   S3. Why, and what removes them?
3. Why is the heartbeat table a worse small-files case than `ledger_entries`?
