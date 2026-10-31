---
status: accepted
date: 2026-10-31
---

# 0011 — Daily FULL: Iceberg tags on silver AND physical partitions in gold

## Context and problem statement

We need an immutable "state at end of business day N" for each entity, both
to compute INCREMENTAL(N) and to publish FULL(N). Iceberg tags give that for
free (time travel). Physical daily partitions cost storage. Which one?

## Considered options

1. **Both**: tag `eod_YYYY_MM_DD` on every silver `*_current` (the frozen
   input), and `<entity>_full_daily` partitioned by `snapshot_date` (the
   published output), both built from the same tag
2. Tags only: consumers time-travel silver
3. Partitions only: INCREMENTAL diffs two FULL partitions

## Decision outcome

Chosen: **option 1**.

* `tag_eod` (pyiceberg `manage_snapshots`) runs right after silver and before gold.
  Gold reads `FOR VERSION AS OF 'eod_…'` (verified on Trino 484). FULL and
  INCREMENTAL therefore read the same frozen snapshot: same cut-off, so they agree
  by construction, and a rerun reads exactly what the first run read.
* Tag retention is on the ref: `eod_*` 90 days, `close_YYYY_MM` (month-end) 7 years.
  It's enforced by snapshot expiry (Phase 2 retention). FULL partitions keep 90 days
  (`full_daily_retention_days`), deleted by a post-hook.

### Consequences

* Good: consumers and second engines (Athena, Phase 3) read plain partitions:
  no time-travel syntax, and no dependency on snapshot retention.
* Good: tags make every EOD of silver reproducible for audit and reruns.
* Bad: FULL partitions duplicate state (cheap at this scale, bounded by retention).
* **Rerun policy (deviation from the brief's "recreate the tag"):** the tag of
  a COMMITTED day is never moved. Silver may already contain later changes,
  so re-pointing it would rewrite history. A rerun re-publishes gold from the
  existing tag (proven: identical checksums). Only an attempt of a not-yet-committed
  batch (re)creates its tag. Rebuilding a past EOD from bronze is `make backfill`
  (Phase 2).
* Note: with the REST catalog, a tag change is a metadata-only commit done by the
  catalog server, so pyiceberg needs no FileIO. With Glue (Phase 3) pyiceberg writes
  the metadata itself and needs a FileIO extra (`pyiceberg[pyarrow]`).
