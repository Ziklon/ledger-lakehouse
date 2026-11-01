"""Post-commit table maintenance (DAG task `optimize`).

Compaction (Trino `optimize`) merges the small files streaming ingestion and
per-batch MERGEs create. Bronze: only the day partitions the batch window
touched -- Trino accepts only partition-aligned filters, so the filter is on
the day, not on the window timestamps. Silver: whole tables (small, unpartitioned).
Runs AFTER commit_batch: it never blocks or changes a publication.
Snapshot expiry / orphan cleanup is Phase 2 (retention).
"""

from __future__ import annotations

from .control import query, tables_in
from .windows import BatchPlan


def optimize(conn, plan: BatchPlan) -> list[str]:
    first = plan.window_start.date().isoformat()
    last = plan.window_end.date().isoformat()
    done = []
    for t in tables_in(conn, "bronze", "_cdc"):
        query(
            conn,
            f"alter table lakehouse.bronze.{t} execute optimize "
            f"where cast(_source_commit_ts as date) between date '{first}' and date '{last}'",
        )
        done.append(f"bronze.{t}")
    for t in tables_in(conn, "silver", "_current"):
        query(conn, f"alter table lakehouse.silver.{t} execute optimize")
        done.append(f"silver.{t}")
    return done
