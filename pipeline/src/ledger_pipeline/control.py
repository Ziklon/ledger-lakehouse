"""control.batches I/O through Trino (docs/adr/0012).

Callers: cli.run_day today; the Airflow tasks open_batch / commit_batch in
milestone (f). Readers of what we write: gold consumer views (COMMITTED only),
the next batch's plan_batch, invariant checks.
"""

from __future__ import annotations

from datetime import date, datetime

import trino

from .config import Settings
from .windows import COMMITTED, RUNNING, BatchPlan, BatchRecord

BATCHES = "lakehouse.control.batches"


def connect(s: Settings) -> trino.dbapi.Connection:
    return trino.dbapi.connect(
        host=s.trino_host, port=s.trino_port, user=s.trino_user, catalog="lakehouse"
    )


def query(conn, sql: str, params: list | None = None) -> list[tuple]:
    cur = conn.cursor()
    cur.execute(sql, params or [])
    return cur.fetchall()


def list_batches(conn) -> list[BatchRecord]:
    rows = query(
        conn,
        f"select batch_id, status, window_start, window_end, prev_batch_id "
        f"from {BATCHES} where batch_type = 'daily'",
    )
    return [
        BatchRecord(
            batch_id=date.fromisoformat(r[0]),
            status=r[1],
            window_start=r[2],
            window_end=r[3],
            prev_batch_id=date.fromisoformat(r[4]) if r[4] else None,
        )
        for r in rows
    ]


def tables_in(conn, schema: str, suffix: str) -> list[str]:
    rows = query(
        conn,
        "select table_name from lakehouse.information_schema.tables "
        "where table_schema = ? and table_name like ? order by 1",
        [schema, f"%{suffix}"],
    )
    return [r[0] for r in rows]


def watermark(conn, plan: BatchPlan) -> tuple[int | None, datetime | None]:
    """Highest WAL position / commit time present in bronze inside the window,
    over every bronze table (heartbeat included, so idle days still advance)."""
    parts = [
        f"select _lsn, _source_commit_ts from lakehouse.bronze.{t} "
        "where _source_commit_ts >= from_iso8601_timestamp(?) "
        "and _source_commit_ts < from_iso8601_timestamp(?)"
        for t in tables_in(conn, "bronze", "_cdc")
    ]
    params = [plan.window_start.isoformat(), plan.window_end.isoformat()] * len(parts)
    row = query(
        conn,
        "select max(_lsn), max(_source_commit_ts) from (" + " union all ".join(parts) + ")",
        params,
    )[0]
    return row[0], row[1]


def open_batch(conn, plan: BatchPlan) -> tuple[int | None, datetime | None]:
    """Upsert the batch as RUNNING. One MERGE = one atomic Iceberg commit.
    On a rerun this also hides the batch from consumers until commit_batch
    (they fall back to the previous COMMITTED day while partitions are rewritten)."""
    lsn, commit_ts = watermark(conn, plan)
    query(
        conn,
        f"""
        merge into {BATCHES} t
        using (select
                 ? as batch_id, 'daily' as batch_type,
                 from_iso8601_timestamp(?) as window_start,
                 from_iso8601_timestamp(?) as window_end,
                 cast(? as bigint) as watermark_lsn,
                 from_iso8601_timestamp(?) as watermark_commit_ts,
                 cast(? as varchar) as prev_batch_id) s
        on t.batch_id = s.batch_id and t.batch_type = s.batch_type
        when matched then update set
            window_start = s.window_start, window_end = s.window_end,
            watermark_lsn = s.watermark_lsn, watermark_commit_ts = s.watermark_commit_ts,
            prev_batch_id = s.prev_batch_id, status = '{RUNNING}',
            row_counts = null, checksums = null,
            started_at = current_timestamp(6), committed_at = null
        when not matched then insert
            (batch_id, batch_type, window_start, window_end, watermark_lsn,
             watermark_commit_ts, prev_batch_id, status, started_at)
        values (s.batch_id, s.batch_type, s.window_start, s.window_end, s.watermark_lsn,
                s.watermark_commit_ts, s.prev_batch_id, '{RUNNING}', current_timestamp(6))
        """,
        [
            plan.batch_id.isoformat(),
            plan.window_start.isoformat(),
            plan.window_end.isoformat(),
            lsn,
            commit_ts.isoformat() if commit_ts else None,
            plan.prev_batch_id.isoformat() if plan.prev_batch_id else None,
        ],
    )
    return lsn, commit_ts


def dataset_stats(conn, batch_id: date) -> tuple[dict[str, int], dict[str, str]]:
    """Row counts and order-independent checksums of every gold dataset for the batch.
    Discovered by naming convention, so a new entity needs no code change."""
    counts: dict[str, int] = {}
    sums: dict[str, str] = {}
    d = batch_id.isoformat()
    for t in tables_in(conn, "gold", "_full_daily"):
        has_soft_delete = bool(
            query(
                conn,
                "select 1 from lakehouse.information_schema.columns "
                "where table_schema = 'gold' and table_name = ? and column_name = 'is_deleted'",
                [t],
            )
        )
        active = "not is_deleted" if has_soft_delete else "true"
        total, n_active, chk = query(
            conn,
            f"select count(*), count_if({active}), "
            f"to_hex(checksum(case when {active} then row_hash end)) "
            f"from lakehouse.gold.{t} where snapshot_date = date '{d}'",
        )[0]
        counts[t] = total
        counts[f"{t}.active"] = n_active
        sums[f"{t}.active"] = chk or ""
    for t in tables_in(conn, "gold", "_incremental"):
        for op, n in query(
            conn,
            f"select op, count(*) from lakehouse.gold.{t} "
            f"where batch_date = date '{d}' group by op",
        ):
            counts[f"{t}.{op}"] = n
        counts[t] = sum(v for k, v in counts.items() if k.startswith(f"{t}."))
    return counts, sums


def commit_batch(conn, batch_id: date, counts: dict[str, int], sums: dict[str, str]) -> None:
    """Flip to COMMITTED -- the LAST write of a batch, in one statement."""
    ck, cv = list(counts), list(counts.values())
    sk, sv = list(sums), list(sums.values())
    query(
        conn,
        f"""
        update {BATCHES} set
            status = '{COMMITTED}',
            row_counts = map(cast(? as array(varchar)), cast(? as array(bigint))),
            checksums = map(cast(? as array(varchar)), cast(? as array(varchar))),
            committed_at = current_timestamp(6)
        where batch_id = ? and batch_type = 'daily' and status = '{RUNNING}'
        """,
        [ck, cv, sk, sv, batch_id.isoformat()],
    )
