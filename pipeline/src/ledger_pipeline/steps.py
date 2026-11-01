"""The publication steps, one function each. Two callers, same code:

  * cli.run_day       -- `make run-day`, sequential, dbt via subprocess
  * Airflow DAG       -- one task per step (dbt via Cosmos), plan passed as XCom

Each step takes/returns JSON-safe values (BatchPlan.to_dict) so it can run in
a separate process: Airflow runs every task in its own process (and on EKS in
its own pod), so nothing may live in memory between steps.
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

from . import control, gate, maintenance, tags
from .config import Settings
from .windows import BatchPlan, ceil_ms, plan_batch

log = logging.getLogger("ledger_pipeline")


def plan(batch_id: str, cutoff: str | None) -> dict:
    """Window for the batch from control.batches (never from Airflow dates)."""
    s = Settings.from_env()
    conn = control.connect(s)
    p = plan_batch(
        date.fromisoformat(batch_id),
        control.list_batches(conn),
        now=datetime.now(UTC),
        cutoff=ceil_ms(datetime.fromisoformat(cutoff)) if cutoff else None,
    )
    log.info(
        "plan: batch %s window [%s, %s) prev %s%s",
        p.batch_id, p.window_start, p.window_end, p.prev_batch_id,
        " (RERUN of committed day)" if p.is_rerun else "",
    )  # fmt: skip
    return p.to_dict()


def next_business_date(start: str) -> str:
    """Demo calendar: the day after the latest COMMITTED batch (or `start`)."""
    s = Settings.from_env()
    committed = [
        b.batch_id for b in control.list_batches(control.connect(s)) if b.status == "COMMITTED"
    ]
    if not committed:
        return start
    return date.fromordinal(max(committed).toordinal() + 1).isoformat()


def completeness_gate(plan_d: dict, timeout_s: int = 300) -> None:
    p = BatchPlan.from_dict(plan_d)
    if p.is_rerun:
        log.info("gate: skipped (rerun of a COMMITTED day: window already proven complete)")
        return
    s = Settings.from_env()
    hb = gate.wait_for_heartbeat(control.connect(s), p.window_end, timeout_s=timeout_s)
    log.info("gate 1/2: heartbeat in bronze at %s >= cut-off %s", hb, p.window_end)
    topics = gate.wait_for_sink(s, timeout_s=timeout_s)
    log.info("gate 2/2: sink %s caught up on %s", s.sink_group, topics)


def open_batch(plan_d: dict) -> str:
    """RUNNING row; returns the dbt --vars JSON (the batch identity for every dbt task)."""
    import json

    p = BatchPlan.from_dict(plan_d)
    lsn, commit_ts = control.open_batch(control.connect(Settings.from_env()), p)
    log.info("open_batch: %s RUNNING, watermark lsn=%s commit_ts=%s", p.batch_id, lsn, commit_ts)
    return json.dumps(p.dbt_vars())


def tag_eod(plan_d: dict) -> dict[str, int]:
    p = BatchPlan.from_dict(plan_d)
    tagged = tags.tag_eod(Settings.from_env(), p.batch_id, committed=p.is_rerun)
    log.info("tag_eod: %s", tagged)
    return tagged


def commit_batch(plan_d: dict) -> dict[str, int]:
    p = BatchPlan.from_dict(plan_d)
    conn = control.connect(Settings.from_env())
    counts, sums = control.dataset_stats(conn, p.batch_id)
    control.commit_batch(conn, p.batch_id, counts, sums)
    log.info("commit_batch: %s COMMITTED %s", p.batch_id, counts)
    return counts


def optimize(plan_d: dict) -> list[str]:
    p = BatchPlan.from_dict(plan_d)
    done = maintenance.optimize(control.connect(Settings.from_env()), p)
    log.info("optimize: %s", done)
    return done
