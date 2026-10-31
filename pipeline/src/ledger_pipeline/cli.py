"""ledger-pipeline: run one daily publication end to end (make run-day).

Step order = the Airflow DAG of milestone (f), which calls the same functions
and replaces the dbt subprocess with Cosmos task groups:

  plan -> completeness gate -> open_batch (RUNNING) -> dbt staging + silver
       -> tag_eod -> dbt gold -> [invariants: milestone e] -> commit_batch (COMMITTED)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import UTC, date, datetime
from pathlib import Path

from . import control, gate, tags
from .config import Settings
from .windows import BatchPlan, BatchRefused, ceil_ms, plan_batch

log = logging.getLogger("ledger_pipeline")


def dbt_build(s: Settings, select: list[str], plan: BatchPlan) -> None:
    dbt = Path(sys.executable).parent / "dbt"
    cmd = [str(dbt), "build", "--select", *select, "--vars", json.dumps(plan.dbt_vars())]
    log.info("dbt %s", " ".join(cmd[1:]))
    subprocess.run(
        cmd,
        cwd=s.dbt_dir,
        check=True,
        env={**os.environ, "DBT_PROFILES_DIR": str(s.dbt_dir)},
    )


def run_day(batch_id: date, cutoff: datetime | None, gate_timeout: int) -> None:
    s = Settings.from_env()
    conn = control.connect(s)
    t0 = time.monotonic()

    plan = plan_batch(batch_id, control.list_batches(conn), now=datetime.now(UTC), cutoff=cutoff)
    log.info(
        "plan: batch %s window [%s, %s) prev %s%s",
        plan.batch_id,
        plan.window_start,
        plan.window_end,
        plan.prev_batch_id,
        " (RERUN of committed day)" if plan.is_rerun else "",
    )

    if not plan.is_rerun:
        hb = gate.wait_for_heartbeat(conn, plan.window_end, timeout_s=gate_timeout)
        log.info("gate: heartbeat in bronze at %s >= cut-off", hb)

    lsn, commit_ts = control.open_batch(conn, plan)
    log.info("open_batch: RUNNING, watermark lsn=%s commit_ts=%s", lsn, commit_ts)

    dbt_build(s, ["path:models/staging", "path:models/silver"], plan)

    tagged = tags.tag_eod(s, plan.batch_id, committed=plan.is_rerun)
    log.info("tag_eod: %s", tagged)

    dbt_build(s, ["path:models/gold"], plan)

    log.info("invariants: added in milestone (e)")

    counts, sums = control.dataset_stats(conn, plan.batch_id)
    control.commit_batch(conn, plan.batch_id, counts, sums)
    log.info(
        "commit_batch: COMMITTED %s in %.0fs  %s", plan.batch_id, time.monotonic() - t0, counts
    )


def parse_cutoff(value: str | None) -> datetime | None:
    if value in (None, ""):
        return None
    if value == "now":
        return ceil_ms(datetime.now(UTC))
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("pyiceberg.io").setLevel(logging.ERROR)  # FileIO notice, see tags.py

    p = argparse.ArgumentParser(prog="ledger-pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run-day", help="publish one business day")
    r.add_argument("--batch-id", required=True, type=date.fromisoformat, help="YYYY-MM-DD")
    r.add_argument(
        "--cutoff",
        help="ISO timestamp or 'now' (demo). Default: 00:00 UTC after the business date",
    )
    r.add_argument("--gate-timeout", type=int, default=300)
    args = p.parse_args(argv)

    try:
        run_day(args.batch_id, parse_cutoff(args.cutoff), args.gate_timeout)
    except BatchRefused as e:
        log.error("refused: %s", e)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
