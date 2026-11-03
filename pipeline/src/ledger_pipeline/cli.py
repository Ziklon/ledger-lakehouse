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

from . import steps
from .config import Settings
from .windows import BatchPlan, BatchRefused, ceil_ms

log = logging.getLogger("ledger_pipeline")


def dbt(s: Settings, command: str, args: list[str], plan: BatchPlan) -> None:
    exe = Path(sys.executable).parent / "dbt"
    cmd = [str(exe), command, *args, "--vars", json.dumps(plan.dbt_vars())]
    log.info("dbt %s", " ".join(cmd[1:]))
    subprocess.run(
        cmd,
        cwd=s.dbt_dir,
        check=True,
        env={**os.environ, "DBT_PROFILES_DIR": str(s.dbt_dir)},
    )


def run_day(batch_id: date, cutoff: datetime | None, gate_timeout: int) -> None:
    """Every step in order -- the same functions the Airflow DAG calls."""
    s = Settings.from_env()
    t0 = time.monotonic()
    plan_d = steps.plan(batch_id.isoformat(), cutoff.isoformat() if cutoff else None)
    plan = BatchPlan.from_dict(plan_d)
    steps.completeness_gate(plan_d, timeout_s=gate_timeout)
    steps.open_batch(plan_d)

    # Unit tests check LOGIC on fixtures (make test / CI), not the batch's data;
    # invariants run as their own step below.
    build = ["--exclude-resource-type", "unit_test", "--exclude", "tag:invariant"]
    dbt(s, "build", ["--select", "path:models/staging", "path:models/silver", *build], plan)
    steps.tag_eod(plan_d)
    dbt(s, "build", ["--select", "path:models/gold", *build], plan)

    # Gate before publication: any failure leaves the batch RUNNING, so the
    # consumer views keep serving the previous COMMITTED day.
    dbt(s, "test", ["--select", "tag:invariant"], plan)
    log.info("invariants: all passed")

    steps.commit_batch(plan_d)
    steps.optimize(plan_d)
    log.info("run-day %s done in %.0fs", plan.batch_id, time.monotonic() - t0)


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
    sub.add_parser(
        "record-offset-baseline",
        help="migration: first Kafka offset per partition that bronze stores (bootstrap)",
    )
    args = p.parse_args(argv)

    if args.cmd == "record-offset-baseline":
        from . import control, gate

        s = Settings.from_env()
        gate.record_offset_baseline(s, control.connect(s))
        return 0

    try:
        run_day(args.batch_id, parse_cutoff(args.cutoff), args.gate_timeout)
    except BatchRefused as e:
        log.error("refused: %s", e)
        return 2
    except subprocess.CalledProcessError as e:
        log.error(
            "step failed (%s): batch %s NOT committed, it stays RUNNING; consumers still "
            "see the previous COMMITTED day. Fix the cause, then rerun the same DATE "
            "(docs/runbooks/invariant-failure.md).",
            " ".join(e.cmd[1:3]),
            args.batch_id,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
