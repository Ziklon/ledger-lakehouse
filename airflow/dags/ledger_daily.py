"""Daily publication of the ledger lakehouse (brief section 8).

  check_connectors -> plan_batch -> completeness_gate -> open_batch (RUNNING)
    -> dbt silver (Cosmos: a task per model, its tests after it)
    -> tag_eod -> dbt gold (Cosmos) -> invariants -> commit_batch (COMMITTED, last)
    -> optimize

Who decides what:
  * Airflow decides WHEN, and names the business date (batch_id).
  * control.batches decides the WINDOW: window_start = last COMMITTED
    window_end, never Airflow's data_interval_start (missed days catch up).
  * The cut-off is data_interval_end (00:00 UTC), or an explicit one.
Every step is a function in ledger_pipeline.steps (the same code `make run-day`
runs). It executes in /opt/ledger-venv via @task.external_python, so the lockfile's
versions never mix with Airflow's constraints.

Schedule (env LEDGER_SCHEDULE) -- configuration, because demo business dates
(2026-01-xx, wall-clock windows) and calendar dates can't share one schedule:
  * daily (default, production): CronDataIntervalTimetable("0 0 * * *", UTC);
    batch_id = data_interval_start's date. Explicit: Airflow 3 defaults to
    CronTriggerTimetable, which has NO data interval.
  * demo: CronDataIntervalTimetable(LEDGER_DEMO_CRON), a short interval;
    batch_id = day after the latest COMMITTED batch.
  * manual: no schedule (local default); runs are triggered with conf.
Any run may carry conf {"batch_id": "...", "cutoff": "now" | ISO}: `make demo`,
`make airflow-run` and reruns. A manual run has no data interval.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import Asset, Param, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from cosmos.operators.local import DbtTestLocalOperator

log = logging.getLogger(__name__)

LEDGER_VENV_PY = "/opt/ledger-venv/bin/python"
DBT_DIR = os.environ.get("PIPELINE_DBT_DIR", "/opt/airflow/dbt")
SCHEDULE_MODE = os.environ.get("LEDGER_SCHEDULE", "daily")
DEMO_START_DATE = os.environ.get("LEDGER_DEMO_START_DATE", "2026-01-01")
SCHEDULES = {
    "daily": CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    "demo": CronDataIntervalTimetable(os.environ.get("LEDGER_DEMO_CRON", "*/3 * * * *"), "UTC"),
    "manual": None,
}
if SCHEDULE_MODE not in SCHEDULES:
    raise ValueError(f"LEDGER_SCHEDULE must be one of {sorted(SCHEDULES)}, got {SCHEDULE_MODE!r}")
GATE_TIMEOUT_S = int(os.environ.get("LEDGER_GATE_TIMEOUT_S", "600"))

PUBLISHED = [
    "accounts_full_daily",
    "accounts_incremental",
    "account_balances_full_daily",
    "account_balances_incremental",
]
# Downstream DAGs (exports, contracts...) schedule on these, not on time.
ASSETS = [Asset(name=f"gold.{t}", uri=f"trino://trino:8080/lakehouse/gold/{t}") for t in PUBLISHED]


def notify_failure(context) -> None:
    """Stub: in production this pages / posts to the on-call channel."""
    ti = context["ti"]
    log.error("ALERT ledger_daily task %s failed (try %s)", ti.task_id, ti.try_number)


# --- dbt through Cosmos --------------------------------------------------------
project = ProjectConfig(
    project_name="ledger_lakehouse",
    manifest_path=f"{DBT_DIR}/target/manifest.json",  # built into the image
)
profile = ProfileConfig(
    profile_name="ledger_lakehouse",
    target_name="docker",
    profiles_yml_filepath=f"{DBT_DIR}/profiles.yml",
)
execution = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/opt/ledger-venv/bin/dbt",
    dbt_project_path=DBT_DIR,
)
# The SAME vars for every dbt task, from the single decision made by open_batch.
# A JSON string on purpose: Cosmos passes strings to --vars verbatim, and JSON is
# YAML with real nulls (a Python dict repr would turn None into the string "None").
DBT_OPERATOR_ARGS = {
    "vars": "{{ ti.xcom_pull(task_ids='open_batch') }}",
    # unit tests check logic in CI, not batch data (docs/adr/0013)
    "env": {"DBT_EXCLUDE_RESOURCE_TYPES": "unit_test"},
    "append_env": True,
}


def dbt_group(group_id: str, paths: list[str]) -> DbtTaskGroup:
    return DbtTaskGroup(
        group_id=group_id,
        project_config=project,
        profile_config=profile,
        execution_config=execution,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,  # parsing the DAG never runs dbt
            select=[f"path:{p}" for p in paths],
            exclude=["tag:invariant"],  # own task below, gates COMMITTED
            test_behavior=TestBehavior.AFTER_EACH,
            # Cosmos would emit an Asset per model as soon as its task succeeds,
            # i.e. BEFORE invariants and COMMITTED. Only commit_batch announces
            # publication. Must be set HERE: Cosmos overwrites the operator-level
            # emit_datasets with RenderConfig's (cosmos/converter.py).
            emit_datasets=False,
        ),
        operator_args=DBT_OPERATOR_ARGS,
    )


@dag(
    dag_id="ledger_daily",
    schedule=SCHEDULES[SCHEDULE_MODE],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "on_failure_callback": notify_failure,
    },
    params={
        "batch_id": Param(None, type=["null", "string"], description="YYYY-MM-DD (manual runs)"),
        "cutoff": Param(None, type=["null", "string"], description="'now' or ISO (manual runs)"),
    },
    tags=["ledger", "publication"],
    doc_md=__doc__,
)
def ledger_daily():
    check_connectors = HttpSensor(
        task_id="check_connectors",
        http_conn_id="kafka_connect",
        endpoint="connectors?expand=status",
        response_check=lambda r: (
            bool(r.json())
            and all(
                c["status"]["connector"]["state"] == "RUNNING"
                and c["status"]["tasks"]
                and all(t["state"] == "RUNNING" for t in c["status"]["tasks"])
                for c in r.json().values()
            )
        ),
        poke_interval=15,
        timeout=600,
        mode="reschedule",  # frees the worker slot while waiting
    )

    # retries=0: a refused plan (closed day, batch in flight) is a decision, not a
    # transient error -- retrying only delays the alert.
    @task.external_python(python=LEDGER_VENV_PY, expect_airflow=False, retries=0)
    def plan_batch(conf_batch_id, conf_cutoff, interval_start, interval_end, demo, demo_start):
        from datetime import UTC, datetime

        from ledger_pipeline import steps

        if conf_batch_id:  # manual trigger / make demo / rerun
            batch_id = conf_batch_id
            cutoff = datetime.now(UTC).isoformat() if conf_cutoff == "now" else conf_cutoff
        elif not interval_end:
            raise ValueError("manual run without conf: pass {'batch_id': ..., 'cutoff': ...}")
        elif demo:
            batch_id, cutoff = steps.next_business_date(demo_start), interval_end
        else:
            batch_id, cutoff = interval_start[:10], interval_end
        return steps.plan(batch_id, cutoff or None)

    @task.external_python(python=LEDGER_VENV_PY, expect_airflow=False)
    def completeness_gate(plan, timeout_s):
        from ledger_pipeline import steps

        steps.completeness_gate(plan, timeout_s=timeout_s)

    @task.external_python(python=LEDGER_VENV_PY, expect_airflow=False, task_id="open_batch")
    def open_batch(plan):
        from ledger_pipeline import steps

        return steps.open_batch(plan)

    @task.external_python(python=LEDGER_VENV_PY, expect_airflow=False)
    def tag_eod(plan):
        from ledger_pipeline import steps

        return steps.tag_eod(plan)

    @task.external_python(python=LEDGER_VENV_PY, expect_airflow=False, outlets=ASSETS)
    def commit_batch(plan):
        from ledger_pipeline import steps

        return steps.commit_batch(plan)

    @task.external_python(
        python=LEDGER_VENV_PY, expect_airflow=False, retries=0, on_failure_callback=None
    )
    def optimize(plan):
        """Best effort, after publication: a failure never un-publishes."""
        from ledger_pipeline import steps

        return steps.optimize(plan)

    plan = plan_batch(
        conf_batch_id="{{ params.batch_id or '' }}",
        conf_cutoff="{{ params.cutoff or '' }}",
        # Airflow 3: a manual run has no data interval -- the names are not even
        # in the template context (strict undefined), hence `is defined`.
        interval_start="{{ data_interval_start.isoformat() if data_interval_start is defined and data_interval_start else '' }}",  # noqa: E501
        interval_end="{{ data_interval_end.isoformat() if data_interval_end is defined and data_interval_end else '' }}",  # noqa: E501
        demo=SCHEDULE_MODE == "demo",
        demo_start=DEMO_START_DATE,
    )
    gate = completeness_gate(plan, timeout_s=GATE_TIMEOUT_S)
    opened = open_batch(plan)
    silver = dbt_group("dbt_silver", ["models/staging", "models/silver"])
    tagged = tag_eod(plan)
    gold = dbt_group("dbt_gold", ["models/gold"])
    invariants = DbtTestLocalOperator(
        task_id="invariants",
        project_dir=DBT_DIR,
        profile_config=profile,
        dbt_executable_path="/opt/ledger-venv/bin/dbt",
        select="tag:invariant",
        retries=0,  # data problems don't fix themselves: fail fast, see runbook
        **DBT_OPERATOR_ARGS,
    )
    committed = commit_batch(plan)

    check_connectors >> plan >> gate >> opened >> silver >> tagged >> gold >> invariants
    invariants >> committed >> optimize(plan)


ledger_daily()
