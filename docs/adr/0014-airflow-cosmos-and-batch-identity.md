---
status: accepted
date: 2026-11-01
---

# 0014 — Airflow 3 + Cosmos for orchestration; batch identity from the pipeline, not dbt

## Context and problem statement

A daily publication is a sequence with gates (connectors, completeness,
invariants) around two dbt phases. It needs retries, alerting, a UI,
per-model visibility, and a batch identity shared by every step.

## Considered options

1. **Airflow 3 (LocalExecutor) + Astronomer Cosmos** (task per dbt model)
2. Dagster (software-defined assets, native dbt integration)
3. Airflow with a single `dbt build` task (the brief's fallback)

## Decision outcome

Chosen: **option 1**. Verified compatible: Cosmos 1.15.1 renders and runs on
Airflow 3.3.2 (30 tasks, every model followed by its tests), so the fallback
isn't needed.

* **Thin DAG, logic in `ledger_pipeline.steps`.** Every task calls a function
  that `make run-day` also calls, so the code is tested once and the orchestrator is replaceable.
* **Two Python environments.** Airflow + Cosmos in Airflow's env (its constraints).
  dbt + `ledger_pipeline` in `/opt/ledger-venv`, installed from `uv.lock`. Tasks run
  there via `@task.external_python`, and Cosmos calls its `dbt`, so constraints
  and lockfile never have to agree.
* **Batch identity:** Airflow names the business date (`data_interval_start`, or
  `conf.batch_id`), and `control.batches` decides the window. `open_batch` returns the
  dbt vars as **JSON** in XCom, and every dbt task receives that one string, because
  Cosmos runs one dbt invocation per model (`invocation_id` would differ per
  task). JSON, not a Python dict: Cosmos passes strings verbatim, and a dict repr would
  turn `None` into the string `"None"`.
* **Schedule is configuration** (`LEDGER_SCHEDULE`): `daily` =
  `CronDataIntervalTimetable("0 0 * * *", UTC)`, explicit because Airflow 3 defaults to
  `CronTriggerTimetable` (no data interval); `demo` = short interval; `manual` =
  local default. Demo business dates (2026-01-xx, wall-clock windows) and calendar
  dates can't share one schedule: a calendar run would make every later demo day a
  "closed day".
* `max_active_runs=1`, `catchup=False` (the window logic catches up, not Airflow),
  retries with exponential backoff. Except `plan_batch` and `invariants`: retries=0,
  because a refusal or a data problem isn't transient. Failure callback (stub).
* `LoadMode.DBT_MANIFEST` (manifest built into the image): DAG parsing takes 0.15 s
  and never runs dbt or touches Trino.
* **Assets only from `commit_batch`.** Cosmos emits an Asset per model when its task
  succeeds, i.e. *before* invariants and COMMITTED. Disabled with
  `RenderConfig(emit_datasets=False)`. The operator-level flag is silently overwritten
  by Cosmos (`cosmos/converter.py`). Verified: exactly 4 asset events per
  successful run, 0 for a run that fails its invariants.

### Consequences

* Good: per-model retries and visibility; tests right after each model.
* Good: a failed invariant leaves the batch RUNNING, `commit_batch` never runs,
  no asset events (rehearsed through Airflow).
* Bad: more moving parts than one `dbt build` (30 tasks, and Airflow 3 needs an
  API server + dag-processor + scheduler + metadata DB: ~3.5 GB of limits locally).
* Option 2 rejected for Phase 1: an equally valid design (assets map naturally to
  gold datasets), but the brief targets Airflow, and Cosmos gives the dbt-native
  task graph Dagster would.
