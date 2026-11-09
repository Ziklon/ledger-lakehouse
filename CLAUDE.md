# ledger-lakehouse — working agreement

CDC (Postgres → Debezium → Redpanda → Iceberg sink) into an Iceberg lakehouse on
S3 + REST catalog, transformed with dbt-trino, orchestrated by Airflow 3 + Cosmos.
Publishes reconciled daily FULL and INCREMENTAL datasets.

## Commands
| | |
|---|---|
| `make up` | build (cached) + start stack, wait healthy, then `bootstrap` |
| `make bootstrap` | idempotent: bronze DDL, `_iceberg_control` topic, PUT connectors |
| `make seed` / `make traffic DATE=2026-01-02 [PACE=s]` | generator: initial load / one business day |
| `make cdc-status` | connectors, replication-slot lag, sink lag, bronze freshness |
| `make trino` | interactive Trino SQL shell (catalog lakehouse, schema bronze) |
| `make run-day DATE=2026-01-02 [CUTOFF=now]` | publish one business day: gate → RUNNING → silver → EOD tags → gold → COMMITTED |
| `make airflow-run DATE=2026-01-02 [CUTOFF=now]` | same publication through the Airflow DAG (trigger + wait, prints task states) |
| `make demo` | RESETS volumes, seed + 3 days through Airflow, prints evidence (~14 min); `scripts/demo-report.sh DATE` for one day |
| `make batches` | show `control.batches` |
| `make test` | pytest (generator, pipeline) + dbt unit tests (needs the stack) |
| `make lint` | ruff + sqlfluff (trino, jinja templater, offline) |
| `make ci-dbt` / `make ci-integration` | CI jobs (ADR 0017); refuse the live project (`scripts/ci-guard.sh`): CI only, or a throwaway copy |
| `make smoke` | end-to-end Trino → REST catalog → S3 round trip |
| `make ps` / `make logs S=<svc>` | status / logs |
| `make down` | stop, keep data |
| `make clean` | stop and delete volumes (images kept) |
| `docker compose --profile ui up -d` | + Redpanda Console on :8088 |

Host ports (see `.env.example`): Airflow UI 18090 (no login locally), Trino 18080, Postgres 15432, Kafka 19092,
schema registry 18081, Connect 8083, MinIO 9000/9001, REST catalog 8181.
Query: `docker compose exec trino trino --execute "..."`.

## Conventions
- **Pin every version.** Multi-arch images pinned by tag, or by OCI *index* digest
  (`docker buildx imagetools inspect`), never a platform manifest digest.
- **All endpoints/credentials are config** (`.env` → env vars → `${ENV:X}` in Trino,
  `${env:X}` in connector JSON, `env_var()` in dbt). Never hard-code; `.env` is gitignored.
- Trino catalog files are the per-environment overlay (Trino rejects unused props).
- Topics are never auto-created (`auto_create_topics_enabled=false`).
- Bronze is an append-only change log; silver mirrors; gold publishes (see docs/adr).
  Tables: `lakehouse.bronze.<table>_cdc` (single-level namespaces: Glue has no nesting).
  Metadata columns: `_op _lsn _source_commit_ts _ingested_at _cdc_table _kafka_partition
  _kafka_offset _tx_id`. New bronze columns = additive DDL (`ALTER … IF NOT EXISTS`) applied
  BEFORE the sink config that fills them (the sink silently drops unknown fields).
  Bronze DDL is explicit (`infra/lakehouse/bronze.sql`) and applied BEFORE the sink starts.
- Order CDC events by `_lsn`, never by `_source_commit_ts` (ms precision, ties are common).
- Delete events (`_op='d'`) carry only the key; NOT NULL columns arrive as type
  DEFAULTS (0, '', 1970-01-01), not NULL. Never take business columns from a delete event.
- dbt: staging = `cdc_latest_event` macro, silver = `cdc_current` macro (row-level carry-forward on
  delete, skip create+delete in window, LSN guard, hash incl. is_deleted). Guard var checks
  with `execute` so parse/ls (Cosmos) work without vars.
- Batch control lives in `pipeline/` (`ledger_pipeline`): window planning (pure, `windows.py`),
  gate, `control.batches` I/O, pyiceberg EOD tags. dbt only receives the batch as vars and never
  writes control. Airflow (milestone f) calls the same functions.
- Windows are `[start, end)` at **millisecond** precision (= `_source_commit_ts`); `now` cut-offs round
  UP to the ms. Closed days are refused; rerunning the latest COMMITTED day reuses its window and tags.
- Gold reads silver `FOR VERSION AS OF 'eod_YYYY_MM_DD'`; consumers use `gold.accounts` /
  `gold.account_balances` (latest COMMITTED, soft deletes filtered).
- Tests (ADR 0013): invariants = `tag:invariant`, own step before COMMITTED (a failure leaves the
  batch RUNNING; runbook docs/runbooks/invariant-failure.md); standard tests inside `dbt build`;
  unit tests in `make test`/CI only. Prove new tests can FAIL (corrupt data / mutate the macro).
- sqlfluff can't parse `FOR VERSION AS OF`: `at_eod` lives alone in `macros/time_travel.sql` so
  `.sqlfluff` overrides it at lint time. `noqa` must be alone in its comment.
- Airflow (ADR 0014): DAG `airflow/dags/ledger_daily.py` is thin; logic in `ledger_pipeline.steps`.
  Tasks run in `/opt/ledger-venv` (`@task.external_python`, from uv.lock); Cosmos calls its dbt.
  The dbt project + manifest AND `ledger_pipeline` are BAKED into the image: dbt or pipeline
  changes need `make up` (rebuild) before Airflow runs them (`make run-day` uses the host code).
  `LEDGER_SCHEDULE` = manual (local) | demo | daily (production). Manual runs: no data interval,
  the names are undefined in templates (`is defined`). Cosmos asset emission off via
  `RenderConfig(emit_datasets=False)` (the operator-level flag is overwritten).
- Completeness gate = heartbeat ≥ cut-off in bronze + sink committed offsets ≥ end offsets
  snapshotted at that moment (never "lag == 0": heartbeats keep it ≥ 1) + every partition's
  record `end - 1` present in its bronze table (`_kafka_offset`). The sink commits source offsets
  BEFORE its Iceberg commit, one commit per table: offsets prove "read", not "visible".
  Partitions with no event since the `_kafka_offset` migration (`control.bronze_offset_baseline`)
  fall back to a heartbeat with `_lsn` > source `pg_current_wal_lsn()` (ADR 0010).
- Freshness: pipeline health = heartbeat (strict); business tables only a soft 12h warning.
- Hashes: silver `row_hash` = all mirrored columns (incl. updated_at) + is_deleted; gold computes its
  OWN hash over exactly the PUBLISHED columns (`_gold_hash`), so flaps publish nothing. Every published
  column must be in the gold hash (ADR 0008). updated_at is not published.
- Ledger entries are immutable; corrections are reversal transactions (`reverses_entry_id`).
- Generator: pure planner (`planner.py`) + I/O shell (`db.py`); one operation = one DB txn.
- SQL: explicit column lists, never `SELECT *`. sqlfluff (trino dialect). Python: uv + ruff.
- `_batch_id` comes from a dbt var supplied by Airflow — never `invocation_id`.
- Decisions and deviations from the brief → `docs/adr/NNNN-*.md` (MADR).
- Conventional commits, one per milestone. No "generated by" lines in commit messages.

## Verification habit
Never claim something works without running it and showing output (query results,
connector status, test output). Healthy ≠ correct: prove behavior (e.g. `make smoke`).

## Known gotchas
- Named volumes mounted on a path absent from the image are root-owned → non-root
  services (iceberg-rest uid 1000) can't write. Mount on an existing, owned dir.
- Trino `map['k']` throws on missing keys; use `element_at(map, 'k')`.
- Config overrides on the shell (`VAR=x make up`) are ephemeral: the next `docker compose up`
  silently recreates services with `.env` values. Persist settings in `.env`.
- `.env` is SOURCED by scripts: quote values with spaces (`LEDGER_DEMO_CRON="*/3 * * * *"`).
- `make env-check` (run by `make up`) fails if `.env` lacks keys from `.env.example`.
- psycopg defaults to autocommit=False: then `conn.transaction()` is only a SAVEPOINT in one
  outer transaction. The generator connects with autocommit=True.
- Sink regex subscription finds new topics only on metadata refresh (override set to 10s).
- Iceberg sink runtime isn't on Maven Central; built from ASF source in
  `infra/connect/Dockerfile` (~7 min cold, cached afterwards). CI pushes it to
  `ghcr.io/ziklon/ledger-lakehouse-connect` (public, registry build cache).
- CI (ADR 0017): GitHub Actions pinned by commit SHA (tag in a comment). dbt unit tests
  need their input relations to exist (CI builds staging + silver with `--empty`);
  gold can't be built `--empty` (Trino rejects `FOR VERSION AS OF` on dbt's `limit 0`
  subquery), so gold is covered by `make ci-integration`.
- The compose project comes from `COMPOSE_PROJECT_NAME` in `.env` (= `ledger-lakehouse`),
  and scripts `source .env`: a copy of the repo with a *prepended* name still targets the
  live stack. Edit the existing line, and check `docker compose config` before running.
- A REST catalog / MinIO restart under a running sink kills its coordinator (task FAILED,
  never auto-restarted): restart `POST /connectors/bronze-sink/tasks/0/restart`, then check
  `_kafka_offset` is distinct and contiguous.
