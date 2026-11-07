# ledger-lakehouse -- developer entry points. CI calls these same targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
TRINO   := $(COMPOSE) exec -T trino trino --output-format=ALIGNED

.PHONY: help env-check up bootstrap down ps logs cdc-status trino smoke clean seed traffic run-day airflow-run demo batches test lint ci-dbt ci-integration

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n",$$1,$$2}'

.env:
	cp .env.example .env
	@echo "created .env from .env.example"

env-check: .env ## Fail fast if .env is missing keys that .env.example defines
	@missing=$$(comm -23 <(grep -oE '^[A-Z_]+=' .env.example | sort) <(grep -oE '^[A-Z_]+=' .env | sort)); \
	if [ -n "$$missing" ]; then echo "ERROR: .env is missing keys (copy them from .env.example):"; echo "$$missing"; exit 1; fi

up: env-check ## Build (cached), start the stack, wait until healthy, then bootstrap
	$(COMPOSE) up -d --build --wait
	./scripts/bootstrap.sh

bootstrap: ## Idempotent: bronze DDL, Iceberg control topic, (re)apply connectors
	./scripts/bootstrap.sh

down: ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

ps: ## Service status
	$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}'

logs: ## Tail logs (make logs S=connect)
	$(COMPOSE) logs -f --tail=100 $(S)

cdc-status: ## CDC health: connectors, slot lag, sink lag, bronze freshness
	./scripts/cdc-status.sh

trino: ## Interactive Trino SQL shell (default catalog lakehouse, schema bronze)
	$(COMPOSE) exec trino trino --catalog lakehouse --schema bronze

smoke: ## End-to-end check: Trino -> REST catalog -> S3 write/read round trip
	./scripts/smoke.sh

clean: ## Stop and DELETE all data volumes (images are kept, so no rebuild)
	$(COMPOSE) --profile ui down -v --remove-orphans

# --- traffic -------------------------------------------------------------------
GEN       := uv run --env-file .env ledger-gen
SEED      ?= 42
START_DATE ?= 2026-01-01

seed: ## Initial load of the source ledger (once)  [START_DATE=, SEED=]
	$(GEN) seed --date $(START_DATE) --seed $(SEED)

traffic: ## One business day of traffic  DATE=YYYY-MM-DD [PACE=seconds SEED=]
	@test -n "$(DATE)" || { echo "usage: make traffic DATE=2026-01-02"; exit 1; }
	$(GEN) day --date $(DATE) --seed $(SEED) --pace-seconds $(or $(PACE),0)

# --- publication ---------------------------------------------------------------
PIPE := uv run --env-file .env ledger-pipeline

# dbt packages (dbt_utils): installed on first use, reinstalled when the lock changes.
# A fresh clone has none; `make run-day`/`make test` used to rely on a past `dbt deps`.
dbt/dbt_packages/.installed: dbt/packages.yml dbt/package-lock.yml
	cd dbt && DBT_PROFILES_DIR=. uv run --env-file ../.env dbt deps
	touch $@

run-day: dbt/dbt_packages/.installed ## Publish one business day  DATE=YYYY-MM-DD [CUTOFF=now|ISO]
	@test -n "$(DATE)" || { echo "usage: make run-day DATE=2026-01-02 [CUTOFF=now]"; exit 1; }
	$(PIPE) run-day --batch-id $(DATE) $(if $(CUTOFF),--cutoff $(CUTOFF))

airflow-run: ## Publish one business day through Airflow  DATE=YYYY-MM-DD [CUTOFF=now|ISO]
	@test -n "$(DATE)" || { echo "usage: make airflow-run DATE=2026-01-02 [CUTOFF=now]"; exit 1; }
	./scripts/airflow-run.sh $(DATE) $(CUTOFF)

demo: ## RESETS the stack, then seed + DAYS=3 business days through Airflow, printing evidence
	./scripts/demo.sh

batches: ## Show control.batches
	@$(TRINO) --execute "select batch_id, status, window_start, window_end, watermark_lsn, prev_batch_id, committed_at from lakehouse.control.batches order by batch_id"

# --- quality -------------------------------------------------------------------
test: dbt/dbt_packages/.installed ## pytest (generator, pipeline) + dbt unit tests (needs the stack: make up)
	uv run pytest -q
	cd dbt && DBT_PROFILES_DIR=. uv run --env-file ../.env dbt test --select test_type:unit

lint: ## ruff (Python) + sqlfluff (SQL, trino dialect; offline)
	uv run ruff check . && uv run ruff format --check .
	uv run sqlfluff lint dbt/models dbt/tests infra/lakehouse

# --- CI (.github/workflows/ci.yml calls these; runnable locally) -----------------
ci-dbt: ## dbt job: bronze DDL, staging+silver built empty, unit tests [SLIM_CI_BASE=<base checkout>]
	./scripts/ci-dbt.sh

ci-integration: ## Fresh-stack end-to-end test with ASSERTIONS (2 days, run-day + Airflow); run after `make clean`
	./scripts/ci-integration.sh
