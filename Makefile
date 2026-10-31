# ledger-lakehouse -- developer entry points. CI calls these same targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
TRINO   := $(COMPOSE) exec -T trino trino --output-format=ALIGNED

.PHONY: help env-check up bootstrap down ps logs cdc-status trino smoke clean seed traffic test lint

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

# --- quality -------------------------------------------------------------------
test: ## Unit tests (generator)
	uv run pytest -q

lint: ## ruff (Python)
	uv run ruff check . && uv run ruff format --check .
