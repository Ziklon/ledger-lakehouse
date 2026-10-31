# ledger-lakehouse -- developer entry points. CI calls these same targets.
SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE := docker compose
TRINO   := $(COMPOSE) exec -T trino trino --output-format=ALIGNED

.PHONY: help up down ps logs smoke clean

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-10s\033[0m %s\n",$$1,$$2}'

.env:
	cp .env.example .env
	@echo "created .env from .env.example"

up: .env ## Build (cached) and start the core stack; waits until every service is healthy
	$(COMPOSE) up -d --build --wait

down: ## Stop the stack (keeps data volumes)
	$(COMPOSE) down

ps: ## Service status
	$(COMPOSE) ps --format 'table {{.Service}}\t{{.Status}}'

logs: ## Tail logs (make logs S=connect)
	$(COMPOSE) logs -f --tail=100 $(S)

smoke: ## End-to-end check: Trino -> REST catalog -> S3 write/read round trip
	./scripts/smoke.sh

clean: ## Stop and DELETE all data volumes (images are kept, so no rebuild)
	$(COMPOSE) --profile ui down -v --remove-orphans
