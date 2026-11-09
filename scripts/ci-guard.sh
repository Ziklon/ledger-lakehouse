#!/usr/bin/env bash
# Refuse to run CI jobs against a stack that holds data you care about.
# The CI scripts recreate services, build models EMPTY and assume a fresh stack.
# The project comes from COMPOSE_PROJECT_NAME, which .env.example sets to
# `ledger-lakehouse`, and every script sources .env. A copy of the repo whose .env
# prepended another name was overridden by that later line, so it recreated the LIVE
# services (2026-11-07). So the guard checks the name compose actually RESOLVES to.
#   CI=true                    GitHub Actions (always a fresh runner)
#   COMPOSE_PROJECT_NAME=<x>   set in the copy's .env (edit the existing line) for a
#                              throwaway local project, with different HOST_*_PORTs
set -euo pipefail
project=$(docker compose config --format json | python3 -c "import json,sys; print(json.load(sys.stdin)['name'])")
if [[ "${CI:-}" == true ]]; then
  exit 0
fi
if [[ "$project" == ledger-lakehouse ]]; then
  echo "REFUSING: compose project is '$project' (your live stack)." >&2
  echo "Run in CI, or locally with an exported throwaway project and separate host ports:" >&2
  echo "  a copy of the repo whose .env sets COMPOSE_PROJECT_NAME=ledgerci and other HOST_*_PORTs" >&2
  exit 1
fi
echo "ci-guard: compose project '$project' (not the live stack)"
