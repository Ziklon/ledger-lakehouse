#!/usr/bin/env bash
# CI job "dbt" (also runnable locally: `make ci-dbt`). Needs no Kafka/Connect/Airflow.
#   1. Trino + its dependencies (Postgres, MinIO, REST catalog), bronze DDL
#   2. staging + silver built EMPTY (dbt --empty): unit tests read the column types
#      of their input relations, so those relations must exist (0/8 pass otherwise)
#   3. all dbt unit tests
#   4. Slim CI (when SLIM_CI_BASE points at a checkout of the base branch): list the
#      models + unit tests changed vs. base and everything downstream of them
# Gold is NOT built here: dbt's --empty wraps refs in a `limit 0` subquery and Trino
# rejects `FOR VERSION AS OF` on a subquery. Gold runs on real data in ci-integration.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD
set -a; source .env; set +a
./scripts/ci-guard.sh

echo "== 1. Trino + catalog, bronze DDL =="
docker compose up -d --wait trino
docker compose exec -T trino trino < infra/lakehouse/bronze.sql > /dev/null

cd dbt
export DBT_PROFILES_DIR=.
dbt() { uv run --project "$ROOT" --env-file "$ROOT/.env" dbt "$@"; }
# Any valid batch: the models only need to compile and create empty relations.
VARS='{"batch_id": "2026-01-01", "window_start": "1970-01-01T00:00:00+00:00",
       "window_end": "2026-01-02T00:00:00+00:00", "prev_batch_id": null}'

echo "== 2. staging + silver, empty =="
dbt deps
dbt run --empty --select path:models/staging path:models/silver --vars "$VARS"

echo "== 3. unit tests =="
dbt test --select test_type:unit

if [[ -n "${SLIM_CI_BASE:-}" ]]; then
  echo "== 4. Slim CI: changed vs. base, plus downstream =="
  (cd "$SLIM_CI_BASE/dbt" && dbt deps > /dev/null && dbt parse > /dev/null)
  dbt ls --select state:modified+ --state "$SLIM_CI_BASE/dbt/target" \
    --resource-type model --resource-type unit_test --output name --quiet \
    | sort > target/slim-ci.txt || true
  if [[ -s target/slim-ci.txt ]]; then
    report="dbt nodes affected by this change ($(wc -l < target/slim-ci.txt | tr -d ' ')):"$'\n\n'"$(sed 's/^/- /' target/slim-ci.txt)"
  else
    report="No dbt model or unit test changed vs. base."
  fi
  echo "$report"
  [[ -n "${GITHUB_STEP_SUMMARY:-}" ]] && printf '### Slim CI\n\n%s\n' "$report" >> "$GITHUB_STEP_SUMMARY"
fi
echo "CI-DBT OK"
