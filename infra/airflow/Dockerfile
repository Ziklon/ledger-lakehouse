# Airflow 3 + Cosmos, with the ledger code in its OWN virtualenv.
#
# Two Python environments, on purpose:
#   * Airflow's  (/usr/local, Airflow constraints): Airflow + Cosmos only
#   * /opt/ledger-venv (exact pins from uv.lock): dbt + ledger_pipeline
# Tasks run in the ledger venv (@task.external_python; Cosmos calls its dbt),
# so Airflow's constraints and our lockfile never have to agree.
# Build context = repo root (see .dockerignore).
FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir "astronomer-cosmos==1.15.1" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY --from=ghcr.io/astral-sh/uv:0.11.28@sha256:0f36cb9361a3346885ca3677e3767016687b5a170c1a6b88465ec14aefec90aa /uv /usr/local/bin/uv

USER root
WORKDIR /build
# Workspace metadata + lockfile: the ledger venv gets EXACTLY the laptop's versions.
COPY pyproject.toml uv.lock ./
COPY generator/pyproject.toml generator/pyproject.toml
COPY pipeline pipeline
RUN mkdir -p generator/src/ledger_gen \
 && uv export --frozen --no-dev --package ledger-pipeline --no-emit-workspace --no-hashes -o /tmp/req.txt \
 && uv venv /opt/ledger-venv --python /usr/local/bin/python \
 && uv pip install --python /opt/ledger-venv/bin/python --no-cache -r /tmp/req.txt \
 && uv pip install --python /opt/ledger-venv/bin/python --no-cache --no-deps ./pipeline \
 && chown -R airflow:0 /opt/ledger-venv && rm -rf /build

# dbt project baked into the image (prod-like: CI builds the image per commit).
# deps + parse at build: packages vendored, manifest.json ready for Cosmos
# (LoadMode.DBT_MANIFEST -> DAG parsing never runs dbt or touches Trino).
COPY --chown=airflow:0 dbt /opt/airflow/dbt
USER airflow
WORKDIR /opt/airflow/dbt
RUN DBT_PROFILES_DIR=. DBT_TARGET=docker /opt/ledger-venv/bin/dbt deps \
 && DBT_PROFILES_DIR=. DBT_TARGET=docker /opt/ledger-venv/bin/dbt parse --no-partial-parse
WORKDIR /opt/airflow
