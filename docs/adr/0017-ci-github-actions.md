---
status: accepted
date: 2026-11-07
---

# 0017 — CI on GitHub Actions: a ladder of jobs, the full stack on PRs

## Context and problem statement

Phase 1 ran every check by hand (`make test`, `make lint`, `make demo`). From
Phase 2 on, every change must be checked automatically before it reaches `main`.
The checks range from seconds (lint) to a full CDC stack (Postgres, Redpanda,
Connect, MinIO, REST catalog, Trino, Airflow) that takes ~15 minutes. What runs
where, and how do we keep it fast and pinned?

## Decision drivers

* Fast feedback on every push; the expensive check before merge
* The same commands locally and in CI (`make` targets, no CI-only logic)
* Pinned and reproducible (project rule: pin every version)
* Free: the repository is public (standard runners: 4 vCPU, 16 GB, no minute limit)
* Never touch a developer's live stack

## Decision outcome

One workflow, `.github/workflows/ci.yml`, five jobs:

| Job | Trigger | Runs |
|---|---|---|
| `lint` | every push | `make lint` (ruff, sqlfluff) |
| `unit` | every push | `pytest` (generator planner, windows, gate against a simulated sink) |
| `dbt` | every push | `make ci-dbt`: Trino + catalog only, bronze DDL, staging + silver built **empty**, all dbt unit tests; on PRs, Slim CI |
| `connect-image` | PRs to `main`, `main` | builds the Connect worker, pushes `ghcr.io/ziklon/ledger-lakehouse-connect:sha-<sha>` (`:main` on main), registry build cache |
| `integration` | PRs to `main`, `main` | `make ci-integration`: a fresh stack, two business days (one via `make run-day`, one via the Airflow DAG), **assertions**, logs uploaded on failure |

Details:
* **Actions pinned by commit SHA**, the release tag in a comment; releases at
  least two weeks old when pinned. uv pinned to the local version.
* **Connect image in GHCR, public.** The Iceberg sink is built from ASF source
  (~7 min cold). A registry cache (`:buildcache`, `mode=max`) never expires,
  unlike the GitHub Actions cache (evicted after 7 days unused), and the exact
  image CI tested can be pulled later (Phase 3). Public, because private
  packages get 500 MB free and the image is ~1.8 GB; it contains only open-source
  software and no credentials. The **Airflow image** bakes the dbt project and
  `ledger_pipeline`, so it changes with most PRs: built in the job with the
  GitHub Actions cache, never published.
* **dbt unit tests need their input relations to exist**: dbt reads their column
  types. On an empty warehouse 0/8 pass; after `dbt run --empty` on staging and
  silver, 8/8. **Gold is not built empty**: `--empty` wraps every `ref` in a
  `limit 0` subquery, and Trino rejects `FOR VERSION AS OF` on a subquery
  (`mismatched input 'for'`). Gold is covered by the integration job on real data.
* **Slim CI as an impact report.** The job checks out the base branch, `dbt parse`s
  it, and lists `state:modified+` models and unit tests in the job summary
  (verified: a one-line change to `stg_accounts_latest` selects it, 4 downstream
  models and 8 unit tests). With 13 models every unit test runs anyway (4 s);
  selection pays off at hundreds of models, and the report shows reviewers the
  blast radius today.
* **The integration job asserts, the demo prints.** `scripts/ci-integration.sh`
  fails unless: both days COMMITTED (so every invariant passed), the gate used the
  exact `_kafka_offset` check on every partition (no migration fallback on a fresh
  stack), `control.bronze_offset_baseline` is empty, every bronze row carries a
  Kafka offset and no offset appears twice. It is also the fresh-stack test of
  milestone h that `make demo` would have run destructively on a laptop.
* **`scripts/ci-guard.sh`**: both CI scripts refuse to run unless `CI=true` or the
  compose project they resolve to is not `ledger-lakehouse`.

### Consequences

* Good: lint/unit/dbt answer in minutes on every push; nothing merges into `main`
  without the full stack having published two days.
* Good: CI runs the same `make` targets a developer runs.
* Bad: the integration job takes ~15–20 minutes and only runs on PRs/`main`; a
  branch can be red there for the first time at PR time.
* Bad: the integration job pulls ~6 GB of images per run; a slower runner or a
  registry outage shows up as a slow or failed run, not as a code problem.
* Bad: one GHCR image per PR commit (`sha-…`); a retention policy for old tags is
  Phase 2 (milestone n).

### Confirmation

Locally, in an isolated compose project: `make ci-dbt` → 7/7 models built empty,
8/8 unit tests, Slim CI report. On GitHub: the PR's run, plus a throwaway branch
with a lint error and a mutated macro showing red runs.

## Incident while building it (why the guard exists)

The first isolated run used a copy of the repo with `COMPOSE_PROJECT_NAME=ledgerci`
prepended to its `.env`. `.env.example` also sets `COMPOSE_PROJECT_NAME=ledger-lakehouse`
further down, and the scripts `source .env`, so the later line won: compose
recreated the **live** Postgres, MinIO, REST catalog and Trino with the CI host
ports, and `dbt run --empty` ran against the live lakehouse. Data was unaffected
(same named volumes, identical credentials, empty-source merges); the live services
were recreated with the real `.env`, and the sink, whose coordinator died when the
catalog restarted, was restarted and checked (15,240 offsets, distinct and
contiguous). The guard checks the project name compose actually resolves to, and
on its first run it refused the same copy, which is how the real cause was found.
