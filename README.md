# MetaWeave

A data pipeline tracking, lineage, blast-radius/RCA, and natural-language
query platform. `agent/task.xml` is the original enterprise spec this repo
implements a working slice of; see [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
for the system topology and how it maps to that spec.

## What's here

- **Backend** (`backend/`) — FastAPI + SQLAlchemy + Postgres, with:
  - A production Postgres schema (Alembic migration) for the 7 core
    metadata tables: `workflow_definition`, `job_definition`, `job_dag`,
    `task_definition`, `job_audit`, `job_dependency`, `job_alert`.
  - A real `sqlglot`-based SQL lineage parser (SELECT/INSERT/MERGE/CTAS →
    column-level source→target mappings).
  - A real Python-`ast`-based PySpark script lineage parser with the same
    output shape.
  - A NetworkX-backed blast-radius/RCA graph engine, plus the equivalent
    recursive-CTE queries for direct Postgres use at scale.
  - An NL query engine: a typed tool registry (Anthropic tool-calling
    schema) dispatched either through the Anthropic API or a deterministic
    regex fallback that works offline.
  - A demo dataset of **175 real SQL task examples**, each wired to a real
    workflow/job/task, run history, alert routing and dependency graph:
    - **55 complex bank finance/treasury cases** (`app/seed_banking.py`) —
      deposit and loan position facts, tiered interest accrual, funds
      transfer pricing, IFRS 9 staging and ECL provisioning, net interest
      income with volume/rate/mix attribution, expense pools, driver-based /
      step-down / **reciprocal (recursive CTE)** cost allocation, full P&L
      waterfalls, product/branch/customer profitability, RWA, economic
      capital and RAROC, repricing gap, LCR, EVE sensitivity, and Call
      Report / FR Y-9C / IFRS 9 / Basel leverage submissions. Every one is a
      genuine multi-CTE, multi-join statement (avg 3.4 CTEs, 5.1 joins, 9.8
      tables; up to 12 joins across 17 tables) over a realistic
      `core`/`gl`/`dim`/`ref`/`fact`/`finance`/`risk`/`alm`/`reg` schema.
    - **120 general pattern examples** (`app/seed_data.py`) across 8 business
      domains — incremental loads, SCD2, dedup, rollups, ranking,
      funnel/cohort analysis, data quality checks, gap filling,
      normalization, sessionization, anomaly detection, governance deletes.

    It auto-seeds on first run against the quick-start SQLite DB (see below);
    run it against Postgres with `python backend/scripts/seed.py`. Browse it
    via the `GET /sql-examples` endpoint (which reports each statement's
    CTE/join/table counts) or the frontend's SQL Examples and Lineage pages.
- **Frontend** (`frontend/`) — React + TypeScript + Vite: a runbook
  dashboard with live stats and job search, a searchable/filterable **SQL
  Examples library** (syntax-highlighted, copy-to-clipboard, paginated, with
  per-statement CTE/join/table complexity), job detail (clickable task DAG +
  code viewer + task dependencies + run history + alerts), a lineage parser
  with a browsable case library and separate **Table Lineage** and
  **Attribute Lineage** tabs, a blast-radius/RCA graph view, and an NL query
  box — with loading skeletons, empty states, and a responsive layout
  throughout.

## The `metaweave` extraction tool

MetaWeave is also a command line tool. Point it at a repository and it
recovers all three lineage levels in one pass — statically, without importing
a DAG, executing SQL, or connecting to a warehouse:

| Level | Recovered from | What you get |
| --- | --- | --- |
| **Job lineage** | orchestration definitions | jobs, their tasks, task→task edges, cross-job dependencies, schedules, owners |
| **Table lineage** | the code each task runs | which tables every task reads and writes |
| **Attribute lineage** | the same code | column→column mappings with the transforming expression |

```bash
cd backend
pip install -e .

metaweave extract ../path/to/your-repo               # all three levels
metaweave extract ./repo --level job                 # just the orchestration graph
metaweave extract ./repo --level attribute           # just column lineage
metaweave extract ./repo --format json --out lineage.json
metaweave extract ./repo --push http://localhost:8000   # load into the API/UI
metaweave lineage path/to/query.sql                  # one file
```

### What it understands

- **Airflow** (`*.py`) — `DAG(...)`, `with DAG(...)`, and `@dag`; tasks from any
  `*Operator`/`*Sensor` call and TaskFlow `@task` functions; dependencies from
  `>>`/`<<` chains including list fan-out/fan-in, `.set_upstream()`/
  `.set_downstream()` and `chain(...)`; cross-DAG edges from
  `ExternalTaskSensor(external_dag_id=)` and `TriggerDagRunOperator(trigger_dag_id=)`;
  Dataset inlets/outlets; module-level `default_args` resolved by name. Task code
  is followed to the referenced `.sql` file or Spark `application=` and parsed.
- **dbt** — a compiled `target/manifest.json` (authoritative, with compiled SQL),
  or an uncompiled `dbt_project.yml` + `models/**/*.sql` by reading `{{ ref() }}`
  and `{{ source() }}` directly.
- **Databricks / Workflows** — Jobs API 2.1 JSON or YAML and Asset Bundles:
  `tasks[].task_key`, `depends_on`, `notebook_task`/`spark_python_task`/
  `sql_task`/`dbt_task`, schedules and `run_job_task` fan-out.
- **Loose SQL and PySpark** files that no orchestrator references, grouped by
  directory and flagged as unorchestrated.

Beyond what each orchestrator declares, the tool also infers job→job edges
wherever one job writes a table another job reads — the implicit coupling that
causes most cross-team breakage.

Anything that cannot be resolved statically (a computed `dag_id`, a task built
in a loop, an uncompiled Jinja template) is reported as a warning rather than
guessed at; `--fail-on-warning` makes that a non-zero exit for CI.

## Quick start (SQLite, no setup)

```bash
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload   # creates ./metaweave.db automatically
```

```bash
cd frontend
npm install
npm run dev   # http://localhost:5173, talks to http://localhost:8000 by default
```

Set `VITE_API_BASE_URL` if the backend isn't on `localhost:8000`.

## Production setup (Postgres)

```bash
docker compose up -d postgres
cd backend
DATABASE_URL=postgresql+psycopg://metaweave:metaweave@localhost:5432/metaweave alembic upgrade head
DATABASE_URL=postgresql+psycopg://metaweave:metaweave@localhost:5432/metaweave uvicorn app.main:app
```

Or `docker compose up` to run backend + Postgres together (see
`docker-compose.yml`).

> **Note on this build:** the sandbox this repo was scaffolded in doesn't
> have Docker, so the Postgres-specific Alembic migration
> (`backend/migrations/versions/0001_initial_schema.py`) was verified with
> `alembic history`/`alembic heads` (confirms the migration graph is valid)
> and by code review, but has **not** been run against a live Postgres.
> Everything else — the ORM models (via a SQLite-compatible column-type
> variant), both lineage parsers, the graph engine, the API, and the
> frontend — was actually executed and tested end-to-end (`pytest`,
> `npm run build`, and a live `uvicorn` + `curl` smoke test). Run
> `alembic upgrade head` against real Postgres before relying on it.

## Tests

```bash
cd backend && pytest        # 41 tests: parsers, extraction tool, graph engine, API flow, seed data
cd frontend && npm run build  # type-checks + bundles
```

## NL query with real LLM understanding

Set `ANTHROPIC_API_KEY` to enable the Anthropic tool-calling path in
`backend/app/nl_query/tools.py`; without it, a regex-based fallback
handles the two example query patterns from the spec ("which downstream
tables are affected if X fails", "which jobs touched table Y this week")
so the endpoint is fully testable offline.

## Explicitly out of scope for this pass

Documented here rather than silently skipped:

- Real **Databricks Jobs API** / **Unity Catalog system tables** / **Delta
  Lake** ingestion — the schema and parsers are shaped to receive this data
  (see `docs/ARCHITECTURE.md` §1), but no ingestion job exists yet.
- Auth/authz, multi-tenancy, deployment infra (k8s manifests, CI/CD).
- Full CRUD surface (update/delete endpoints) — only what the UI needs.
- Temporal lineage (how lineage changes across historical runs) — needs a
  `lineage_snapshot`-style table not yet in the schema.
- The §5 high-scale synthetic benchmark harness (1,000+ jobs / 10,000+
  tasks) — the traversal engine and its indexes are built for it (see
  `docs/ARCHITECTURE.md` §3), and `test_blast_radius.py` proves correctness
  on fan-in/fan-out shapes at small scale, but the load-generation script
  and latency assertions at full scale aren't built yet.
