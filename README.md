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
  - A demo dataset (`app/seed_data.py`) of **120 real SQL task examples**
    across 8 business domains (e-commerce, marketing, finance, logistics,
    HR, support, product analytics, IoT) — incremental loads, SCD2, dedup,
    rollups, ranking, funnel/cohort analysis, data quality checks, gap
    filling, rate normalization, sessionization, anomaly detection and
    governance deletes — each wired to a real workflow/job/task, run
    history, alert routing, and dependency graph. It auto-seeds on first
    run against the quick-start SQLite DB (see below); run it against
    Postgres with `python backend/scripts/seed.py`. Browse it via the
    `GET /sql-examples` endpoint or the frontend's SQL Examples page.
- **Frontend** (`frontend/`) — React + TypeScript + Vite: a runbook
  dashboard with live stats and job search, a searchable/filterable **SQL
  Examples library** (syntax-highlighted, copy-to-clipboard, paginated), job
  detail (clickable task DAG + code viewer + task dependencies + run
  history + alerts), a lineage parser UI, a blast-radius/RCA graph view, and
  an NL query box — with loading skeletons, empty states, and a responsive
  layout throughout.

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
cd backend && pytest        # 24 tests: parsers, graph engine, full API flow, seed dataset
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
