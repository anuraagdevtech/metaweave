# MetaWeave Architecture

This covers spec §1 (system topology + Databricks/Unity Catalog integration)
and §5 (high-scale validation strategy) for what's implemented in this repo.
See `agent/task.xml` for the full original spec and `README.md` for how to
run things.

## 1. System topology

```mermaid
flowchart LR
    subgraph Client
        FE[React/TS Frontend\nDashboard · Lineage · Blast Radius · NL Query]
    end

    subgraph API["API Layer (FastAPI)"]
        WF[/workflows, /jobs, /tasks\n/audits, /alerts, /dependencies/]
        LIN[/lineage/parse-sql\n/lineage/parse-pyspark/]
        BR[/blast-radius/{id}\n/rca/{id}/]
        NL[/nl-query/]
    end

    subgraph Engines
        PARSE[Lineage Parser Engine\nsqlglot AST + Python ast]
        GRAPH[Blast Radius / RCA Engine\nNetworkX in-process +\nrecursive-CTE reference queries]
        NLQ[NL Query Engine\nAnthropic tool-calling\nor regex fallback]
    end

    subgraph Store["Metadata Store (Postgres)"]
        DB[(workflow_definition · job_definition · job_dag\ntask_definition · job_audit\njob_dependency · job_alert)]
    end

    subgraph External["External Systems (integration point, not yet wired)"]
        DBX[Databricks Jobs API]
        UC[Unity Catalog System Tables\nsystem.access.table_lineage\nsystem.access.column_lineage]
        DELTA[Delta Lake metadata\n/ Delta transaction log]
    end

    FE --> WF & LIN & BR & NL
    WF --> DB
    LIN --> PARSE
    BR --> GRAPH
    GRAPH --> DB
    NL --> NLQ
    NLQ --> GRAPH
    NLQ --> DB
    PARSE -. "extracted lineage rows\n(ingestion path, not yet wired)" .-> DB

    DBX -. "job runs/status → job_audit\n(ingestion path, not yet wired)" .-> DB
    UC -. "authoritative lineage,\nreconciled against PARSE output" .-> DB
    DELTA -. "table version/schema history\n(temporal lineage)" .-> DB
```

**What's implemented in this repo:** the API layer, the two lineage parser
engines, the blast-radius/RCA graph engine (both the in-process NetworkX path
and the recursive-CTE reference queries), the NL query engine's tool registry
+ fallback, the Postgres schema/migration, and the frontend.

**What's an integration point, not yet built:** a scheduled ingestion job
that pulls job runs from the **Databricks Jobs API** into `job_audit`, and a
reconciliation job that pulls **Unity Catalog's `system.access.table_lineage`
/ `system.access.column_lineage`** system tables plus **Delta Lake** table
version history into `job_dependency` and a (future) `lineage_snapshot`
table for temporal lineage (spec §4.2). The static parsers in this repo
(`app/lineage/sql_parser.py`, `app/lineage/pyspark_parser.py`) produce the
same shape of output UC's system tables provide, so once ingestion exists
either source can populate `job_dependency` — UC lineage is authoritative
when available, and the static parsers are always usable as a cross-check
or for engines UC doesn't cover (e.g. raw PySpark run outside a UC-enabled
cluster).

## 2. Component responsibilities

| Component | Responsibility | Code |
|---|---|---|
| Frontend | Runbook views, lineage/blast-radius visualization, NL query box | `frontend/src` |
| API layer | REST surface over the metadata schema + engines | `backend/app/api` |
| Metadata store | System of record: workflows, jobs, tasks, DAG edges, audit runs, dependencies, alerts | `backend/app/models`, `backend/migrations` |
| Lineage parser engine | Static extraction of column-level source→target mappings from SQL and PySpark | `backend/app/lineage/sql_parser.py`, `pyspark_parser.py` |
| Blast radius / RCA engine | Graph traversal over `job_dependency` for downstream impact / upstream root cause | `backend/app/lineage/graph.py` |
| NL query engine | Maps a natural-language question to a typed tool call against the engines above | `backend/app/nl_query` |

## 3. High-scale validation strategy (§5)

Target: 1,000+ active multi-task jobs, 10,000+ tasks, many-to-one
reconciliation fan-in, one-to-many fan-out, graph traversal under 100ms.

**Data generation.** A synthetic-fixture script (not yet built — natural
next addition to `backend/tests`) should generate: 1,000 `job_definition`
rows across ~50 workflows; 10–15 tasks per job (~10-15k `task_definition`
rows); `job_dependency` edges forming (a) long linear chains 20+ deep to
stress traversal depth, (b) fan-in nodes with 50+ upstream dependents
writing to one reconciliation table, and (c) fan-out nodes with 20+
downstream tables from a single task — mirroring `test_blast_radius.py`'s
`test_many_to_one_reconciliation_fan_in` / `test_one_to_many_fan_out`, just
at scale.

**Two traversal paths, both indexed for sub-100ms:**
- *In-process* (`LineageGraph.blast_radius`/`.rca` in `graph.py`): loads all
  `job_dependency` rows into a NetworkX `DiGraph` once per request and runs
  `single_source_shortest_path_length` — O(V+E) BFS, comfortably under 100ms
  in-memory even at 10k+ tasks. Appropriate for the API's live traversal
  endpoints where the whole graph (or a workflow-scoped subset) fits in
  memory.
- *Direct SQL* (`BLAST_RADIUS_CTE` / `RCA_CTE` in `graph.py`): recursive
  CTEs driven off `job_dependency`, using the `ix_job_dependency_task_id`
  and `ix_job_dependency_on_task_id` indexes from migration `0001` so each
  recursive step is an index lookup, not a scan. Use `EXPLAIN (ANALYZE,
  BUFFERS)` against the synthetic dataset above to confirm index usage
  before trusting the 100ms target in production; add a partial index on
  `job_dependency(depends_on_table)` (already present as
  `ix_job_dependency_on_table`) to keep many-to-one dataset lookups fast as
  fan-in grows.

**Benchmark harness.** Time both paths at increasing scale (100 / 1k / 10k
tasks) and increasing fan-in/fan-out (10 / 50 / 200 edges per node),
asserting p95 latency stays under 100ms; regress this in CI once the
synthetic-fixture script exists, the same way `test_blast_radius.py`
currently checks correctness at small scale.
