"""Blast Radius / RCA graph engine (spec §4, §5).

Builds a directed graph of task-level dependencies from `job_dependency` (and
job-level edges from `job_dag`) and answers two traversal questions:

  * Blast radius — "if this task/table fails or changes, what breaks
    downstream?" (forward/successor traversal)
  * RCA (root-cause analysis) — "this task/table failed; what upstream
    tasks/tables could be the root cause?" (reverse/ancestor traversal)

For an in-process API request over a graph that already fits in memory,
NetworkX's `descendants`/`ancestors` are O(V+E) and comfortably sub-100ms even
at 10,000+ task scale (§5). For the same query pushed down into Postgres
directly (e.g. for a BI tool or a bulk batch job), the equivalent recursive
CTE is provided as a string constant below, driven off the same
`job_dependency` table with the indexes created in the Alembic migration
(`ix_job_dependency_task_id`, `ix_job_dependency_on_task_id`,
`ix_job_dependency_on_table`).
"""
from __future__ import annotations

from dataclasses import dataclass, field

import networkx as nx
from sqlalchemy import select
from sqlalchemy.orm import Session

# ---------------------------------------------------------------------------
# Recursive CTE reference queries (§5) — run these directly against Postgres
# for traversal without a round trip through the API/graph layer. Both are
# designed to terminate at <=100ms for graphs with 10,000+ tasks given the
# indexes in migration 0001, because each recursive step is an index lookup
# on `depends_on_task_id`/`task_id`, not a table scan.
# ---------------------------------------------------------------------------

BLAST_RADIUS_CTE = """
-- Downstream impact of a failing/changing task: every task that
-- (transitively) depends on it, closest first.
WITH RECURSIVE blast_radius AS (
    SELECT task_id, 0 AS depth
    FROM task_definition
    WHERE task_id = :root_task_id

    UNION ALL

    SELECT jd.task_id, br.depth + 1
    FROM job_dependency jd
    JOIN blast_radius br ON jd.depends_on_task_id = br.task_id
    WHERE br.depth < :max_depth
)
SELECT DISTINCT task_id, depth
FROM blast_radius
WHERE depth > 0
ORDER BY depth, task_id;
"""

RCA_CTE = """
-- Root-cause candidates for a failed task: every upstream task/dataset it
-- (transitively) depends on, closest first.
WITH RECURSIVE rca AS (
    SELECT task_id, 0 AS depth
    FROM task_definition
    WHERE task_id = :root_task_id

    UNION ALL

    SELECT jd.depends_on_task_id AS task_id, rca.depth + 1
    FROM job_dependency jd
    JOIN rca ON jd.task_id = rca.task_id
    WHERE jd.depends_on_kind = 'TASK'
      AND jd.depends_on_task_id IS NOT NULL
      AND rca.depth < :max_depth
)
SELECT DISTINCT task_id, depth
FROM rca
WHERE depth > 0
ORDER BY depth, task_id;
"""


@dataclass
class DependencyEdge:
    """One `job_dependency` row, normalized to a graph edge."""

    task_id: str
    depends_on_task_id: str | None = None
    depends_on_table: str | None = None


@dataclass
class ImpactedNode:
    node_id: str
    depth: int
    is_dataset: bool = False


@dataclass
class LineageGraph:
    """In-memory task/dataset dependency graph for blast-radius & RCA queries."""

    graph: nx.DiGraph = field(default_factory=nx.DiGraph)

    @classmethod
    def from_dependencies(cls, edges: list[DependencyEdge]) -> "LineageGraph":
        g = nx.DiGraph()
        for edge in edges:
            upstream = edge.depends_on_task_id or f"dataset:{edge.depends_on_table}"
            is_dataset = edge.depends_on_task_id is None
            g.add_node(upstream, is_dataset=is_dataset)
            g.add_node(edge.task_id, is_dataset=False)
            # Edge points upstream -> downstream, matching data/failure flow.
            g.add_edge(upstream, edge.task_id)
        return cls(graph=g)

    def blast_radius(self, root: str, max_depth: int = 50) -> list[ImpactedNode]:
        """Everything downstream of `root` (what breaks if root fails), by depth."""
        if root not in self.graph:
            return []
        depths = nx.single_source_shortest_path_length(self.graph, root, cutoff=max_depth)
        return [
            ImpactedNode(node, depth, self.graph.nodes[node].get("is_dataset", False))
            for node, depth in sorted(depths.items(), key=lambda kv: (kv[1], kv[0]))
            if depth > 0
        ]

    def rca(self, root: str, max_depth: int = 50) -> list[ImpactedNode]:
        """Everything upstream of `root` (candidate root causes), by depth."""
        if root not in self.graph:
            return []
        reverse = self.graph.reverse(copy=False)
        depths = nx.single_source_shortest_path_length(reverse, root, cutoff=max_depth)
        return [
            ImpactedNode(node, depth, self.graph.nodes[node].get("is_dataset", False))
            for node, depth in sorted(depths.items(), key=lambda kv: (kv[1], kv[0]))
            if depth > 0
        ]

    def has_cycle(self) -> bool:
        return not nx.is_directed_acyclic_graph(self.graph)


def build_dependency_graph(db: Session) -> "LineageGraph":
    """Build a LineageGraph from all `job_dependency` rows in the database."""
    from app.models.metadata import JobDependency  # local import avoids a cycle with models

    rows = db.execute(select(JobDependency)).scalars().all()
    edges = [DependencyEdge(r.task_id, r.depends_on_task_id, r.depends_on_table) for r in rows]
    return LineageGraph.from_dependencies(edges)
