"""Shared result types for lineage extraction.

Three levels, produced from one pass over a repository:

1. **Job lineage** — from orchestration definitions (Airflow DAGs, dbt
   projects, Databricks job specs): which jobs exist, which tasks they
   contain, how tasks depend on each other, and how jobs depend on
   other jobs (cross-DAG sensors, triggers, datasets).
2. **Table lineage** — from the code each task runs: which tables it
   reads and which it writes.
3. **Attribute lineage** — column-level source -> target mappings from
   that same code.

The levels are correlated: a `ColumnEdge` knows the task that produced it,
and that task knows the job it belongs to, so you can walk from a column
all the way up to the DAG that owns it.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass
class ColumnEdge:
    """One attribute-level mapping produced by a task."""

    target_table: str | None
    target_column: str
    source_table: str | None
    source_column: str | None
    transformation: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ExtractedTask:
    """A single unit of work inside a job, plus whatever its code reveals."""

    task_id: str
    job_id: str
    operator: str
    language: str | None = None          # SQL | PYSPARK | PYTHON | NOTEBOOK | DBT
    source_ref: str | None = None        # file path, notebook path, or "inline"
    source_code: str | None = None
    upstream_task_ids: list[str] = field(default_factory=list)
    reads: list[str] = field(default_factory=list)
    writes: list[str] = field(default_factory=list)
    column_lineage: list[ColumnEdge] = field(default_factory=list)
    # Tables the task refers to but which no other task in the scan writes.
    notes: list[str] = field(default_factory=list)

    @property
    def qualified_id(self) -> str:
        return f"{self.job_id}.{self.task_id}"

    def to_dict(self) -> dict:
        data = asdict(self)
        data["qualified_id"] = self.qualified_id
        return data


@dataclass
class ExtractedJob:
    """An orchestration job — an Airflow DAG, dbt project, Databricks job."""

    job_id: str
    system: str                          # airflow | dbt | databricks | sql | pyspark
    source_file: str
    schedule: str | None = None
    owner: str | None = None
    description: str | None = None
    tags: list[str] = field(default_factory=list)
    tasks: list[ExtractedTask] = field(default_factory=list)
    upstream_job_ids: list[str] = field(default_factory=list)
    downstream_job_ids: list[str] = field(default_factory=list)
    # Airflow Datasets / external assets this job consumes or produces.
    inlet_datasets: list[str] = field(default_factory=list)
    outlet_datasets: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        data = asdict(self)
        data["tasks"] = [t.to_dict() for t in self.tasks]
        return data


@dataclass
class ExtractionResult:
    """Everything one extraction run found."""

    jobs: list[ExtractedJob] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    files_scanned: int = 0

    # -- derived views -----------------------------------------------------

    def all_tasks(self) -> list[ExtractedTask]:
        return [task for job in self.jobs for task in job.tasks]

    def job_edges(self) -> list[tuple[str, str]]:
        """(upstream_job, downstream_job) pairs, de-duplicated."""
        edges: set[tuple[str, str]] = set()
        for job in self.jobs:
            for upstream in job.upstream_job_ids:
                edges.add((upstream, job.job_id))
            for downstream in job.downstream_job_ids:
                edges.add((job.job_id, downstream))
        return sorted(edges)

    def task_edges(self) -> list[tuple[str, str]]:
        """(upstream_task, downstream_task) as job-qualified ids."""
        edges: set[tuple[str, str]] = set()
        for job in self.jobs:
            for task in job.tasks:
                for upstream in task.upstream_task_ids:
                    edges.add((f"{job.job_id}.{upstream}", task.qualified_id))
        return sorted(edges)

    def table_edges(self) -> list[tuple[str, str, str]]:
        """(source_table, target_table, producing_task) triples."""
        edges: set[tuple[str, str, str]] = set()
        for task in self.all_tasks():
            for target in task.writes:
                for source in task.reads:
                    edges.add((source, target, task.qualified_id))
        return sorted(edges)

    def column_edges(self) -> list[tuple[ColumnEdge, str]]:
        """Every attribute mapping paired with the task that produced it."""
        return [(edge, task.qualified_id) for task in self.all_tasks() for edge in task.column_lineage]

    def tables(self) -> dict[str, dict[str, list[str]]]:
        """Table -> {written_by: [...], read_by: [...]} index."""
        index: dict[str, dict[str, list[str]]] = {}
        for task in self.all_tasks():
            for table in task.writes:
                index.setdefault(table, {"written_by": [], "read_by": []})["written_by"].append(task.qualified_id)
            for table in task.reads:
                index.setdefault(table, {"written_by": [], "read_by": []})["read_by"].append(task.qualified_id)
        return dict(sorted(index.items()))

    def summary(self) -> dict:
        return {
            "files_scanned": self.files_scanned,
            "jobs": len(self.jobs),
            "tasks": len(self.all_tasks()),
            "job_dependencies": len(self.job_edges()),
            "task_dependencies": len(self.task_edges()),
            "tables": len(self.tables()),
            "table_dependencies": len(self.table_edges()),
            "column_mappings": len(self.column_edges()),
            "warnings": len(self.warnings),
        }

    def to_dict(self) -> dict:
        return {
            "summary": self.summary(),
            "jobs": [job.to_dict() for job in self.jobs],
            "job_lineage": [{"upstream": u, "downstream": d} for u, d in self.job_edges()],
            "task_lineage": [{"upstream": u, "downstream": d} for u, d in self.task_edges()],
            "table_lineage": [
                {"source_table": s, "target_table": t, "via_task": v} for s, t, v in self.table_edges()
            ],
            "attribute_lineage": [
                {**edge.to_dict(), "via_task": task_id} for edge, task_id in self.column_edges()
            ],
            "tables": self.tables(),
            "warnings": self.warnings,
        }

    def merge(self, other: "ExtractionResult") -> None:
        self.jobs.extend(other.jobs)
        self.warnings.extend(other.warnings)
        self.files_scanned += other.files_scanned
