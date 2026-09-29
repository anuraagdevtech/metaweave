"""Walk a repository, extract all three lineage levels, correlate them.

    job lineage      <- orchestration definitions (Airflow / dbt / Databricks)
    table lineage    <- the code each of those jobs' tasks runs
    attribute lineage <- the same code, column by column

The correlation is the point: orchestration says *which task* runs *which
file*, and the code parsers say what that file reads and writes, so a column
in a target table can be traced back to the DAG that owns it.
"""
from __future__ import annotations

from pathlib import Path

from app.extract import airflow as airflow_parser
from app.extract import databricks as databricks_parser
from app.extract import dbt as dbt_parser
from app.extract.models import ColumnEdge, ExtractedJob, ExtractedTask, ExtractionResult
from app.lineage.pyspark_parser import parse_pyspark_lineage
from app.lineage.sql_parser import parse_sql_script

_SKIP_DIRS = {
    ".git", ".venv", "venv", "node_modules", "__pycache__", ".pytest_cache",
    ".mypy_cache", ".ruff_cache", "dist", "build", ".tox", ".idea", ".vscode",
}
_MAX_BYTES = 2_000_000


def extract_path(
    path: str | Path,
    *,
    include_standalone: bool = True,
    sql_dialect: str = "postgres",
) -> ExtractionResult:
    """Extract job, table and attribute lineage from a file or directory.

    `include_standalone` also reports .sql/.py files that no orchestration
    definition references, grouped into synthetic jobs by directory — useful
    when a repo holds SQL that is scheduled somewhere this scan cannot see.
    """
    root = Path(path).resolve()
    result = ExtractionResult()

    if root.is_file():
        files = [root]
        root = root.parent
    else:
        files = sorted(_walk(root))

    result.files_scanned = len(files)
    claimed: set[Path] = set()

    # 1. Orchestration -> jobs + tasks
    for file in files:
        jobs, warnings = _parse_orchestration(file, root)
        result.warnings.extend(warnings)
        for job in jobs:
            result.jobs.append(job)
            claimed.add(file)

    # 2/3. Task code -> table + attribute lineage
    for job in result.jobs:
        for task in job.tasks:
            _resolve_task_code(task, root, claimed, sql_dialect, result.warnings)

    # Optional: code the orchestration layer never pointed at
    if include_standalone:
        orphan_jobs, warnings = _standalone_jobs(files, claimed, root, sql_dialect)
        result.jobs.extend(orphan_jobs)
        result.warnings.extend(warnings)

    _link_jobs_by_dataset(result)
    return result


# ---------------------------------------------------------------- walking


def _walk(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in _SKIP_DIRS for part in path.parts):
            continue
        if path.suffix.lower() not in {".py", ".sql", ".json", ".yml", ".yaml"}:
            continue
        try:
            if path.stat().st_size > _MAX_BYTES:
                continue
        except OSError:
            continue
        yield path


def _parse_orchestration(file: Path, root: Path) -> tuple[list[ExtractedJob], list[str]]:
    suffix = file.suffix.lower()

    if suffix == ".py":
        try:
            source = file.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            return [], [f"{file}: {exc}"]
        if airflow_parser.looks_like_airflow(source):
            return airflow_parser.parse_airflow_dag(file, root)
        return [], []

    if file.name == "dbt_project.yml":
        return dbt_parser.parse_dbt_project(file, root)

    if suffix == ".json":
        if file.name == "manifest.json":
            return dbt_parser.parse_dbt_manifest(file, root)
        spec = databricks_parser.load_spec(file)
        if spec is not None and databricks_parser.looks_like_databricks_job(spec):
            return databricks_parser.parse_databricks_job(file, root, spec)
        return [], []

    if suffix in {".yml", ".yaml"}:
        spec = databricks_parser.load_spec(file)
        if spec is not None and databricks_parser.looks_like_databricks_job(spec):
            return databricks_parser.parse_databricks_job(file, root, spec)
        return [], []

    return [], []


# ------------------------------------------------- table/attribute layer


def _resolve_task_code(
    task: ExtractedTask,
    root: Path,
    claimed: set[Path],
    sql_dialect: str,
    warnings: list[str],
) -> None:
    """Run the code parsers over a task's source and fill in reads/writes/columns."""
    if task.source_ref and task.source_ref != "inline":
        candidate = (root / task.source_ref).resolve()
        if candidate.is_file():
            claimed.add(candidate)

    if not task.source_code or not task.language:
        return

    if task.language == "SQL" and "{{" in task.source_code:
        # Uncompiled Jinja (a dbt model before `dbt compile`). The ref()/source()
        # calls were already resolved by the dbt parser; handing the raw template
        # to a SQL parser would only produce a syntax error.
        task.notes.append("template not compiled — column lineage needs `dbt compile`")
        return

    reads: set[str] = set(task.reads)
    writes: list[str] = list(task.writes)
    columns: list[ColumnEdge] = []

    try:
        if task.language == "SQL":
            results = parse_sql_script(task.source_code, dialect=sql_dialect)
        elif task.language == "PYSPARK":
            results = parse_pyspark_lineage(task.source_code)
        else:
            return
    except Exception as exc:  # pragma: no cover - parser guards its own errors
        warnings.append(f"{task.job_id}.{task.task_id}: could not parse {task.language} ({exc})")
        return

    for lineage in results:
        reads.update(lineage.source_tables)
        if lineage.target_table and lineage.target_table not in writes:
            writes.append(lineage.target_table)
        for column in lineage.column_lineage:
            columns.append(
                ColumnEdge(
                    target_table=column.target_table,
                    target_column=column.target_column,
                    source_table=column.source_table,
                    source_column=column.source_column,
                    transformation=column.transformation,
                )
            )

    task.reads = sorted(reads - set(writes))
    task.writes = writes
    task.column_lineage = columns


def _standalone_jobs(
    files: list[Path], claimed: set[Path], root: Path, sql_dialect: str
) -> tuple[list[ExtractedJob], list[str]]:
    """Group unreferenced .sql / PySpark files into per-directory pseudo-jobs."""
    warnings: list[str] = []
    by_dir: dict[Path, list[ExtractedTask]] = {}

    for file in files:
        if file in claimed or file.suffix.lower() not in {".sql", ".py"}:
            continue
        try:
            source = file.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if not source.strip():
            continue

        if file.suffix.lower() == ".py":
            if "spark" not in source:
                continue
            language = "PYSPARK"
        else:
            language = "SQL"

        rel = _safe_rel(file, root)
        task = ExtractedTask(
            task_id=file.stem,
            job_id=str(rel.parent) if str(rel.parent) != "." else root.name,
            operator=f"file.{language.lower()}",
            language=language,
            source_ref=str(rel),
            source_code=source,
            notes=["not referenced by any orchestration definition in this scan"],
        )
        _resolve_task_code(task, root, claimed, sql_dialect, warnings)
        if task.reads or task.writes:
            by_dir.setdefault(rel.parent, []).append(task)

    jobs = []
    for directory, tasks in sorted(by_dir.items()):
        job_id = str(directory) if str(directory) != "." else root.name
        jobs.append(
            ExtractedJob(
                job_id=job_id,
                system="sql" if all(t.language == "SQL" for t in tasks) else "pyspark",
                source_file=str(directory),
                description=f"{len(tasks)} unorchestrated file(s) under {job_id}",
                tasks=sorted(tasks, key=lambda t: t.task_id),
            )
        )
    return jobs, warnings


def _link_jobs_by_dataset(result: ExtractionResult) -> None:
    """Infer job->job edges where one job writes a table another job reads.

    Orchestration only states dependencies someone declared. This adds the
    ones the *code* implies, which is how most cross-team breakage happens.
    """
    writer_of: dict[str, set[str]] = {}
    for job in result.jobs:
        for task in job.tasks:
            for table in task.writes:
                writer_of.setdefault(table, set()).add(job.job_id)

    for job in result.jobs:
        implied: set[str] = set()
        for task in job.tasks:
            for table in task.reads:
                for producer in writer_of.get(table, set()):
                    if producer != job.job_id:
                        implied.add(producer)
        for producer in sorted(implied):
            if producer not in job.upstream_job_ids:
                job.upstream_job_ids.append(producer)


def _safe_rel(path: Path, root: Path) -> Path:
    try:
        return path.relative_to(root)
    except ValueError:
        return path
