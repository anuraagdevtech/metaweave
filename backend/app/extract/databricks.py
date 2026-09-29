"""Job lineage from Databricks / Workflows job specifications.

Reads the JSON or YAML shape returned by the Jobs API 2.1 (`jobs/get`,
`jobs/list`) and used by Databricks Asset Bundles:

    {"name": "...", "schedule": {...},
     "tasks": [{"task_key": "...", "depends_on": [{"task_key": "..."}],
                "notebook_task"|"spark_python_task"|"sql_task"|"dbt_task": {...}}]}

Bundles nest jobs under `resources.jobs.<key>`, which is also handled. Task
code is resolved to a file inside the scan root where the spec points at one,
so the table/attribute layer can parse it.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from app.extract.models import ExtractedJob, ExtractedTask

_TASK_KINDS = {
    "notebook_task": ("NOTEBOOK", "notebook_path"),
    "spark_python_task": ("PYSPARK", "python_file"),
    "python_wheel_task": ("PYTHON", "entry_point"),
    "sql_task": ("SQL", None),
    "dbt_task": ("SQL", None),
    "spark_jar_task": ("PYTHON", "main_class_name"),
}


def load_spec(path: Path) -> Any | None:
    """Parse a JSON or YAML file, returning None if it is neither."""
    text = path.read_text(encoding="utf-8", errors="replace")
    try:
        if path.suffix == ".json":
            return json.loads(text)
        return yaml.safe_load(text)
    except (json.JSONDecodeError, yaml.YAMLError):
        return None


def _candidate_jobs(spec: Any) -> list[dict]:
    """Every job object in a spec: bare, list, `jobs:`, or bundle `resources.jobs`."""
    found: list[dict] = []
    if isinstance(spec, dict):
        if isinstance(spec.get("tasks"), list) and ("name" in spec or "job_id" in spec):
            found.append(spec)
        if isinstance(spec.get("settings"), dict):  # jobs/get response envelope
            found.extend(_candidate_jobs(spec["settings"]))
        for key in ("jobs", "resources"):
            nested = spec.get(key)
            if isinstance(nested, dict):
                for value in nested.values():
                    found.extend(_candidate_jobs(value))
            elif isinstance(nested, list):
                for value in nested:
                    found.extend(_candidate_jobs(value))
    elif isinstance(spec, list):
        for value in spec:
            found.extend(_candidate_jobs(value))
    return found


def looks_like_databricks_job(spec: Any) -> bool:
    for job in _candidate_jobs(spec):
        for task in job.get("tasks") or []:
            if isinstance(task, dict) and "task_key" in task:
                return True
    return False


def parse_databricks_job(path: Path, root: Path, spec: Any | None = None) -> tuple[list[ExtractedJob], list[str]]:
    spec = spec if spec is not None else load_spec(path)
    if spec is None:
        return [], [f"{path}: not valid JSON/YAML"]

    warnings: list[str] = []
    jobs: list[ExtractedJob] = []

    for raw_job in _candidate_jobs(spec):
        job_name = raw_job.get("name") or str(raw_job.get("job_id") or path.stem)
        schedule = None
        if isinstance(raw_job.get("schedule"), dict):
            schedule = raw_job["schedule"].get("quartz_cron_expression")
        elif isinstance(raw_job.get("trigger"), dict):
            schedule = "file_arrival" if "file_arrival" in raw_job["trigger"] else None

        tags = raw_job.get("tags")
        tag_list = sorted(f"{k}={v}" for k, v in tags.items()) if isinstance(tags, dict) else []

        tasks: list[ExtractedTask] = []
        downstream_jobs: set[str] = set()

        for raw_task in raw_job.get("tasks") or []:
            if not isinstance(raw_task, dict) or "task_key" not in raw_task:
                continue
            task_key = str(raw_task["task_key"])
            upstream = [
                str(dep["task_key"])
                for dep in raw_task.get("depends_on") or []
                if isinstance(dep, dict) and "task_key" in dep
            ]

            language: str | None = None
            source_ref: str | None = None
            source_code: str | None = None
            operator = "databricks.task"
            notes: list[str] = []

            for kind, (lang, path_key) in _TASK_KINDS.items():
                payload = raw_task.get(kind)
                if not isinstance(payload, dict):
                    continue
                operator = f"databricks.{kind}"
                language = lang
                if kind == "sql_task":
                    inline = (payload.get("query") or {}).get("query") if isinstance(payload.get("query"), dict) else None
                    file_ref = (payload.get("file") or {}).get("path") if isinstance(payload.get("file"), dict) else None
                    if inline:
                        source_ref, source_code = "inline", inline
                    elif file_ref:
                        source_ref = file_ref
                        resolved = _resolve(file_ref, path, root)
                        if resolved is not None:
                            source_code = resolved.read_text(encoding="utf-8", errors="replace")
                            source_ref = str(_safe_rel(resolved, root))
                        else:
                            notes.append(f"referenced SQL file not found in scan root: {file_ref}")
                elif path_key and isinstance(payload.get(path_key), str):
                    source_ref = payload[path_key]
                    resolved = _resolve(source_ref, path, root)
                    if resolved is not None:
                        source_code = resolved.read_text(encoding="utf-8", errors="replace")
                        source_ref = str(_safe_rel(resolved, root))
                break

            if isinstance(raw_task.get("run_job_task"), dict):
                target = raw_task["run_job_task"].get("job_id")
                if target:
                    downstream_jobs.add(str(target))
                    notes.append(f"triggers job {target}")
                operator = "databricks.run_job_task"

            tasks.append(
                ExtractedTask(
                    task_id=task_key,
                    job_id=job_name,
                    operator=operator,
                    language=language,
                    source_ref=source_ref,
                    source_code=source_code,
                    upstream_task_ids=sorted(set(upstream)),
                    notes=notes,
                )
            )

        if tasks:
            jobs.append(
                ExtractedJob(
                    job_id=job_name,
                    system="databricks",
                    source_file=str(_safe_rel(path, root)),
                    schedule=schedule,
                    owner=(raw_job.get("run_as") or {}).get("user_name")
                    if isinstance(raw_job.get("run_as"), dict)
                    else None,
                    tags=tag_list,
                    tasks=sorted(tasks, key=lambda t: t.task_id),
                    downstream_job_ids=sorted(downstream_jobs),
                )
            )

    return jobs, warnings


def _resolve(reference: str, spec_path: Path, root: Path) -> Path | None:
    candidate = (spec_path.parent / reference.lstrip("/")).resolve()
    if candidate.is_file():
        return candidate
    for match in root.rglob(Path(reference).name):
        if match.is_file():
            return match.resolve()
    return None


def _safe_rel(path: Path, root: Path) -> Path:
    try:
        return path.relative_to(root)
    except ValueError:
        return path
