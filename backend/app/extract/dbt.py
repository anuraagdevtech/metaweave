"""Job lineage from dbt projects.

Two modes, because both occur in practice:

* **manifest** — `target/manifest.json` produced by `dbt compile`/`dbt run`.
  Authoritative: it carries compiled SQL and a resolved `depends_on` graph.
* **project** — a bare `dbt_project.yml` plus `models/**/*.sql` with no
  manifest (nothing has been compiled yet). The `{{ ref() }}` and
  `{{ source() }}` calls are read directly from the model files, which is
  enough to recover the model DAG and the tables it touches.

Each dbt project becomes one job; each model, seed or snapshot becomes a task.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from app.extract.models import ExtractedJob, ExtractedTask

_REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"]([\w.]+)['\"](?:\s*,\s*['\"]([\w.]+)['\"])?\s*\)\s*\}\}")
_SOURCE_RE = re.compile(r"\{\{\s*source\(\s*['\"]([\w.]+)['\"]\s*,\s*['\"]([\w.]+)['\"]\s*\)\s*\}\}")
_CONFIG_ALIAS_RE = re.compile(r"\{\{\s*config\([^}]*alias\s*=\s*['\"]([\w.]+)['\"]", re.DOTALL)

_MODEL_RESOURCE_TYPES = {"model", "seed", "snapshot"}


def parse_dbt_manifest(path: Path, root: Path) -> tuple[list[ExtractedJob], list[str]]:
    """Parse a compiled `manifest.json`."""
    warnings: list[str] = []
    try:
        manifest = json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        return [], [f"{path}: not valid JSON ({exc.msg})"]

    nodes = manifest.get("nodes")
    if not isinstance(nodes, dict):
        return [], []

    project_name = (manifest.get("metadata") or {}).get("project_name") or path.parent.name
    sources = manifest.get("sources") or {}

    def relation_for(unique_id: str) -> str | None:
        node = nodes.get(unique_id) or sources.get(unique_id)
        if not isinstance(node, dict):
            return None
        relation = node.get("relation_name")
        if isinstance(relation, str) and relation:
            return relation.replace('"', "").replace("`", "")
        schema, name = node.get("schema"), node.get("alias") or node.get("name")
        return f"{schema}.{name}" if schema and name else name

    tasks: list[ExtractedTask] = []
    for unique_id, node in nodes.items():
        if node.get("resource_type") not in _MODEL_RESOURCE_TYPES:
            continue
        name = node.get("name") or unique_id
        upstream = [
            dep.split(".")[-1]
            for dep in (node.get("depends_on") or {}).get("nodes", [])
            if dep.startswith("model.") or dep.startswith("seed.")
        ]
        reads = [
            relation
            for dep in (node.get("depends_on") or {}).get("nodes", [])
            if (relation := relation_for(dep))
        ]
        writes = relation_for(unique_id)
        task = ExtractedTask(
            task_id=name,
            job_id=project_name,
            operator=f"dbt.{node.get('resource_type')}",
            language="SQL",
            source_ref=node.get("original_file_path") or node.get("path"),
            source_code=node.get("compiled_code") or node.get("raw_code"),
            upstream_task_ids=sorted(set(upstream)),
            reads=sorted(set(reads)),
            writes=[writes] if writes else [],
        )
        tasks.append(task)

    if not tasks:
        return [], warnings

    job = ExtractedJob(
        job_id=project_name,
        system="dbt",
        source_file=str(_safe_rel(path, root)),
        description=f"dbt project '{project_name}' ({len(tasks)} models)",
        tasks=sorted(tasks, key=lambda t: t.task_id),
    )
    return [job], warnings


def parse_dbt_project(project_file: Path, root: Path) -> tuple[list[ExtractedJob], list[str]]:
    """Parse an uncompiled project: `dbt_project.yml` + `models/**/*.sql`."""
    warnings: list[str] = []
    try:
        config = yaml.safe_load(project_file.read_text(encoding="utf-8", errors="replace")) or {}
    except yaml.YAMLError as exc:
        return [], [f"{project_file}: not valid YAML ({exc})"]

    project_name = config.get("name") or project_file.parent.name
    project_dir = project_file.parent
    model_dirs = config.get("model-paths") or config.get("source-paths") or ["models"]

    default_schema = ((config.get("models") or {}).get(project_name) or {}).get("+schema")
    tasks: list[ExtractedTask] = []

    for model_dir in model_dirs:
        for sql_file in sorted((project_dir / model_dir).rglob("*.sql")):
            body = sql_file.read_text(encoding="utf-8", errors="replace")
            model_name = sql_file.stem
            refs = [match.group(2) or match.group(1) for match in _REF_RE.finditer(body)]
            source_tables = [f"{m.group(1)}.{m.group(2)}" for m in _SOURCE_RE.finditer(body)]

            alias_match = _CONFIG_ALIAS_RE.search(body)
            relation = alias_match.group(1) if alias_match else model_name
            target = f"{default_schema}.{relation}" if default_schema else relation

            tasks.append(
                ExtractedTask(
                    task_id=model_name,
                    job_id=project_name,
                    operator="dbt.model",
                    language="SQL",
                    source_ref=str(_safe_rel(sql_file, root)),
                    source_code=body,
                    upstream_task_ids=sorted(set(refs)),
                    reads=sorted(set(source_tables) | {f"{default_schema}.{r}" if default_schema else r for r in refs}),
                    writes=[target],
                    notes=["resolved from Jinja ref()/source(); compile the project for exact relations"]
                    if refs or source_tables
                    else [],
                )
            )

    if not tasks:
        return [], warnings

    job = ExtractedJob(
        job_id=project_name,
        system="dbt",
        source_file=str(_safe_rel(project_file, root)),
        description=f"dbt project '{project_name}' ({len(tasks)} models, uncompiled)",
        tasks=sorted(tasks, key=lambda t: t.task_id),
    )
    return [job], warnings


def _safe_rel(path: Path, root: Path) -> Path:
    try:
        return path.relative_to(root)
    except ValueError:
        return path
