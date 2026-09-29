"""MetaWeave command line tool.

    metaweave extract <path>            job + table + attribute lineage
    metaweave extract <path> --level job --format json
    metaweave extract <path> --push http://localhost:8000
    metaweave lineage <file.sql|file.py>   code lineage for one file
    metaweave serve                     run the API
    metaweave seed                      load the demo dataset

Everything is static analysis: no DAG is imported, no SQL is executed, and no
warehouse connection is needed.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from app.extract import ExtractionResult, extract_path


# ------------------------------------------------------------- rendering


def _bullet(label: str, value) -> str:
    return f"  {label:<22} {value}"


def render_text(result: ExtractionResult, level: str) -> str:
    out: list[str] = []
    summary = result.summary()
    out.append("MetaWeave extraction")
    out.append("=" * 60)
    for key in (
        "files_scanned", "jobs", "tasks", "job_dependencies",
        "task_dependencies", "tables", "table_dependencies", "column_mappings",
    ):
        out.append(_bullet(key.replace("_", " ") + ":", summary[key]))

    if level in {"all", "job"}:
        out.append("")
        out.append("JOB LINEAGE (from orchestration)")
        out.append("-" * 60)
        for job in result.jobs:
            header = f"  {job.job_id}  [{job.system}]"
            if job.schedule:
                header += f"  schedule={job.schedule}"
            out.append(header)
            out.append(f"    source: {job.source_file}")
            if job.owner:
                out.append(f"    owner: {job.owner}")
            if job.upstream_job_ids:
                out.append(f"    depends on jobs: {', '.join(job.upstream_job_ids)}")
            if job.downstream_job_ids:
                out.append(f"    triggers jobs: {', '.join(job.downstream_job_ids)}")
            if job.inlet_datasets:
                out.append(f"    consumes datasets: {', '.join(job.inlet_datasets)}")
            if job.outlet_datasets:
                out.append(f"    produces datasets: {', '.join(job.outlet_datasets)}")
            for task in job.tasks:
                line = f"      - {task.task_id} ({task.operator}"
                if task.language:
                    line += f", {task.language}"
                line += ")"
                if task.upstream_task_ids:
                    line += f"  <- {', '.join(task.upstream_task_ids)}"
                out.append(line)
                for note in task.notes:
                    out.append(f"          ! {note}")

    if level in {"all", "table"}:
        out.append("")
        out.append("TABLE LINEAGE (from code)")
        out.append("-" * 60)
        edges = result.table_edges()
        if not edges:
            out.append("  (none found)")
        for source, target, via in edges:
            out.append(f"  {source}  ->  {target}")
            out.append(f"      via {via}")

    if level in {"all", "attribute"}:
        out.append("")
        out.append("ATTRIBUTE LINEAGE (from code)")
        out.append("-" * 60)
        edges = result.column_edges()
        if not edges:
            out.append("  (none found)")
        current_task = None
        for edge, via in edges:
            if via != current_task:
                out.append(f"  [{via}]")
                current_task = via
            source = (
                f"{edge.source_table}.{edge.source_column}"
                if edge.source_table and edge.source_column
                else (edge.source_column or "<literal>")
            )
            target = f"{edge.target_table}.{edge.target_column}" if edge.target_table else edge.target_column
            out.append(f"      {target:<45} <- {source}")

    if result.warnings:
        out.append("")
        out.append("WARNINGS")
        out.append("-" * 60)
        for warning in result.warnings:
            out.append(f"  ! {warning}")

    return "\n".join(out)


def render_json(result: ExtractionResult, level: str) -> str:
    data = result.to_dict()
    if level == "job":
        data.pop("table_lineage", None)
        data.pop("attribute_lineage", None)
    elif level == "table":
        data = {"summary": data["summary"], "table_lineage": data["table_lineage"], "tables": data["tables"]}
    elif level == "attribute":
        data = {"summary": data["summary"], "attribute_lineage": data["attribute_lineage"]}
    return json.dumps(data, indent=2, default=str)


# ------------------------------------------------------------- push mode


def push_to_api(result: ExtractionResult, base_url: str) -> str:
    """Load an extraction into a running MetaWeave API so the UI can show it."""
    import httpx

    base_url = base_url.rstrip("/")
    created_jobs = 0
    created_tasks = 0
    created_deps = 0

    with httpx.Client(base_url=base_url, timeout=30.0) as client:
        client.get("/health").raise_for_status()

        systems = sorted({job.system for job in result.jobs})
        workflow_ids: dict[str, str] = {}
        for system in systems:
            response = client.post(
                "/workflows",
                json={
                    "name": f"extracted_{system}",
                    "owner_team": "metaweave-extract",
                    "owner_email": "metaweave-extract@metaweave.example",
                    "sla_minutes": 120,
                    "description": f"Jobs extracted from {system} definitions",
                    "tags": {"source": "extract", "system": system},
                },
            )
            if response.status_code == 201:
                workflow_ids[system] = response.json()["workflow_id"]
            else:
                existing = client.get("/workflows").json()
                match = next((w for w in existing if w["name"] == f"extracted_{system}"), None)
                if match is None:
                    raise RuntimeError(f"could not create or find workflow for {system}: {response.text}")
                workflow_ids[system] = match["workflow_id"]

        task_ids: dict[str, str] = {}
        for job in result.jobs:
            response = client.post(
                "/jobs",
                json={
                    "workflow_id": workflow_ids[job.system],
                    "name": job.job_id,
                    "cron_expression": job.schedule,
                    "cluster_config": {"source_file": job.source_file, "system": job.system},
                },
            )
            if response.status_code != 201:
                continue
            created_jobs += 1
            job_row_id = response.json()["job_id"]

            for order, task in enumerate(job.tasks):
                task_response = client.post(
                    "/tasks",
                    json={
                        "job_id": job_row_id,
                        "name": task.task_id,
                        "task_order": order,
                        "exec_type": task.language if task.language in
                        {"SQL", "PYSPARK", "PYTHON", "NOTEBOOK"} else "PYTHON",
                        "script_path": task.source_ref,
                        "source_code": task.source_code,
                        "environment": {
                            "operator": task.operator,
                            "reads": task.reads,
                            "writes": task.writes,
                        },
                    },
                )
                if task_response.status_code == 201:
                    created_tasks += 1
                    task_ids[task.qualified_id] = task_response.json()["task_id"]

        for job in result.jobs:
            for task in job.tasks:
                child = task_ids.get(task.qualified_id)
                if child is None:
                    continue
                for upstream in task.upstream_task_ids:
                    parent = task_ids.get(f"{job.job_id}.{upstream}")
                    if parent and client.post(
                        "/dependencies",
                        json={
                            "task_id": child,
                            "depends_on_kind": "TASK",
                            "depends_on_task_id": parent,
                        },
                    ).status_code == 201:
                        created_deps += 1
                for table in task.reads:
                    if client.post(
                        "/dependencies",
                        json={
                            "task_id": child,
                            "depends_on_kind": "DATASET",
                            "depends_on_table": table,
                        },
                    ).status_code == 201:
                        created_deps += 1

    return (
        f"Pushed to {base_url}: {created_jobs} jobs, {created_tasks} tasks, "
        f"{created_deps} dependencies."
    )


# ------------------------------------------------------------- commands


def cmd_extract(args: argparse.Namespace) -> int:
    target = Path(args.path)
    if not target.exists():
        print(f"error: {target} does not exist", file=sys.stderr)
        return 2

    result = extract_path(
        target,
        include_standalone=not args.orchestrated_only,
        sql_dialect=args.dialect,
    )

    rendered = (render_json if args.format == "json" else render_text)(result, args.level)

    if args.out:
        Path(args.out).write_text(rendered + "\n", encoding="utf-8")
        print(f"Wrote {args.out} ({result.summary()['jobs']} jobs, {result.summary()['tasks']} tasks)")
    else:
        print(rendered)

    if args.push:
        try:
            print(push_to_api(result, args.push))
        except Exception as exc:
            print(f"error: push failed: {exc}", file=sys.stderr)
            return 1

    if args.fail_on_warning and result.warnings:
        return 1
    return 0


def cmd_lineage(args: argparse.Namespace) -> int:
    from app.lineage.pyspark_parser import parse_pyspark_lineage
    from app.lineage.sql_parser import parse_sql_script

    target = Path(args.file)
    if not target.is_file():
        print(f"error: {target} is not a file", file=sys.stderr)
        return 2

    source = target.read_text(encoding="utf-8", errors="replace")
    results = (
        parse_pyspark_lineage(source)
        if target.suffix.lower() == ".py"
        else parse_sql_script(source, dialect=args.dialect)
    )

    if args.format == "json":
        print(json.dumps([
            {
                "statement_type": r.statement_type,
                "target_table": r.target_table,
                "source_tables": r.source_tables,
                "column_lineage": [c.__dict__ for c in r.column_lineage],
            }
            for r in results
        ], indent=2))
        return 0

    for index, lineage in enumerate(results, start=1):
        print(f"[{index}] {lineage.statement_type}  {lineage.target_table or '(no target)'}")
        print(f"    reads: {', '.join(lineage.source_tables) or '(none)'}")
        for column in lineage.column_lineage:
            source = (
                f"{column.source_table}.{column.source_column}"
                if column.source_table and column.source_column
                else (column.source_column or "<literal>")
            )
            print(f"      {column.target_column:<38} <- {source}")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    import uvicorn

    uvicorn.run("app.main:app", host=args.host, port=args.port, reload=args.reload)
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    from app.core.config import settings
    from app.core.db import SessionLocal, init_db
    from app.seed_data import seed_demo_data

    if settings.is_sqlite:
        init_db()
    db = SessionLocal()
    try:
        created = seed_demo_data(db)
        print(f"Seeded {created} SQL task examples." if created else "Database already has data — skipped.")
    finally:
        db.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="metaweave",
        description="Extract job lineage from orchestration, and table + attribute lineage from code.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser(
        "extract", help="scan a repo for orchestration definitions and code, and extract all lineage levels"
    )
    extract.add_argument("path", help="file or directory to scan")
    extract.add_argument(
        "--level", choices=["all", "job", "table", "attribute"], default="all",
        help="which lineage level to report (default: all)",
    )
    extract.add_argument("--format", choices=["text", "json"], default="text")
    extract.add_argument("--out", help="write output to this file instead of stdout")
    extract.add_argument("--push", metavar="URL", help="also load results into a running MetaWeave API")
    extract.add_argument("--dialect", default="postgres", help="SQL dialect for parsing (default: postgres)")
    extract.add_argument(
        "--orchestrated-only", action="store_true",
        help="skip .sql/.py files that no orchestration definition references",
    )
    extract.add_argument("--fail-on-warning", action="store_true", help="exit 1 if anything could not be resolved")
    extract.set_defaults(func=cmd_extract)

    lineage = subparsers.add_parser("lineage", help="table + attribute lineage for a single SQL or PySpark file")
    lineage.add_argument("file")
    lineage.add_argument("--format", choices=["text", "json"], default="text")
    lineage.add_argument("--dialect", default="postgres")
    lineage.set_defaults(func=cmd_lineage)

    serve = subparsers.add_parser("serve", help="run the MetaWeave API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--reload", action="store_true")
    serve.set_defaults(func=cmd_serve)

    seed = subparsers.add_parser("seed", help="load the demo dataset into the configured database")
    seed.set_defaults(func=cmd_seed)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
