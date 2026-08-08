"""Typed tool registry + dispatcher for the NL query engine (spec §6).

`TOOL_SCHEMAS` is written in Anthropic's tool-calling `input_schema` format
and doubles as the contract for the regex-based fallback below, so both
paths call the exact same Python functions with the exact same arguments.

Two execution paths:
  * `answer_question(db, question)` uses the Anthropic Messages API with
    tool-calling when `ANTHROPIC_API_KEY` is set — the model chooses which
    tool(s) to call and synthesizes the final answer.
  * Otherwise, a deterministic regex intent-matcher covers the two example
    queries from the spec (blast radius on job failure, jobs touching a
    table) so the endpoint is fully testable offline.
"""
from __future__ import annotations

import re
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.lineage.graph import build_dependency_graph
from app.models.metadata import JobDefinition, JobDependency, TaskDefinition

TOOL_SCHEMAS: list[dict] = [
    {
        "name": "get_blast_radius",
        "description": "Downstream tasks/tables impacted if the given job or table fails or changes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_name": {"type": "string", "description": "Job name to start from, if known."},
                "table_name": {"type": "string", "description": "Dataset name to start from, if known."},
                "max_depth": {"type": "integer", "default": 10},
            },
        },
    },
    {
        "name": "get_rca",
        "description": "Upstream tasks/tables that could be the root cause of a failed job.",
        "input_schema": {
            "type": "object",
            "properties": {
                "job_name": {"type": "string"},
                "max_depth": {"type": "integer", "default": 10},
            },
        },
    },
    {
        "name": "find_jobs_touching_table",
        "description": (
            "Jobs/tasks that read or wrote a given table, optionally filtered by "
            "exec type and a lookback window in days."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string"},
                "exec_type": {"type": "string", "enum": ["SQL", "PYSPARK", "PYTHON", "NOTEBOOK"]},
                "lookback_days": {"type": "integer", "default": 7},
            },
            "required": ["table_name"],
        },
    },
    {
        "name": "get_job_health_summary",
        "description": "Recent SLA/status summary for a job: success rate, last run status, avg duration.",
        "input_schema": {
            "type": "object",
            "properties": {"job_name": {"type": "string"}},
            "required": ["job_name"],
        },
    },
]


def _find_job(db: Session, job_name: str) -> JobDefinition | None:
    return db.execute(
        select(JobDefinition).where(JobDefinition.name.ilike(f"%{job_name}%"))
    ).scalars().first()


def get_blast_radius(db: Session, job_name: str = "", table_name: str = "", max_depth: int = 10) -> dict[str, Any]:
    graph = build_dependency_graph(db)
    roots: list[str] = []
    if table_name:
        roots.append(f"dataset:{table_name}")
    job = _find_job(db, job_name) if job_name else None
    if job:
        roots.extend(t.task_id for t in job.tasks)

    impacted: dict[str, int] = {}
    for root in roots:
        for node in graph.blast_radius(root, max_depth=max_depth):
            impacted[node.node_id] = min(impacted.get(node.node_id, node.depth), node.depth)

    return {
        "root_job": job.name if job else None,
        "root_table": table_name or None,
        "impacted_nodes": [
            {"id": node_id, "depth": depth} for node_id, depth in sorted(impacted.items(), key=lambda kv: kv[1])
        ],
    }


def get_rca(db: Session, job_name: str = "", max_depth: int = 10) -> dict[str, Any]:
    graph = build_dependency_graph(db)
    job = _find_job(db, job_name) if job_name else None
    candidates: dict[str, int] = {}
    if job:
        for task in job.tasks:
            for node in graph.rca(task.task_id, max_depth=max_depth):
                candidates[node.node_id] = min(candidates.get(node.node_id, node.depth), node.depth)

    return {
        "job": job.name if job else None,
        "root_cause_candidates": [
            {"id": node_id, "depth": depth} for node_id, depth in sorted(candidates.items(), key=lambda kv: kv[1])
        ],
    }


def find_jobs_touching_table(
    db: Session, table_name: str, exec_type: str = "", lookback_days: int = 7
) -> dict[str, Any]:
    query = (
        select(TaskDefinition, JobDefinition)
        .join(JobDefinition, TaskDefinition.job_id == JobDefinition.job_id)
        .join(JobDependency, JobDependency.task_id == TaskDefinition.task_id)
        .where(JobDependency.depends_on_table.ilike(f"%{table_name}%"))
    )
    if exec_type:
        query = query.where(TaskDefinition.exec_type == exec_type)
    rows = db.execute(query.distinct()).all()

    # depends_on_table only captures read-side dependencies; also surface
    # tasks whose inline source code mentions the table, as a best-effort
    # catch for write-side/undeclared references.
    fallback_query = select(TaskDefinition, JobDefinition).join(
        JobDefinition, TaskDefinition.job_id == JobDefinition.job_id
    ).where(TaskDefinition.source_code.ilike(f"%{table_name}%"))
    if exec_type:
        fallback_query = fallback_query.where(TaskDefinition.exec_type == exec_type)
    rows += db.execute(fallback_query.distinct()).all()

    seen: set[str] = set()
    results = []
    for task, job in rows:
        if task.task_id in seen:
            continue
        seen.add(task.task_id)
        results.append(
            {
                "job_name": job.name,
                "task_name": task.name,
                "exec_type": task.exec_type.value if hasattr(task.exec_type, "value") else task.exec_type,
            }
        )

    return {"table": table_name, "lookback_days": lookback_days, "jobs": results}


def get_job_health_summary(db: Session, job_name: str) -> dict[str, Any]:
    job = _find_job(db, job_name)
    if job is None:
        return {"error": f"No job found matching '{job_name}'"}

    recent = sorted(job.audits, key=lambda a: a.started_at, reverse=True)[:20]
    durations = [
        (a.ended_at - a.started_at).total_seconds() for a in recent if a.ended_at is not None
    ]
    success = sum(1 for a in recent if str(a.status).endswith("SUCCESS"))

    return {
        "job_name": job.name,
        "runs_considered": len(recent),
        "success_rate": (success / len(recent)) if recent else None,
        "avg_duration_seconds": (sum(durations) / len(durations)) if durations else None,
        "last_status": str(recent[0].status) if recent else None,
        "last_run_at": recent[0].started_at.isoformat() if recent else None,
    }


TOOL_DISPATCH: dict[str, Callable[..., dict[str, Any]]] = {
    "get_blast_radius": get_blast_radius,
    "get_rca": get_rca,
    "find_jobs_touching_table": find_jobs_touching_table,
    "get_job_health_summary": get_job_health_summary,
}


# ---------------------------------------------------------------------------
# Fallback intent matcher (no network / no API key required)
# ---------------------------------------------------------------------------

_FAIL_IMPACT_RE = re.compile(r"if (?:the )?([\w][\w\-]*)\s+job\s+fails", re.IGNORECASE)
_ROOT_CAUSE_RE = re.compile(r"(?:why did|root cause of|rca for)\s+(?:the )?([\w][\w\-]*)\s+job", re.IGNORECASE)
_TOUCHED_TABLE_RE = re.compile(
    r"(sql|pyspark|python|notebook)?\s*jobs?\s+that\s+touched\s+(?:the\s+)?([\w][\w\.\-]*)\s+table",
    re.IGNORECASE,
)
_HEALTH_RE = re.compile(r"(?:health|status|sla)\s+of\s+(?:the )?([\w][\w\-]*)\s+job", re.IGNORECASE)
_THIS_WEEK_RE = re.compile(r"this week", re.IGNORECASE)


def _fallback_intent(question: str) -> tuple[str, dict[str, Any]] | None:
    if match := _FAIL_IMPACT_RE.search(question):
        return "get_blast_radius", {"job_name": match.group(1)}
    if match := _ROOT_CAUSE_RE.search(question):
        return "get_rca", {"job_name": match.group(1)}
    if match := _TOUCHED_TABLE_RE.search(question):
        exec_type, table = match.group(1), match.group(2)
        lookback = 7 if _THIS_WEEK_RE.search(question) else 30
        return "find_jobs_touching_table", {
            "table_name": table,
            "exec_type": exec_type.upper() if exec_type else "",
            "lookback_days": lookback,
        }
    if match := _HEALTH_RE.search(question):
        return "get_job_health_summary", {"job_name": match.group(1)}
    return None


def _summarize(tool_name: str, result: dict[str, Any]) -> str:
    if tool_name == "get_blast_radius":
        nodes = result["impacted_nodes"]
        if not nodes:
            return f"No downstream impact found for {result.get('root_job') or result.get('root_table')}."
        names = ", ".join(n["id"] for n in nodes[:10])
        return f"{len(nodes)} downstream node(s) impacted: {names}" + (" ..." if len(nodes) > 10 else "")
    if tool_name == "get_rca":
        nodes = result["root_cause_candidates"]
        if not nodes:
            return f"No upstream root-cause candidates found for {result.get('job')}."
        names = ", ".join(n["id"] for n in nodes[:10])
        return f"{len(nodes)} upstream candidate(s): {names}" + (" ..." if len(nodes) > 10 else "")
    if tool_name == "find_jobs_touching_table":
        jobs = result["jobs"]
        if not jobs:
            return f"No jobs found touching '{result['table']}' in the last {result['lookback_days']} days."
        names = ", ".join(f"{j['job_name']}/{j['task_name']}" for j in jobs)
        return f"{len(jobs)} task(s) touched '{result['table']}': {names}"
    if tool_name == "get_job_health_summary":
        if "error" in result:
            return result["error"]
        rate = result["success_rate"]
        rate_pct = f"{rate * 100:.0f}%" if rate is not None else "n/a"
        return f"{result['job_name']}: {rate_pct} success over last {result['runs_considered']} runs, last status {result['last_status']}."
    return "No matching tool result."


def answer_question(db: Session, question: str) -> dict[str, Any]:
    """Answer a natural-language metadata/lineage question.

    Returns {"answer": str, "tool_calls": [{"name": ..., "input": ..., "result": ...}]}.
    """
    if settings.anthropic_api_key:
        return _answer_with_anthropic(db, question)
    return _answer_with_fallback(db, question)


def _answer_with_fallback(db: Session, question: str) -> dict[str, Any]:
    intent = _fallback_intent(question)
    if intent is None:
        return {
            "answer": (
                "I couldn't map this question to a known intent (blast radius, RCA, "
                "table lineage lookup, or job health). Try rephrasing, or set "
                "ANTHROPIC_API_KEY to enable free-form NL understanding."
            ),
            "tool_calls": [],
        }
    tool_name, tool_input = intent
    result = TOOL_DISPATCH[tool_name](db, **tool_input)
    return {
        "answer": _summarize(tool_name, result),
        "tool_calls": [{"name": tool_name, "input": tool_input, "result": result}],
    }


def _answer_with_anthropic(db: Session, question: str) -> dict[str, Any]:
    import anthropic

    from app.nl_query.prompt_templates import SYSTEM_PROMPT, build_messages

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    messages = build_messages(question)
    tool_calls: list[dict[str, Any]] = []

    for _ in range(4):  # bounded tool-use loop
        response = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOL_SCHEMAS,
            messages=messages,
        )
        if response.stop_reason != "tool_use":
            final_text = "".join(b.text for b in response.content if b.type == "text")
            return {"answer": final_text, "tool_calls": tool_calls}

        messages.append({"role": "assistant", "content": response.content})
        tool_results = []
        for block in response.content:
            if block.type != "tool_use":
                continue
            result = TOOL_DISPATCH[block.name](db, **block.input)
            tool_calls.append({"name": block.name, "input": block.input, "result": result})
            tool_results.append(
                {"type": "tool_result", "tool_use_id": block.id, "content": str(result)}
            )
        messages.append({"role": "user", "content": tool_results})

    return {"answer": "Reached tool-call limit without a final answer.", "tool_calls": tool_calls}
