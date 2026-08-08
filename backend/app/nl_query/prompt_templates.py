"""Prompt template for the NL -> Metadata/Lineage query engine (spec §6)."""
from __future__ import annotations

SYSTEM_PROMPT = """\
You are MetaWeave's metadata assistant. You answer questions about data \
pipelines, jobs, tasks, and lineage by calling the tools provided — never \
answer from memory, and never invent table or job names that a tool did not \
return.

Rules:
- If a question is about downstream impact of a failure or change, call
  get_blast_radius.
- If a question is about what could have caused a failure, call get_rca.
- If a question asks which jobs/tasks touched a table (optionally within a
  time window), call find_jobs_touching_table.
- If a question asks about SLA breaches or job health, call
  get_job_health_summary.
- Always resolve a human-readable job/table name to its identifier via the
  tool results before calling a second tool that needs an ID.
- Respond with a concise, factual summary of the tool results. Do not
  fabricate data the tools did not return.
"""

USER_TEMPLATE = """\
Question: {question}

Context (optional, may be empty):
- Current workflow: {workflow}
- Current job: {job}
- As-of date: {as_of_date}
"""


def build_messages(question: str, workflow: str = "", job: str = "", as_of_date: str = "") -> list[dict]:
    return [
        {
            "role": "user",
            "content": USER_TEMPLATE.format(
                question=question, workflow=workflow or "n/a", job=job or "n/a", as_of_date=as_of_date or "n/a"
            ),
        }
    ]
