"""Read-only browser over every SQL task in the system, joined with its
owning job/workflow — backs the frontend's "SQL Examples" library page.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.models.metadata import JobDefinition, JobExecType, TaskDefinition, WorkflowDefinition
from app.schemas.metadata import SqlExampleOut

router = APIRouter(tags=["sql-examples"])


@router.get("/sql-examples", response_model=list[SqlExampleOut])
def list_sql_examples(db: Session = Depends(get_db)) -> list[SqlExampleOut]:
    query = (
        select(TaskDefinition, JobDefinition, WorkflowDefinition)
        .join(JobDefinition, TaskDefinition.job_id == JobDefinition.job_id)
        .join(WorkflowDefinition, JobDefinition.workflow_id == WorkflowDefinition.workflow_id)
        .where(TaskDefinition.exec_type == JobExecType.SQL)
        .where(TaskDefinition.source_code.is_not(None))
        .order_by(WorkflowDefinition.name, JobDefinition.name, TaskDefinition.task_order)
    )
    rows = db.execute(query).all()
    return [
        SqlExampleOut(
            task_id=task.task_id,
            task_name=task.name,
            category=(task.environment or {}).get("category"),
            description=(task.environment or {}).get("description"),
            domain=(task.environment or {}).get("domain"),
            complexity=(task.environment or {}).get("complexity"),
            sql=task.source_code or "",
            job_id=job.job_id,
            job_name=job.name,
            workflow_id=workflow.workflow_id,
            workflow_name=workflow.name,
            owner_team=workflow.owner_team,
        )
        for task, job, workflow in rows
    ]
