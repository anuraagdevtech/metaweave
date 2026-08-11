"""Pydantic request/response models for the metadata API."""
from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from app.models.metadata import AlertType, DependencyKind, JobExecType, RunStatus


class WorkflowCreate(BaseModel):
    name: str
    owner_team: str
    owner_email: EmailStr
    sla_minutes: int = Field(gt=0)
    description: str | None = None
    tags: dict = Field(default_factory=dict)


class WorkflowOut(WorkflowCreate):
    model_config = ConfigDict(from_attributes=True)

    workflow_id: str
    created_at: dt.datetime
    updated_at: dt.datetime


class JobCreate(BaseModel):
    workflow_id: str
    name: str
    cron_expression: str | None = None
    max_retries: int = 0
    retry_delay_seconds: int = 60
    max_concurrency: int = 1
    cluster_config: dict = Field(default_factory=dict)
    is_active: bool = True


class JobOut(JobCreate):
    model_config = ConfigDict(from_attributes=True)

    job_id: str
    created_at: dt.datetime
    updated_at: dt.datetime


class TaskCreate(BaseModel):
    job_id: str
    name: str
    task_order: int = 0
    exec_type: JobExecType
    script_path: str | None = None
    source_code: str | None = None
    environment: dict = Field(default_factory=dict)


class TaskOut(TaskCreate):
    model_config = ConfigDict(from_attributes=True)

    task_id: str
    created_at: dt.datetime
    updated_at: dt.datetime


class JobAuditCreate(BaseModel):
    job_id: str
    run_id: str
    status: RunStatus
    started_at: dt.datetime
    ended_at: dt.datetime | None = None
    rows_processed: int | None = None
    cost_usd: float | None = None
    error_message: str | None = None
    logs_url: str | None = None


class JobAuditOut(JobAuditCreate):
    model_config = ConfigDict(from_attributes=True)

    audit_id: str
    created_at: dt.datetime


class JobDependencyCreate(BaseModel):
    task_id: str
    depends_on_kind: DependencyKind
    depends_on_task_id: str | None = None
    depends_on_table: str | None = None


class JobDependencyOut(JobDependencyCreate):
    model_config = ConfigDict(from_attributes=True)

    dependency_id: str
    created_at: dt.datetime


class JobAlertCreate(BaseModel):
    job_id: str
    alert_type: AlertType
    distribution_list: list[str] = Field(default_factory=list)
    routing_rule: dict = Field(default_factory=dict)
    is_active: bool = True


class JobAlertOut(JobAlertCreate):
    model_config = ConfigDict(from_attributes=True)

    alert_id: str
    created_at: dt.datetime
    updated_at: dt.datetime


class SqlExampleOut(BaseModel):
    """A SQL task flattened with its owning job/workflow, for the SQL library UI."""

    task_id: str
    task_name: str
    category: str | None = None
    description: str | None = None
    sql: str
    job_id: str
    job_name: str
    workflow_id: str
    workflow_name: str
    owner_team: str
