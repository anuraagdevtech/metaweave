"""ORM models for the MetaWeave enterprise metadata schema (spec §2).

These mirror the production Postgres DDL in
``backend/migrations/versions/0001_initial_schema.py`` table-for-table:

    Workflow_definition, Job_definition, Job_dag, Task_definition,
    Job_audit, Job_dependency, Job_alert

JSONB columns use ``.with_variant(JSON(), "sqlite")`` and UUID primary keys use
``.with_variant(String(36), "sqlite")`` so the identical model set backs both
production Postgres and the SQLite path used for local dev / tests. Enums are
declared ``native_enum=False`` so they materialize as VARCHAR + CHECK on every
dialect, matching the migration exactly instead of relying on Postgres
``CREATE TYPE``.
"""
from __future__ import annotations

import datetime as dt
import enum
import uuid
from typing import Optional

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from app.core.db import Base

# Portable column type helpers -------------------------------------------------
UUIDType = PG_UUID(as_uuid=False).with_variant(String(36), "sqlite")
JSONBType = JSONB().with_variant(JSON(), "sqlite")


def _uuid() -> str:
    return str(uuid.uuid4())


class JobExecType(str, enum.Enum):
    SQL = "SQL"
    PYSPARK = "PYSPARK"
    PYTHON = "PYTHON"
    NOTEBOOK = "NOTEBOOK"


class RunStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class DependencyKind(str, enum.Enum):
    TASK = "TASK"
    DATASET = "DATASET"


class AlertType(str, enum.Enum):
    SLA_BREACH = "SLA_BREACH"
    FAILURE = "FAILURE"
    DATA_DRIFT = "DATA_DRIFT"
    RETRY_EXHAUSTED = "RETRY_EXHAUSTED"


def _enum(python_enum: type[enum.Enum], name: str):
    return Enum(python_enum, name=name, native_enum=False, validate_strings=True)


class TimestampMixin:
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class WorkflowDefinition(TimestampMixin, Base):
    """Orchestration group: owning team, SLA, tags."""

    __tablename__ = "workflow_definition"

    workflow_id: Mapped[str] = mapped_column(UUIDType, primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    owner_team: Mapped[str] = mapped_column(String(255), nullable=False)
    owner_email: Mapped[str] = mapped_column(String(255), nullable=False)
    sla_minutes: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    tags: Mapped[dict] = mapped_column(JSONBType, nullable=False, default=dict)

    jobs: Mapped[list["JobDefinition"]] = relationship(
        back_populates="workflow", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint("sla_minutes > 0", name="ck_workflow_sla_positive"),
    )


class JobDefinition(TimestampMixin, Base):
    """Job-level configuration: retries, concurrency, schedule."""

    __tablename__ = "job_definition"

    job_id: Mapped[str] = mapped_column(UUIDType, primary_key=True, default=_uuid)
    workflow_id: Mapped[str] = mapped_column(
        UUIDType, ForeignKey("workflow_definition.workflow_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    cron_expression: Mapped[Optional[str]] = mapped_column(String(120))
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    max_concurrency: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    cluster_config: Mapped[dict] = mapped_column(JSONBType, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    workflow: Mapped[WorkflowDefinition] = relationship(back_populates="jobs")
    tasks: Mapped[list["TaskDefinition"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    audits: Mapped[list["JobAudit"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    alerts: Mapped[list["JobAlert"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("workflow_id", "name", name="uq_job_workflow_name"),
    )


class JobDag(TimestampMixin, Base):
    """Graph edges between jobs (and optionally external triggers)."""

    __tablename__ = "job_dag"

    dag_edge_id: Mapped[str] = mapped_column(UUIDType, primary_key=True, default=_uuid)
    workflow_id: Mapped[str] = mapped_column(
        UUIDType, ForeignKey("workflow_definition.workflow_id", ondelete="CASCADE"), nullable=False
    )
    upstream_job_id: Mapped[Optional[str]] = mapped_column(
        UUIDType, ForeignKey("job_definition.job_id", ondelete="CASCADE"), nullable=True
    )
    downstream_job_id: Mapped[str] = mapped_column(
        UUIDType, ForeignKey("job_definition.job_id", ondelete="CASCADE"), nullable=False
    )
    # Populated instead of upstream_job_id when the edge originates outside
    # MetaWeave, e.g. {"type": "s3_event", "path": "s3://landing/orders/"}.
    external_trigger: Mapped[Optional[dict]] = mapped_column(JSONBType)

    __table_args__ = (
        CheckConstraint(
            "upstream_job_id IS NOT NULL OR external_trigger IS NOT NULL",
            name="ck_job_dag_has_source",
        ),
    )


class TaskDefinition(TimestampMixin, Base):
    """Task-level execution spec: language, environment, script pointer."""

    __tablename__ = "task_definition"

    task_id: Mapped[str] = mapped_column(UUIDType, primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        UUIDType, ForeignKey("job_definition.job_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    task_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    exec_type: Mapped[JobExecType] = mapped_column(_enum(JobExecType, "job_exec_type"), nullable=False)
    script_path: Mapped[Optional[str]] = mapped_column(String(1024))
    source_code: Mapped[Optional[str]] = mapped_column(Text)
    environment: Mapped[dict] = mapped_column(JSONBType, nullable=False, default=dict)

    job: Mapped[JobDefinition] = relationship(back_populates="tasks")
    dependencies: Mapped[list["JobDependency"]] = relationship(
        back_populates="task",
        cascade="all, delete-orphan",
        foreign_keys="JobDependency.task_id",
    )

    __table_args__ = (
        UniqueConstraint("job_id", "task_order", name="uq_task_job_order"),
    )


class JobAudit(Base):
    """One row per execution run: duration, status, rows processed, logs."""

    __tablename__ = "job_audit"

    audit_id: Mapped[str] = mapped_column(UUIDType, primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        UUIDType, ForeignKey("job_definition.job_id", ondelete="CASCADE"), nullable=False
    )
    run_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[RunStatus] = mapped_column(_enum(RunStatus, "run_status"), nullable=False)
    started_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[Optional[dt.datetime]] = mapped_column(DateTime(timezone=True))
    rows_processed: Mapped[Optional[int]] = mapped_column(Integer)
    cost_usd: Mapped[Optional[float]] = mapped_column()
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    logs_url: Mapped[Optional[str]] = mapped_column(String(1024))
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    job: Mapped[JobDefinition] = relationship(back_populates="audits")

    __table_args__ = (UniqueConstraint("job_id", "run_id", name="uq_audit_job_run"),)


class JobDependency(Base):
    """Upstream task- or dataset-level dependency for a task."""

    __tablename__ = "job_dependency"

    dependency_id: Mapped[str] = mapped_column(UUIDType, primary_key=True, default=_uuid)
    task_id: Mapped[str] = mapped_column(
        UUIDType, ForeignKey("task_definition.task_id", ondelete="CASCADE"), nullable=False
    )
    depends_on_kind: Mapped[DependencyKind] = mapped_column(
        _enum(DependencyKind, "dependency_kind"), nullable=False
    )
    depends_on_task_id: Mapped[Optional[str]] = mapped_column(
        UUIDType, ForeignKey("task_definition.task_id", ondelete="CASCADE")
    )
    depends_on_table: Mapped[Optional[str]] = mapped_column(String(512))
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    task: Mapped[TaskDefinition] = relationship(
        back_populates="dependencies", foreign_keys=[task_id]
    )

    __table_args__ = (
        CheckConstraint(
            "(depends_on_kind = 'TASK' AND depends_on_task_id IS NOT NULL) OR "
            "(depends_on_kind = 'DATASET' AND depends_on_table IS NOT NULL)",
            name="ck_dependency_target_matches_kind",
        ),
    )


class JobAlert(TimestampMixin, Base):
    """SLA breach / failure notification routing for a job."""

    __tablename__ = "job_alert"

    alert_id: Mapped[str] = mapped_column(UUIDType, primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        UUIDType, ForeignKey("job_definition.job_id", ondelete="CASCADE"), nullable=False
    )
    alert_type: Mapped[AlertType] = mapped_column(_enum(AlertType, "alert_type"), nullable=False)
    distribution_list: Mapped[list] = mapped_column(JSONBType, nullable=False, default=list)
    routing_rule: Mapped[dict] = mapped_column(JSONBType, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    job: Mapped[JobDefinition] = relationship(back_populates="alerts")
