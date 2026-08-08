"""Initial MetaWeave enterprise metadata schema.

Creates the 7 core tables from spec §2: workflow_definition, job_definition,
job_dag, task_definition, job_audit, job_dependency, job_alert.

Revision ID: 0001_initial_schema
Revises:
Create Date: 2026-08-08
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_initial_schema"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')  # gen_random_uuid()

    op.create_table(
        "workflow_definition",
        sa.Column("workflow_id", postgresql.UUID(as_uuid=False), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("owner_team", sa.String(255), nullable=False),
        sa.Column("owner_email", sa.String(255), nullable=False),
        sa.Column("sla_minutes", sa.Integer, nullable=False),
        sa.Column("description", sa.Text),
        sa.Column("tags", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("workflow_id", name="pk_workflow_definition"),
        sa.UniqueConstraint("name", name="uq_workflow_name"),
        sa.CheckConstraint("sla_minutes > 0", name="ck_workflow_sla_positive"),
    )

    op.create_table(
        "job_definition",
        sa.Column("job_id", postgresql.UUID(as_uuid=False), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workflow_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("cron_expression", sa.String(120)),
        sa.Column("max_retries", sa.Integer, nullable=False, server_default="0"),
        sa.Column("retry_delay_seconds", sa.Integer, nullable=False, server_default="60"),
        sa.Column("max_concurrency", sa.Integer, nullable=False, server_default="1"),
        sa.Column("cluster_config", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("job_id", name="pk_job_definition"),
        sa.ForeignKeyConstraint(
            ["workflow_id"], ["workflow_definition.workflow_id"], name="fk_job_workflow", ondelete="CASCADE"
        ),
        sa.UniqueConstraint("workflow_id", "name", name="uq_job_workflow_name"),
    )
    op.create_index("ix_job_definition_workflow_id", "job_definition", ["workflow_id"])
    op.create_index("ix_job_definition_is_active", "job_definition", ["is_active"])

    op.create_table(
        "task_definition",
        sa.Column("task_id", postgresql.UUID(as_uuid=False), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("task_order", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "exec_type",
            sa.String(20),
            nullable=False,
        ),
        sa.Column("script_path", sa.String(1024)),
        sa.Column("source_code", sa.Text),
        sa.Column("environment", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("task_id", name="pk_task_definition"),
        sa.ForeignKeyConstraint(["job_id"], ["job_definition.job_id"], name="fk_task_job", ondelete="CASCADE"),
        sa.UniqueConstraint("job_id", "task_order", name="uq_task_job_order"),
        sa.CheckConstraint(
            "exec_type IN ('SQL','PYSPARK','PYTHON','NOTEBOOK')", name="ck_task_exec_type"
        ),
    )
    op.create_index("ix_task_definition_job_id", "task_definition", ["job_id"])

    op.create_table(
        "job_dag",
        sa.Column("dag_edge_id", postgresql.UUID(as_uuid=False), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("workflow_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("upstream_job_id", postgresql.UUID(as_uuid=False)),
        sa.Column("downstream_job_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("external_trigger", postgresql.JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("dag_edge_id", name="pk_job_dag"),
        sa.ForeignKeyConstraint(
            ["workflow_id"], ["workflow_definition.workflow_id"], name="fk_dag_workflow", ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["upstream_job_id"], ["job_definition.job_id"], name="fk_dag_upstream", ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["downstream_job_id"], ["job_definition.job_id"], name="fk_dag_downstream", ondelete="CASCADE"
        ),
        sa.CheckConstraint(
            "upstream_job_id IS NOT NULL OR external_trigger IS NOT NULL", name="ck_job_dag_has_source"
        ),
    )
    # Traversal indexes: downstream lookup (blast radius) and upstream lookup (RCA).
    op.create_index("ix_job_dag_upstream", "job_dag", ["upstream_job_id"])
    op.create_index("ix_job_dag_downstream", "job_dag", ["downstream_job_id"])

    op.create_table(
        "job_audit",
        sa.Column("audit_id", postgresql.UUID(as_uuid=False), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("run_id", sa.String(255), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("rows_processed", sa.BigInteger),
        sa.Column("cost_usd", sa.Numeric(12, 4)),
        sa.Column("error_message", sa.Text),
        sa.Column("logs_url", sa.String(1024)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("audit_id", name="pk_job_audit"),
        sa.ForeignKeyConstraint(["job_id"], ["job_definition.job_id"], name="fk_audit_job", ondelete="CASCADE"),
        sa.UniqueConstraint("job_id", "run_id", name="uq_audit_job_run"),
        sa.CheckConstraint(
            "status IN ('PENDING','RUNNING','SUCCESS','FAILED','SKIPPED','CANCELLED')",
            name="ck_audit_status",
        ),
    )
    # SLA/monitoring dashboards filter by job + time range and by status.
    op.create_index("ix_job_audit_job_started", "job_audit", ["job_id", "started_at"])
    op.create_index("ix_job_audit_status", "job_audit", ["status"])

    op.create_table(
        "job_dependency",
        sa.Column("dependency_id", postgresql.UUID(as_uuid=False), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("task_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("depends_on_kind", sa.String(10), nullable=False),
        sa.Column("depends_on_task_id", postgresql.UUID(as_uuid=False)),
        sa.Column("depends_on_table", sa.String(512)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("dependency_id", name="pk_job_dependency"),
        sa.ForeignKeyConstraint(["task_id"], ["task_definition.task_id"], name="fk_dependency_task", ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["depends_on_task_id"], ["task_definition.task_id"], name="fk_dependency_on_task", ondelete="CASCADE"
        ),
        sa.CheckConstraint(
            "(depends_on_kind = 'TASK' AND depends_on_task_id IS NOT NULL) OR "
            "(depends_on_kind = 'DATASET' AND depends_on_table IS NOT NULL)",
            name="ck_dependency_target_matches_kind",
        ),
    )
    op.create_index("ix_job_dependency_task_id", "job_dependency", ["task_id"])
    op.create_index("ix_job_dependency_on_task_id", "job_dependency", ["depends_on_task_id"])
    # Many-to-one reconciliation / one-to-many fan-out lookups by dataset name.
    op.create_index("ix_job_dependency_on_table", "job_dependency", ["depends_on_table"])

    op.create_table(
        "job_alert",
        sa.Column("alert_id", postgresql.UUID(as_uuid=False), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("job_id", postgresql.UUID(as_uuid=False), nullable=False),
        sa.Column("alert_type", sa.String(20), nullable=False),
        sa.Column("distribution_list", postgresql.JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("routing_rule", postgresql.JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("alert_id", name="pk_job_alert"),
        sa.ForeignKeyConstraint(["job_id"], ["job_definition.job_id"], name="fk_alert_job", ondelete="CASCADE"),
        sa.CheckConstraint(
            "alert_type IN ('SLA_BREACH','FAILURE','DATA_DRIFT','RETRY_EXHAUSTED')",
            name="ck_alert_type",
        ),
    )
    op.create_index("ix_job_alert_job_id", "job_alert", ["job_id"])


def downgrade() -> None:
    op.drop_table("job_alert")
    op.drop_table("job_dependency")
    op.drop_table("job_audit")
    op.drop_table("job_dag")
    op.drop_table("task_definition")
    op.drop_table("job_definition")
    op.drop_table("workflow_definition")
