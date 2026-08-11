"""Tests for the demo seed dataset: at least 100 SQL examples, each wired to
a real job/task, and the /sql-examples endpoint that surfaces them.

Uses its own isolated in-memory SQLite engine rather than the shared
conftest test DB, so it doesn't collide with the workflow/job/task rows
other test modules create via the API.
"""
from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.api.routes_sql_examples import list_sql_examples
from app.core.db import Base
from app.models.metadata import JobExecType, TaskDefinition
from app.seed_data import SQL_EXAMPLES, seed_demo_data


def _fresh_session() -> Session:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(engine)
    return Session(bind=engine)


def test_sql_example_dataset_has_at_least_100_distinct_examples():
    assert len(SQL_EXAMPLES) >= 100
    # every example carries real, non-empty SQL and a category for the UI filter
    assert all(example["sql"].strip() for example in SQL_EXAMPLES)
    assert all(example["category"] for example in SQL_EXAMPLES)
    # no two examples share identical SQL text
    assert len({example["sql"] for example in SQL_EXAMPLES}) == len(SQL_EXAMPLES)


def test_seed_demo_data_creates_full_graph_and_is_idempotent():
    db = _fresh_session()
    try:
        created = seed_demo_data(db)
        assert created == len(SQL_EXAMPLES)
        assert created >= 100

        sql_task_count = (
            db.query(TaskDefinition).filter(TaskDefinition.exec_type == JobExecType.SQL).count()
        )
        assert sql_task_count == created

        # every seeded task lives under a workflow -> job with a task DAG,
        # run history and alert routing already in place
        for task in db.query(TaskDefinition).all():
            assert task.job is not None
            assert task.job.workflow is not None

        # idempotent: calling again on a non-empty DB is a no-op
        assert seed_demo_data(db) == 0
    finally:
        db.close()


def test_sql_examples_endpoint_returns_joined_job_and_workflow_context():
    db = _fresh_session()
    try:
        seed_demo_data(db)
        results = list_sql_examples(db=db)
        assert len(results) >= 100

        first = results[0]
        assert first.sql.strip()
        assert first.job_name
        assert first.workflow_name
        assert first.owner_team
        assert first.category in {example["category"] for example in SQL_EXAMPLES}
    finally:
        db.close()
