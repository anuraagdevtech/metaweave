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
        from app.seed_banking import BANKING_EXAMPLES

        created = seed_demo_data(db)
        assert created == len(SQL_EXAMPLES) + len(BANKING_EXAMPLES)
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


def test_banking_library_has_50_plus_genuinely_complex_examples():
    from app.lineage.sql_parser import parse_sql_script
    from app.seed_banking import BANKING_EXAMPLES, sql_complexity

    assert len(BANKING_EXAMPLES) >= 50

    for example in BANKING_EXAMPLES:
        metrics = sql_complexity(example["sql"])
        # every banking example is a real multi-CTE, multi-table statement,
        # not a single-table select
        assert metrics["ctes"] >= 2, example["title"]
        assert metrics["joins"] >= 1, example["title"]
        assert metrics["tables"] >= 4, example["title"]

        # and each one parses into usable lineage
        results = parse_sql_script(example["sql"])
        assert results, example["title"]
        assert results[0].target_table, example["title"]
        assert len(results[0].source_tables) >= 2, example["title"]
        assert results[0].column_lineage, example["title"]


def test_banking_lineage_reports_physical_tables_not_cte_names():
    """Regression: CTE references parse as tables and must not be reported as sources."""
    from app.lineage.sql_parser import parse_sql_script
    from app.seed_banking import BANKING_EXAMPLES

    example = next(e for e in BANKING_EXAMPLES if e["title"].startswith("Assemble net interest income"))
    result = parse_sql_script(example["sql"])[0]

    # the statement's CTE names must not leak into the source table list
    for cte_name in ("asset_side", "liability_side", "combined"):
        assert cte_name not in result.source_tables

    # the real physical tables it reads are all present, schema-qualified
    assert "fact.loan_balance_daily" in result.source_tables
    assert "fact.deposit_balance_daily" in result.source_tables
    assert "fact.ftp_assignment" in result.source_tables
    assert all("." in name for name in result.source_tables)


def test_seeded_banking_tasks_carry_domain_and_complexity_metadata():
    from app.api.routes_sql_examples import list_sql_examples

    db = _fresh_session()
    try:
        seed_demo_data(db)
        examples = list_sql_examples(db=db)
        banking = [e for e in examples if e.domain == "banking"]
        assert len(banking) >= 50

        for example in banking:
            assert example.complexity is not None
            assert example.complexity.ctes >= 2
            assert example.complexity.tables >= 4
    finally:
        db.close()
