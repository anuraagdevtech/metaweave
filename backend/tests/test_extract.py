"""Tests for the lineage extraction tool.

Runs against `tests/fixtures/sample_repo`, a small but realistic repo holding
an Airflow DAG (with referenced .sql and PySpark files), an uncompiled dbt
project, and a Databricks job spec.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.cli import main as cli_main
from app.extract import extract_path

FIXTURE_REPO = Path(__file__).parent / "fixtures" / "sample_repo"


@pytest.fixture(scope="module")
def result():
    return extract_path(FIXTURE_REPO)


# -- job lineage (from orchestration) --------------------------------------


def test_extracts_jobs_from_every_orchestration_system(result):
    systems = {job.system for job in result.jobs}
    assert systems == {"airflow", "databricks", "dbt"}

    by_id = {job.job_id: job for job in result.jobs}
    assert by_id["daily_finance_pipeline"].schedule == "0 2 * * *"
    assert by_id["daily_finance_pipeline"].owner == "fpna-data-eng"
    assert by_id["daily_finance_pipeline"].tags == ["finance", "daily"]
    assert by_id["regulatory_reporting"].schedule == "0 0 5 * * ?"


def test_recovers_intra_dag_task_dependencies_from_shift_operators(result):
    edges = set(result.task_edges())
    dag = "daily_finance_pipeline"

    # `wait_for_core >> [build_deposits, enrich]` — list fan-out
    assert (f"{dag}.wait_for_core_banking_load", f"{dag}.build_deposit_positions") in edges
    assert (f"{dag}.wait_for_core_banking_load", f"{dag}.enrich_customer_dim") in edges

    # two separate statements converging on one task — fan-in
    assert (f"{dag}.build_deposit_positions", f"{dag}.build_net_interest_income") in edges
    assert (f"{dag}.enrich_customer_dim", f"{dag}.build_net_interest_income") in edges

    # `build_nii >> reconcile >> [notify, trigger]` — chained, then fan-out
    assert (f"{dag}.build_net_interest_income", f"{dag}.reconcile_to_gl") in edges
    assert (f"{dag}.reconcile_to_gl", f"{dag}.notify_controllers") in edges
    assert (f"{dag}.reconcile_to_gl", f"{dag}.kick_off_regulatory") in edges


def test_recovers_cross_dag_job_dependencies(result):
    job_edges = set(result.job_edges())
    # ExternalTaskSensor(external_dag_id=...) -> upstream job
    assert ("core_banking_ingest", "daily_finance_pipeline") in job_edges
    # TriggerDagRunOperator(trigger_dag_id=...) -> downstream job
    assert ("daily_finance_pipeline", "regulatory_reporting") in job_edges


def test_recovers_databricks_task_graph(result):
    job = next(j for j in result.jobs if j.job_id == "regulatory_reporting")
    tasks = {t.task_id: t for t in job.tasks}
    assert tasks["build_call_report"].operator == "databricks.sql_task"
    assert tasks["validate_submission"].operator == "databricks.notebook_task"
    assert tasks["validate_submission"].upstream_task_ids == ["build_call_report"]


def test_recovers_dbt_model_graph_from_jinja_refs(result):
    job = next(j for j in result.jobs if j.system == "dbt")
    assert job.job_id == "bank_marts"
    tasks = {t.task_id: t for t in job.tasks}
    assert tasks["fct_loan_pnl"].upstream_task_ids == ["stg_loans"]
    # {{ source('core', 'loan_account') }} resolves to a real table
    assert "core.loan_account" in tasks["stg_loans"].reads


# -- table lineage (from code) ---------------------------------------------


def test_resolves_operator_file_references_into_table_lineage(result):
    """The DAG only says `sql="sql/build_deposits.sql"` — the file is read and parsed."""
    task = _task(result, "daily_finance_pipeline", "build_deposit_positions")
    assert task.source_ref == "dags/sql/build_deposits.sql"
    assert task.writes == ["fact.deposit_balance_daily"]
    assert "core.deposit_balance_history" in task.reads
    assert "ref.rate_plan" in task.reads


def test_parses_inline_sql_and_pyspark_application(result):
    inline = _task(result, "daily_finance_pipeline", "reconcile_to_gl")
    assert inline.source_ref == "inline"
    assert inline.writes == ["finance.gl_reconciliation"]
    assert set(inline.reads) == {"finance.nii_daily", "gl.gl_posting"}

    spark = _task(result, "daily_finance_pipeline", "enrich_customer_dim")
    assert spark.language == "PYSPARK"
    assert spark.writes == ["dim.dim_customer"]
    assert "core.customer_master" in spark.reads


def test_table_lineage_crosses_orchestration_systems(result):
    edges = {(s, t) for s, t, _ in result.table_edges()}
    # written by the Airflow DAG, read by the Databricks job
    assert ("fact.loan_balance_daily", "reg.call_report_rc_c") in edges
    # dbt model chain
    assert ("mart.stg_loans", "mart.fct_loan_pnl") in edges


def test_infers_job_dependencies_the_orchestration_never_declared(result):
    """deposit fact is written by one task and read by another in the same DAG."""
    index = result.tables()
    assert index["fact.deposit_balance_daily"]["written_by"] == [
        "daily_finance_pipeline.build_deposit_positions"
    ]
    assert "daily_finance_pipeline.build_net_interest_income" in index["fact.deposit_balance_daily"]["read_by"]


# -- attribute lineage (from code) -----------------------------------------


def test_attribute_lineage_resolves_through_ctes_to_physical_columns(result):
    """build_nii.sql selects from two CTEs; columns must trace past them."""
    task = _task(result, "daily_finance_pipeline", "build_net_interest_income")
    mapping = {
        (edge.target_column, edge.source_table, edge.source_column) for edge in task.column_lineage
    }

    # interest_income comes through the `assets` CTE from the loan fact
    assert ("interest_income", "fact.loan_balance_daily", "accrued_interest") in mapping
    # interest_expense comes through the `liabilities` CTE from the deposit fact
    assert ("interest_expense", "fact.deposit_balance_daily", "average_daily_balance") in mapping
    assert ("interest_expense", "fact.deposit_balance_daily", "contractual_rate") in mapping

    # no CTE name is ever reported as a source table
    assert not {edge.source_table for edge in task.column_lineage} & {"assets", "liabilities"}


def test_attribute_lineage_is_attributed_back_to_its_task_and_job(result):
    """The three levels are correlated: a column knows the job that produces it."""
    edges = result.column_edges()
    owners = {
        via for edge, via in edges
        if edge.target_table == "reg.call_report_rc_c" and edge.target_column == "total_amount"
    }
    assert owners == {"regulatory_reporting.build_call_report"}


# -- CLI --------------------------------------------------------------------


def test_cli_extract_json_output(tmp_path, capsys):
    out = tmp_path / "lineage.json"
    exit_code = cli_main(["extract", str(FIXTURE_REPO), "--format", "json", "--out", str(out)])
    assert exit_code == 0

    import json

    payload = json.loads(out.read_text())
    assert payload["summary"]["jobs"] == 3
    assert payload["summary"]["column_mappings"] > 0
    assert {"job_lineage", "task_lineage", "table_lineage", "attribute_lineage"} <= payload.keys()


def test_cli_extract_level_filters_output(capsys):
    assert cli_main(["extract", str(FIXTURE_REPO), "--level", "job"]) == 0
    printed = capsys.readouterr().out
    assert "JOB LINEAGE" in printed
    assert "ATTRIBUTE LINEAGE" not in printed


def test_cli_lineage_single_file(capsys):
    target = FIXTURE_REPO / "dags" / "sql" / "build_deposits.sql"
    assert cli_main(["lineage", str(target)]) == 0
    printed = capsys.readouterr().out
    assert "fact.deposit_balance_daily" in printed
    assert "core.deposit_balance_history" in printed


def _task(result, job_id: str, task_id: str):
    job = next(j for j in result.jobs if j.job_id == job_id)
    return next(t for t in job.tasks if t.task_id == task_id)
