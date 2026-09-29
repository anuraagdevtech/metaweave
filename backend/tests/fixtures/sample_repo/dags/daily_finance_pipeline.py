"""Daily finance pipeline — fixture DAG exercising the Airflow extractor."""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {"owner": "fpna-data-eng", "retries": 2}

with DAG(
    "daily_finance_pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    description="Deposits and loans into net interest income",
    tags=["finance", "daily"],
) as dag:

    wait_for_core = ExternalTaskSensor(
        task_id="wait_for_core_banking_load",
        external_dag_id="core_banking_ingest",
        external_task_id="publish_balances",
    )

    build_deposits = SQLExecuteQueryOperator(
        task_id="build_deposit_positions",
        sql="sql/build_deposits.sql",
        conn_id="warehouse",
    )

    build_nii = SQLExecuteQueryOperator(
        task_id="build_net_interest_income",
        sql="sql/build_nii.sql",
        conn_id="warehouse",
    )

    enrich = SparkSubmitOperator(
        task_id="enrich_customer_dim",
        application="spark/enrich_customers.py",
    )

    reconcile = SQLExecuteQueryOperator(
        task_id="reconcile_to_gl",
        sql="""
            INSERT INTO finance.gl_reconciliation (as_of_date, nii_amount, gl_amount, variance)
            SELECT n.as_of_date,
                   SUM(n.net_interest_income) AS nii_amount,
                   SUM(g.posting_amount) AS gl_amount,
                   SUM(n.net_interest_income) - SUM(g.posting_amount) AS variance
            FROM finance.nii_daily n
            JOIN gl.gl_posting g ON g.posting_date = n.as_of_date
            GROUP BY n.as_of_date
        """,
    )

    notify = PythonOperator(task_id="notify_controllers", python_callable=lambda: None)

    trigger_regulatory = TriggerDagRunOperator(
        task_id="kick_off_regulatory",
        trigger_dag_id="regulatory_reporting",
    )

    wait_for_core >> [build_deposits, enrich]
    build_deposits >> build_nii
    enrich >> build_nii
    build_nii >> reconcile >> [notify, trigger_regulatory]
