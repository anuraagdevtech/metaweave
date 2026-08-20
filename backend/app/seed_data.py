"""Deterministic demo dataset for local dev / the quick-start SQLite path.

Generates ~120 realistic SQL task examples spread across 8 business domains
(e-commerce, marketing, finance, logistics, HR, support, product analytics,
IoT), each wired into a full workflow -> job -> task -> run-history -> alert
-> dependency graph. This gives every page in the UI (dashboard, job detail,
lineage, blast radius/RCA, SQL example library) real data to show on first
run instead of an empty state.

Each domain applies the same 15 SQL patterns (incremental load, SCD2,
dedup, rollups, ranking, funnel/cohort analysis, data quality checks, gap
filling, rate normalization, sessionization, anomaly detection, governance
deletes) to its own tables/columns, grouped into 4 jobs per domain
(ingest -> quality_checks -> analytics -> governance):

    8 domains x 15 patterns = 120 SQL task examples  (>= 100 required)

``seed_demo_data`` is idempotent: it's a no-op if any workflow already
exists, so it's safe to call unconditionally on every app startup.
"""
from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.metadata import (
    AlertType,
    DependencyKind,
    JobAlert,
    JobAudit,
    JobDag,
    JobDefinition,
    JobDependency,
    JobExecType,
    RunStatus,
    TaskDefinition,
    WorkflowDefinition,
)

_RNG = random.Random(20240611)


@dataclass(frozen=True)
class Domain:
    key: str
    workflow_name: str
    owner_team: str
    owner_email: str
    description: str
    schema: str
    fact: str
    id: str
    actor: str
    actor_dim: str
    actor_name: str
    amount: str
    ts: str
    status: str
    category: str
    region: str
    rate_join_col: str
    rate_table: str
    rate_multiplier: str
    rate_label: str
    staging: str
    event: str
    event_type: str


DOMAINS: list[Domain] = [
    Domain(
        key="ecommerce",
        workflow_name="ecommerce_orders_platform",
        owner_team="commerce-data-eng",
        owner_email="commerce-data-eng@metaweave.example",
        description="Order, customer and revenue pipelines for the e-commerce storefront.",
        schema="sales", fact="orders", id="order_id", actor="customer_id", actor_dim="customers",
        actor_name="full_name", amount="order_amount", ts="order_date", status="order_status",
        category="product_category", region="region", rate_join_col="currency_code",
        rate_table="reference.fx_rates", rate_multiplier="rate_to_usd", rate_label="Currency",
        staging="stg_orders", event="order_events", event_type="event_type",
    ),
    Domain(
        key="marketing",
        workflow_name="marketing_attribution_platform",
        owner_team="growth-data-eng",
        owner_email="growth-data-eng@metaweave.example",
        description="Campaign spend, lead attribution and channel performance pipelines.",
        schema="marketing", fact="campaign_spend", id="spend_id", actor="lead_id", actor_dim="leads",
        actor_name="lead_name", amount="spend_amount", ts="spend_date", status="campaign_status",
        category="channel", region="geo", rate_join_col="currency_code",
        rate_table="reference.fx_rates", rate_multiplier="rate_to_usd", rate_label="Currency",
        staging="stg_campaign_spend", event="campaign_events", event_type="event_type",
    ),
    Domain(
        key="finance",
        workflow_name="finance_ledger_platform",
        owner_team="finance-data-eng",
        owner_email="finance-data-eng@metaweave.example",
        description="Transaction ledger, account reconciliation and reporting pipelines.",
        schema="finance", fact="transactions", id="transaction_id", actor="account_id", actor_dim="accounts",
        actor_name="account_name", amount="txn_amount", ts="txn_timestamp", status="txn_status",
        category="txn_type", region="country", rate_join_col="currency_code",
        rate_table="reference.fx_rates", rate_multiplier="rate_to_usd", rate_label="Currency",
        staging="stg_transactions", event="ledger_events", event_type="event_type",
    ),
    Domain(
        key="logistics",
        workflow_name="logistics_fulfillment_platform",
        owner_team="supply-chain-data-eng",
        owner_email="supply-chain-data-eng@metaweave.example",
        description="Shipment tracking, carrier cost and warehouse throughput pipelines.",
        schema="logistics", fact="shipments", id="shipment_id", actor="warehouse_id", actor_dim="warehouses",
        actor_name="warehouse_name", amount="freight_cost", ts="ship_date", status="shipment_status",
        category="carrier", region="region", rate_join_col="carrier",
        rate_table="reference.fuel_surcharge_rates", rate_multiplier="surcharge_multiplier",
        rate_label="Fuel surcharge", staging="stg_shipments", event="tracking_events", event_type="event_type",
    ),
    Domain(
        key="hr",
        workflow_name="workforce_payroll_platform",
        owner_team="people-data-eng",
        owner_email="people-data-eng@metaweave.example",
        description="Payroll run, headcount and workforce cost pipelines.",
        schema="hr", fact="payroll_runs", id="payroll_id", actor="employee_id", actor_dim="employees",
        actor_name="employee_name", amount="gross_pay", ts="pay_date", status="payroll_status",
        category="department", region="work_location", rate_join_col="currency_code",
        rate_table="reference.fx_rates", rate_multiplier="rate_to_usd", rate_label="Currency",
        staging="stg_payroll", event="hr_events", event_type="event_type",
    ),
    Domain(
        key="support",
        workflow_name="customer_support_platform",
        owner_team="support-data-eng",
        owner_email="support-data-eng@metaweave.example",
        description="Ticket volume, handle-time and SLA compliance pipelines.",
        schema="support", fact="tickets", id="ticket_id", actor="customer_id", actor_dim="customers",
        actor_name="customer_name", amount="handle_time_minutes", ts="created_at", status="ticket_status",
        category="issue_type", region="region", rate_join_col="priority_level",
        rate_table="reference.sla_weights", rate_multiplier="weight_multiplier", rate_label="SLA weighting",
        staging="stg_tickets", event="support_events", event_type="event_type",
    ),
    Domain(
        key="productanalytics",
        workflow_name="product_analytics_platform",
        owner_team="product-data-eng",
        owner_email="product-data-eng@metaweave.example",
        description="In-app event tracking, feature adoption and engagement pipelines.",
        schema="product", fact="user_events", id="event_id", actor="user_id", actor_dim="users",
        actor_name="user_name", amount="event_value", ts="event_ts", status="event_status",
        category="event_category", region="region", rate_join_col="currency_code",
        rate_table="reference.fx_rates", rate_multiplier="rate_to_usd", rate_label="Currency",
        staging="stg_user_events", event="user_events", event_type="event_name",
    ),
    Domain(
        key="iot",
        workflow_name="iot_telemetry_platform",
        owner_team="platform-data-eng",
        owner_email="platform-data-eng@metaweave.example",
        description="Sensor telemetry ingestion, calibration and device health pipelines.",
        schema="iot", fact="sensor_readings", id="reading_id", actor="device_id", actor_dim="devices",
        actor_name="device_name", amount="reading_value", ts="reading_ts", status="device_status",
        category="sensor_type", region="site", rate_join_col="sensor_type",
        rate_table="reference.calibration_factors", rate_multiplier="calibration_multiplier",
        rate_label="Calibration", staging="stg_sensor_readings", event="device_events", event_type="event_type",
    ),
]


# --- SQL pattern library ------------------------------------------------------
# Each function renders one SQL pattern against a domain's vocabulary. Grouped
# below into pipeline stages (ingest / quality / analytics / governance).

def _p_incremental_upsert(d: Domain) -> dict:
    return dict(
        title=f"Upsert incoming {d.fact} into {d.schema}.{d.fact}",
        category="Incremental Load",
        description=f"Merge newly landed rows from {d.schema}.{d.staging} into {d.schema}.{d.fact}, "
        "updating changed records and inserting new ones.",
        sql=f"""MERGE INTO {d.schema}.{d.fact} AS tgt
USING {d.schema}.{d.staging} AS src
  ON tgt.{d.id} = src.{d.id}
WHEN MATCHED AND src.{d.ts} > tgt.{d.ts} THEN
  UPDATE SET
    {d.amount} = src.{d.amount},
    {d.status} = src.{d.status},
    {d.ts} = src.{d.ts},
    updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
  INSERT ({d.id}, {d.actor}, {d.amount}, {d.status}, {d.ts}, updated_at)
  VALUES (src.{d.id}, src.{d.actor}, src.{d.amount}, src.{d.status}, src.{d.ts}, CURRENT_TIMESTAMP);""",
    )


def _p_late_arriving_merge(d: Domain) -> dict:
    return dict(
        title=f"Reconcile late-arriving {d.fact} rows",
        category="Incremental Load",
        description=f"Apply {d.schema}.{d.staging}_late_arrivals — rows that landed after the fact table's "
        "normal ingestion window — without double-counting already-loaded rows.",
        sql=f"""MERGE INTO {d.schema}.{d.fact} AS tgt
USING (
  SELECT * FROM {d.schema}.{d.staging}_late_arrivals
  WHERE {d.ts} < CURRENT_DATE - INTERVAL '1 day'
) AS src
  ON tgt.{d.id} = src.{d.id}
WHEN MATCHED THEN
  UPDATE SET
    {d.amount} = src.{d.amount},
    {d.status} = src.{d.status},
    updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
  INSERT ({d.id}, {d.actor}, {d.amount}, {d.status}, {d.ts}, updated_at)
  VALUES (src.{d.id}, src.{d.actor}, src.{d.amount}, src.{d.status}, src.{d.ts}, CURRENT_TIMESTAMP);""",
    )


def _p_scd2_merge(d: Domain) -> dict:
    return dict(
        title=f"SCD2 history merge for {d.schema}.{d.actor_dim}",
        category="Slowly Changing Dimension",
        description=f"Close out changed {d.actor_dim} rows and insert new current versions, "
        "preserving full change history with valid_from/valid_to.",
        sql=f"""UPDATE {d.schema}.{d.actor_dim}_history AS tgt
SET is_current = FALSE, valid_to = CURRENT_DATE
FROM {d.schema}.{d.staging}_{d.actor_dim} AS src
WHERE tgt.{d.actor} = src.{d.actor}
  AND tgt.is_current = TRUE
  AND (tgt.{d.actor_name} <> src.{d.actor_name} OR tgt.{d.region} <> src.{d.region});

INSERT INTO {d.schema}.{d.actor_dim}_history ({d.actor}, {d.actor_name}, {d.region}, valid_from, valid_to, is_current)
SELECT src.{d.actor}, src.{d.actor_name}, src.{d.region}, CURRENT_DATE, NULL, TRUE
FROM {d.schema}.{d.staging}_{d.actor_dim} src
LEFT JOIN {d.schema}.{d.actor_dim}_history cur
  ON cur.{d.actor} = src.{d.actor} AND cur.is_current = TRUE
WHERE cur.{d.actor} IS NULL;""",
    )


def _p_dedup_latest(d: Domain) -> dict:
    return dict(
        title=f"De-duplicate {d.schema}.{d.fact} to latest record per {d.id}",
        category="Deduplication",
        description=f"Collapse duplicate {d.id} rows (retries, replays, CDC re-sends) down to the most "
        "recently updated version of each record.",
        sql=f"""CREATE OR REPLACE TABLE {d.schema}.{d.fact}_deduped AS
SELECT *
FROM (
  SELECT
    f.*,
    ROW_NUMBER() OVER (PARTITION BY {d.id} ORDER BY {d.ts} DESC) AS rn
  FROM {d.schema}.{d.fact} f
) ranked
WHERE rn = 1;""",
    )


def _p_null_check(d: Domain) -> dict:
    return dict(
        title=f"Completeness check for {d.schema}.{d.fact}",
        category="Data Quality",
        description=f"Fail the pipeline's quality gate if key columns on {d.schema}.{d.fact} "
        "(id, amount, timestamp) are unexpectedly null.",
        sql=f"""INSERT INTO data_quality.check_results (check_name, schema_name, table_name, checked_at, failing_rows)
SELECT
  'null_check_{d.id}_{d.amount}' AS check_name,
  '{d.schema}' AS schema_name,
  '{d.fact}' AS table_name,
  CURRENT_TIMESTAMP AS checked_at,
  COUNT(*) AS failing_rows
FROM {d.schema}.{d.fact}
WHERE {d.id} IS NULL OR {d.amount} IS NULL OR {d.ts} IS NULL;""",
    )


def _p_referential_check(d: Domain) -> dict:
    return dict(
        title=f"Referential integrity check: {d.fact} -> {d.actor_dim}",
        category="Data Quality",
        description=f"Find {d.fact} rows whose {d.actor} has no matching row in {d.schema}.{d.actor_dim} "
        "— an orphaned foreign key that would break downstream joins.",
        sql=f"""SELECT f.{d.id}, f.{d.actor}
FROM {d.schema}.{d.fact} f
LEFT JOIN {d.schema}.{d.actor_dim} dim ON f.{d.actor} = dim.{d.actor}
WHERE dim.{d.actor} IS NULL;""",
    )


def _p_daily_rollup(d: Domain) -> dict:
    return dict(
        title=f"Daily {d.category} rollup for {d.schema}.{d.fact}",
        category="Aggregation Rollup",
        description=f"Summarize the previous day's {d.fact} activity by {d.category} for reporting dashboards.",
        sql=f"""INSERT INTO {d.schema}.{d.fact}_daily_summary ({d.ts}, {d.category}, total_{d.amount}, record_count)
SELECT
  DATE_TRUNC('day', {d.ts}) AS {d.ts},
  {d.category},
  SUM({d.amount}) AS total_{d.amount},
  COUNT(*) AS record_count
FROM {d.schema}.{d.fact}
WHERE {d.ts} >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY DATE_TRUNC('day', {d.ts}), {d.category};""",
    )


def _p_top_n(d: Domain) -> dict:
    return dict(
        title=f"Top 5 {d.actor_dim} by {d.amount} within each {d.category}",
        category="Ranking",
        description=f"Rank {d.actor_dim} within each {d.category} by total {d.amount} and keep the top 5.",
        sql=f"""SELECT {d.category}, {d.actor}, total_{d.amount}
FROM (
  SELECT
    {d.category},
    {d.actor},
    SUM({d.amount}) AS total_{d.amount},
    RANK() OVER (PARTITION BY {d.category} ORDER BY SUM({d.amount}) DESC) AS rnk
  FROM {d.schema}.{d.fact}
  GROUP BY {d.category}, {d.actor}
) ranked
WHERE rnk <= 5
ORDER BY {d.category}, rnk;""",
    )


def _p_funnel(d: Domain) -> dict:
    return dict(
        title=f"{d.event} funnel conversion",
        category="Funnel Analysis",
        description=f"Compute a 3-step conversion funnel over {d.schema}.{d.event} for the trailing 30 days.",
        sql=f"""WITH steps AS (
  SELECT
    {d.actor},
    MAX(CASE WHEN {d.event_type} = 'step_1_viewed' THEN 1 ELSE 0 END) AS step_1,
    MAX(CASE WHEN {d.event_type} = 'step_2_started' THEN 1 ELSE 0 END) AS step_2,
    MAX(CASE WHEN {d.event_type} = 'step_3_completed' THEN 1 ELSE 0 END) AS step_3
  FROM {d.schema}.{d.event}
  WHERE {d.ts} >= CURRENT_DATE - INTERVAL '30 day'
  GROUP BY {d.actor}
)
SELECT
  SUM(step_1) AS viewed,
  SUM(step_2) AS started,
  SUM(step_3) AS completed,
  ROUND(100.0 * SUM(step_3) / NULLIF(SUM(step_1), 0), 2) AS conversion_pct
FROM steps;""",
    )


def _p_cohort(d: Domain) -> dict:
    return dict(
        title=f"Monthly cohort retention for {d.schema}.{d.actor_dim}",
        category="Cohort Analysis",
        description=f"Group {d.actor_dim} by signup month and measure how many remain active in each "
        "subsequent month.",
        sql=f"""WITH first_activity AS (
  SELECT {d.actor}, DATE_TRUNC('month', MIN({d.ts})) AS cohort_month
  FROM {d.schema}.{d.fact}
  GROUP BY {d.actor}
),
monthly_activity AS (
  SELECT {d.actor}, DATE_TRUNC('month', {d.ts}) AS activity_month
  FROM {d.schema}.{d.fact}
  GROUP BY {d.actor}, DATE_TRUNC('month', {d.ts})
)
SELECT
  f.cohort_month,
  DATEDIFF('month', f.cohort_month, a.activity_month) AS months_since_start,
  COUNT(DISTINCT a.{d.actor}) AS active_count
FROM first_activity f
JOIN monthly_activity a ON a.{d.actor} = f.{d.actor}
GROUP BY f.cohort_month, months_since_start
ORDER BY f.cohort_month, months_since_start;""",
    )


def _p_gap_fill(d: Domain) -> dict:
    return dict(
        title=f"Fill missing days in {d.schema}.{d.fact}_daily_summary",
        category="Gap Filling",
        description="Generate a full date spine and left-join actuals so days with zero activity still "
        "show up as zero rather than being absent from the report.",
        sql=f"""WITH date_spine AS (
  SELECT generate_series(
    (SELECT MIN({d.ts}) FROM {d.schema}.{d.fact}),
    (SELECT MAX({d.ts}) FROM {d.schema}.{d.fact}),
    INTERVAL '1 day'
  )::date AS day
)
SELECT
  ds.day,
  COALESCE(SUM(f.{d.amount}), 0) AS total_{d.amount}
FROM date_spine ds
LEFT JOIN {d.schema}.{d.fact} f ON DATE_TRUNC('day', f.{d.ts}) = ds.day
GROUP BY ds.day
ORDER BY ds.day;""",
    )


def _p_rate_normalization(d: Domain) -> dict:
    return dict(
        title=f"{d.rate_label} normalization for {d.schema}.{d.fact}",
        category="Normalization",
        description=f"Join {d.rate_table} to convert raw {d.amount} into a normalized value comparable "
        "across every record regardless of source rate.",
        sql=f"""CREATE OR REPLACE TABLE {d.schema}.{d.fact}_normalized AS
SELECT
  f.*,
  f.{d.amount} * r.{d.rate_multiplier} AS {d.amount}_normalized
FROM {d.schema}.{d.fact} f
JOIN {d.rate_table} r ON r.{d.rate_join_col} = f.{d.rate_join_col};""",
    )


def _p_sessionize(d: Domain) -> dict:
    return dict(
        title=f"Sessionize {d.schema}.{d.event} by {d.actor}",
        category="Sessionization",
        description=f"Split each {d.actor}'s event stream into sessions, starting a new session whenever "
        "the gap since the previous event exceeds 30 minutes.",
        sql=f"""WITH ordered_events AS (
  SELECT
    {d.actor},
    {d.ts},
    {d.event_type},
    LAG({d.ts}) OVER (PARTITION BY {d.actor} ORDER BY {d.ts}) AS prev_ts
  FROM {d.schema}.{d.event}
),
session_flags AS (
  SELECT *,
    CASE WHEN prev_ts IS NULL OR {d.ts} - prev_ts > INTERVAL '30 minute' THEN 1 ELSE 0 END AS is_new_session
  FROM ordered_events
)
SELECT
  {d.actor},
  {d.ts},
  {d.event_type},
  SUM(is_new_session) OVER (PARTITION BY {d.actor} ORDER BY {d.ts}) AS session_id
FROM session_flags
ORDER BY {d.actor}, {d.ts};""",
    )


def _p_anomaly_zscore(d: Domain) -> dict:
    return dict(
        title=f"Z-score anomaly detection on {d.schema}.{d.fact}.{d.amount}",
        category="Anomaly Detection",
        description=f"Flag {d.fact} rows whose {d.amount} is more than 3 standard deviations from the "
        "90-day mean.",
        sql=f"""WITH stats AS (
  SELECT
    AVG({d.amount}) AS mean_{d.amount},
    STDDEV({d.amount}) AS stddev_{d.amount}
  FROM {d.schema}.{d.fact}
  WHERE {d.ts} >= CURRENT_DATE - INTERVAL '90 day'
)
SELECT
  f.{d.id},
  f.{d.actor},
  f.{d.amount},
  (f.{d.amount} - s.mean_{d.amount}) / NULLIF(s.stddev_{d.amount}, 0) AS z_score
FROM {d.schema}.{d.fact} f
CROSS JOIN stats s
WHERE ABS((f.{d.amount} - s.mean_{d.amount}) / NULLIF(s.stddev_{d.amount}, 0)) > 3
ORDER BY z_score DESC;""",
    )


def _p_gdpr_erasure(d: Domain) -> dict:
    return dict(
        title=f"Right-to-erasure delete for {d.schema}.{d.actor_dim}",
        category="Governance & Compliance",
        description=f"Purge {d.actor} data from both the fact and dimension tables for approved erasure "
        "requests, then mark the requests completed.",
        sql=f"""DELETE FROM {d.schema}.{d.fact}
WHERE {d.actor} IN (SELECT {d.actor} FROM governance.erasure_requests WHERE status = 'APPROVED');

DELETE FROM {d.schema}.{d.actor_dim}
WHERE {d.actor} IN (SELECT {d.actor} FROM governance.erasure_requests WHERE status = 'APPROVED');

UPDATE governance.erasure_requests
SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP
WHERE status = 'APPROVED';""",
    )


PatternFn = Callable[[Domain], dict]

# (pipeline stage, pattern) — order matters: it fixes task_order within a job
# and which task each dependency-wiring step below points at.
STAGE_PATTERNS: list[tuple[str, PatternFn]] = [
    ("ingest", _p_incremental_upsert),
    ("ingest", _p_late_arriving_merge),
    ("ingest", _p_scd2_merge),
    ("quality", _p_dedup_latest),
    ("quality", _p_null_check),
    ("quality", _p_referential_check),
    ("analytics", _p_daily_rollup),
    ("analytics", _p_top_n),
    ("analytics", _p_funnel),
    ("analytics", _p_cohort),
    ("analytics", _p_gap_fill),
    ("analytics", _p_rate_normalization),
    ("analytics", _p_sessionize),
    ("analytics", _p_anomaly_zscore),
    ("governance", _p_gdpr_erasure),
]

STAGE_ORDER = ["ingest", "quality", "analytics", "governance"]
STAGE_JOB_SUFFIX = {"ingest": "ingest", "quality": "quality_checks", "analytics": "analytics", "governance": "governance"}
STAGE_CRON = {
    "ingest": "0 * * * *",
    "quality": "15 * * * *",
    "analytics": "0 6 * * *",
    "governance": "0 3 * * 1",
}


def build_sql_examples() -> list[dict]:
    """Render every (domain, pattern) pair into a flat list of SQL examples."""
    examples = []
    for domain in DOMAINS:
        for stage, pattern_fn in STAGE_PATTERNS:
            example = pattern_fn(domain)
            example["domain_key"] = domain.key
            example["stage"] = stage
            examples.append(example)
    return examples


SQL_EXAMPLES: list[dict] = build_sql_examples()
assert len(SQL_EXAMPLES) >= 100, "seed dataset must contain at least 100 SQL examples"


def seed_demo_data(db: Session) -> int:
    """Populate an empty database with the full demo dataset.

    No-ops (returns 0) if any workflow already exists, so this is safe to
    call unconditionally on every app startup. Returns the number of SQL
    task rows created.
    """
    if db.execute(select(WorkflowDefinition.workflow_id).limit(1)).first() is not None:
        return 0

    now = dt.datetime.now(dt.timezone.utc)
    sql_task_count = 0

    for domain_idx, domain in enumerate(DOMAINS):
        workflow = WorkflowDefinition(
            name=domain.workflow_name,
            owner_team=domain.owner_team,
            owner_email=domain.owner_email,
            sla_minutes=60 + (domain_idx % 4) * 30,
            description=domain.description,
            tags={"domain": domain.key},
        )
        db.add(workflow)
        db.flush()

        jobs_by_stage: dict[str, JobDefinition] = {}
        for stage_idx, stage in enumerate(STAGE_ORDER):
            upstream_job = jobs_by_stage[STAGE_ORDER[stage_idx - 1]] if stage_idx > 0 else None
            job = JobDefinition(
                workflow_id=workflow.workflow_id,
                name=f"{domain.key}_{STAGE_JOB_SUFFIX[stage]}",
                cron_expression=STAGE_CRON[stage],
                max_retries=0 if stage == "governance" else 2,
                retry_delay_seconds=120,
                max_concurrency=2 if stage == "analytics" else 1,
                cluster_config={
                    "node_type": "i3.xlarge",
                    "workers": 2 + stage_idx,
                    "autoscale": stage == "analytics",
                },
                is_active=not (stage == "governance" and domain.key == "iot"),
            )
            db.add(job)
            db.flush()
            jobs_by_stage[stage] = job

            db.add(
                JobDag(
                    workflow_id=workflow.workflow_id,
                    upstream_job_id=upstream_job.job_id if upstream_job else None,
                    downstream_job_id=job.job_id,
                    external_trigger=None if upstream_job else {"type": "schedule", "cron": STAGE_CRON[stage]},
                )
            )

            for alert_type in (AlertType.SLA_BREACH, AlertType.FAILURE):
                db.add(
                    JobAlert(
                        job_id=job.job_id,
                        alert_type=alert_type,
                        distribution_list=[domain.owner_email],
                        routing_rule={"channel": "email", "severity": "high" if alert_type == AlertType.FAILURE else "medium"},
                    )
                )

            for run_idx in range(_RNG.randint(3, 6)):
                started = now - dt.timedelta(days=run_idx, hours=_RNG.randint(0, 5))
                status = _RNG.choices(
                    [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.RUNNING],
                    weights=[85, 10, 5],
                )[0]
                ended = None if status == RunStatus.RUNNING else started + dt.timedelta(minutes=_RNG.randint(2, 45))
                db.add(
                    JobAudit(
                        job_id=job.job_id,
                        run_id=f"run-{domain.key}-{stage}-{run_idx:03d}",
                        status=status,
                        started_at=started,
                        ended_at=ended,
                        rows_processed=_RNG.randint(500, 250_000) if status == RunStatus.SUCCESS else None,
                        cost_usd=round(_RNG.uniform(0.5, 40.0), 2),
                        error_message="Upstream table not ready" if status == RunStatus.FAILED else None,
                        logs_url=f"https://logs.metaweave.example/{domain.key}/{job.name}/{run_idx}",
                    )
                )

        stage_tasks: dict[str, list[TaskDefinition]] = {stage: [] for stage in STAGE_ORDER}
        task_order_per_stage = {stage: 0 for stage in STAGE_ORDER}
        for stage, pattern_fn in STAGE_PATTERNS:
            example = pattern_fn(domain)
            job = jobs_by_stage[stage]
            task = TaskDefinition(
                job_id=job.job_id,
                name=example["title"],
                task_order=task_order_per_stage[stage],
                exec_type=JobExecType.SQL,
                source_code=example["sql"],
                environment={"category": example["category"], "description": example["description"]},
            )
            db.add(task)
            db.flush()
            stage_tasks[stage].append(task)
            task_order_per_stage[stage] += 1
            sql_task_count += 1

        # Wire cross-stage dependencies so blast-radius/RCA has a real graph
        # to traverse: quality reads the raw fact table ingest just wrote,
        # analytics depends on the dedup task, governance depends on the
        # last analytics task.
        dataset_name = f"{domain.schema}.{domain.fact}"
        for task in stage_tasks["quality"]:
            db.add(JobDependency(task_id=task.task_id, depends_on_kind=DependencyKind.DATASET, depends_on_table=dataset_name))

        dedup_task = stage_tasks["quality"][0]
        for task in stage_tasks["analytics"]:
            db.add(JobDependency(task_id=task.task_id, depends_on_kind=DependencyKind.TASK, depends_on_task_id=dedup_task.task_id))

        last_analytics_task = stage_tasks["analytics"][-1]
        for task in stage_tasks["governance"]:
            db.add(JobDependency(task_id=task.task_id, depends_on_kind=DependencyKind.TASK, depends_on_task_id=last_analytics_task.task_id))

    # The complex bank finance/treasury library (multi-CTE, multi-join
    # statements over deposits, loans, FTP, allocations, P&L, RWA, ALM and
    # regulatory reporting) shares the same tables and endpoints.
    from app.seed_banking import seed_banking_data

    sql_task_count += seed_banking_data(db)

    db.commit()
    return sql_task_count
