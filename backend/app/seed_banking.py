"""Complex bank finance/treasury SQL examples for the demo dataset.

Unlike the pattern library in ``seed_data.py`` (one idiom per example), these
mirror the shape of real bank warehouse SQL: multi-CTE statements joining
positions, dimensions, reference curves and GL together to produce deposits,
loans, funds-transfer-priced net interest income, expense pools, driver-based
and reciprocal cost allocations, full P&L waterfalls, RWA/capital, ALM and
regulatory reporting.

Every statement is real, parseable SQL (postgres dialect) and is wired to a
workflow/job/task so it shows up in the SQL library, the lineage parser and
the blast-radius graph with genuine multi-table lineage.

Schema conventions used throughout:

    core   core banking source (deposit/loan accounts, balances, payments)
    gl     general ledger postings and journal entries
    dim    conformed dimensions (customer, product, branch, cost centre, org)
    ref    reference//rate data (FTP curves, risk weights, PD/LGD, drivers)
    fact   daily position facts (deposit/loan balances, FTP, fees, expense)
    finance NII, revenue, expense pools, allocations, P&L
    risk   ECL staging/provision, delinquency, RWA, economic capital
    alm    repricing gap, liquidity, behavioural cashflows
    mart   product/branch/customer/segment profitability marts
    reg    regulatory report staging
    ctl    batch control (as-of date for the run)
"""
from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass

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

_RNG = random.Random(19970218)


@dataclass(frozen=True)
class BankingWorkflow:
    key: str
    name: str
    owner_team: str
    owner_email: str
    description: str
    sla_minutes: int
    jobs: tuple[str, ...]


BANKING_WORKFLOWS: list[BankingWorkflow] = [
    BankingWorkflow(
        key="deposits",
        name="retail_deposits_platform",
        owner_team="deposits-finance-eng",
        owner_email="deposits-finance-eng@metaweave.example",
        description="Deposit positions, interest expense, FTP credit and balance behaviour analytics.",
        sla_minutes=90,
        jobs=("deposits_positions", "deposits_pricing", "deposits_analytics"),
    ),
    BankingWorkflow(
        key="lending",
        name="lending_credit_platform",
        owner_team="lending-finance-eng",
        owner_email="lending-finance-eng@metaweave.example",
        description="Loan positions, interest accrual, delinquency, IFRS9/CECL provisioning and FTP charge.",
        sla_minutes=120,
        jobs=("loans_positions", "loans_credit_risk", "loans_analytics"),
    ),
    BankingWorkflow(
        key="profitability",
        name="profitability_analytics_platform",
        owner_team="fpna-data-eng",
        owner_email="fpna-data-eng@metaweave.example",
        description="Net interest income, fee revenue, expense pools, cost allocations and the P&L close.",
        sla_minutes=180,
        jobs=("revenue_assembly", "expense_management", "cost_allocation", "pnl_close"),
    ),
    BankingWorkflow(
        key="riskcapital",
        name="risk_capital_platform",
        owner_team="risk-analytics-eng",
        owner_email="risk-analytics-eng@metaweave.example",
        description="Risk-weighted assets, economic capital, RAROC and stress-test projections.",
        sla_minutes=150,
        jobs=("rwa_capital", "stress_testing"),
    ),
    BankingWorkflow(
        key="treasury",
        name="treasury_alm_platform",
        owner_team="treasury-alm-eng",
        owner_email="treasury-alm-eng@metaweave.example",
        description="Repricing gap, liquidity coverage, behavioural cashflows and FTP curve construction.",
        sla_minutes=120,
        jobs=("alm_positions", "ftp_curves"),
    ),
    BankingWorkflow(
        key="regulatory",
        name="regulatory_reporting_platform",
        owner_team="regulatory-reporting-eng",
        owner_email="regulatory-reporting-eng@metaweave.example",
        description="Call Report, FR Y-9C, IFRS9 disclosure and Basel leverage submissions.",
        sla_minutes=240,
        jobs=("regulatory_filings",),
    ),
]


# ---------------------------------------------------------------------------
# Examples. Each entry: workflow key, job, category, title, description, sql.
# ---------------------------------------------------------------------------
BANKING_EXAMPLES: list[dict] = [
    dict(
        workflow="deposits", job="deposits_positions", category="Deposits",
        title="Build daily deposit position fact with ADB and core/volatile split",
        description=(
            "Joins open deposit accounts to their balance history, rate plan and conformed "
            "dimensions to produce the daily deposit position fact, including 30-day average "
            "daily balance and the core vs volatile balance split used by ALM."
        ),
        sql="""INSERT INTO fact.deposit_balance_daily (
  as_of_date, account_sk, customer_sk, product_sk, branch_sk, currency_code,
  eod_balance, average_daily_balance, core_balance, volatile_balance, contractual_rate
)
WITH run_date AS (
  SELECT as_of_date FROM ctl.batch_control WHERE process_name = 'deposits_positions'
),
open_accounts AS (
  SELECT
    a.account_id, a.customer_id, a.product_id, a.branch_id,
    a.currency_code, a.rate_plan_id, a.open_date
  FROM core.deposit_account a
  CROSS JOIN run_date r
  WHERE a.open_date <= r.as_of_date
    AND (a.close_date IS NULL OR a.close_date > r.as_of_date)
),
balance_window AS (
  SELECT
    h.account_id,
    h.as_of_date,
    h.ledger_balance,
    AVG(h.ledger_balance) OVER (
      PARTITION BY h.account_id ORDER BY h.as_of_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_balance_30d,
    MIN(h.ledger_balance) OVER (
      PARTITION BY h.account_id ORDER BY h.as_of_date
      ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS trough_balance_90d
  FROM core.deposit_balance_history h
  CROSS JOIN run_date r
  WHERE h.as_of_date BETWEEN r.as_of_date - INTERVAL '90 day' AND r.as_of_date
),
current_balance AS (
  SELECT b.account_id, b.as_of_date, b.ledger_balance, b.avg_balance_30d, b.trough_balance_90d
  FROM balance_window b
  JOIN run_date r ON r.as_of_date = b.as_of_date
)
SELECT
  c.as_of_date,
  da.account_sk,
  dc.customer_sk,
  dp.product_sk,
  db.branch_sk,
  oa.currency_code,
  c.ledger_balance,
  c.avg_balance_30d,
  LEAST(c.trough_balance_90d, c.ledger_balance) AS core_balance,
  GREATEST(c.ledger_balance - c.trough_balance_90d, 0) AS volatile_balance,
  rp.contractual_rate
FROM current_balance c
JOIN open_accounts oa ON oa.account_id = c.account_id
JOIN dim.dim_account da ON da.account_id = oa.account_id AND da.is_current = TRUE
JOIN dim.dim_customer dc ON dc.customer_id = oa.customer_id AND dc.is_current = TRUE
JOIN dim.dim_product dp ON dp.product_id = oa.product_id AND dp.is_current = TRUE
JOIN dim.dim_branch db ON db.branch_id = oa.branch_id AND db.is_current = TRUE
JOIN ref.rate_plan rp ON rp.rate_plan_id = oa.rate_plan_id
 AND c.as_of_date BETWEEN rp.effective_from AND COALESCE(rp.effective_to, DATE '9999-12-31');""",
    ),
    dict(
        workflow="deposits", job="deposits_positions", category="Deposits",
        title="Accrue tiered deposit interest expense by balance band",
        description=(
            "Prices each account's average daily balance against its tiered rate schedule, "
            "accruing interest expense band by band rather than at a single blended rate."
        ),
        sql="""INSERT INTO fact.deposit_interest_expense (
  as_of_date, account_sk, product_sk, tier_id, tier_balance, tier_rate, interest_expense
)
WITH positions AS (
  SELECT
    f.as_of_date, f.account_sk, f.product_sk, f.average_daily_balance, f.contractual_rate,
    da.rate_plan_id
  FROM fact.deposit_balance_daily f
  JOIN dim.dim_account da ON da.account_sk = f.account_sk
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
tier_bands AS (
  SELECT
    t.rate_plan_id, t.tier_id, t.lower_bound, t.upper_bound, t.tier_rate
  FROM ref.rate_plan_tier t
  WHERE CURRENT_DATE BETWEEN t.effective_from AND COALESCE(t.effective_to, DATE '9999-12-31')
),
banded AS (
  SELECT
    p.as_of_date,
    p.account_sk,
    p.product_sk,
    tb.tier_id,
    tb.tier_rate,
    GREATEST(
      LEAST(p.average_daily_balance, tb.upper_bound) - tb.lower_bound,
      0
    ) AS tier_balance
  FROM positions p
  JOIN tier_bands tb ON tb.rate_plan_id = p.rate_plan_id
)
SELECT
  b.as_of_date,
  b.account_sk,
  b.product_sk,
  b.tier_id,
  b.tier_balance,
  b.tier_rate,
  b.tier_balance * b.tier_rate / dc.day_count_basis AS interest_expense
FROM banded b
JOIN dim.dim_product dp ON dp.product_sk = b.product_sk
JOIN ref.day_count_convention dc ON dc.convention_code = dp.day_count_convention
WHERE b.tier_balance > 0;""",
    ),
    dict(
        workflow="deposits", job="deposits_pricing", category="Funds Transfer Pricing",
        title="Assign FTP credit to deposits from the transfer pricing curve",
        description=(
            "Matches each deposit account to the FTP curve point for its behavioural "
            "repricing tenor, producing the funding credit the deposit business earns for "
            "supplying funds to the balance sheet."
        ),
        sql="""INSERT INTO fact.ftp_assignment (
  as_of_date, account_sk, instrument_type, assigned_tenor_months, ftp_rate, ftp_amount, curve_id
)
WITH deposits AS (
  SELECT
    f.as_of_date, f.account_sk, f.product_sk, f.currency_code, f.average_daily_balance
  FROM fact.deposit_balance_daily f
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
behavioural_tenor AS (
  SELECT
    d.account_sk,
    d.currency_code,
    d.average_daily_balance,
    d.as_of_date,
    COALESCE(bl.behavioural_life_months, dp.contractual_tenor_months) AS assigned_tenor_months,
    dp.ftp_curve_id
  FROM deposits d
  JOIN dim.dim_product dp ON dp.product_sk = d.product_sk
  LEFT JOIN alm.behavioural_life bl
    ON bl.product_sk = d.product_sk
   AND bl.as_of_month = DATE_TRUNC('month', d.as_of_date)
),
curve_points AS (
  SELECT c.curve_id, c.currency_code, c.tenor_months, c.zero_rate, c.as_of_date
  FROM ref.ftp_curve c
  WHERE c.as_of_date = CURRENT_DATE - INTERVAL '1 day'
)
SELECT
  bt.as_of_date,
  bt.account_sk,
  'DEPOSIT' AS instrument_type,
  bt.assigned_tenor_months,
  cp.zero_rate AS ftp_rate,
  bt.average_daily_balance * cp.zero_rate / 365.0 AS ftp_amount,
  cp.curve_id
FROM behavioural_tenor bt
JOIN curve_points cp
  ON cp.curve_id = bt.ftp_curve_id
 AND cp.currency_code = bt.currency_code
 AND cp.tenor_months = bt.assigned_tenor_months;""",
    ),
    dict(
        workflow="deposits", job="deposits_analytics", category="Deposits",
        title="Roll deposit balances to customer relationship level",
        description=(
            "Aggregates account balances up the customer hierarchy so a corporate group's "
            "total deposit relationship — across subsidiaries and products — is visible in one row."
        ),
        sql="""INSERT INTO mart.customer_deposit_relationship (
  as_of_date, ultimate_parent_id, segment_code, account_count, total_balance,
  weighted_avg_rate, demand_balance, term_balance
)
WITH balances AS (
  SELECT
    f.as_of_date, f.account_sk, f.customer_sk, f.product_sk,
    f.average_daily_balance, f.contractual_rate
  FROM fact.deposit_balance_daily f
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
customer_tree AS (
  SELECT
    dc.customer_sk,
    dc.customer_id,
    COALESCE(h.ultimate_parent_id, dc.customer_id) AS ultimate_parent_id,
    dc.segment_code
  FROM dim.dim_customer dc
  LEFT JOIN dim.customer_hierarchy h ON h.customer_id = dc.customer_id
  WHERE dc.is_current = TRUE
)
SELECT
  b.as_of_date,
  ct.ultimate_parent_id,
  ct.segment_code,
  COUNT(DISTINCT b.account_sk) AS account_count,
  SUM(b.average_daily_balance) AS total_balance,
  SUM(b.average_daily_balance * b.contractual_rate)
    / NULLIF(SUM(b.average_daily_balance), 0) AS weighted_avg_rate,
  SUM(CASE WHEN dp.maturity_type = 'DEMAND' THEN b.average_daily_balance ELSE 0 END) AS demand_balance,
  SUM(CASE WHEN dp.maturity_type = 'TERM' THEN b.average_daily_balance ELSE 0 END) AS term_balance
FROM balances b
JOIN customer_tree ct ON ct.customer_sk = b.customer_sk
JOIN dim.dim_product dp ON dp.product_sk = b.product_sk
GROUP BY b.as_of_date, ct.ultimate_parent_id, ct.segment_code;""",
    ),
    dict(
        workflow="deposits", job="deposits_analytics", category="Deposits",
        title="Deposit attrition and balance runoff analysis",
        description=(
            "Compares each account's current balance to its trailing average to classify "
            "accounts as growing, stable, attriting or closed, for retention targeting."
        ),
        sql="""INSERT INTO mart.deposit_attrition (
  as_of_month, customer_sk, product_sk, opening_balance, closing_balance,
  net_change, runoff_pct, attrition_flag
)
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', f.as_of_date) AS as_of_month,
    f.customer_sk,
    f.product_sk,
    f.account_sk,
    FIRST_VALUE(f.eod_balance) OVER (
      PARTITION BY f.account_sk, DATE_TRUNC('month', f.as_of_date)
      ORDER BY f.as_of_date
    ) AS opening_balance,
    LAST_VALUE(f.eod_balance) OVER (
      PARTITION BY f.account_sk, DATE_TRUNC('month', f.as_of_date)
      ORDER BY f.as_of_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS closing_balance
  FROM fact.deposit_balance_daily f
  WHERE f.as_of_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 month'
),
deduped AS (
  SELECT DISTINCT m.as_of_month, m.customer_sk, m.product_sk, m.account_sk,
         m.opening_balance, m.closing_balance
  FROM monthly m
),
aggregated AS (
  SELECT
    d.as_of_month,
    d.customer_sk,
    d.product_sk,
    SUM(d.opening_balance) AS opening_balance,
    SUM(d.closing_balance) AS closing_balance
  FROM deduped d
  GROUP BY d.as_of_month, d.customer_sk, d.product_sk
)
SELECT
  a.as_of_month,
  a.customer_sk,
  a.product_sk,
  a.opening_balance,
  a.closing_balance,
  a.closing_balance - a.opening_balance AS net_change,
  (a.opening_balance - a.closing_balance) / NULLIF(a.opening_balance, 0) AS runoff_pct,
  CASE
    WHEN a.closing_balance = 0 THEN 'CLOSED'
    WHEN (a.opening_balance - a.closing_balance) / NULLIF(a.opening_balance, 0) > 0.5 THEN 'HIGH_ATTRITION'
    WHEN (a.opening_balance - a.closing_balance) / NULLIF(a.opening_balance, 0) > 0.2 THEN 'AT_RISK'
    ELSE 'STABLE'
  END AS attrition_flag
FROM aggregated a
JOIN dim.dim_customer dc ON dc.customer_sk = a.customer_sk
WHERE dc.is_current = TRUE;""",
    ),
    dict(
        workflow="deposits", job="deposits_analytics", category="Deposits",
        title="Large depositor concentration report",
        description=(
            "Ranks depositors by total balance and computes the cumulative share held by the "
            "top relationships — the concentration metric liquidity risk committees monitor."
        ),
        sql="""INSERT INTO risk.deposit_concentration (
  as_of_date, ultimate_parent_id, segment_code, total_balance,
  balance_rank, pct_of_total, cumulative_pct, is_top_20
)
WITH relationship_balances AS (
  SELECT
    f.as_of_date,
    COALESCE(h.ultimate_parent_id, dc.customer_id) AS ultimate_parent_id,
    dc.segment_code,
    SUM(f.average_daily_balance) AS total_balance
  FROM fact.deposit_balance_daily f
  JOIN dim.dim_customer dc ON dc.customer_sk = f.customer_sk AND dc.is_current = TRUE
  LEFT JOIN dim.customer_hierarchy h ON h.customer_id = dc.customer_id
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY f.as_of_date, COALESCE(h.ultimate_parent_id, dc.customer_id), dc.segment_code
),
book_total AS (
  SELECT rb.as_of_date, SUM(rb.total_balance) AS book_balance
  FROM relationship_balances rb
  GROUP BY rb.as_of_date
),
ranked AS (
  SELECT
    rb.as_of_date,
    rb.ultimate_parent_id,
    rb.segment_code,
    rb.total_balance,
    bt.book_balance,
    ROW_NUMBER() OVER (ORDER BY rb.total_balance DESC) AS balance_rank,
    SUM(rb.total_balance) OVER (ORDER BY rb.total_balance DESC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
  FROM relationship_balances rb
  JOIN book_total bt ON bt.as_of_date = rb.as_of_date
)
SELECT
  r.as_of_date,
  r.ultimate_parent_id,
  r.segment_code,
  r.total_balance,
  r.balance_rank,
  r.total_balance / NULLIF(r.book_balance, 0) AS pct_of_total,
  r.running_balance / NULLIF(r.book_balance, 0) AS cumulative_pct,
  r.balance_rank <= 20 AS is_top_20
FROM ranked r;""",
    ),
    dict(
        workflow="deposits", job="deposits_pricing", category="Deposits",
        title="Deposit rate-paid versus market benchmark beta analysis",
        description=(
            "Joins paid rates to the benchmark policy rate history to measure deposit beta — "
            "how much of each policy rate move the bank passed through to depositors."
        ),
        sql="""INSERT INTO mart.deposit_beta (
  as_of_month, product_sk, avg_rate_paid, benchmark_rate,
  rate_delta, benchmark_delta, deposit_beta
)
WITH monthly_rates AS (
  SELECT
    DATE_TRUNC('month', f.as_of_date) AS as_of_month,
    f.product_sk,
    SUM(f.average_daily_balance * f.contractual_rate)
      / NULLIF(SUM(f.average_daily_balance), 0) AS avg_rate_paid
  FROM fact.deposit_balance_daily f
  WHERE f.as_of_date >= CURRENT_DATE - INTERVAL '24 month'
  GROUP BY DATE_TRUNC('month', f.as_of_date), f.product_sk
),
benchmark AS (
  SELECT
    DATE_TRUNC('month', b.rate_date) AS as_of_month,
    AVG(b.policy_rate) AS benchmark_rate
  FROM ref.benchmark_rate b
  WHERE b.benchmark_code = 'POLICY_TARGET'
    AND b.rate_date >= CURRENT_DATE - INTERVAL '24 month'
  GROUP BY DATE_TRUNC('month', b.rate_date)
),
joined AS (
  SELECT
    mr.as_of_month,
    mr.product_sk,
    mr.avg_rate_paid,
    bm.benchmark_rate,
    LAG(mr.avg_rate_paid) OVER (PARTITION BY mr.product_sk ORDER BY mr.as_of_month) AS prior_rate_paid,
    LAG(bm.benchmark_rate) OVER (PARTITION BY mr.product_sk ORDER BY mr.as_of_month) AS prior_benchmark
  FROM monthly_rates mr
  JOIN benchmark bm ON bm.as_of_month = mr.as_of_month
)
SELECT
  j.as_of_month,
  j.product_sk,
  j.avg_rate_paid,
  j.benchmark_rate,
  j.avg_rate_paid - j.prior_rate_paid AS rate_delta,
  j.benchmark_rate - j.prior_benchmark AS benchmark_delta,
  (j.avg_rate_paid - j.prior_rate_paid)
    / NULLIF(j.benchmark_rate - j.prior_benchmark, 0) AS deposit_beta
FROM joined j
JOIN dim.dim_product dp ON dp.product_sk = j.product_sk
WHERE j.prior_rate_paid IS NOT NULL;""",
    ),
]


BANKING_EXAMPLES += [
    dict(
        workflow="lending", job="loans_positions", category="Loans",
        title="Build daily loan position fact with accrual status and amortisation",
        description=(
            "Assembles the daily loan fact from the servicing system: outstanding principal, "
            "accrued interest, next repricing date and accrual/non-accrual status derived from "
            "days past due."
        ),
        sql="""INSERT INTO fact.loan_balance_daily (
  as_of_date, account_sk, customer_sk, product_sk, branch_sk, currency_code,
  outstanding_principal, average_daily_balance, accrued_interest, days_past_due,
  accrual_status, next_reprice_date, contractual_rate
)
WITH run_date AS (
  SELECT as_of_date FROM ctl.batch_control WHERE process_name = 'loans_positions'
),
active_loans AS (
  SELECT
    l.loan_id, l.customer_id, l.product_id, l.branch_id, l.currency_code,
    l.origination_date, l.maturity_date, l.contractual_rate, l.reprice_frequency_months,
    l.last_reprice_date
  FROM core.loan_account l
  CROSS JOIN run_date r
  WHERE l.origination_date <= r.as_of_date
    AND (l.closed_date IS NULL OR l.closed_date > r.as_of_date)
),
balances AS (
  SELECT
    b.loan_id,
    b.as_of_date,
    b.outstanding_principal,
    b.accrued_interest,
    AVG(b.outstanding_principal) OVER (
      PARTITION BY b.loan_id ORDER BY b.as_of_date
      ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS avg_balance_30d
  FROM core.loan_balance_history b
  CROSS JOIN run_date r
  WHERE b.as_of_date BETWEEN r.as_of_date - INTERVAL '30 day' AND r.as_of_date
),
arrears AS (
  SELECT
    p.loan_id,
    MAX(CASE WHEN p.paid_date IS NULL THEN DATE_PART('day', r.as_of_date - p.due_date) ELSE 0 END) AS days_past_due
  FROM core.loan_payment_schedule p
  CROSS JOIN run_date r
  WHERE p.due_date <= r.as_of_date
  GROUP BY p.loan_id
)
SELECT
  b.as_of_date,
  da.account_sk,
  dc.customer_sk,
  dp.product_sk,
  db.branch_sk,
  al.currency_code,
  b.outstanding_principal,
  b.avg_balance_30d,
  b.accrued_interest,
  COALESCE(ar.days_past_due, 0) AS days_past_due,
  CASE WHEN COALESCE(ar.days_past_due, 0) >= 90 THEN 'NONACCRUAL' ELSE 'ACCRUING' END AS accrual_status,
  al.last_reprice_date + (al.reprice_frequency_months || ' month')::INTERVAL AS next_reprice_date,
  al.contractual_rate
FROM balances b
JOIN run_date r ON r.as_of_date = b.as_of_date
JOIN active_loans al ON al.loan_id = b.loan_id
LEFT JOIN arrears ar ON ar.loan_id = b.loan_id
JOIN dim.dim_account da ON da.account_id = al.loan_id AND da.is_current = TRUE
JOIN dim.dim_customer dc ON dc.customer_id = al.customer_id AND dc.is_current = TRUE
JOIN dim.dim_product dp ON dp.product_id = al.product_id AND dp.is_current = TRUE
JOIN dim.dim_branch db ON db.branch_id = al.branch_id AND db.is_current = TRUE;""",
    ),
    dict(
        workflow="lending", job="loans_positions", category="Loans",
        title="Accrue loan interest income with non-accrual suppression",
        description=(
            "Accrues interest income only on performing balances, reversing accrual on loans "
            "that crossed into non-accrual status during the period."
        ),
        sql="""INSERT INTO fact.loan_interest_income (
  as_of_date, account_sk, product_sk, accruable_balance, effective_rate,
  interest_income, reversal_amount
)
WITH positions AS (
  SELECT
    f.as_of_date, f.account_sk, f.product_sk, f.average_daily_balance,
    f.contractual_rate, f.accrual_status, f.accrued_interest,
    LAG(f.accrual_status) OVER (PARTITION BY f.account_sk ORDER BY f.as_of_date) AS prior_status
  FROM fact.loan_balance_daily f
  WHERE f.as_of_date >= CURRENT_DATE - INTERVAL '2 day'
),
current_day AS (
  SELECT p.* FROM positions p WHERE p.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
rate_spreads AS (
  SELECT
    s.product_id, s.index_code, s.spread_bps
  FROM ref.product_rate_spread s
  WHERE CURRENT_DATE BETWEEN s.effective_from AND COALESCE(s.effective_to, DATE '9999-12-31')
)
SELECT
  cd.as_of_date,
  cd.account_sk,
  cd.product_sk,
  CASE WHEN cd.accrual_status = 'ACCRUING' THEN cd.average_daily_balance ELSE 0 END AS accruable_balance,
  COALESCE(idx.index_rate + rs.spread_bps / 10000.0, cd.contractual_rate) AS effective_rate,
  CASE WHEN cd.accrual_status = 'ACCRUING'
       THEN cd.average_daily_balance
            * COALESCE(idx.index_rate + rs.spread_bps / 10000.0, cd.contractual_rate)
            / dcc.day_count_basis
       ELSE 0 END AS interest_income,
  CASE WHEN cd.accrual_status = 'NONACCRUAL' AND cd.prior_status = 'ACCRUING'
       THEN cd.accrued_interest ELSE 0 END AS reversal_amount
FROM current_day cd
JOIN dim.dim_product dp ON dp.product_sk = cd.product_sk
LEFT JOIN rate_spreads rs ON rs.product_id = dp.product_id
LEFT JOIN ref.index_rate idx
  ON idx.index_code = rs.index_code AND idx.rate_date = cd.as_of_date
JOIN ref.day_count_convention dcc ON dcc.convention_code = dp.day_count_convention;""",
    ),
    dict(
        workflow="lending", job="loans_credit_risk", category="Credit Risk",
        title="Delinquency aging buckets with roll-rate transitions",
        description=(
            "Buckets the loan book by days past due and compares each account's bucket to last "
            "month's to produce the roll-rate matrix credit committees use to forecast losses."
        ),
        sql="""INSERT INTO risk.delinquency_roll_rate (
  as_of_month, product_sk, prior_bucket, current_bucket, account_count,
  balance_amount, roll_rate
)
WITH bucketed AS (
  SELECT
    DATE_TRUNC('month', f.as_of_date) AS as_of_month,
    f.account_sk,
    f.product_sk,
    f.outstanding_principal,
    CASE
      WHEN f.days_past_due = 0 THEN 'CURRENT'
      WHEN f.days_past_due BETWEEN 1 AND 29 THEN 'DPD_1_29'
      WHEN f.days_past_due BETWEEN 30 AND 59 THEN 'DPD_30_59'
      WHEN f.days_past_due BETWEEN 60 AND 89 THEN 'DPD_60_89'
      ELSE 'DPD_90_PLUS'
    END AS bucket
  FROM fact.loan_balance_daily f
  WHERE f.as_of_date = (DATE_TRUNC('month', f.as_of_date) + INTERVAL '1 month' - INTERVAL '1 day')::date
    AND f.as_of_date >= CURRENT_DATE - INTERVAL '13 month'
),
transitions AS (
  SELECT
    b.as_of_month,
    b.account_sk,
    b.product_sk,
    b.outstanding_principal,
    b.bucket AS current_bucket,
    LAG(b.bucket) OVER (PARTITION BY b.account_sk ORDER BY b.as_of_month) AS prior_bucket
  FROM bucketed b
),
prior_totals AS (
  SELECT
    t.as_of_month, t.product_sk, t.prior_bucket,
    COUNT(*) AS prior_account_count
  FROM transitions t
  WHERE t.prior_bucket IS NOT NULL
  GROUP BY t.as_of_month, t.product_sk, t.prior_bucket
)
SELECT
  t.as_of_month,
  t.product_sk,
  t.prior_bucket,
  t.current_bucket,
  COUNT(*) AS account_count,
  SUM(t.outstanding_principal) AS balance_amount,
  COUNT(*)::numeric / NULLIF(pt.prior_account_count, 0) AS roll_rate
FROM transitions t
JOIN prior_totals pt
  ON pt.as_of_month = t.as_of_month
 AND pt.product_sk = t.product_sk
 AND pt.prior_bucket = t.prior_bucket
JOIN dim.dim_product dp ON dp.product_sk = t.product_sk
WHERE t.prior_bucket IS NOT NULL
GROUP BY t.as_of_month, t.product_sk, t.prior_bucket, t.current_bucket, pt.prior_account_count;""",
    ),
    dict(
        workflow="lending", job="loans_credit_risk", category="Credit Risk",
        title="IFRS 9 staging with significant-increase-in-credit-risk test",
        description=(
            "Assigns each exposure to IFRS 9 stage 1, 2 or 3 by comparing current lifetime PD "
            "against origination PD (the SICR test), overlaid with days-past-due backstops and "
            "watchlist flags."
        ),
        sql="""INSERT INTO risk.ecl_staging (
  as_of_date, account_sk, customer_sk, origination_pd, current_pd, pd_ratio,
  days_past_due, ifrs9_stage, stage_trigger
)
WITH exposures AS (
  SELECT
    f.as_of_date, f.account_sk, f.customer_sk, f.product_sk,
    f.outstanding_principal, f.days_past_due, f.accrual_status
  FROM fact.loan_balance_daily f
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
origination_pd AS (
  SELECT
    o.account_sk,
    o.lifetime_pd AS origination_pd
  FROM risk.origination_risk_profile o
),
current_pd AS (
  SELECT
    s.account_sk,
    s.lifetime_pd AS current_pd,
    s.rating_grade
  FROM risk.pd_scorecard_result s
  WHERE s.score_date = CURRENT_DATE - INTERVAL '1 day'
),
watchlist AS (
  SELECT w.customer_id, w.watch_reason
  FROM risk.credit_watchlist w
  WHERE w.is_active = TRUE
)
SELECT
  e.as_of_date,
  e.account_sk,
  e.customer_sk,
  op.origination_pd,
  cp.current_pd,
  cp.current_pd / NULLIF(op.origination_pd, 0) AS pd_ratio,
  e.days_past_due,
  CASE
    WHEN e.days_past_due >= 90 OR e.accrual_status = 'NONACCRUAL' THEN 3
    WHEN e.days_past_due >= 30 THEN 2
    WHEN cp.current_pd / NULLIF(op.origination_pd, 0) >= 2.0 THEN 2
    WHEN wl.customer_id IS NOT NULL THEN 2
    ELSE 1
  END AS ifrs9_stage,
  CASE
    WHEN e.days_past_due >= 90 OR e.accrual_status = 'NONACCRUAL' THEN 'DEFAULT'
    WHEN e.days_past_due >= 30 THEN 'DPD_BACKSTOP'
    WHEN cp.current_pd / NULLIF(op.origination_pd, 0) >= 2.0 THEN 'SICR_PD_DOUBLING'
    WHEN wl.customer_id IS NOT NULL THEN 'WATCHLIST'
    ELSE 'PERFORMING'
  END AS stage_trigger
FROM exposures e
JOIN dim.dim_customer dc ON dc.customer_sk = e.customer_sk AND dc.is_current = TRUE
LEFT JOIN origination_pd op ON op.account_sk = e.account_sk
LEFT JOIN current_pd cp ON cp.account_sk = e.account_sk
LEFT JOIN watchlist wl ON wl.customer_id = dc.customer_id;""",
    ),
    dict(
        workflow="lending", job="loans_credit_risk", category="Credit Risk",
        title="Expected credit loss provision from PD, LGD and EAD",
        description=(
            "Computes discounted expected credit loss per exposure — 12-month ECL for stage 1, "
            "lifetime ECL for stages 2 and 3 — from the PD term structure, collateral-adjusted "
            "LGD and exposure at default."
        ),
        sql="""INSERT INTO risk.ecl_provision (
  as_of_date, account_sk, ifrs9_stage, exposure_at_default, lgd_rate,
  horizon_months, undiscounted_ecl, discounted_ecl
)
WITH staged AS (
  SELECT
    s.as_of_date, s.account_sk, s.customer_sk, s.ifrs9_stage
  FROM risk.ecl_staging s
  WHERE s.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
exposure AS (
  SELECT
    f.account_sk,
    f.product_sk,
    f.outstanding_principal,
    f.accrued_interest,
    f.contractual_rate,
    f.outstanding_principal + f.accrued_interest
      + COALESCE(u.undrawn_amount * ref_ccf.credit_conversion_factor, 0) AS exposure_at_default
  FROM fact.loan_balance_daily f
  LEFT JOIN core.loan_undrawn_commitment u ON u.account_sk = f.account_sk
  LEFT JOIN ref.credit_conversion_factor ref_ccf ON ref_ccf.facility_type = u.facility_type
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
collateral_lgd AS (
  SELECT
    c.account_sk,
    GREATEST(
      base.base_lgd * (1 - COALESCE(SUM(c.appraised_value * c.haircut_pct) / NULLIF(MAX(e.exposure_at_default), 0), 0)),
      base.floor_lgd
    ) AS lgd_rate
  FROM core.loan_collateral c
  JOIN exposure e ON e.account_sk = c.account_sk
  JOIN ref.lgd_baseline base ON base.product_sk = e.product_sk
  GROUP BY c.account_sk, base.base_lgd, base.floor_lgd
),
pd_curve AS (
  SELECT p.account_sk, p.horizon_months, p.marginal_pd
  FROM risk.pd_term_structure p
  WHERE p.score_date = CURRENT_DATE - INTERVAL '1 day'
)
SELECT
  st.as_of_date,
  st.account_sk,
  st.ifrs9_stage,
  e.exposure_at_default,
  COALESCE(cl.lgd_rate, lb.base_lgd) AS lgd_rate,
  pc.horizon_months,
  e.exposure_at_default * pc.marginal_pd * COALESCE(cl.lgd_rate, lb.base_lgd) AS undiscounted_ecl,
  e.exposure_at_default * pc.marginal_pd * COALESCE(cl.lgd_rate, lb.base_lgd)
    / POWER(1 + e.contractual_rate / 12.0, pc.horizon_months) AS discounted_ecl
FROM staged st
JOIN exposure e ON e.account_sk = st.account_sk
JOIN pd_curve pc ON pc.account_sk = st.account_sk
JOIN ref.lgd_baseline lb ON lb.product_sk = e.product_sk
LEFT JOIN collateral_lgd cl ON cl.account_sk = st.account_sk
WHERE (st.ifrs9_stage = 1 AND pc.horizon_months <= 12)
   OR (st.ifrs9_stage IN (2, 3));""",
    ),
    dict(
        workflow="lending", job="loans_credit_risk", category="Credit Risk",
        title="Charge-off and recovery reconciliation to the allowance",
        description=(
            "Ties gross charge-offs and subsequent recoveries back to the allowance roll-forward, "
            "producing the net charge-off line and the closing allowance balance."
        ),
        sql="""INSERT INTO risk.allowance_rollforward (
  as_of_month, product_sk, opening_allowance, provision_expense,
  gross_chargeoffs, recoveries, net_chargeoffs, closing_allowance
)
WITH opening AS (
  SELECT
    DATE_TRUNC('month', a.as_of_month) AS as_of_month,
    a.product_sk,
    a.closing_allowance AS opening_allowance
  FROM risk.allowance_rollforward a
  WHERE a.as_of_month = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
),
provision AS (
  SELECT
    DATE_TRUNC('month', p.as_of_date) AS as_of_month,
    f.product_sk,
    SUM(p.discounted_ecl) AS provision_expense
  FROM risk.ecl_provision p
  JOIN fact.loan_balance_daily f
    ON f.account_sk = p.account_sk AND f.as_of_date = p.as_of_date
  WHERE p.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', p.as_of_date), f.product_sk
),
chargeoffs AS (
  SELECT
    DATE_TRUNC('month', c.chargeoff_date) AS as_of_month,
    da.product_sk,
    SUM(c.chargeoff_amount) AS gross_chargeoffs
  FROM core.loan_chargeoff c
  JOIN dim.dim_account da ON da.account_id = c.loan_id AND da.is_current = TRUE
  WHERE c.chargeoff_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', c.chargeoff_date), da.product_sk
),
recoveries AS (
  SELECT
    DATE_TRUNC('month', rc.recovery_date) AS as_of_month,
    da.product_sk,
    SUM(rc.recovery_amount) AS recoveries
  FROM core.loan_recovery rc
  JOIN dim.dim_account da ON da.account_id = rc.loan_id AND da.is_current = TRUE
  WHERE rc.recovery_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', rc.recovery_date), da.product_sk
)
SELECT
  DATE_TRUNC('month', CURRENT_DATE) AS as_of_month,
  dp.product_sk,
  COALESCE(o.opening_allowance, 0) AS opening_allowance,
  COALESCE(pv.provision_expense, 0) AS provision_expense,
  COALESCE(co.gross_chargeoffs, 0) AS gross_chargeoffs,
  COALESCE(rv.recoveries, 0) AS recoveries,
  COALESCE(co.gross_chargeoffs, 0) - COALESCE(rv.recoveries, 0) AS net_chargeoffs,
  COALESCE(o.opening_allowance, 0) + COALESCE(pv.provision_expense, 0)
    - COALESCE(co.gross_chargeoffs, 0) + COALESCE(rv.recoveries, 0) AS closing_allowance
FROM dim.dim_product dp
LEFT JOIN opening o ON o.product_sk = dp.product_sk
LEFT JOIN provision pv ON pv.product_sk = dp.product_sk
LEFT JOIN chargeoffs co ON co.product_sk = dp.product_sk
LEFT JOIN recoveries rv ON rv.product_sk = dp.product_sk
WHERE dp.is_current = TRUE AND dp.product_class = 'LOAN';""",
    ),
    dict(
        workflow="lending", job="loans_positions", category="Funds Transfer Pricing",
        title="Assign FTP charge to loans by repricing profile",
        description=(
            "Charges each loan the cost of funds matched to its repricing tenor, so the lending "
            "margin is isolated from interest-rate risk taken by treasury."
        ),
        sql="""INSERT INTO fact.ftp_assignment (
  as_of_date, account_sk, instrument_type, assigned_tenor_months, ftp_rate, ftp_amount, curve_id
)
WITH loans AS (
  SELECT
    f.as_of_date, f.account_sk, f.product_sk, f.currency_code,
    f.average_daily_balance, f.next_reprice_date
  FROM fact.loan_balance_daily f
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
    AND f.accrual_status = 'ACCRUING'
),
tenor_assignment AS (
  SELECT
    l.as_of_date,
    l.account_sk,
    l.currency_code,
    l.average_daily_balance,
    dp.ftp_curve_id,
    GREATEST(
      CEIL(DATE_PART('day', l.next_reprice_date - l.as_of_date) / 30.0),
      1
    ) AS assigned_tenor_months
  FROM loans l
  JOIN dim.dim_product dp ON dp.product_sk = l.product_sk
),
curve AS (
  SELECT c.curve_id, c.currency_code, c.tenor_months, c.zero_rate
  FROM ref.ftp_curve c
  WHERE c.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
matched AS (
  SELECT
    ta.*,
    cv.zero_rate,
    ROW_NUMBER() OVER (
      PARTITION BY ta.account_sk
      ORDER BY ABS(cv.tenor_months - ta.assigned_tenor_months)
    ) AS tenor_match_rank
  FROM tenor_assignment ta
  JOIN curve cv ON cv.curve_id = ta.ftp_curve_id AND cv.currency_code = ta.currency_code
)
SELECT
  m.as_of_date,
  m.account_sk,
  'LOAN' AS instrument_type,
  m.assigned_tenor_months,
  -m.zero_rate AS ftp_rate,
  -m.average_daily_balance * m.zero_rate / 365.0 AS ftp_amount,
  m.ftp_curve_id AS curve_id
FROM matched m
WHERE m.tenor_match_rank = 1;""",
    ),
    dict(
        workflow="lending", job="loans_analytics", category="Loans",
        title="Loan origination vintage performance curves",
        description=(
            "Tracks each origination cohort's cumulative loss and prepayment by months on book, "
            "the vintage curve used to compare underwriting quality across periods."
        ),
        sql="""INSERT INTO mart.loan_vintage_performance (
  vintage_month, months_on_book, product_sk, original_balance,
  current_balance, cumulative_chargeoff, cumulative_loss_rate, survival_rate
)
WITH cohorts AS (
  SELECT
    DATE_TRUNC('month', l.origination_date) AS vintage_month,
    da.account_sk,
    da.product_sk,
    l.original_principal
  FROM core.loan_account l
  JOIN dim.dim_account da ON da.account_id = l.loan_id AND da.is_current = TRUE
  WHERE l.origination_date >= CURRENT_DATE - INTERVAL '60 month'
),
monthly_balances AS (
  SELECT
    c.vintage_month,
    c.product_sk,
    c.account_sk,
    c.original_principal,
    DATE_TRUNC('month', f.as_of_date) AS observation_month,
    DATE_PART('month', AGE(DATE_TRUNC('month', f.as_of_date), c.vintage_month))
      + 12 * DATE_PART('year', AGE(DATE_TRUNC('month', f.as_of_date), c.vintage_month)) AS months_on_book,
    f.outstanding_principal
  FROM cohorts c
  JOIN fact.loan_balance_daily f ON f.account_sk = c.account_sk
  WHERE f.as_of_date = (DATE_TRUNC('month', f.as_of_date) + INTERVAL '1 month' - INTERVAL '1 day')::date
),
losses AS (
  SELECT
    c.vintage_month,
    c.product_sk,
    DATE_PART('month', AGE(DATE_TRUNC('month', co.chargeoff_date), c.vintage_month))
      + 12 * DATE_PART('year', AGE(DATE_TRUNC('month', co.chargeoff_date), c.vintage_month)) AS months_on_book,
    SUM(co.chargeoff_amount) AS chargeoff_amount
  FROM cohorts c
  JOIN dim.dim_account da ON da.account_sk = c.account_sk
  JOIN core.loan_chargeoff co ON co.loan_id = da.account_id
  GROUP BY c.vintage_month, c.product_sk, 3
)
SELECT
  mb.vintage_month,
  mb.months_on_book,
  mb.product_sk,
  SUM(mb.original_principal) AS original_balance,
  SUM(mb.outstanding_principal) AS current_balance,
  COALESCE(SUM(ls.chargeoff_amount), 0) AS cumulative_chargeoff,
  COALESCE(SUM(ls.chargeoff_amount), 0) / NULLIF(SUM(mb.original_principal), 0) AS cumulative_loss_rate,
  SUM(mb.outstanding_principal) / NULLIF(SUM(mb.original_principal), 0) AS survival_rate
FROM monthly_balances mb
LEFT JOIN losses ls
  ON ls.vintage_month = mb.vintage_month
 AND ls.product_sk = mb.product_sk
 AND ls.months_on_book <= mb.months_on_book
GROUP BY mb.vintage_month, mb.months_on_book, mb.product_sk;""",
    ),
]


BANKING_EXAMPLES += [
    dict(
        workflow="profitability", job="revenue_assembly", category="Net Interest Income",
        title="Assemble net interest income from loans, deposits and FTP",
        description=(
            "The core NII build: full-outer-joins the asset side (loan interest income less FTP "
            "charge) to the liability side (deposit interest expense plus FTP credit) so every "
            "product line carries both its customer spread and its funding spread."
        ),
        sql="""INSERT INTO finance.nii_daily (
  as_of_date, product_sk, branch_sk, segment_code, earning_assets, funding_liabilities,
  interest_income, interest_expense, ftp_charge, ftp_credit, net_interest_income
)
WITH asset_side AS (
  SELECT
    l.as_of_date,
    l.product_sk,
    l.branch_sk,
    SUM(l.average_daily_balance) AS earning_assets,
    SUM(ii.interest_income) AS interest_income,
    SUM(COALESCE(ftp.ftp_amount, 0)) AS ftp_charge
  FROM fact.loan_balance_daily l
  LEFT JOIN fact.loan_interest_income ii
    ON ii.account_sk = l.account_sk AND ii.as_of_date = l.as_of_date
  LEFT JOIN fact.ftp_assignment ftp
    ON ftp.account_sk = l.account_sk AND ftp.as_of_date = l.as_of_date
   AND ftp.instrument_type = 'LOAN'
  WHERE l.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY l.as_of_date, l.product_sk, l.branch_sk
),
liability_side AS (
  SELECT
    d.as_of_date,
    d.product_sk,
    d.branch_sk,
    SUM(d.average_daily_balance) AS funding_liabilities,
    SUM(ie.interest_expense) AS interest_expense,
    SUM(COALESCE(ftp.ftp_amount, 0)) AS ftp_credit
  FROM fact.deposit_balance_daily d
  LEFT JOIN fact.deposit_interest_expense ie
    ON ie.account_sk = d.account_sk AND ie.as_of_date = d.as_of_date
  LEFT JOIN fact.ftp_assignment ftp
    ON ftp.account_sk = d.account_sk AND ftp.as_of_date = d.as_of_date
   AND ftp.instrument_type = 'DEPOSIT'
  WHERE d.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY d.as_of_date, d.product_sk, d.branch_sk
),
combined AS (
  SELECT
    COALESCE(a.as_of_date, l.as_of_date) AS as_of_date,
    COALESCE(a.product_sk, l.product_sk) AS product_sk,
    COALESCE(a.branch_sk, l.branch_sk) AS branch_sk,
    COALESCE(a.earning_assets, 0) AS earning_assets,
    COALESCE(l.funding_liabilities, 0) AS funding_liabilities,
    COALESCE(a.interest_income, 0) AS interest_income,
    COALESCE(l.interest_expense, 0) AS interest_expense,
    COALESCE(a.ftp_charge, 0) AS ftp_charge,
    COALESCE(l.ftp_credit, 0) AS ftp_credit
  FROM asset_side a
  FULL OUTER JOIN liability_side l
    ON l.product_sk = a.product_sk AND l.branch_sk = a.branch_sk AND l.as_of_date = a.as_of_date
)
SELECT
  c.as_of_date,
  c.product_sk,
  c.branch_sk,
  db.segment_code,
  c.earning_assets,
  c.funding_liabilities,
  c.interest_income,
  c.interest_expense,
  c.ftp_charge,
  c.ftp_credit,
  c.interest_income - c.interest_expense + c.ftp_credit + c.ftp_charge AS net_interest_income
FROM combined c
JOIN dim.dim_product dp ON dp.product_sk = c.product_sk
JOIN dim.dim_branch db ON db.branch_sk = c.branch_sk;""",
    ),
    dict(
        workflow="profitability", job="revenue_assembly", category="Net Interest Income",
        title="Net interest income volume/rate/mix variance attribution",
        description=(
            "Decomposes the month-over-month NII movement into the part explained by balance "
            "growth (volume), the part by rate changes, and the residual mix effect."
        ),
        sql="""INSERT INTO finance.nii_attribution (
  as_of_month, product_sk, prior_balance, current_balance, prior_yield, current_yield,
  volume_variance, rate_variance, mix_variance, total_variance
)
WITH monthly_nii AS (
  SELECT
    DATE_TRUNC('month', n.as_of_date) AS as_of_month,
    n.product_sk,
    AVG(n.earning_assets) AS avg_balance,
    SUM(n.net_interest_income) AS nii,
    SUM(n.net_interest_income) / NULLIF(AVG(n.earning_assets), 0) * 12 AS annualised_yield
  FROM finance.nii_daily n
  WHERE n.as_of_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '13 month'
  GROUP BY DATE_TRUNC('month', n.as_of_date), n.product_sk
),
paired AS (
  SELECT
    m.as_of_month,
    m.product_sk,
    m.avg_balance AS current_balance,
    m.annualised_yield AS current_yield,
    m.nii AS current_nii,
    LAG(m.avg_balance) OVER (PARTITION BY m.product_sk ORDER BY m.as_of_month) AS prior_balance,
    LAG(m.annualised_yield) OVER (PARTITION BY m.product_sk ORDER BY m.as_of_month) AS prior_yield,
    LAG(m.nii) OVER (PARTITION BY m.product_sk ORDER BY m.as_of_month) AS prior_nii
  FROM monthly_nii m
)
SELECT
  p.as_of_month,
  p.product_sk,
  p.prior_balance,
  p.current_balance,
  p.prior_yield,
  p.current_yield,
  (p.current_balance - p.prior_balance) * p.prior_yield / 12 AS volume_variance,
  (p.current_yield - p.prior_yield) * p.prior_balance / 12 AS rate_variance,
  (p.current_balance - p.prior_balance) * (p.current_yield - p.prior_yield) / 12 AS mix_variance,
  p.current_nii - p.prior_nii AS total_variance
FROM paired p
JOIN dim.dim_product dp ON dp.product_sk = p.product_sk AND dp.is_current = TRUE
WHERE p.prior_balance IS NOT NULL;""",
    ),
    dict(
        workflow="profitability", job="revenue_assembly", category="Revenue",
        title="Consolidate fee and non-interest income from transaction systems",
        description=(
            "Unions fee events from card, payments, wealth and service-charge systems onto a "
            "common product/customer grain, net of fee waivers and reversals."
        ),
        sql="""INSERT INTO fact.fee_income (
  as_of_date, customer_sk, product_sk, branch_sk, fee_category,
  gross_fee, waived_fee, reversed_fee, net_fee
)
WITH raw_fees AS (
  SELECT c.event_date, c.customer_id, c.product_id, c.branch_id,
         'CARD_INTERCHANGE' AS fee_category, c.interchange_amount AS gross_fee
  FROM core.card_transaction c
  WHERE c.event_date = CURRENT_DATE - INTERVAL '1 day'
  UNION ALL
  SELECT p.event_date, p.customer_id, p.product_id, p.branch_id,
         'PAYMENT_FEE' AS fee_category, p.fee_amount AS gross_fee
  FROM core.payment_transaction p
  WHERE p.event_date = CURRENT_DATE - INTERVAL '1 day'
  UNION ALL
  SELECT w.event_date, w.customer_id, w.product_id, w.branch_id,
         'WEALTH_ADVISORY' AS fee_category, w.advisory_fee AS gross_fee
  FROM core.wealth_fee_event w
  WHERE w.event_date = CURRENT_DATE - INTERVAL '1 day'
  UNION ALL
  SELECT s.event_date, s.customer_id, s.product_id, s.branch_id,
         'SERVICE_CHARGE' AS fee_category, s.charge_amount AS gross_fee
  FROM core.service_charge s
  WHERE s.event_date = CURRENT_DATE - INTERVAL '1 day'
),
waivers AS (
  SELECT
    wv.customer_id, wv.product_id, wv.fee_category, wv.event_date,
    SUM(wv.waived_amount) AS waived_fee
  FROM core.fee_waiver wv
  WHERE wv.event_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY wv.customer_id, wv.product_id, wv.fee_category, wv.event_date
),
reversals AS (
  SELECT
    rv.customer_id, rv.product_id, rv.fee_category, rv.event_date,
    SUM(rv.reversal_amount) AS reversed_fee
  FROM core.fee_reversal rv
  WHERE rv.event_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY rv.customer_id, rv.product_id, rv.fee_category, rv.event_date
)
SELECT
  rf.event_date AS as_of_date,
  dc.customer_sk,
  dp.product_sk,
  db.branch_sk,
  rf.fee_category,
  SUM(rf.gross_fee) AS gross_fee,
  COALESCE(MAX(wv.waived_fee), 0) AS waived_fee,
  COALESCE(MAX(rl.reversed_fee), 0) AS reversed_fee,
  SUM(rf.gross_fee) - COALESCE(MAX(wv.waived_fee), 0) - COALESCE(MAX(rl.reversed_fee), 0) AS net_fee
FROM raw_fees rf
JOIN dim.dim_customer dc ON dc.customer_id = rf.customer_id AND dc.is_current = TRUE
JOIN dim.dim_product dp ON dp.product_id = rf.product_id AND dp.is_current = TRUE
JOIN dim.dim_branch db ON db.branch_id = rf.branch_id AND db.is_current = TRUE
LEFT JOIN waivers wv
  ON wv.customer_id = rf.customer_id AND wv.product_id = rf.product_id
 AND wv.fee_category = rf.fee_category AND wv.event_date = rf.event_date
LEFT JOIN reversals rl
  ON rl.customer_id = rf.customer_id AND rl.product_id = rf.product_id
 AND rl.fee_category = rf.fee_category AND rl.event_date = rf.event_date
GROUP BY rf.event_date, dc.customer_sk, dp.product_sk, db.branch_sk, rf.fee_category;""",
    ),
    dict(
        workflow="profitability", job="revenue_assembly", category="Revenue",
        title="Total revenue by product combining NII and fee income",
        description=(
            "Brings spread income and fee income onto one product-month grain — the revenue line "
            "that feeds product P&L and the efficiency ratio."
        ),
        sql="""INSERT INTO finance.revenue_summary (
  as_of_month, product_sk, segment_code, net_interest_income, fee_income,
  total_revenue, avg_earning_assets, revenue_yield
)
WITH nii AS (
  SELECT
    DATE_TRUNC('month', n.as_of_date) AS as_of_month,
    n.product_sk,
    SUM(n.net_interest_income) AS net_interest_income,
    AVG(n.earning_assets) AS avg_earning_assets
  FROM finance.nii_daily n
  WHERE n.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', n.as_of_date), n.product_sk
),
fees AS (
  SELECT
    DATE_TRUNC('month', f.as_of_date) AS as_of_month,
    f.product_sk,
    SUM(f.net_fee) AS fee_income
  FROM fact.fee_income f
  WHERE f.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', f.as_of_date), f.product_sk
)
SELECT
  COALESCE(n.as_of_month, f.as_of_month) AS as_of_month,
  COALESCE(n.product_sk, f.product_sk) AS product_sk,
  dp.segment_code,
  COALESCE(n.net_interest_income, 0) AS net_interest_income,
  COALESCE(f.fee_income, 0) AS fee_income,
  COALESCE(n.net_interest_income, 0) + COALESCE(f.fee_income, 0) AS total_revenue,
  COALESCE(n.avg_earning_assets, 0) AS avg_earning_assets,
  (COALESCE(n.net_interest_income, 0) + COALESCE(f.fee_income, 0))
    / NULLIF(n.avg_earning_assets, 0) * 12 AS revenue_yield
FROM nii n
FULL OUTER JOIN fees f ON f.product_sk = n.product_sk AND f.as_of_month = n.as_of_month
JOIN dim.dim_product dp ON dp.product_sk = COALESCE(n.product_sk, f.product_sk)
WHERE dp.is_current = TRUE;""",
    ),
    dict(
        workflow="profitability", job="revenue_assembly", category="Revenue",
        title="Net interest margin on average earning assets",
        description=(
            "Computes NIM and its spread/free-funds decomposition against average earning assets, "
            "rolled to the reporting segment via the org hierarchy."
        ),
        sql="""INSERT INTO mart.net_interest_margin (
  as_of_month, segment_code, lob_code, avg_earning_assets, avg_interest_bearing_liabilities,
  net_interest_income, net_interest_margin, interest_spread, free_funds_benefit
)
WITH monthly AS (
  SELECT
    DATE_TRUNC('month', n.as_of_date) AS as_of_month,
    n.product_sk,
    n.branch_sk,
    AVG(n.earning_assets) AS avg_earning_assets,
    AVG(n.funding_liabilities) AS avg_liabilities,
    SUM(n.net_interest_income) AS net_interest_income,
    SUM(n.interest_income) AS interest_income,
    SUM(n.interest_expense) AS interest_expense
  FROM finance.nii_daily n
  WHERE n.as_of_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
  GROUP BY DATE_TRUNC('month', n.as_of_date), n.product_sk, n.branch_sk
),
org_rollup AS (
  SELECT
    m.as_of_month,
    o.segment_code,
    o.lob_code,
    SUM(m.avg_earning_assets) AS avg_earning_assets,
    SUM(m.avg_liabilities) AS avg_liabilities,
    SUM(m.net_interest_income) AS net_interest_income,
    SUM(m.interest_income) AS interest_income,
    SUM(m.interest_expense) AS interest_expense
  FROM monthly m
  JOIN dim.dim_branch db ON db.branch_sk = m.branch_sk
  JOIN dim.org_hierarchy o ON o.cost_center_id = db.cost_center_id
  GROUP BY m.as_of_month, o.segment_code, o.lob_code
)
SELECT
  r.as_of_month,
  r.segment_code,
  r.lob_code,
  r.avg_earning_assets,
  r.avg_liabilities AS avg_interest_bearing_liabilities,
  r.net_interest_income,
  r.net_interest_income / NULLIF(r.avg_earning_assets, 0) * 12 AS net_interest_margin,
  (r.interest_income / NULLIF(r.avg_earning_assets, 0)
    - r.interest_expense / NULLIF(r.avg_liabilities, 0)) * 12 AS interest_spread,
  (r.avg_earning_assets - r.avg_liabilities)
    * (r.interest_income / NULLIF(r.avg_earning_assets, 0)) * 12 AS free_funds_benefit
FROM org_rollup r;""",
    ),
    dict(
        workflow="profitability", job="revenue_assembly", category="Revenue",
        title="Customer relationship revenue across all product holdings",
        description=(
            "Sums spread and fee revenue per customer across every product they hold, with "
            "product-holding breadth — the basis for relationship-level pricing decisions."
        ),
        sql="""INSERT INTO mart.customer_revenue (
  as_of_month, customer_sk, segment_code, tenure_months, product_holdings,
  deposit_revenue, loan_revenue, fee_revenue, total_revenue
)
WITH deposit_rev AS (
  SELECT
    DATE_TRUNC('month', d.as_of_date) AS as_of_month,
    d.customer_sk,
    SUM(COALESCE(ftp.ftp_amount, 0) - COALESCE(ie.interest_expense, 0)) AS deposit_revenue,
    COUNT(DISTINCT d.product_sk) AS deposit_products
  FROM fact.deposit_balance_daily d
  LEFT JOIN fact.deposit_interest_expense ie
    ON ie.account_sk = d.account_sk AND ie.as_of_date = d.as_of_date
  LEFT JOIN fact.ftp_assignment ftp
    ON ftp.account_sk = d.account_sk AND ftp.as_of_date = d.as_of_date
  WHERE d.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', d.as_of_date), d.customer_sk
),
loan_rev AS (
  SELECT
    DATE_TRUNC('month', l.as_of_date) AS as_of_month,
    l.customer_sk,
    SUM(COALESCE(ii.interest_income, 0) + COALESCE(ftp.ftp_amount, 0)) AS loan_revenue,
    COUNT(DISTINCT l.product_sk) AS loan_products
  FROM fact.loan_balance_daily l
  LEFT JOIN fact.loan_interest_income ii
    ON ii.account_sk = l.account_sk AND ii.as_of_date = l.as_of_date
  LEFT JOIN fact.ftp_assignment ftp
    ON ftp.account_sk = l.account_sk AND ftp.as_of_date = l.as_of_date
  WHERE l.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', l.as_of_date), l.customer_sk
),
fee_rev AS (
  SELECT
    DATE_TRUNC('month', f.as_of_date) AS as_of_month,
    f.customer_sk,
    SUM(f.net_fee) AS fee_revenue
  FROM fact.fee_income f
  WHERE f.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', f.as_of_date), f.customer_sk
)
SELECT
  DATE_TRUNC('month', CURRENT_DATE) AS as_of_month,
  dc.customer_sk,
  dc.segment_code,
  DATE_PART('month', AGE(CURRENT_DATE, dc.relationship_start_date)) AS tenure_months,
  COALESCE(dr.deposit_products, 0) + COALESCE(lr.loan_products, 0) AS product_holdings,
  COALESCE(dr.deposit_revenue, 0) AS deposit_revenue,
  COALESCE(lr.loan_revenue, 0) AS loan_revenue,
  COALESCE(fr.fee_revenue, 0) AS fee_revenue,
  COALESCE(dr.deposit_revenue, 0) + COALESCE(lr.loan_revenue, 0)
    + COALESCE(fr.fee_revenue, 0) AS total_revenue
FROM dim.dim_customer dc
LEFT JOIN deposit_rev dr ON dr.customer_sk = dc.customer_sk
LEFT JOIN loan_rev lr ON lr.customer_sk = dc.customer_sk
LEFT JOIN fee_rev fr ON fr.customer_sk = dc.customer_sk
WHERE dc.is_current = TRUE;""",
    ),
    dict(
        workflow="profitability", job="revenue_assembly", category="Revenue",
        title="Segment revenue rollup through the org hierarchy",
        description=(
            "Walks the recursive org hierarchy from cost centre up to division so revenue can be "
            "reported at every level of the management structure from one fact table."
        ),
        sql="""INSERT INTO mart.segment_revenue (
  as_of_month, node_id, node_level, node_name, parent_node_id,
  direct_revenue, rollup_revenue
)
WITH RECURSIVE org_tree AS (
  SELECT
    o.node_id, o.parent_node_id, o.node_name, 0 AS node_level, o.node_id AS root_id
  FROM dim.org_hierarchy o
  WHERE o.parent_node_id IS NULL
  UNION ALL
  SELECT
    c.node_id, c.parent_node_id, c.node_name, p.node_level + 1, p.root_id
  FROM dim.org_hierarchy c
  JOIN org_tree p ON p.node_id = c.parent_node_id
  WHERE p.node_level < 8
),
direct_revenue AS (
  SELECT
    r.as_of_month,
    db.org_node_id AS node_id,
    SUM(r.total_revenue) AS direct_revenue
  FROM finance.revenue_summary r
  JOIN dim.dim_product dp ON dp.product_sk = r.product_sk
  JOIN dim.dim_branch db ON db.segment_code = dp.segment_code
  WHERE r.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY r.as_of_month, db.org_node_id
),
descendants AS (
  SELECT
    a.node_id AS ancestor_id,
    d.node_id AS descendant_id
  FROM org_tree a
  JOIN org_tree d ON d.root_id = a.root_id AND d.node_level >= a.node_level
)
SELECT
  dr.as_of_month,
  t.node_id,
  t.node_level,
  t.node_name,
  t.parent_node_id,
  COALESCE(dr.direct_revenue, 0) AS direct_revenue,
  SUM(COALESCE(sub.direct_revenue, 0)) AS rollup_revenue
FROM org_tree t
LEFT JOIN direct_revenue dr ON dr.node_id = t.node_id
LEFT JOIN descendants ds ON ds.ancestor_id = t.node_id
LEFT JOIN direct_revenue sub ON sub.node_id = ds.descendant_id
GROUP BY dr.as_of_month, t.node_id, t.node_level, t.node_name, t.parent_node_id, dr.direct_revenue;""",
    ),
]


BANKING_EXAMPLES += [
    dict(
        workflow="profitability", job="expense_management", category="Expenses",
        title="Derive direct operating expense from the general ledger",
        description=(
            "Filters GL postings to expense accounts, maps them to cost centres and expense "
            "categories, and nets out intercompany eliminations to give direct expense by owner."
        ),
        sql="""INSERT INTO fact.expense (
  accounting_period, cost_center_sk, gl_account_sk, expense_category,
  gross_amount, elimination_amount, direct_expense
)
WITH period AS (
  SELECT DATE_TRUNC('month', CURRENT_DATE) AS accounting_period
),
postings AS (
  SELECT
    DATE_TRUNC('month', p.posting_date) AS accounting_period,
    p.cost_center_id,
    p.gl_account_id,
    p.legal_entity_id,
    SUM(p.debit_amount - p.credit_amount) AS net_amount
  FROM gl.gl_posting p
  CROSS JOIN period pr
  WHERE DATE_TRUNC('month', p.posting_date) = pr.accounting_period
    AND p.posting_status = 'POSTED'
  GROUP BY DATE_TRUNC('month', p.posting_date), p.cost_center_id, p.gl_account_id, p.legal_entity_id
),
expense_accounts AS (
  SELECT
    ga.gl_account_id, ga.gl_account_sk, ga.expense_category, ga.is_intercompany
  FROM dim.dim_gl_account ga
  WHERE ga.account_type = 'EXPENSE' AND ga.is_current = TRUE
),
eliminations AS (
  SELECT
    e.accounting_period, e.cost_center_id, e.gl_account_id,
    SUM(e.elimination_amount) AS elimination_amount
  FROM gl.intercompany_elimination e
  CROSS JOIN period pr
  WHERE e.accounting_period = pr.accounting_period
  GROUP BY e.accounting_period, e.cost_center_id, e.gl_account_id
)
SELECT
  po.accounting_period,
  cc.cost_center_sk,
  ea.gl_account_sk,
  ea.expense_category,
  po.net_amount AS gross_amount,
  COALESCE(el.elimination_amount, 0) AS elimination_amount,
  po.net_amount - COALESCE(el.elimination_amount, 0) AS direct_expense
FROM postings po
JOIN expense_accounts ea ON ea.gl_account_id = po.gl_account_id
JOIN dim.dim_cost_center cc ON cc.cost_center_id = po.cost_center_id AND cc.is_current = TRUE
LEFT JOIN eliminations el
  ON el.accounting_period = po.accounting_period
 AND el.cost_center_id = po.cost_center_id
 AND el.gl_account_id = po.gl_account_id;""",
    ),
    dict(
        workflow="profitability", job="expense_management", category="Expenses",
        title="Personnel expense with fully-loaded FTE cost",
        description=(
            "Combines base salary, incentive accrual, benefits and payroll taxes into a "
            "fully-loaded cost per FTE, split across cost centres by timesheet allocation."
        ),
        sql="""INSERT INTO fact.personnel_expense (
  accounting_period, cost_center_sk, employee_count, fte_count,
  base_salary, incentive_accrual, benefits, payroll_taxes, fully_loaded_cost, cost_per_fte
)
WITH payroll AS (
  SELECT
    DATE_TRUNC('month', pr.pay_date) AS accounting_period,
    pr.employee_id,
    SUM(pr.base_pay) AS base_salary,
    SUM(pr.benefits_amount) AS benefits,
    SUM(pr.employer_tax_amount) AS payroll_taxes
  FROM core.payroll_register pr
  WHERE DATE_TRUNC('month', pr.pay_date) = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', pr.pay_date), pr.employee_id
),
incentive AS (
  SELECT
    ia.accounting_period, ia.employee_id, SUM(ia.accrual_amount) AS incentive_accrual
  FROM core.incentive_accrual ia
  WHERE ia.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY ia.accounting_period, ia.employee_id
),
timesheet_split AS (
  SELECT
    ts.accounting_period,
    ts.employee_id,
    ts.cost_center_id,
    SUM(ts.hours) / NULLIF(SUM(SUM(ts.hours)) OVER (PARTITION BY ts.employee_id, ts.accounting_period), 0) AS allocation_pct
  FROM core.timesheet_summary ts
  WHERE ts.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY ts.accounting_period, ts.employee_id, ts.cost_center_id
)
SELECT
  p.accounting_period,
  cc.cost_center_sk,
  COUNT(DISTINCT p.employee_id) AS employee_count,
  SUM(e.fte_factor * tsp.allocation_pct) AS fte_count,
  SUM(p.base_salary * tsp.allocation_pct) AS base_salary,
  SUM(COALESCE(i.incentive_accrual, 0) * tsp.allocation_pct) AS incentive_accrual,
  SUM(p.benefits * tsp.allocation_pct) AS benefits,
  SUM(p.payroll_taxes * tsp.allocation_pct) AS payroll_taxes,
  SUM((p.base_salary + COALESCE(i.incentive_accrual, 0) + p.benefits + p.payroll_taxes)
      * tsp.allocation_pct) AS fully_loaded_cost,
  SUM((p.base_salary + COALESCE(i.incentive_accrual, 0) + p.benefits + p.payroll_taxes)
      * tsp.allocation_pct) / NULLIF(SUM(e.fte_factor * tsp.allocation_pct), 0) AS cost_per_fte
FROM payroll p
JOIN dim.dim_employee e ON e.employee_id = p.employee_id AND e.is_current = TRUE
JOIN timesheet_split tsp ON tsp.employee_id = p.employee_id AND tsp.accounting_period = p.accounting_period
JOIN dim.dim_cost_center cc ON cc.cost_center_id = tsp.cost_center_id AND cc.is_current = TRUE
LEFT JOIN incentive i ON i.employee_id = p.employee_id AND i.accounting_period = p.accounting_period
GROUP BY p.accounting_period, cc.cost_center_sk;""",
    ),
    dict(
        workflow="profitability", job="expense_management", category="Expenses",
        title="Vendor expense with prepaid amortisation and accrual true-up",
        description=(
            "Recognises vendor cost in the correct period: amortising prepaid contracts over their "
            "term and truing up prior accruals against invoices actually received."
        ),
        sql="""INSERT INTO fact.vendor_expense (
  accounting_period, cost_center_sk, vendor_id, contract_id,
  invoiced_amount, prepaid_amortisation, accrual_truup, recognised_expense
)
WITH invoices AS (
  SELECT
    DATE_TRUNC('month', i.invoice_date) AS accounting_period,
    i.vendor_id, i.contract_id, i.cost_center_id,
    SUM(i.invoice_amount) AS invoiced_amount
  FROM core.vendor_invoice i
  WHERE DATE_TRUNC('month', i.invoice_date) = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', i.invoice_date), i.vendor_id, i.contract_id, i.cost_center_id
),
prepaid AS (
  SELECT
    pp.contract_id,
    pp.vendor_id,
    pp.cost_center_id,
    pp.prepaid_amount / NULLIF(pp.term_months, 0) AS monthly_amortisation
  FROM core.prepaid_contract pp
  WHERE DATE_TRUNC('month', CURRENT_DATE)
    BETWEEN pp.amortisation_start AND pp.amortisation_end
),
prior_accrual AS (
  SELECT
    a.contract_id, a.vendor_id, a.cost_center_id, SUM(a.accrual_amount) AS prior_accrual
  FROM gl.expense_accrual a
  WHERE a.accounting_period = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
    AND a.reversal_status = 'OPEN'
  GROUP BY a.contract_id, a.vendor_id, a.cost_center_id
)
SELECT
  DATE_TRUNC('month', CURRENT_DATE) AS accounting_period,
  cc.cost_center_sk,
  COALESCE(inv.vendor_id, pp.vendor_id, pa.vendor_id) AS vendor_id,
  COALESCE(inv.contract_id, pp.contract_id, pa.contract_id) AS contract_id,
  COALESCE(inv.invoiced_amount, 0) AS invoiced_amount,
  COALESCE(pp.monthly_amortisation, 0) AS prepaid_amortisation,
  -COALESCE(pa.prior_accrual, 0) AS accrual_truup,
  COALESCE(inv.invoiced_amount, 0) + COALESCE(pp.monthly_amortisation, 0)
    - COALESCE(pa.prior_accrual, 0) AS recognised_expense
FROM invoices inv
FULL OUTER JOIN prepaid pp ON pp.contract_id = inv.contract_id
FULL OUTER JOIN prior_accrual pa ON pa.contract_id = COALESCE(inv.contract_id, pp.contract_id)
JOIN dim.dim_cost_center cc
  ON cc.cost_center_id = COALESCE(inv.cost_center_id, pp.cost_center_id, pa.cost_center_id)
 AND cc.is_current = TRUE;""",
    ),
    dict(
        workflow="profitability", job="expense_management", category="Expenses",
        title="Build allocable cost pools from support cost centres",
        description=(
            "Consolidates direct, personnel and vendor expense into the cost pools that the "
            "allocation engine later pushes out to revenue-generating business lines."
        ),
        sql="""INSERT INTO finance.expense_pool (
  accounting_period, pool_id, cost_center_sk, pool_type, allocation_basis, pool_amount
)
WITH direct AS (
  SELECT e.accounting_period, e.cost_center_sk, SUM(e.direct_expense) AS amount
  FROM fact.expense e
  WHERE e.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY e.accounting_period, e.cost_center_sk
),
personnel AS (
  SELECT p.accounting_period, p.cost_center_sk, SUM(p.fully_loaded_cost) AS amount
  FROM fact.personnel_expense p
  WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY p.accounting_period, p.cost_center_sk
),
vendor AS (
  SELECT v.accounting_period, v.cost_center_sk, SUM(v.recognised_expense) AS amount
  FROM fact.vendor_expense v
  WHERE v.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY v.accounting_period, v.cost_center_sk
),
combined AS (
  SELECT d.accounting_period, d.cost_center_sk, d.amount FROM direct d
  UNION ALL
  SELECT p.accounting_period, p.cost_center_sk, p.amount FROM personnel p
  UNION ALL
  SELECT v.accounting_period, v.cost_center_sk, v.amount FROM vendor v
)
SELECT
  c.accounting_period,
  pd.pool_id,
  c.cost_center_sk,
  pd.pool_type,
  pd.allocation_basis,
  SUM(c.amount) AS pool_amount
FROM combined c
JOIN dim.dim_cost_center cc ON cc.cost_center_sk = c.cost_center_sk
JOIN ref.cost_pool_definition pd ON pd.cost_center_type = cc.cost_center_type
WHERE cc.cost_center_type IN ('SUPPORT', 'OVERHEAD', 'TECHNOLOGY')
GROUP BY c.accounting_period, pd.pool_id, c.cost_center_sk, pd.pool_type, pd.allocation_basis;""",
    ),
    dict(
        workflow="profitability", job="expense_management", category="Expenses",
        title="Efficiency ratio and operating leverage by line of business",
        description=(
            "Divides operating expense by revenue for the efficiency ratio, and compares revenue "
            "growth to expense growth to give operating leverage."
        ),
        sql="""INSERT INTO mart.efficiency_ratio (
  accounting_period, lob_code, total_revenue, operating_expense, efficiency_ratio,
  revenue_growth_pct, expense_growth_pct, operating_leverage
)
WITH revenue_by_lob AS (
  SELECT
    r.as_of_month AS accounting_period,
    o.lob_code,
    SUM(r.total_revenue) AS total_revenue
  FROM finance.revenue_summary r
  JOIN dim.dim_product dp ON dp.product_sk = r.product_sk
  JOIN dim.org_hierarchy o ON o.segment_code = dp.segment_code
  WHERE r.as_of_month >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '13 month'
  GROUP BY r.as_of_month, o.lob_code
),
expense_by_lob AS (
  SELECT
    e.accounting_period,
    o.lob_code,
    SUM(e.direct_expense) AS operating_expense
  FROM fact.expense e
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = e.cost_center_sk
  JOIN dim.org_hierarchy o ON o.cost_center_id = cc.cost_center_id
  WHERE e.accounting_period >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '13 month'
  GROUP BY e.accounting_period, o.lob_code
),
joined AS (
  SELECT
    r.accounting_period,
    r.lob_code,
    r.total_revenue,
    x.operating_expense,
    LAG(r.total_revenue, 12) OVER (PARTITION BY r.lob_code ORDER BY r.accounting_period) AS prior_revenue,
    LAG(x.operating_expense, 12) OVER (PARTITION BY r.lob_code ORDER BY r.accounting_period) AS prior_expense
  FROM revenue_by_lob r
  JOIN expense_by_lob x ON x.lob_code = r.lob_code AND x.accounting_period = r.accounting_period
)
SELECT
  j.accounting_period,
  j.lob_code,
  j.total_revenue,
  j.operating_expense,
  j.operating_expense / NULLIF(j.total_revenue, 0) AS efficiency_ratio,
  (j.total_revenue - j.prior_revenue) / NULLIF(j.prior_revenue, 0) AS revenue_growth_pct,
  (j.operating_expense - j.prior_expense) / NULLIF(j.prior_expense, 0) AS expense_growth_pct,
  (j.total_revenue - j.prior_revenue) / NULLIF(j.prior_revenue, 0)
    - (j.operating_expense - j.prior_expense) / NULLIF(j.prior_expense, 0) AS operating_leverage
FROM joined j
WHERE j.prior_revenue IS NOT NULL;""",
    ),
    dict(
        workflow="profitability", job="expense_management", category="Expenses",
        title="Expense variance against budget and forecast",
        description=(
            "Three-way comparison of actual expense to budget and latest forecast per cost centre, "
            "flagging material overruns for the monthly business review."
        ),
        sql="""INSERT INTO mart.expense_variance (
  accounting_period, cost_center_sk, expense_category, actual_amount, budget_amount,
  forecast_amount, budget_variance, budget_variance_pct, variance_flag
)
WITH actual AS (
  SELECT
    e.accounting_period, e.cost_center_sk, e.expense_category,
    SUM(e.direct_expense) AS actual_amount
  FROM fact.expense e
  WHERE e.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY e.accounting_period, e.cost_center_sk, e.expense_category
),
budget AS (
  SELECT
    b.accounting_period, cc.cost_center_sk, b.expense_category,
    SUM(b.budget_amount) AS budget_amount
  FROM finance.expense_budget b
  JOIN dim.dim_cost_center cc ON cc.cost_center_id = b.cost_center_id AND cc.is_current = TRUE
  WHERE b.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND b.budget_version = 'APPROVED'
  GROUP BY b.accounting_period, cc.cost_center_sk, b.expense_category
),
forecast AS (
  SELECT
    f.accounting_period, cc.cost_center_sk, f.expense_category,
    SUM(f.forecast_amount) AS forecast_amount
  FROM finance.expense_forecast f
  JOIN dim.dim_cost_center cc ON cc.cost_center_id = f.cost_center_id AND cc.is_current = TRUE
  WHERE f.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND f.forecast_cycle = (SELECT MAX(f2.forecast_cycle) FROM finance.expense_forecast f2)
  GROUP BY f.accounting_period, cc.cost_center_sk, f.expense_category
)
SELECT
  a.accounting_period,
  a.cost_center_sk,
  a.expense_category,
  a.actual_amount,
  COALESCE(b.budget_amount, 0) AS budget_amount,
  COALESCE(fc.forecast_amount, 0) AS forecast_amount,
  a.actual_amount - COALESCE(b.budget_amount, 0) AS budget_variance,
  (a.actual_amount - COALESCE(b.budget_amount, 0)) / NULLIF(b.budget_amount, 0) AS budget_variance_pct,
  CASE
    WHEN (a.actual_amount - COALESCE(b.budget_amount, 0)) / NULLIF(b.budget_amount, 0) > 0.10 THEN 'OVER_BUDGET'
    WHEN (a.actual_amount - COALESCE(b.budget_amount, 0)) / NULLIF(b.budget_amount, 0) < -0.10 THEN 'UNDER_BUDGET'
    ELSE 'ON_TRACK'
  END AS variance_flag
FROM actual a
LEFT JOIN budget b
  ON b.cost_center_sk = a.cost_center_sk AND b.expense_category = a.expense_category
 AND b.accounting_period = a.accounting_period
LEFT JOIN forecast fc
  ON fc.cost_center_sk = a.cost_center_sk AND fc.expense_category = a.expense_category
 AND fc.accounting_period = a.accounting_period;""",
    ),
]


BANKING_EXAMPLES += [
    dict(
        workflow="profitability", job="cost_allocation", category="Cost Allocation",
        title="Driver-based cost allocation to receiving business lines",
        description=(
            "Spreads each support cost pool across receiving cost centres in proportion to its "
            "allocation driver (headcount, transaction volume, square footage), normalising driver "
            "weights so every pool allocates to exactly 100%."
        ),
        sql="""INSERT INTO finance.allocation_result (
  accounting_period, allocation_pass, pool_id, sender_cost_center_sk,
  receiver_cost_center_sk, driver_code, driver_value, driver_share, allocated_amount
)
WITH pools AS (
  SELECT
    p.accounting_period, p.pool_id, p.cost_center_sk AS sender_cost_center_sk,
    p.allocation_basis, p.pool_amount
  FROM finance.expense_pool p
  WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND p.pool_amount <> 0
),
driver_values AS (
  SELECT
    d.accounting_period,
    d.driver_code,
    cc.cost_center_sk AS receiver_cost_center_sk,
    SUM(d.driver_value) AS driver_value
  FROM finance.allocation_driver_value d
  JOIN dim.dim_cost_center cc ON cc.cost_center_id = d.cost_center_id AND cc.is_current = TRUE
  WHERE d.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND cc.cost_center_type = 'BUSINESS'
  GROUP BY d.accounting_period, d.driver_code, cc.cost_center_sk
),
driver_totals AS (
  SELECT
    dv.accounting_period, dv.driver_code, SUM(dv.driver_value) AS total_driver_value
  FROM driver_values dv
  GROUP BY dv.accounting_period, dv.driver_code
)
SELECT
  p.accounting_period,
  1 AS allocation_pass,
  p.pool_id,
  p.sender_cost_center_sk,
  dv.receiver_cost_center_sk,
  dv.driver_code,
  dv.driver_value,
  dv.driver_value / NULLIF(dt.total_driver_value, 0) AS driver_share,
  p.pool_amount * dv.driver_value / NULLIF(dt.total_driver_value, 0) AS allocated_amount
FROM pools p
JOIN ref.cost_pool_definition cpd ON cpd.pool_id = p.pool_id
JOIN driver_values dv
  ON dv.driver_code = cpd.driver_code AND dv.accounting_period = p.accounting_period
JOIN driver_totals dt
  ON dt.driver_code = dv.driver_code AND dt.accounting_period = dv.accounting_period
WHERE dv.receiver_cost_center_sk <> p.sender_cost_center_sk;""",
    ),
    dict(
        workflow="profitability", job="cost_allocation", category="Cost Allocation",
        title="Sequential step-down allocation across support centres",
        description=(
            "Runs the step-down (sequential) method: support centres allocate in a fixed ranked "
            "order, each absorbing costs pushed down from higher-ranked centres before "
            "redistributing its own total."
        ),
        sql="""INSERT INTO finance.stepdown_allocation (
  accounting_period, step_sequence, sender_cost_center_sk, receiver_cost_center_sk,
  opening_pool, absorbed_amount, allocable_amount, allocated_amount
)
WITH RECURSIVE step_order AS (
  SELECT
    cc.cost_center_sk, so.step_sequence, so.driver_code
  FROM ref.stepdown_sequence so
  JOIN dim.dim_cost_center cc ON cc.cost_center_id = so.cost_center_id AND cc.is_current = TRUE
),
initial_pools AS (
  SELECT
    p.cost_center_sk, SUM(p.pool_amount) AS opening_pool
  FROM finance.expense_pool p
  WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY p.cost_center_sk
),
cascade (step_sequence, sender_cost_center_sk, receiver_cost_center_sk, allocable_amount, allocated_amount) AS (
  SELECT
    so.step_sequence,
    so.cost_center_sk AS sender_cost_center_sk,
    dv.cost_center_sk AS receiver_cost_center_sk,
    ip.opening_pool AS allocable_amount,
    ip.opening_pool * dv.driver_share AS allocated_amount
  FROM step_order so
  JOIN initial_pools ip ON ip.cost_center_sk = so.cost_center_sk
  JOIN finance.driver_share_matrix dv ON dv.driver_code = so.driver_code
  WHERE so.step_sequence = 1
  UNION ALL
  SELECT
    so.step_sequence,
    so.cost_center_sk,
    dv.cost_center_sk,
    ip.opening_pool + SUM(c.allocated_amount) AS allocable_amount,
    (ip.opening_pool + SUM(c.allocated_amount)) * dv.driver_share AS allocated_amount
  FROM cascade c
  JOIN step_order so ON so.step_sequence = c.step_sequence + 1
  JOIN initial_pools ip ON ip.cost_center_sk = so.cost_center_sk
  JOIN finance.driver_share_matrix dv ON dv.driver_code = so.driver_code
  WHERE c.receiver_cost_center_sk = so.cost_center_sk AND so.step_sequence <= 12
  GROUP BY so.step_sequence, so.cost_center_sk, dv.cost_center_sk, ip.opening_pool, dv.driver_share
)
SELECT
  DATE_TRUNC('month', CURRENT_DATE) AS accounting_period,
  c.step_sequence,
  c.sender_cost_center_sk,
  c.receiver_cost_center_sk,
  ip.opening_pool,
  c.allocable_amount - ip.opening_pool AS absorbed_amount,
  c.allocable_amount,
  c.allocated_amount
FROM cascade c
JOIN initial_pools ip ON ip.cost_center_sk = c.sender_cost_center_sk;""",
    ),
    dict(
        workflow="profitability", job="cost_allocation", category="Cost Allocation",
        title="Reciprocal allocation solved by iterative convergence",
        description=(
            "Handles support centres that consume each other's services: costs are pushed round "
            "the reciprocal matrix repeatedly, each pass shrinking the residual, until the "
            "unallocated remainder falls below the materiality threshold."
        ),
        sql="""INSERT INTO finance.reciprocal_allocation (
  accounting_period, iteration, sender_cost_center_sk, receiver_cost_center_sk,
  iteration_amount, cumulative_amount, residual_amount
)
WITH RECURSIVE seed_pools AS (
  SELECT
    p.cost_center_sk AS sender_cost_center_sk,
    SUM(p.pool_amount) AS pool_amount
  FROM finance.expense_pool p
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = p.cost_center_sk
  WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND cc.cost_center_type IN ('SUPPORT', 'TECHNOLOGY')
  GROUP BY p.cost_center_sk
),
reciprocal_matrix AS (
  SELECT
    sender.cost_center_sk AS sender_cost_center_sk,
    receiver.cost_center_sk AS receiver_cost_center_sk,
    rm.consumption_share
  FROM ref.reciprocal_consumption rm
  JOIN dim.dim_cost_center sender ON sender.cost_center_id = rm.sender_cost_center_id
  JOIN dim.dim_cost_center receiver ON receiver.cost_center_id = rm.receiver_cost_center_id
  WHERE sender.is_current = TRUE AND receiver.is_current = TRUE
),
iterate (iteration, sender_cost_center_sk, receiver_cost_center_sk, iteration_amount) AS (
  SELECT
    1 AS iteration,
    sp.sender_cost_center_sk,
    rm.receiver_cost_center_sk,
    sp.pool_amount * rm.consumption_share AS iteration_amount
  FROM seed_pools sp
  JOIN reciprocal_matrix rm ON rm.sender_cost_center_sk = sp.sender_cost_center_sk
  UNION ALL
  SELECT
    it.iteration + 1,
    it.receiver_cost_center_sk AS sender_cost_center_sk,
    rm.receiver_cost_center_sk,
    it.iteration_amount * rm.consumption_share AS iteration_amount
  FROM iterate it
  JOIN reciprocal_matrix rm ON rm.sender_cost_center_sk = it.receiver_cost_center_sk
  WHERE it.iteration < 10 AND ABS(it.iteration_amount) > 1.0
),
accumulated AS (
  SELECT
    it.iteration,
    it.sender_cost_center_sk,
    it.receiver_cost_center_sk,
    SUM(it.iteration_amount) AS iteration_amount,
    SUM(SUM(it.iteration_amount)) OVER (
      PARTITION BY it.sender_cost_center_sk, it.receiver_cost_center_sk
      ORDER BY it.iteration
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_amount
  FROM iterate it
  GROUP BY it.iteration, it.sender_cost_center_sk, it.receiver_cost_center_sk
)
SELECT
  DATE_TRUNC('month', CURRENT_DATE) AS accounting_period,
  a.iteration,
  a.sender_cost_center_sk,
  a.receiver_cost_center_sk,
  a.iteration_amount,
  a.cumulative_amount,
  sp.pool_amount - a.cumulative_amount AS residual_amount
FROM accumulated a
JOIN seed_pools sp ON sp.sender_cost_center_sk = a.sender_cost_center_sk;""",
    ),
    dict(
        workflow="profitability", job="cost_allocation", category="Cost Allocation",
        title="Allocate corporate overhead to lines of business",
        description=(
            "Pushes unallocated corporate centre cost out to LOBs on a blended revenue-and-"
            "headcount basis, keeping a residual 'corporate retained' bucket for costs deemed "
            "non-attributable."
        ),
        sql="""INSERT INTO finance.overhead_allocation (
  accounting_period, lob_code, overhead_pool, revenue_share, headcount_share,
  blended_share, allocated_overhead, corporate_retained
)
WITH overhead_pool AS (
  SELECT
    p.accounting_period,
    SUM(p.pool_amount) AS total_overhead,
    SUM(p.pool_amount * COALESCE(cpd.non_attributable_pct, 0)) AS retained_overhead
  FROM finance.expense_pool p
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = p.cost_center_sk
  JOIN ref.cost_pool_definition cpd ON cpd.pool_id = p.pool_id
  WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND cc.cost_center_type = 'OVERHEAD'
  GROUP BY p.accounting_period
),
lob_revenue AS (
  SELECT
    o.lob_code,
    SUM(r.total_revenue) AS lob_revenue
  FROM finance.revenue_summary r
  JOIN dim.dim_product dp ON dp.product_sk = r.product_sk
  JOIN dim.org_hierarchy o ON o.segment_code = dp.segment_code
  WHERE r.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY o.lob_code
),
lob_headcount AS (
  SELECT
    o.lob_code,
    SUM(pe.fte_count) AS lob_fte
  FROM fact.personnel_expense pe
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = pe.cost_center_sk
  JOIN dim.org_hierarchy o ON o.cost_center_id = cc.cost_center_id
  WHERE pe.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY o.lob_code
),
shares AS (
  SELECT
    lr.lob_code,
    lr.lob_revenue / NULLIF(SUM(lr.lob_revenue) OVER (), 0) AS revenue_share,
    lh.lob_fte / NULLIF(SUM(lh.lob_fte) OVER (), 0) AS headcount_share
  FROM lob_revenue lr
  JOIN lob_headcount lh ON lh.lob_code = lr.lob_code
)
SELECT
  op.accounting_period,
  s.lob_code,
  op.total_overhead AS overhead_pool,
  s.revenue_share,
  s.headcount_share,
  0.6 * s.revenue_share + 0.4 * s.headcount_share AS blended_share,
  (op.total_overhead - op.retained_overhead)
    * (0.6 * s.revenue_share + 0.4 * s.headcount_share) AS allocated_overhead,
  op.retained_overhead AS corporate_retained
FROM shares s
CROSS JOIN overhead_pool op;""",
    ),
    dict(
        workflow="profitability", job="cost_allocation", category="Cost Allocation",
        title="Allocate technology cost by measured consumption",
        description=(
            "Charges application and infrastructure cost to consuming business lines using metered "
            "usage (compute hours, storage, API calls) weighted by unit rates from the service catalogue."
        ),
        sql="""INSERT INTO finance.technology_allocation (
  accounting_period, application_id, lob_code, compute_units, storage_units,
  transaction_units, weighted_consumption, consumption_share, allocated_cost
)
WITH app_cost AS (
  SELECT
    p.accounting_period,
    a.application_id,
    SUM(p.pool_amount) AS application_cost
  FROM finance.expense_pool p
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = p.cost_center_sk
  JOIN dim.application_registry a ON a.owning_cost_center_id = cc.cost_center_id
  WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND cc.cost_center_type = 'TECHNOLOGY'
  GROUP BY p.accounting_period, a.application_id
),
usage AS (
  SELECT
    u.accounting_period,
    u.application_id,
    o.lob_code,
    SUM(u.compute_hours) AS compute_units,
    SUM(u.storage_gb) AS storage_units,
    SUM(u.api_calls) AS transaction_units
  FROM ops.application_usage u
  JOIN dim.dim_cost_center cc ON cc.cost_center_id = u.consuming_cost_center_id AND cc.is_current = TRUE
  JOIN dim.org_hierarchy o ON o.cost_center_id = cc.cost_center_id
  WHERE u.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY u.accounting_period, u.application_id, o.lob_code
),
weighted AS (
  SELECT
    us.accounting_period,
    us.application_id,
    us.lob_code,
    us.compute_units,
    us.storage_units,
    us.transaction_units,
    us.compute_units * sc.compute_unit_rate
      + us.storage_units * sc.storage_unit_rate
      + us.transaction_units * sc.transaction_unit_rate AS weighted_consumption
  FROM usage us
  JOIN ref.service_catalogue_rate sc ON sc.application_id = us.application_id
),
totals AS (
  SELECT
    w.application_id, SUM(w.weighted_consumption) AS total_consumption
  FROM weighted w
  GROUP BY w.application_id
)
SELECT
  w.accounting_period,
  w.application_id,
  w.lob_code,
  w.compute_units,
  w.storage_units,
  w.transaction_units,
  w.weighted_consumption,
  w.weighted_consumption / NULLIF(t.total_consumption, 0) AS consumption_share,
  ac.application_cost * w.weighted_consumption / NULLIF(t.total_consumption, 0) AS allocated_cost
FROM weighted w
JOIN totals t ON t.application_id = w.application_id
JOIN app_cost ac ON ac.application_id = w.application_id AND ac.accounting_period = w.accounting_period;""",
    ),
    dict(
        workflow="profitability", job="cost_allocation", category="Cost Allocation",
        title="Allocation tie-out and reconciliation control",
        description=(
            "Control check proving the allocation engine conserves money: total allocated out must "
            "equal total pool in, and the fully-allocated expense must tie back to the GL."
        ),
        sql="""INSERT INTO finance.allocation_reconciliation (
  accounting_period, source_pool_total, allocated_total, gl_expense_total,
  allocation_difference, gl_difference, reconciliation_status
)
WITH pool_total AS (
  SELECT
    p.accounting_period, SUM(p.pool_amount) AS source_pool_total
  FROM finance.expense_pool p
  WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY p.accounting_period
),
allocated_total AS (
  SELECT
    a.accounting_period, SUM(a.allocated_amount) AS allocated_total
  FROM finance.allocation_result a
  WHERE a.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY a.accounting_period
),
gl_total AS (
  SELECT
    DATE_TRUNC('month', g.posting_date) AS accounting_period,
    SUM(g.debit_amount - g.credit_amount) AS gl_expense_total
  FROM gl.gl_posting g
  JOIN dim.dim_gl_account ga ON ga.gl_account_id = g.gl_account_id AND ga.is_current = TRUE
  WHERE ga.account_type = 'EXPENSE'
    AND DATE_TRUNC('month', g.posting_date) = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', g.posting_date)
)
SELECT
  pt.accounting_period,
  pt.source_pool_total,
  at.allocated_total,
  gt.gl_expense_total,
  pt.source_pool_total - at.allocated_total AS allocation_difference,
  pt.source_pool_total - gt.gl_expense_total AS gl_difference,
  CASE
    WHEN ABS(pt.source_pool_total - at.allocated_total) < 1.0
     AND ABS(pt.source_pool_total - gt.gl_expense_total) < 1.0 THEN 'BALANCED'
    WHEN ABS(pt.source_pool_total - at.allocated_total) >= 1.0 THEN 'ALLOCATION_LEAKAGE'
    ELSE 'GL_MISMATCH'
  END AS reconciliation_status
FROM pool_total pt
JOIN allocated_total at ON at.accounting_period = pt.accounting_period
JOIN gl_total gt ON gt.accounting_period = pt.accounting_period;""",
    ),
]


BANKING_EXAMPLES += [
    dict(
        workflow="profitability", job="pnl_close", category="P&L",
        title="Full P&L waterfall from revenue to net income after tax",
        description=(
            "The management P&L waterfall: net interest income plus fees, less direct and allocated "
            "expense, less credit provision, giving pre-tax profit and — after the effective tax "
            "rate — net income, per line of business."
        ),
        sql="""INSERT INTO finance.pnl_actual (
  accounting_period, lob_code, net_interest_income, fee_income, total_revenue,
  direct_expense, allocated_expense, total_expense, pre_provision_profit,
  credit_provision, pre_tax_profit, tax_expense, net_income
)
WITH revenue AS (
  SELECT
    r.as_of_month AS accounting_period,
    o.lob_code,
    SUM(r.net_interest_income) AS net_interest_income,
    SUM(r.fee_income) AS fee_income
  FROM finance.revenue_summary r
  JOIN dim.dim_product dp ON dp.product_sk = r.product_sk
  JOIN dim.org_hierarchy o ON o.segment_code = dp.segment_code
  WHERE r.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY r.as_of_month, o.lob_code
),
direct_exp AS (
  SELECT
    e.accounting_period,
    o.lob_code,
    SUM(e.direct_expense) AS direct_expense
  FROM fact.expense e
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = e.cost_center_sk
  JOIN dim.org_hierarchy o ON o.cost_center_id = cc.cost_center_id
  WHERE e.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY e.accounting_period, o.lob_code
),
allocated_exp AS (
  SELECT
    a.accounting_period,
    o.lob_code,
    SUM(a.allocated_amount) AS allocated_expense
  FROM finance.allocation_result a
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = a.receiver_cost_center_sk
  JOIN dim.org_hierarchy o ON o.cost_center_id = cc.cost_center_id
  WHERE a.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY a.accounting_period, o.lob_code
),
provision AS (
  SELECT
    ar.as_of_month AS accounting_period,
    o.lob_code,
    SUM(ar.provision_expense) AS credit_provision
  FROM risk.allowance_rollforward ar
  JOIN dim.dim_product dp ON dp.product_sk = ar.product_sk
  JOIN dim.org_hierarchy o ON o.segment_code = dp.segment_code
  WHERE ar.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY ar.as_of_month, o.lob_code
)
SELECT
  rv.accounting_period,
  rv.lob_code,
  rv.net_interest_income,
  rv.fee_income,
  rv.net_interest_income + rv.fee_income AS total_revenue,
  COALESCE(de.direct_expense, 0) AS direct_expense,
  COALESCE(ae.allocated_expense, 0) AS allocated_expense,
  COALESCE(de.direct_expense, 0) + COALESCE(ae.allocated_expense, 0) AS total_expense,
  rv.net_interest_income + rv.fee_income
    - COALESCE(de.direct_expense, 0) - COALESCE(ae.allocated_expense, 0) AS pre_provision_profit,
  COALESCE(pv.credit_provision, 0) AS credit_provision,
  rv.net_interest_income + rv.fee_income
    - COALESCE(de.direct_expense, 0) - COALESCE(ae.allocated_expense, 0)
    - COALESCE(pv.credit_provision, 0) AS pre_tax_profit,
  (rv.net_interest_income + rv.fee_income
    - COALESCE(de.direct_expense, 0) - COALESCE(ae.allocated_expense, 0)
    - COALESCE(pv.credit_provision, 0)) * tr.effective_tax_rate AS tax_expense,
  (rv.net_interest_income + rv.fee_income
    - COALESCE(de.direct_expense, 0) - COALESCE(ae.allocated_expense, 0)
    - COALESCE(pv.credit_provision, 0)) * (1 - tr.effective_tax_rate) AS net_income
FROM revenue rv
LEFT JOIN direct_exp de ON de.lob_code = rv.lob_code AND de.accounting_period = rv.accounting_period
LEFT JOIN allocated_exp ae ON ae.lob_code = rv.lob_code AND ae.accounting_period = rv.accounting_period
LEFT JOIN provision pv ON pv.lob_code = rv.lob_code AND pv.accounting_period = rv.accounting_period
CROSS JOIN ref.tax_rate tr
WHERE tr.jurisdiction = 'CONSOLIDATED' AND tr.accounting_period = rv.accounting_period;""",
    ),
    dict(
        workflow="profitability", job="pnl_close", category="P&L",
        title="Product-level P&L with fully-loaded cost",
        description=(
            "Product P&L carrying spread income, fee income, direct cost, its share of allocated "
            "overhead and credit cost, down to net contribution and return on assets."
        ),
        sql="""INSERT INTO mart.product_pnl (
  accounting_period, product_sk, product_name, avg_balance, net_interest_income,
  fee_income, direct_cost, allocated_cost, credit_cost, net_contribution, return_on_assets
)
WITH product_revenue AS (
  SELECT
    r.as_of_month AS accounting_period,
    r.product_sk,
    r.net_interest_income,
    r.fee_income,
    r.avg_earning_assets AS avg_balance
  FROM finance.revenue_summary r
  WHERE r.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
),
product_direct_cost AS (
  SELECT
    e.accounting_period,
    pm.product_sk,
    SUM(e.direct_expense * pm.product_weight) AS direct_cost
  FROM fact.expense e
  JOIN ref.expense_product_map pm ON pm.cost_center_sk = e.cost_center_sk
  WHERE e.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY e.accounting_period, pm.product_sk
),
product_allocated_cost AS (
  SELECT
    a.accounting_period,
    pm.product_sk,
    SUM(a.allocated_amount * pm.product_weight) AS allocated_cost
  FROM finance.allocation_result a
  JOIN ref.expense_product_map pm ON pm.cost_center_sk = a.receiver_cost_center_sk
  WHERE a.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY a.accounting_period, pm.product_sk
),
product_credit_cost AS (
  SELECT
    ar.as_of_month AS accounting_period,
    ar.product_sk,
    SUM(ar.provision_expense) AS credit_cost
  FROM risk.allowance_rollforward ar
  WHERE ar.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY ar.as_of_month, ar.product_sk
)
SELECT
  pr.accounting_period,
  pr.product_sk,
  dp.product_name,
  pr.avg_balance,
  pr.net_interest_income,
  pr.fee_income,
  COALESCE(dc.direct_cost, 0) AS direct_cost,
  COALESCE(ac.allocated_cost, 0) AS allocated_cost,
  COALESCE(cc.credit_cost, 0) AS credit_cost,
  pr.net_interest_income + pr.fee_income
    - COALESCE(dc.direct_cost, 0) - COALESCE(ac.allocated_cost, 0)
    - COALESCE(cc.credit_cost, 0) AS net_contribution,
  (pr.net_interest_income + pr.fee_income
    - COALESCE(dc.direct_cost, 0) - COALESCE(ac.allocated_cost, 0)
    - COALESCE(cc.credit_cost, 0)) / NULLIF(pr.avg_balance, 0) * 12 AS return_on_assets
FROM product_revenue pr
JOIN dim.dim_product dp ON dp.product_sk = pr.product_sk AND dp.is_current = TRUE
LEFT JOIN product_direct_cost dc ON dc.product_sk = pr.product_sk AND dc.accounting_period = pr.accounting_period
LEFT JOIN product_allocated_cost ac ON ac.product_sk = pr.product_sk AND ac.accounting_period = pr.accounting_period
LEFT JOIN product_credit_cost cc ON cc.product_sk = pr.product_sk AND cc.accounting_period = pr.accounting_period;""",
    ),
    dict(
        workflow="profitability", job="pnl_close", category="P&L",
        title="Branch profitability with occupancy and channel cost",
        description=(
            "Branch-level P&L: locally-booked spread and fee revenue net of staffing, occupancy "
            "and allocated channel cost, with revenue per FTE and per square foot."
        ),
        sql="""INSERT INTO mart.branch_pnl (
  accounting_period, branch_sk, branch_name, region_code, total_revenue,
  staffing_cost, occupancy_cost, allocated_cost, net_contribution,
  revenue_per_fte, revenue_per_sqft
)
WITH branch_revenue AS (
  SELECT
    DATE_TRUNC('month', n.as_of_date) AS accounting_period,
    n.branch_sk,
    SUM(n.net_interest_income) AS spread_revenue
  FROM finance.nii_daily n
  WHERE n.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', n.as_of_date), n.branch_sk
),
branch_fees AS (
  SELECT
    DATE_TRUNC('month', f.as_of_date) AS accounting_period,
    f.branch_sk,
    SUM(f.net_fee) AS fee_revenue
  FROM fact.fee_income f
  WHERE f.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', f.as_of_date), f.branch_sk
),
branch_staffing AS (
  SELECT
    pe.accounting_period,
    db.branch_sk,
    SUM(pe.fully_loaded_cost) AS staffing_cost,
    SUM(pe.fte_count) AS fte_count
  FROM fact.personnel_expense pe
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = pe.cost_center_sk
  JOIN dim.dim_branch db ON db.cost_center_id = cc.cost_center_id AND db.is_current = TRUE
  WHERE pe.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY pe.accounting_period, db.branch_sk
),
branch_occupancy AS (
  SELECT
    e.accounting_period,
    db.branch_sk,
    SUM(e.direct_expense) AS occupancy_cost
  FROM fact.expense e
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = e.cost_center_sk
  JOIN dim.dim_branch db ON db.cost_center_id = cc.cost_center_id AND db.is_current = TRUE
  WHERE e.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    AND e.expense_category = 'OCCUPANCY'
  GROUP BY e.accounting_period, db.branch_sk
),
branch_allocated AS (
  SELECT
    a.accounting_period,
    db.branch_sk,
    SUM(a.allocated_amount) AS allocated_cost
  FROM finance.allocation_result a
  JOIN dim.dim_cost_center cc ON cc.cost_center_sk = a.receiver_cost_center_sk
  JOIN dim.dim_branch db ON db.cost_center_id = cc.cost_center_id AND db.is_current = TRUE
  WHERE a.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY a.accounting_period, db.branch_sk
)
SELECT
  br.accounting_period,
  br.branch_sk,
  db.branch_name,
  db.region_code,
  br.spread_revenue + COALESCE(bf.fee_revenue, 0) AS total_revenue,
  COALESCE(bs.staffing_cost, 0) AS staffing_cost,
  COALESCE(bo.occupancy_cost, 0) AS occupancy_cost,
  COALESCE(ba.allocated_cost, 0) AS allocated_cost,
  br.spread_revenue + COALESCE(bf.fee_revenue, 0)
    - COALESCE(bs.staffing_cost, 0) - COALESCE(bo.occupancy_cost, 0)
    - COALESCE(ba.allocated_cost, 0) AS net_contribution,
  (br.spread_revenue + COALESCE(bf.fee_revenue, 0)) / NULLIF(bs.fte_count, 0) AS revenue_per_fte,
  (br.spread_revenue + COALESCE(bf.fee_revenue, 0)) / NULLIF(db.floor_area_sqft, 0) AS revenue_per_sqft
FROM branch_revenue br
JOIN dim.dim_branch db ON db.branch_sk = br.branch_sk AND db.is_current = TRUE
LEFT JOIN branch_fees bf ON bf.branch_sk = br.branch_sk AND bf.accounting_period = br.accounting_period
LEFT JOIN branch_staffing bs ON bs.branch_sk = br.branch_sk AND bs.accounting_period = br.accounting_period
LEFT JOIN branch_occupancy bo ON bo.branch_sk = br.branch_sk AND bo.accounting_period = br.accounting_period
LEFT JOIN branch_allocated ba ON ba.branch_sk = br.branch_sk AND ba.accounting_period = br.accounting_period;""",
    ),
    dict(
        workflow="profitability", job="pnl_close", category="P&L",
        title="Customer-level P&L with cost-to-serve",
        description=(
            "Customer profitability: relationship revenue less activity-based cost-to-serve and "
            "allocated credit cost, ranked into profitability deciles for relationship management."
        ),
        sql="""INSERT INTO mart.customer_pnl (
  accounting_period, customer_sk, segment_code, total_revenue, cost_to_serve,
  credit_cost, net_profit, profit_margin, profitability_decile
)
WITH revenue AS (
  SELECT
    cr.as_of_month AS accounting_period,
    cr.customer_sk,
    cr.segment_code,
    cr.total_revenue
  FROM mart.customer_revenue cr
  WHERE cr.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
),
activity_cost AS (
  SELECT
    DATE_TRUNC('month', ae.event_date) AS accounting_period,
    dc.customer_sk,
    SUM(ae.event_count * ur.unit_cost) AS cost_to_serve
  FROM ops.customer_activity_event ae
  JOIN dim.dim_customer dc ON dc.customer_id = ae.customer_id AND dc.is_current = TRUE
  JOIN ref.activity_unit_cost ur ON ur.activity_code = ae.activity_code
  WHERE DATE_TRUNC('month', ae.event_date) = DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', ae.event_date), dc.customer_sk
),
credit_cost AS (
  SELECT
    DATE_TRUNC('month', p.as_of_date) AS accounting_period,
    f.customer_sk,
    SUM(p.discounted_ecl) AS credit_cost
  FROM risk.ecl_provision p
  JOIN fact.loan_balance_daily f
    ON f.account_sk = p.account_sk AND f.as_of_date = p.as_of_date
  WHERE p.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', p.as_of_date), f.customer_sk
),
assembled AS (
  SELECT
    rv.accounting_period,
    rv.customer_sk,
    rv.segment_code,
    rv.total_revenue,
    COALESCE(ac.cost_to_serve, 0) AS cost_to_serve,
    COALESCE(cc.credit_cost, 0) AS credit_cost,
    rv.total_revenue - COALESCE(ac.cost_to_serve, 0) - COALESCE(cc.credit_cost, 0) AS net_profit
  FROM revenue rv
  LEFT JOIN activity_cost ac
    ON ac.customer_sk = rv.customer_sk AND ac.accounting_period = rv.accounting_period
  LEFT JOIN credit_cost cc
    ON cc.customer_sk = rv.customer_sk AND cc.accounting_period = rv.accounting_period
)
SELECT
  a.accounting_period,
  a.customer_sk,
  a.segment_code,
  a.total_revenue,
  a.cost_to_serve,
  a.credit_cost,
  a.net_profit,
  a.net_profit / NULLIF(a.total_revenue, 0) AS profit_margin,
  NTILE(10) OVER (PARTITION BY a.segment_code ORDER BY a.net_profit DESC) AS profitability_decile
FROM assembled a;""",
    ),
    dict(
        workflow="profitability", job="pnl_close", category="P&L",
        title="P&L actual versus budget and prior year variance",
        description=(
            "Three-way P&L variance — actual against approved budget and against the same period "
            "last year — at every level of the management hierarchy."
        ),
        sql="""INSERT INTO mart.pnl_variance (
  accounting_period, lob_code, metric_name, actual_amount, budget_amount,
  prior_year_amount, budget_variance, budget_variance_pct, yoy_variance, yoy_variance_pct
)
WITH actual_unpivoted AS (
  SELECT p.accounting_period, p.lob_code, 'TOTAL_REVENUE' AS metric_name, p.total_revenue AS actual_amount
  FROM finance.pnl_actual p WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  UNION ALL
  SELECT p.accounting_period, p.lob_code, 'TOTAL_EXPENSE', p.total_expense
  FROM finance.pnl_actual p WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  UNION ALL
  SELECT p.accounting_period, p.lob_code, 'CREDIT_PROVISION', p.credit_provision
  FROM finance.pnl_actual p WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  UNION ALL
  SELECT p.accounting_period, p.lob_code, 'NET_INCOME', p.net_income
  FROM finance.pnl_actual p WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
),
budget AS (
  SELECT b.accounting_period, b.lob_code, b.metric_name, SUM(b.budget_amount) AS budget_amount
  FROM finance.pnl_budget b
  WHERE b.accounting_period = DATE_TRUNC('month', CURRENT_DATE) AND b.budget_version = 'APPROVED'
  GROUP BY b.accounting_period, b.lob_code, b.metric_name
),
prior_year AS (
  SELECT
    py.accounting_period + INTERVAL '12 month' AS accounting_period,
    py.lob_code,
    'NET_INCOME' AS metric_name,
    py.net_income AS prior_year_amount
  FROM finance.pnl_actual py
  WHERE py.accounting_period = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 month'
)
SELECT
  a.accounting_period,
  a.lob_code,
  a.metric_name,
  a.actual_amount,
  COALESCE(b.budget_amount, 0) AS budget_amount,
  COALESCE(py.prior_year_amount, 0) AS prior_year_amount,
  a.actual_amount - COALESCE(b.budget_amount, 0) AS budget_variance,
  (a.actual_amount - COALESCE(b.budget_amount, 0)) / NULLIF(b.budget_amount, 0) AS budget_variance_pct,
  a.actual_amount - COALESCE(py.prior_year_amount, 0) AS yoy_variance,
  (a.actual_amount - COALESCE(py.prior_year_amount, 0)) / NULLIF(py.prior_year_amount, 0) AS yoy_variance_pct
FROM actual_unpivoted a
LEFT JOIN budget b
  ON b.lob_code = a.lob_code AND b.metric_name = a.metric_name
 AND b.accounting_period = a.accounting_period
LEFT JOIN prior_year py
  ON py.lob_code = a.lob_code AND py.metric_name = a.metric_name
 AND py.accounting_period = a.accounting_period;""",
    ),
    dict(
        workflow="profitability", job="pnl_close", category="P&L",
        title="Month-end P&L close with adjustments and eliminations",
        description=(
            "Applies late manual journal adjustments and intercompany eliminations to the "
            "preliminary P&L, producing the final closed position with a full audit trail of what moved."
        ),
        sql="""MERGE INTO finance.pnl_closed AS tgt
USING (
  WITH preliminary AS (
    SELECT
      p.accounting_period, p.lob_code, p.total_revenue, p.total_expense,
      p.credit_provision, p.net_income
    FROM finance.pnl_actual p
    WHERE p.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
  ),
  adjustments AS (
    SELECT
      j.accounting_period,
      j.lob_code,
      SUM(CASE WHEN j.adjustment_type = 'REVENUE' THEN j.adjustment_amount ELSE 0 END) AS revenue_adj,
      SUM(CASE WHEN j.adjustment_type = 'EXPENSE' THEN j.adjustment_amount ELSE 0 END) AS expense_adj,
      SUM(CASE WHEN j.adjustment_type = 'PROVISION' THEN j.adjustment_amount ELSE 0 END) AS provision_adj
    FROM gl.manual_journal_adjustment j
    WHERE j.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
      AND j.approval_status = 'APPROVED'
    GROUP BY j.accounting_period, j.lob_code
  ),
  eliminations AS (
    SELECT
      e.accounting_period, e.lob_code, SUM(e.elimination_amount) AS elimination_amount
    FROM gl.intercompany_elimination e
    WHERE e.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY e.accounting_period, e.lob_code
  )
  SELECT
    pr.accounting_period,
    pr.lob_code,
    pr.total_revenue + COALESCE(ad.revenue_adj, 0) - COALESCE(el.elimination_amount, 0) AS final_revenue,
    pr.total_expense + COALESCE(ad.expense_adj, 0) AS final_expense,
    pr.credit_provision + COALESCE(ad.provision_adj, 0) AS final_provision,
    COALESCE(ad.revenue_adj, 0) - COALESCE(ad.expense_adj, 0) - COALESCE(ad.provision_adj, 0) AS total_adjustment
  FROM preliminary pr
  LEFT JOIN adjustments ad ON ad.lob_code = pr.lob_code AND ad.accounting_period = pr.accounting_period
  LEFT JOIN eliminations el ON el.lob_code = pr.lob_code AND el.accounting_period = pr.accounting_period
) AS src
  ON tgt.accounting_period = src.accounting_period AND tgt.lob_code = src.lob_code
WHEN MATCHED THEN
  UPDATE SET
    final_revenue = src.final_revenue,
    final_expense = src.final_expense,
    final_provision = src.final_provision,
    total_adjustment = src.total_adjustment,
    close_status = 'CLOSED'
WHEN NOT MATCHED THEN
  INSERT (accounting_period, lob_code, final_revenue, final_expense, final_provision, total_adjustment, close_status)
  VALUES (src.accounting_period, src.lob_code, src.final_revenue, src.final_expense,
          src.final_provision, src.total_adjustment, 'CLOSED');""",
    ),
]


BANKING_EXAMPLES += [
    dict(
        workflow="riskcapital", job="rwa_capital", category="Risk & Capital",
        title="Credit risk-weighted assets under the standardised approach",
        description=(
            "Applies Basel standardised risk weights by exposure class and external rating to "
            "on-balance exposures and credit-converted off-balance commitments, net of eligible "
            "collateral mitigation."
        ),
        sql="""INSERT INTO risk.rwa_credit (
  as_of_date, account_sk, exposure_class, external_rating, on_balance_exposure,
  off_balance_exposure, eligible_collateral, net_exposure, risk_weight, risk_weighted_assets
)
WITH exposures AS (
  SELECT
    f.as_of_date, f.account_sk, f.customer_sk, f.product_sk,
    f.outstanding_principal + f.accrued_interest AS on_balance_exposure
  FROM fact.loan_balance_daily f
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
off_balance AS (
  SELECT
    u.account_sk,
    SUM(u.undrawn_amount * ccf.credit_conversion_factor) AS off_balance_exposure
  FROM core.loan_undrawn_commitment u
  JOIN ref.credit_conversion_factor ccf ON ccf.facility_type = u.facility_type
  GROUP BY u.account_sk
),
collateral AS (
  SELECT
    c.account_sk,
    SUM(c.appraised_value * (1 - ch.supervisory_haircut)) AS eligible_collateral
  FROM core.loan_collateral c
  JOIN ref.collateral_haircut ch ON ch.collateral_type = c.collateral_type
  WHERE ch.is_eligible = TRUE
  GROUP BY c.account_sk
),
classified AS (
  SELECT
    e.as_of_date,
    e.account_sk,
    ec.exposure_class,
    cr.external_rating,
    e.on_balance_exposure,
    COALESCE(ob.off_balance_exposure, 0) AS off_balance_exposure,
    COALESCE(col.eligible_collateral, 0) AS eligible_collateral
  FROM exposures e
  JOIN dim.dim_customer dc ON dc.customer_sk = e.customer_sk AND dc.is_current = TRUE
  JOIN dim.dim_product dp ON dp.product_sk = e.product_sk AND dp.is_current = TRUE
  JOIN ref.exposure_classification ec
    ON ec.product_class = dp.product_class AND ec.counterparty_type = dc.counterparty_type
  LEFT JOIN risk.counterparty_rating cr ON cr.customer_sk = e.customer_sk
  LEFT JOIN off_balance ob ON ob.account_sk = e.account_sk
  LEFT JOIN collateral col ON col.account_sk = e.account_sk
)
SELECT
  c.as_of_date,
  c.account_sk,
  c.exposure_class,
  c.external_rating,
  c.on_balance_exposure,
  c.off_balance_exposure,
  c.eligible_collateral,
  GREATEST(c.on_balance_exposure + c.off_balance_exposure - c.eligible_collateral, 0) AS net_exposure,
  rw.risk_weight,
  GREATEST(c.on_balance_exposure + c.off_balance_exposure - c.eligible_collateral, 0)
    * rw.risk_weight AS risk_weighted_assets
FROM classified c
JOIN ref.basel_risk_weight rw
  ON rw.exposure_class = c.exposure_class
 AND COALESCE(c.external_rating, 'UNRATED') = rw.rating_bucket;""",
    ),
    dict(
        workflow="riskcapital", job="rwa_capital", category="Risk & Capital",
        title="Economic capital allocation across risk types",
        description=(
            "Aggregates credit, market, operational and interest-rate risk capital with a "
            "correlation-based diversification benefit, then pushes the diversified total back to "
            "business lines on a standalone-contribution basis."
        ),
        sql="""INSERT INTO risk.economic_capital (
  as_of_date, lob_code, credit_capital, market_capital, operational_capital,
  irrbb_capital, undiversified_capital, diversification_benefit, diversified_capital
)
WITH credit_cap AS (
  SELECT
    r.as_of_date,
    o.lob_code,
    SUM(r.risk_weighted_assets) * cr.capital_ratio AS credit_capital
  FROM risk.rwa_credit r
  JOIN fact.loan_balance_daily f ON f.account_sk = r.account_sk AND f.as_of_date = r.as_of_date
  JOIN dim.dim_product dp ON dp.product_sk = f.product_sk
  JOIN dim.org_hierarchy o ON o.segment_code = dp.segment_code
  CROSS JOIN ref.capital_requirement cr
  WHERE r.as_of_date = CURRENT_DATE - INTERVAL '1 day' AND cr.risk_type = 'CREDIT'
  GROUP BY r.as_of_date, o.lob_code, cr.capital_ratio
),
market_cap AS (
  SELECT m.as_of_date, m.lob_code, SUM(m.var_99 * mult.capital_multiplier) AS market_capital
  FROM risk.market_var m
  CROSS JOIN ref.capital_requirement mult
  WHERE m.as_of_date = CURRENT_DATE - INTERVAL '1 day' AND mult.risk_type = 'MARKET'
  GROUP BY m.as_of_date, m.lob_code
),
operational_cap AS (
  SELECT
    op.as_of_date, o.lob_code, SUM(op.gross_income * bi.beta_factor) AS operational_capital
  FROM risk.operational_gross_income op
  JOIN dim.org_hierarchy o ON o.lob_code = op.lob_code
  JOIN ref.business_indicator bi ON bi.business_line = op.business_line
  WHERE op.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY op.as_of_date, o.lob_code
),
irrbb_cap AS (
  SELECT e.as_of_date, e.lob_code, SUM(ABS(e.eve_sensitivity)) AS irrbb_capital
  FROM alm.eve_sensitivity e
  WHERE e.as_of_date = CURRENT_DATE - INTERVAL '1 day' AND e.scenario_code = 'PARALLEL_UP_200'
  GROUP BY e.as_of_date, e.lob_code
),
combined AS (
  SELECT
    cc.as_of_date,
    cc.lob_code,
    cc.credit_capital,
    COALESCE(mc.market_capital, 0) AS market_capital,
    COALESCE(oc.operational_capital, 0) AS operational_capital,
    COALESCE(ic.irrbb_capital, 0) AS irrbb_capital
  FROM credit_cap cc
  LEFT JOIN market_cap mc ON mc.lob_code = cc.lob_code AND mc.as_of_date = cc.as_of_date
  LEFT JOIN operational_cap oc ON oc.lob_code = cc.lob_code AND oc.as_of_date = cc.as_of_date
  LEFT JOIN irrbb_cap ic ON ic.lob_code = cc.lob_code AND ic.as_of_date = cc.as_of_date
)
SELECT
  c.as_of_date,
  c.lob_code,
  c.credit_capital,
  c.market_capital,
  c.operational_capital,
  c.irrbb_capital,
  c.credit_capital + c.market_capital + c.operational_capital + c.irrbb_capital AS undiversified_capital,
  (c.credit_capital + c.market_capital + c.operational_capital + c.irrbb_capital)
    * (1 - dm.diversification_factor) AS diversification_benefit,
  (c.credit_capital + c.market_capital + c.operational_capital + c.irrbb_capital)
    * dm.diversification_factor AS diversified_capital
FROM combined c
JOIN ref.diversification_matrix dm ON dm.lob_code = c.lob_code;""",
    ),
    dict(
        workflow="riskcapital", job="rwa_capital", category="Risk & Capital",
        title="RAROC by product and customer segment",
        description=(
            "Risk-adjusted return on capital: risk-adjusted earnings (revenue less expected loss "
            "and cost) over allocated economic capital, compared to the hurdle rate."
        ),
        sql="""INSERT INTO mart.raroc (
  accounting_period, lob_code, product_sk, risk_adjusted_revenue, expected_loss,
  operating_cost, allocated_capital, raroc, hurdle_rate, economic_profit
)
WITH revenue AS (
  SELECT
    r.as_of_month AS accounting_period,
    r.product_sk,
    o.lob_code,
    r.total_revenue
  FROM finance.revenue_summary r
  JOIN dim.dim_product dp ON dp.product_sk = r.product_sk
  JOIN dim.org_hierarchy o ON o.segment_code = dp.segment_code
  WHERE r.as_of_month = DATE_TRUNC('month', CURRENT_DATE)
),
expected_loss AS (
  SELECT
    DATE_TRUNC('month', p.as_of_date) AS accounting_period,
    f.product_sk,
    SUM(p.discounted_ecl) AS expected_loss
  FROM risk.ecl_provision p
  JOIN fact.loan_balance_daily f ON f.account_sk = p.account_sk AND f.as_of_date = p.as_of_date
  WHERE p.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', p.as_of_date), f.product_sk
),
cost AS (
  SELECT
    pp.accounting_period, pp.product_sk,
    pp.direct_cost + pp.allocated_cost AS operating_cost
  FROM mart.product_pnl pp
  WHERE pp.accounting_period = DATE_TRUNC('month', CURRENT_DATE)
),
capital AS (
  SELECT
    DATE_TRUNC('month', ec.as_of_date) AS accounting_period,
    ec.lob_code,
    AVG(ec.diversified_capital) AS allocated_capital
  FROM risk.economic_capital ec
  WHERE ec.as_of_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', ec.as_of_date), ec.lob_code
)
SELECT
  rv.accounting_period,
  rv.lob_code,
  rv.product_sk,
  rv.total_revenue AS risk_adjusted_revenue,
  COALESCE(el.expected_loss, 0) AS expected_loss,
  COALESCE(c.operating_cost, 0) AS operating_cost,
  cap.allocated_capital,
  (rv.total_revenue - COALESCE(el.expected_loss, 0) - COALESCE(c.operating_cost, 0)) * 12
    / NULLIF(cap.allocated_capital, 0) AS raroc,
  hr.hurdle_rate,
  (rv.total_revenue - COALESCE(el.expected_loss, 0) - COALESCE(c.operating_cost, 0)) * 12
    - cap.allocated_capital * hr.hurdle_rate AS economic_profit
FROM revenue rv
LEFT JOIN expected_loss el ON el.product_sk = rv.product_sk AND el.accounting_period = rv.accounting_period
LEFT JOIN cost c ON c.product_sk = rv.product_sk AND c.accounting_period = rv.accounting_period
JOIN capital cap ON cap.lob_code = rv.lob_code AND cap.accounting_period = rv.accounting_period
JOIN ref.hurdle_rate hr ON hr.lob_code = rv.lob_code;""",
    ),
    dict(
        workflow="riskcapital", job="rwa_capital", category="Risk & Capital",
        title="Capital adequacy ratios against regulatory minimums",
        description=(
            "Builds CET1, tier 1 and total capital from the regulatory capital stack over total "
            "RWA, and compares each ratio to its minimum plus buffer requirement."
        ),
        sql="""INSERT INTO reg.capital_adequacy (
  as_of_date, cet1_capital, tier1_capital, total_capital, total_rwa,
  cet1_ratio, tier1_ratio, total_capital_ratio, minimum_requirement, buffer_surplus
)
WITH capital_stack AS (
  SELECT
    c.as_of_date,
    SUM(CASE WHEN c.capital_tier = 'CET1' THEN c.capital_amount ELSE 0 END) AS cet1_gross,
    SUM(CASE WHEN c.capital_tier = 'AT1' THEN c.capital_amount ELSE 0 END) AS at1_capital,
    SUM(CASE WHEN c.capital_tier = 'TIER2' THEN c.capital_amount ELSE 0 END) AS tier2_capital
  FROM reg.regulatory_capital_component c
  WHERE c.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY c.as_of_date
),
deductions AS (
  SELECT
    d.as_of_date, SUM(d.deduction_amount) AS total_deductions
  FROM reg.capital_deduction d
  WHERE d.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY d.as_of_date
),
rwa_total AS (
  SELECT
    r.as_of_date,
    SUM(r.risk_weighted_assets) AS credit_rwa
  FROM risk.rwa_credit r
  WHERE r.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY r.as_of_date
),
other_rwa AS (
  SELECT
    o.as_of_date,
    SUM(o.rwa_amount) AS other_rwa
  FROM risk.rwa_other o
  WHERE o.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY o.as_of_date
)
SELECT
  cs.as_of_date,
  cs.cet1_gross - COALESCE(dd.total_deductions, 0) AS cet1_capital,
  cs.cet1_gross - COALESCE(dd.total_deductions, 0) + cs.at1_capital AS tier1_capital,
  cs.cet1_gross - COALESCE(dd.total_deductions, 0) + cs.at1_capital + cs.tier2_capital AS total_capital,
  rt.credit_rwa + COALESCE(orw.other_rwa, 0) AS total_rwa,
  (cs.cet1_gross - COALESCE(dd.total_deductions, 0))
    / NULLIF(rt.credit_rwa + COALESCE(orw.other_rwa, 0), 0) AS cet1_ratio,
  (cs.cet1_gross - COALESCE(dd.total_deductions, 0) + cs.at1_capital)
    / NULLIF(rt.credit_rwa + COALESCE(orw.other_rwa, 0), 0) AS tier1_ratio,
  (cs.cet1_gross - COALESCE(dd.total_deductions, 0) + cs.at1_capital + cs.tier2_capital)
    / NULLIF(rt.credit_rwa + COALESCE(orw.other_rwa, 0), 0) AS total_capital_ratio,
  rr.minimum_ratio + rr.conservation_buffer AS minimum_requirement,
  (cs.cet1_gross - COALESCE(dd.total_deductions, 0))
    / NULLIF(rt.credit_rwa + COALESCE(orw.other_rwa, 0), 0)
    - (rr.minimum_ratio + rr.conservation_buffer) AS buffer_surplus
FROM capital_stack cs
JOIN rwa_total rt ON rt.as_of_date = cs.as_of_date
LEFT JOIN other_rwa orw ON orw.as_of_date = cs.as_of_date
LEFT JOIN deductions dd ON dd.as_of_date = cs.as_of_date
CROSS JOIN ref.regulatory_ratio_requirement rr
WHERE rr.ratio_type = 'CET1';""",
    ),
    dict(
        workflow="riskcapital", job="stress_testing", category="Risk & Capital",
        title="Stress-test pre-provision net revenue projection",
        description=(
            "Projects PPNR across the nine-quarter stress horizon by applying scenario "
            "macroeconomic factor sensitivities to the current revenue and expense run-rate."
        ),
        sql="""INSERT INTO risk.stress_ppnr (
  scenario_code, projection_quarter, lob_code, baseline_revenue, baseline_expense,
  gdp_impact, unemployment_impact, rate_impact, stressed_revenue, stressed_expense, stressed_ppnr
)
WITH baseline AS (
  SELECT
    o.lob_code,
    SUM(p.total_revenue) AS baseline_revenue,
    SUM(p.total_expense) AS baseline_expense
  FROM finance.pnl_actual p
  JOIN dim.org_hierarchy o ON o.lob_code = p.lob_code
  WHERE p.accounting_period >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '3 month'
  GROUP BY o.lob_code
),
scenario_path AS (
  SELECT
    s.scenario_code, s.projection_quarter, s.gdp_growth, s.unemployment_rate, s.short_rate
  FROM risk.stress_scenario_path s
  WHERE s.scenario_version = (SELECT MAX(s2.scenario_version) FROM risk.stress_scenario_path s2)
),
sensitivities AS (
  SELECT
    sn.lob_code, sn.gdp_beta, sn.unemployment_beta, sn.rate_beta
  FROM risk.ppnr_sensitivity sn
),
projected AS (
  SELECT
    sp.scenario_code,
    sp.projection_quarter,
    b.lob_code,
    b.baseline_revenue,
    b.baseline_expense,
    sp.gdp_growth * sn.gdp_beta * b.baseline_revenue AS gdp_impact,
    sp.unemployment_rate * sn.unemployment_beta * b.baseline_revenue AS unemployment_impact,
    sp.short_rate * sn.rate_beta * b.baseline_revenue AS rate_impact
  FROM baseline b
  JOIN sensitivities sn ON sn.lob_code = b.lob_code
  CROSS JOIN scenario_path sp
)
SELECT
  p.scenario_code,
  p.projection_quarter,
  p.lob_code,
  p.baseline_revenue,
  p.baseline_expense,
  p.gdp_impact,
  p.unemployment_impact,
  p.rate_impact,
  p.baseline_revenue + p.gdp_impact + p.unemployment_impact + p.rate_impact AS stressed_revenue,
  p.baseline_expense * ef.expense_stress_factor AS stressed_expense,
  p.baseline_revenue + p.gdp_impact + p.unemployment_impact + p.rate_impact
    - p.baseline_expense * ef.expense_stress_factor AS stressed_ppnr
FROM projected p
JOIN ref.expense_stress_factor ef
  ON ef.scenario_code = p.scenario_code AND ef.projection_quarter = p.projection_quarter;""",
    ),
    dict(
        workflow="riskcapital", job="stress_testing", category="Risk & Capital",
        title="Single-name and sector concentration against limits",
        description=(
            "Aggregates exposure to the ultimate parent across every product and legal entity, "
            "then tests it against single-name and sector limits expressed as a share of capital."
        ),
        sql="""INSERT INTO risk.concentration_breach (
  as_of_date, ultimate_parent_id, sector_code, total_exposure, eligible_capital,
  exposure_pct_capital, single_name_limit_pct, sector_limit_pct, breach_status
)
WITH group_exposure AS (
  SELECT
    f.as_of_date,
    COALESCE(h.ultimate_parent_id, dc.customer_id) AS ultimate_parent_id,
    dc.sector_code,
    SUM(f.outstanding_principal + f.accrued_interest) AS funded_exposure,
    SUM(COALESCE(u.undrawn_amount, 0)) AS unfunded_exposure
  FROM fact.loan_balance_daily f
  JOIN dim.dim_customer dc ON dc.customer_sk = f.customer_sk AND dc.is_current = TRUE
  LEFT JOIN dim.customer_hierarchy h ON h.customer_id = dc.customer_id
  LEFT JOIN core.loan_undrawn_commitment u ON u.account_sk = f.account_sk
  WHERE f.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY f.as_of_date, COALESCE(h.ultimate_parent_id, dc.customer_id), dc.sector_code
),
capital_base AS (
  SELECT ca.as_of_date, ca.tier1_capital AS eligible_capital
  FROM reg.capital_adequacy ca
  WHERE ca.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
sector_totals AS (
  SELECT
    ge.as_of_date, ge.sector_code,
    SUM(ge.funded_exposure + ge.unfunded_exposure) AS sector_exposure
  FROM group_exposure ge
  GROUP BY ge.as_of_date, ge.sector_code
)
SELECT
  ge.as_of_date,
  ge.ultimate_parent_id,
  ge.sector_code,
  ge.funded_exposure + ge.unfunded_exposure AS total_exposure,
  cb.eligible_capital,
  (ge.funded_exposure + ge.unfunded_exposure) / NULLIF(cb.eligible_capital, 0) AS exposure_pct_capital,
  lim.single_name_limit_pct,
  lim.sector_limit_pct,
  CASE
    WHEN (ge.funded_exposure + ge.unfunded_exposure) / NULLIF(cb.eligible_capital, 0)
         > lim.single_name_limit_pct THEN 'SINGLE_NAME_BREACH'
    WHEN st.sector_exposure / NULLIF(cb.eligible_capital, 0) > lim.sector_limit_pct THEN 'SECTOR_BREACH'
    ELSE 'WITHIN_LIMIT'
  END AS breach_status
FROM group_exposure ge
JOIN capital_base cb ON cb.as_of_date = ge.as_of_date
JOIN sector_totals st ON st.sector_code = ge.sector_code AND st.as_of_date = ge.as_of_date
JOIN ref.concentration_limit lim ON lim.sector_code = ge.sector_code;""",
    ),
]


BANKING_EXAMPLES += [
    dict(
        workflow="treasury", job="alm_positions", category="ALM & Treasury",
        title="Interest rate repricing gap by time bucket",
        description=(
            "Slots every asset and liability into its repricing time bucket — contractual for term "
            "instruments, behavioural for non-maturity deposits — to produce the repricing gap "
            "ladder and cumulative gap the ALCO reviews."
        ),
        sql="""INSERT INTO alm.repricing_gap (
  as_of_date, time_bucket, bucket_order, rate_sensitive_assets,
  rate_sensitive_liabilities, period_gap, cumulative_gap, gap_to_assets_ratio
)
WITH asset_slotting AS (
  SELECT
    l.as_of_date,
    tb.bucket_code,
    tb.bucket_order,
    SUM(l.outstanding_principal) AS asset_amount
  FROM fact.loan_balance_daily l
  JOIN ref.time_bucket tb
    ON DATE_PART('day', l.next_reprice_date - l.as_of_date)
       BETWEEN tb.lower_days AND tb.upper_days
  WHERE l.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY l.as_of_date, tb.bucket_code, tb.bucket_order
),
liability_slotting AS (
  SELECT
    d.as_of_date,
    tb.bucket_code,
    tb.bucket_order,
    SUM(
      CASE WHEN dp.maturity_type = 'TERM' THEN d.eod_balance
           ELSE d.core_balance * bd.decay_rate + d.volatile_balance END
    ) AS liability_amount
  FROM fact.deposit_balance_daily d
  JOIN dim.dim_product dp ON dp.product_sk = d.product_sk
  LEFT JOIN alm.nmd_decay_profile bd ON bd.product_sk = d.product_sk
  JOIN ref.time_bucket tb
    ON tb.bucket_code = COALESCE(bd.bucket_code, dp.default_repricing_bucket)
  WHERE d.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY d.as_of_date, tb.bucket_code, tb.bucket_order
),
combined AS (
  SELECT
    COALESCE(a.as_of_date, l.as_of_date) AS as_of_date,
    COALESCE(a.bucket_code, l.bucket_code) AS bucket_code,
    COALESCE(a.bucket_order, l.bucket_order) AS bucket_order,
    COALESCE(a.asset_amount, 0) AS rate_sensitive_assets,
    COALESCE(l.liability_amount, 0) AS rate_sensitive_liabilities
  FROM asset_slotting a
  FULL OUTER JOIN liability_slotting l
    ON l.bucket_code = a.bucket_code AND l.as_of_date = a.as_of_date
),
total_assets AS (
  SELECT c.as_of_date, SUM(c.rate_sensitive_assets) AS total_assets
  FROM combined c GROUP BY c.as_of_date
)
SELECT
  c.as_of_date,
  c.bucket_code AS time_bucket,
  c.bucket_order,
  c.rate_sensitive_assets,
  c.rate_sensitive_liabilities,
  c.rate_sensitive_assets - c.rate_sensitive_liabilities AS period_gap,
  SUM(c.rate_sensitive_assets - c.rate_sensitive_liabilities) OVER (
    ORDER BY c.bucket_order ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_gap,
  (c.rate_sensitive_assets - c.rate_sensitive_liabilities) / NULLIF(ta.total_assets, 0) AS gap_to_assets_ratio
FROM combined c
JOIN total_assets ta ON ta.as_of_date = c.as_of_date;""",
    ),
    dict(
        workflow="treasury", job="alm_positions", category="ALM & Treasury",
        title="Liquidity coverage ratio with HQLA haircuts and outflow rates",
        description=(
            "Builds the LCR: high-quality liquid assets after supervisory haircuts and level-2 caps, "
            "over 30-day net stressed cash outflows derived from deposit runoff rates by counterparty type."
        ),
        sql="""INSERT INTO reg.liquidity_coverage (
  as_of_date, level1_hqla, level2a_hqla, level2b_hqla, total_hqla,
  stressed_outflows, stressed_inflows, net_cash_outflows, lcr_ratio, lcr_surplus
)
WITH hqla AS (
  SELECT
    s.as_of_date,
    hc.hqla_level,
    SUM(s.market_value * (1 - hc.haircut_pct)) AS post_haircut_value
  FROM treasury.securities_position s
  JOIN ref.hqla_classification hc ON hc.security_type = s.security_type
  WHERE s.as_of_date = CURRENT_DATE - INTERVAL '1 day'
    AND s.encumbrance_status = 'UNENCUMBERED'
  GROUP BY s.as_of_date, hc.hqla_level
),
outflows AS (
  SELECT
    d.as_of_date,
    SUM(d.eod_balance * ro.runoff_rate) AS stressed_outflows
  FROM fact.deposit_balance_daily d
  JOIN dim.dim_customer dc ON dc.customer_sk = d.customer_sk AND dc.is_current = TRUE
  JOIN dim.dim_product dp ON dp.product_sk = d.product_sk
  JOIN ref.lcr_runoff_rate ro
    ON ro.counterparty_type = dc.counterparty_type
   AND ro.deposit_type = dp.maturity_type
   AND ro.is_insured = dp.is_insured
  WHERE d.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY d.as_of_date
),
inflows AS (
  SELECT
    l.as_of_date,
    SUM(LEAST(sch.scheduled_principal + sch.scheduled_interest, l.outstanding_principal)
        * ir.inflow_rate) AS stressed_inflows
  FROM fact.loan_balance_daily l
  JOIN core.loan_payment_schedule sch ON sch.loan_id = l.account_sk
  JOIN dim.dim_product dp ON dp.product_sk = l.product_sk
  JOIN ref.lcr_inflow_rate ir ON ir.product_class = dp.product_class
  WHERE l.as_of_date = CURRENT_DATE - INTERVAL '1 day'
    AND sch.due_date BETWEEN l.as_of_date AND l.as_of_date + INTERVAL '30 day'
  GROUP BY l.as_of_date
),
hqla_stack AS (
  SELECT
    h.as_of_date,
    SUM(CASE WHEN h.hqla_level = 'LEVEL_1' THEN h.post_haircut_value ELSE 0 END) AS level1_hqla,
    SUM(CASE WHEN h.hqla_level = 'LEVEL_2A' THEN h.post_haircut_value ELSE 0 END) AS level2a_hqla,
    SUM(CASE WHEN h.hqla_level = 'LEVEL_2B' THEN h.post_haircut_value ELSE 0 END) AS level2b_hqla
  FROM hqla h
  GROUP BY h.as_of_date
)
SELECT
  hs.as_of_date,
  hs.level1_hqla,
  LEAST(hs.level2a_hqla, (hs.level1_hqla + hs.level2a_hqla) * 0.40) AS level2a_hqla,
  LEAST(hs.level2b_hqla, (hs.level1_hqla + hs.level2a_hqla + hs.level2b_hqla) * 0.15) AS level2b_hqla,
  hs.level1_hqla
    + LEAST(hs.level2a_hqla, (hs.level1_hqla + hs.level2a_hqla) * 0.40)
    + LEAST(hs.level2b_hqla, (hs.level1_hqla + hs.level2a_hqla + hs.level2b_hqla) * 0.15) AS total_hqla,
  o.stressed_outflows,
  LEAST(COALESCE(i.stressed_inflows, 0), o.stressed_outflows * 0.75) AS stressed_inflows,
  o.stressed_outflows - LEAST(COALESCE(i.stressed_inflows, 0), o.stressed_outflows * 0.75) AS net_cash_outflows,
  (hs.level1_hqla + LEAST(hs.level2a_hqla, (hs.level1_hqla + hs.level2a_hqla) * 0.40))
    / NULLIF(o.stressed_outflows - LEAST(COALESCE(i.stressed_inflows, 0), o.stressed_outflows * 0.75), 0) AS lcr_ratio,
  (hs.level1_hqla + LEAST(hs.level2a_hqla, (hs.level1_hqla + hs.level2a_hqla) * 0.40))
    / NULLIF(o.stressed_outflows - LEAST(COALESCE(i.stressed_inflows, 0), o.stressed_outflows * 0.75), 0)
    - rr.minimum_ratio AS lcr_surplus
FROM hqla_stack hs
JOIN outflows o ON o.as_of_date = hs.as_of_date
LEFT JOIN inflows i ON i.as_of_date = hs.as_of_date
CROSS JOIN ref.regulatory_ratio_requirement rr
WHERE rr.ratio_type = 'LCR';""",
    ),
    dict(
        workflow="treasury", job="alm_positions", category="ALM & Treasury",
        title="Behavioural cashflow profile for non-maturity deposits",
        description=(
            "Turns contractually-open deposits into a modelled amortising cashflow ladder using "
            "fitted decay rates, so they can be slotted like term instruments in ALM."
        ),
        sql="""INSERT INTO alm.behavioural_cashflow (
  as_of_date, product_sk, projection_month, opening_balance, decay_rate,
  runoff_amount, closing_balance, discount_factor, present_value
)
WITH RECURSIVE base_balances AS (
  SELECT
    d.as_of_date,
    d.product_sk,
    SUM(d.core_balance) AS core_balance
  FROM fact.deposit_balance_daily d
  JOIN dim.dim_product dp ON dp.product_sk = d.product_sk
  WHERE d.as_of_date = CURRENT_DATE - INTERVAL '1 day'
    AND dp.maturity_type = 'DEMAND'
  GROUP BY d.as_of_date, d.product_sk
),
decay_curve AS (
  SELECT dc.product_sk, dc.projection_month, dc.decay_rate
  FROM alm.nmd_decay_profile dc
),
projection (as_of_date, product_sk, projection_month, opening_balance, decay_rate, runoff_amount, closing_balance) AS (
  SELECT
    bb.as_of_date,
    bb.product_sk,
    1 AS projection_month,
    bb.core_balance AS opening_balance,
    dc.decay_rate,
    bb.core_balance * dc.decay_rate AS runoff_amount,
    bb.core_balance * (1 - dc.decay_rate) AS closing_balance
  FROM base_balances bb
  JOIN decay_curve dc ON dc.product_sk = bb.product_sk AND dc.projection_month = 1
  UNION ALL
  SELECT
    p.as_of_date,
    p.product_sk,
    p.projection_month + 1,
    p.closing_balance,
    dc.decay_rate,
    p.closing_balance * dc.decay_rate,
    p.closing_balance * (1 - dc.decay_rate)
  FROM projection p
  JOIN decay_curve dc ON dc.product_sk = p.product_sk AND dc.projection_month = p.projection_month + 1
  WHERE p.projection_month < 120 AND p.closing_balance > 1000
)
SELECT
  p.as_of_date,
  p.product_sk,
  p.projection_month,
  p.opening_balance,
  p.decay_rate,
  p.runoff_amount,
  p.closing_balance,
  1.0 / POWER(1 + dr.discount_rate / 12.0, p.projection_month) AS discount_factor,
  p.runoff_amount / POWER(1 + dr.discount_rate / 12.0, p.projection_month) AS present_value
FROM projection p
JOIN ref.discount_curve dr
  ON dr.tenor_months = p.projection_month AND dr.as_of_date = p.as_of_date;""",
    ),
    dict(
        workflow="treasury", job="alm_positions", category="ALM & Treasury",
        title="Economic value of equity sensitivity under rate shocks",
        description=(
            "Revalues asset and liability cashflows under each supervisory rate shock to measure "
            "the change in economic value of equity — the IRRBB outlier test."
        ),
        sql="""INSERT INTO alm.eve_sensitivity (
  as_of_date, scenario_code, lob_code, base_asset_pv, base_liability_pv, base_eve,
  shocked_asset_pv, shocked_liability_pv, shocked_eve, eve_sensitivity, eve_pct_capital
)
WITH asset_cashflows AS (
  SELECT
    cf.as_of_date, cf.lob_code, cf.tenor_months, SUM(cf.cashflow_amount) AS cashflow_amount
  FROM alm.instrument_cashflow cf
  WHERE cf.as_of_date = CURRENT_DATE - INTERVAL '1 day' AND cf.instrument_side = 'ASSET'
  GROUP BY cf.as_of_date, cf.lob_code, cf.tenor_months
),
liability_cashflows AS (
  SELECT
    cf.as_of_date, cf.lob_code, cf.tenor_months, SUM(cf.cashflow_amount) AS cashflow_amount
  FROM alm.instrument_cashflow cf
  WHERE cf.as_of_date = CURRENT_DATE - INTERVAL '1 day' AND cf.instrument_side = 'LIABILITY'
  GROUP BY cf.as_of_date, cf.lob_code, cf.tenor_months
),
valued AS (
  SELECT
    ac.as_of_date,
    sh.scenario_code,
    ac.lob_code,
    SUM(ac.cashflow_amount / POWER(1 + bc.zero_rate / 12.0, ac.tenor_months)) AS base_asset_pv,
    SUM(lc.cashflow_amount / POWER(1 + bc.zero_rate / 12.0, lc.tenor_months)) AS base_liability_pv,
    SUM(ac.cashflow_amount / POWER(1 + (bc.zero_rate + sh.shock_bps / 10000.0) / 12.0, ac.tenor_months)) AS shocked_asset_pv,
    SUM(lc.cashflow_amount / POWER(1 + (bc.zero_rate + sh.shock_bps / 10000.0) / 12.0, lc.tenor_months)) AS shocked_liability_pv
  FROM asset_cashflows ac
  JOIN liability_cashflows lc
    ON lc.lob_code = ac.lob_code AND lc.tenor_months = ac.tenor_months AND lc.as_of_date = ac.as_of_date
  JOIN ref.base_yield_curve bc ON bc.tenor_months = ac.tenor_months AND bc.as_of_date = ac.as_of_date
  CROSS JOIN ref.rate_shock_scenario sh
  GROUP BY ac.as_of_date, sh.scenario_code, ac.lob_code
)
SELECT
  v.as_of_date,
  v.scenario_code,
  v.lob_code,
  v.base_asset_pv,
  v.base_liability_pv,
  v.base_asset_pv - v.base_liability_pv AS base_eve,
  v.shocked_asset_pv,
  v.shocked_liability_pv,
  v.shocked_asset_pv - v.shocked_liability_pv AS shocked_eve,
  (v.shocked_asset_pv - v.shocked_liability_pv) - (v.base_asset_pv - v.base_liability_pv) AS eve_sensitivity,
  ((v.shocked_asset_pv - v.shocked_liability_pv) - (v.base_asset_pv - v.base_liability_pv))
    / NULLIF(ca.tier1_capital, 0) AS eve_pct_capital
FROM valued v
JOIN reg.capital_adequacy ca ON ca.as_of_date = v.as_of_date;""",
    ),
    dict(
        workflow="treasury", job="ftp_curves", category="Funds Transfer Pricing",
        title="Bootstrap the funds transfer pricing curve from market instruments",
        description=(
            "Builds the FTP zero curve by bootstrapping observed deposit, swap and debt issuance "
            "rates, then adds the bank's own liquidity premium by tenor."
        ),
        sql="""INSERT INTO ref.ftp_curve (
  as_of_date, curve_id, currency_code, tenor_months, market_rate,
  liquidity_premium, zero_rate, discount_factor
)
WITH market_quotes AS (
  SELECT
    q.as_of_date, q.currency_code, q.tenor_months, q.instrument_type, q.quoted_rate,
    ROW_NUMBER() OVER (
      PARTITION BY q.currency_code, q.tenor_months
      ORDER BY ip.priority_rank
    ) AS source_priority
  FROM treasury.market_quote q
  JOIN ref.instrument_priority ip ON ip.instrument_type = q.instrument_type
  WHERE q.as_of_date = CURRENT_DATE - INTERVAL '1 day'
),
selected_quotes AS (
  SELECT mq.as_of_date, mq.currency_code, mq.tenor_months, mq.quoted_rate AS market_rate
  FROM market_quotes mq
  WHERE mq.source_priority = 1
),
interpolated AS (
  SELECT
    tg.tenor_months,
    sq.currency_code,
    sq.as_of_date,
    COALESCE(
      sq.market_rate,
      LAG(sq.market_rate) OVER (PARTITION BY sq.currency_code ORDER BY tg.tenor_months)
    ) AS market_rate
  FROM ref.tenor_grid tg
  LEFT JOIN selected_quotes sq ON sq.tenor_months = tg.tenor_months
),
premium AS (
  SELECT lp.currency_code, lp.tenor_months, lp.liquidity_premium_bps
  FROM ref.liquidity_premium lp
  WHERE lp.effective_date = CURRENT_DATE - INTERVAL '1 day'
)
SELECT
  i.as_of_date,
  cd.curve_id,
  i.currency_code,
  i.tenor_months,
  i.market_rate,
  COALESCE(p.liquidity_premium_bps, 0) / 10000.0 AS liquidity_premium,
  i.market_rate + COALESCE(p.liquidity_premium_bps, 0) / 10000.0 AS zero_rate,
  1.0 / POWER(1 + (i.market_rate + COALESCE(p.liquidity_premium_bps, 0) / 10000.0) / 12.0,
              i.tenor_months) AS discount_factor
FROM interpolated i
JOIN ref.curve_definition cd ON cd.currency_code = i.currency_code AND cd.curve_purpose = 'FTP'
LEFT JOIN premium p ON p.currency_code = i.currency_code AND p.tenor_months = i.tenor_months
WHERE i.market_rate IS NOT NULL;""",
    ),
    dict(
        workflow="regulatory", job="regulatory_filings", category="Regulatory Reporting",
        title="Call Report schedule RC-C loan classification",
        description=(
            "Maps the internal loan book onto the regulatory loan categories of Call Report "
            "schedule RC-C, splitting by collateral type and borrower purpose as the instructions require."
        ),
        sql="""INSERT INTO reg.call_report_rc_c (
  report_date, line_item, line_description, domestic_amount,
  foreign_amount, total_amount, loan_count
)
WITH loan_book AS (
  SELECT
    f.as_of_date,
    f.account_sk,
    f.outstanding_principal,
    dp.product_class,
    dp.collateral_type,
    dc.borrower_purpose,
    le.is_domestic
  FROM fact.loan_balance_daily f
  JOIN dim.dim_product dp ON dp.product_sk = f.product_sk AND dp.is_current = TRUE
  JOIN dim.dim_customer dc ON dc.customer_sk = f.customer_sk AND dc.is_current = TRUE
  JOIN dim.legal_entity le ON le.legal_entity_id = dc.legal_entity_id
  WHERE f.as_of_date = (SELECT MAX(f2.as_of_date) FROM fact.loan_balance_daily f2)
),
mapped AS (
  SELECT
    lb.as_of_date,
    rm.line_item,
    rm.line_description,
    lb.is_domestic,
    lb.outstanding_principal,
    lb.account_sk
  FROM loan_book lb
  JOIN ref.rc_c_mapping rm
    ON rm.product_class = lb.product_class
   AND rm.collateral_type = lb.collateral_type
   AND rm.borrower_purpose = lb.borrower_purpose
)
SELECT
  m.as_of_date AS report_date,
  m.line_item,
  m.line_description,
  SUM(CASE WHEN m.is_domestic THEN m.outstanding_principal ELSE 0 END) AS domestic_amount,
  SUM(CASE WHEN NOT m.is_domestic THEN m.outstanding_principal ELSE 0 END) AS foreign_amount,
  SUM(m.outstanding_principal) AS total_amount,
  COUNT(DISTINCT m.account_sk) AS loan_count
FROM mapped m
GROUP BY m.as_of_date, m.line_item, m.line_description;""",
    ),
    dict(
        workflow="regulatory", job="regulatory_filings", category="Regulatory Reporting",
        title="FR Y-9C consolidated income statement mapping",
        description=(
            "Maps the management P&L and GL balances onto FR Y-9C schedule HI line items, "
            "including the intercompany eliminations required for the consolidated holding company view."
        ),
        sql="""INSERT INTO reg.fry9c_income (
  report_date, schedule_code, line_item, line_description,
  reported_amount, prior_quarter_amount, variance_amount
)
WITH gl_balances AS (
  SELECT
    DATE_TRUNC('quarter', g.posting_date) AS report_quarter,
    g.gl_account_id,
    SUM(g.debit_amount - g.credit_amount) AS gl_amount
  FROM gl.gl_posting g
  WHERE g.posting_status = 'POSTED'
    AND g.posting_date >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 month'
  GROUP BY DATE_TRUNC('quarter', g.posting_date), g.gl_account_id
),
eliminations AS (
  SELECT
    DATE_TRUNC('quarter', e.accounting_period) AS report_quarter,
    e.gl_account_id,
    SUM(e.elimination_amount) AS elimination_amount
  FROM gl.intercompany_elimination e
  WHERE e.accounting_period >= DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '6 month'
  GROUP BY DATE_TRUNC('quarter', e.accounting_period), e.gl_account_id
),
mapped AS (
  SELECT
    gb.report_quarter,
    rm.schedule_code,
    rm.line_item,
    rm.line_description,
    SUM(gb.gl_amount - COALESCE(el.elimination_amount, 0)) AS reported_amount
  FROM gl_balances gb
  JOIN ref.fry9c_mapping rm ON rm.gl_account_id = gb.gl_account_id
  LEFT JOIN eliminations el
    ON el.gl_account_id = gb.gl_account_id AND el.report_quarter = gb.report_quarter
  GROUP BY gb.report_quarter, rm.schedule_code, rm.line_item, rm.line_description
),
with_prior AS (
  SELECT
    m.report_quarter,
    m.schedule_code,
    m.line_item,
    m.line_description,
    m.reported_amount,
    LAG(m.reported_amount) OVER (
      PARTITION BY m.schedule_code, m.line_item ORDER BY m.report_quarter
    ) AS prior_quarter_amount
  FROM mapped m
)
SELECT
  wp.report_quarter AS report_date,
  wp.schedule_code,
  wp.line_item,
  wp.line_description,
  wp.reported_amount,
  COALESCE(wp.prior_quarter_amount, 0) AS prior_quarter_amount,
  wp.reported_amount - COALESCE(wp.prior_quarter_amount, 0) AS variance_amount
FROM with_prior wp
WHERE wp.report_quarter = DATE_TRUNC('quarter', CURRENT_DATE);""",
    ),
    dict(
        workflow="regulatory", job="regulatory_filings", category="Regulatory Reporting",
        title="IFRS 9 loss allowance movement disclosure by stage",
        description=(
            "Produces the stage-by-stage allowance movement table IFRS 9 requires: opening balance, "
            "transfers between stages, remeasurement, new originations, derecognitions and write-offs."
        ),
        sql="""INSERT INTO reg.ifrs9_disclosure (
  report_period, ifrs9_stage, opening_allowance, transfers_in, transfers_out,
  remeasurement, new_originations, derecognitions, writeoffs, closing_allowance
)
WITH period_staging AS (
  SELECT
    DATE_TRUNC('month', s.as_of_date) AS report_period,
    s.account_sk,
    s.ifrs9_stage,
    p.discounted_ecl
  FROM risk.ecl_staging s
  JOIN risk.ecl_provision p ON p.account_sk = s.account_sk AND p.as_of_date = s.as_of_date
  WHERE s.as_of_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
),
stage_transitions AS (
  SELECT
    ps.report_period,
    ps.account_sk,
    ps.ifrs9_stage AS current_stage,
    ps.discounted_ecl AS current_ecl,
    LAG(ps.ifrs9_stage) OVER (PARTITION BY ps.account_sk ORDER BY ps.report_period) AS prior_stage,
    LAG(ps.discounted_ecl) OVER (PARTITION BY ps.account_sk ORDER BY ps.report_period) AS prior_ecl
  FROM period_staging ps
),
writeoffs AS (
  SELECT
    DATE_TRUNC('month', c.chargeoff_date) AS report_period,
    da.account_sk,
    SUM(c.chargeoff_amount) AS writeoff_amount
  FROM core.loan_chargeoff c
  JOIN dim.dim_account da ON da.account_id = c.loan_id AND da.is_current = TRUE
  WHERE c.chargeoff_date >= DATE_TRUNC('month', CURRENT_DATE)
  GROUP BY DATE_TRUNC('month', c.chargeoff_date), da.account_sk
)
SELECT
  st.report_period,
  st.current_stage AS ifrs9_stage,
  SUM(COALESCE(st.prior_ecl, 0)) AS opening_allowance,
  SUM(CASE WHEN st.prior_stage IS NOT NULL AND st.prior_stage <> st.current_stage
           THEN st.current_ecl ELSE 0 END) AS transfers_in,
  SUM(CASE WHEN st.prior_stage IS NOT NULL AND st.prior_stage <> st.current_stage
           THEN st.prior_ecl ELSE 0 END) AS transfers_out,
  SUM(CASE WHEN st.prior_stage = st.current_stage
           THEN st.current_ecl - COALESCE(st.prior_ecl, 0) ELSE 0 END) AS remeasurement,
  SUM(CASE WHEN st.prior_stage IS NULL THEN st.current_ecl ELSE 0 END) AS new_originations,
  SUM(CASE WHEN st.current_ecl IS NULL THEN COALESCE(st.prior_ecl, 0) ELSE 0 END) AS derecognitions,
  SUM(COALESCE(wo.writeoff_amount, 0)) AS writeoffs,
  SUM(st.current_ecl) - SUM(COALESCE(wo.writeoff_amount, 0)) AS closing_allowance
FROM stage_transitions st
LEFT JOIN writeoffs wo ON wo.account_sk = st.account_sk AND wo.report_period = st.report_period
WHERE st.report_period = DATE_TRUNC('month', CURRENT_DATE)
GROUP BY st.report_period, st.current_stage;""",
    ),
    dict(
        workflow="regulatory", job="regulatory_filings", category="Regulatory Reporting",
        title="Basel leverage ratio total exposure measure",
        description=(
            "Assembles the leverage ratio denominator — on-balance assets, derivative replacement "
            "cost and add-on, securities financing exposure and credit-converted off-balance items — "
            "against tier 1 capital."
        ),
        sql="""INSERT INTO reg.leverage_ratio (
  as_of_date, on_balance_exposure, derivative_exposure, sft_exposure,
  off_balance_exposure, total_exposure_measure, tier1_capital, leverage_ratio, minimum_requirement
)
WITH on_balance AS (
  SELECT
    b.as_of_date,
    SUM(b.carrying_amount) AS on_balance_exposure
  FROM finance.balance_sheet_position b
  JOIN dim.dim_gl_account ga ON ga.gl_account_sk = b.gl_account_sk AND ga.is_current = TRUE
  WHERE b.as_of_date = CURRENT_DATE - INTERVAL '1 day'
    AND ga.account_type = 'ASSET'
  GROUP BY b.as_of_date
),
derivatives AS (
  SELECT
    d.as_of_date,
    SUM(GREATEST(d.mark_to_market, 0) + d.notional_amount * ao.addon_factor) AS derivative_exposure
  FROM treasury.derivative_position d
  JOIN ref.derivative_addon ao
    ON ao.asset_class = d.asset_class AND ao.residual_maturity_bucket = d.maturity_bucket
  WHERE d.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY d.as_of_date
),
sft AS (
  SELECT
    s.as_of_date,
    SUM(GREATEST(s.gross_asset_value - s.cash_collateral_received, 0)) AS sft_exposure
  FROM treasury.securities_financing s
  WHERE s.as_of_date = CURRENT_DATE - INTERVAL '1 day'
  GROUP BY s.as_of_date
),
off_balance AS (
  SELECT
    CURRENT_DATE - INTERVAL '1 day' AS as_of_date,
    SUM(u.undrawn_amount * ccf.leverage_ccf) AS off_balance_exposure
  FROM core.loan_undrawn_commitment u
  JOIN ref.credit_conversion_factor ccf ON ccf.facility_type = u.facility_type
)
SELECT
  ob.as_of_date,
  ob.on_balance_exposure,
  COALESCE(dv.derivative_exposure, 0) AS derivative_exposure,
  COALESCE(sf.sft_exposure, 0) AS sft_exposure,
  COALESCE(offb.off_balance_exposure, 0) AS off_balance_exposure,
  ob.on_balance_exposure + COALESCE(dv.derivative_exposure, 0)
    + COALESCE(sf.sft_exposure, 0) + COALESCE(offb.off_balance_exposure, 0) AS total_exposure_measure,
  ca.tier1_capital,
  ca.tier1_capital / NULLIF(
    ob.on_balance_exposure + COALESCE(dv.derivative_exposure, 0)
    + COALESCE(sf.sft_exposure, 0) + COALESCE(offb.off_balance_exposure, 0), 0) AS leverage_ratio,
  rr.minimum_ratio AS minimum_requirement
FROM on_balance ob
LEFT JOIN derivatives dv ON dv.as_of_date = ob.as_of_date
LEFT JOIN sft sf ON sf.as_of_date = ob.as_of_date
LEFT JOIN off_balance offb ON offb.as_of_date = ob.as_of_date
JOIN reg.capital_adequacy ca ON ca.as_of_date = ob.as_of_date
CROSS JOIN ref.regulatory_ratio_requirement rr
WHERE rr.ratio_type = 'LEVERAGE';""",
    ),
]


assert len(BANKING_EXAMPLES) >= 50, "banking library must carry at least 50 complex examples"

_WORKFLOW_BY_KEY = {wf.key: wf for wf in BANKING_WORKFLOWS}


def sql_complexity(sql: str) -> dict:
    """Structural complexity of a statement, surfaced in the UI.

    Derived from the parsed AST rather than text matching, so the counts hold
    up regardless of formatting. Falls back to zeros if a statement somehow
    fails to parse — the metric is decoration, never a reason to fail a seed.
    """
    import sqlglot
    from sqlglot import exp

    try:
        tree = sqlglot.parse_one(sql, dialect="postgres")
    except Exception:  # pragma: no cover - defensive
        return {"ctes": 0, "joins": 0, "tables": 0, "lines": len(sql.splitlines())}

    return {
        "ctes": len(list(tree.find_all(exp.CTE))),
        "joins": len(list(tree.find_all(exp.Join))),
        "tables": len({t.sql() for t in tree.find_all(exp.Table)}),
        "lines": len(sql.splitlines()),
    }


def seed_banking_data(db: Session) -> int:
    """Create the banking workflows/jobs/tasks. Returns tasks created.

    Assumes the caller has already checked the database is empty; called from
    ``app.seed_data.seed_demo_data`` inside the same transaction.
    """
    now = dt.datetime.now(dt.timezone.utc)
    created = 0

    examples_by_workflow: dict[str, list[dict]] = {}
    for example in BANKING_EXAMPLES:
        examples_by_workflow.setdefault(example["workflow"], []).append(example)

    for wf_def in BANKING_WORKFLOWS:
        workflow = WorkflowDefinition(
            name=wf_def.name,
            owner_team=wf_def.owner_team,
            owner_email=wf_def.owner_email,
            sla_minutes=wf_def.sla_minutes,
            description=wf_def.description,
            tags={"domain": "banking", "area": wf_def.key},
        )
        db.add(workflow)
        db.flush()

        jobs: dict[str, JobDefinition] = {}
        previous_job: JobDefinition | None = None
        for job_idx, job_name in enumerate(wf_def.jobs):
            job = JobDefinition(
                workflow_id=workflow.workflow_id,
                name=job_name,
                cron_expression=f"{15 * job_idx} 2 * * *",
                max_retries=2,
                retry_delay_seconds=300,
                max_concurrency=1,
                cluster_config={"node_type": "r6g.2xlarge", "workers": 4 + job_idx, "autoscale": True},
                is_active=True,
            )
            db.add(job)
            db.flush()
            jobs[job_name] = job

            db.add(
                JobDag(
                    workflow_id=workflow.workflow_id,
                    upstream_job_id=previous_job.job_id if previous_job else None,
                    downstream_job_id=job.job_id,
                    external_trigger=None if previous_job else {"type": "schedule", "cron": "0 2 * * *"},
                )
            )
            previous_job = job

            for alert_type in (AlertType.SLA_BREACH, AlertType.FAILURE):
                db.add(
                    JobAlert(
                        job_id=job.job_id,
                        alert_type=alert_type,
                        distribution_list=[wf_def.owner_email, "financial-control@metaweave.example"],
                        routing_rule={"channel": "pagerduty", "severity": "critical"},
                    )
                )

            for run_idx in range(_RNG.randint(3, 5)):
                started = now - dt.timedelta(days=run_idx, hours=_RNG.randint(0, 4))
                status = _RNG.choices(
                    [RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.RUNNING], weights=[88, 8, 4]
                )[0]
                db.add(
                    JobAudit(
                        job_id=job.job_id,
                        run_id=f"run-{wf_def.key}-{job_name}-{run_idx:03d}",
                        status=status,
                        started_at=started,
                        ended_at=None if status == RunStatus.RUNNING
                        else started + dt.timedelta(minutes=_RNG.randint(5, 90)),
                        rows_processed=_RNG.randint(50_000, 8_000_000) if status == RunStatus.SUCCESS else None,
                        cost_usd=round(_RNG.uniform(5.0, 180.0), 2),
                        error_message="Upstream GL close not finalised" if status == RunStatus.FAILED else None,
                        logs_url=f"https://logs.metaweave.example/{wf_def.key}/{job_name}/{run_idx}",
                    )
                )

        # Tasks, ordered within their job.
        order_by_job: dict[str, int] = {name: 0 for name in wf_def.jobs}
        tasks_by_job: dict[str, list[TaskDefinition]] = {name: [] for name in wf_def.jobs}
        for example in examples_by_workflow.get(wf_def.key, []):
            job = jobs[example["job"]]
            task = TaskDefinition(
                job_id=job.job_id,
                name=example["title"],
                task_order=order_by_job[example["job"]],
                exec_type=JobExecType.SQL,
                script_path=f"sql/{wf_def.key}/{example['job']}/{order_by_job[example['job']]:02d}.sql",
                source_code=example["sql"],
                environment={
                    "category": example["category"],
                    "description": example["description"],
                    "domain": "banking",
                    "complexity": sql_complexity(example["sql"]),
                },
            )
            db.add(task)
            db.flush()
            order_by_job[example["job"]] += 1
            tasks_by_job[example["job"]].append(task)
            created += 1

        # Wire dependencies: the first task of each job depends on the last
        # task of the preceding job, and every later task in a job depends on
        # the first — enough to give blast-radius/RCA a real graph to walk.
        prior_job_last: TaskDefinition | None = None
        for job_name in wf_def.jobs:
            job_tasks = tasks_by_job[job_name]
            if not job_tasks:
                continue
            head = job_tasks[0]
            if prior_job_last is not None:
                db.add(
                    JobDependency(
                        task_id=head.task_id,
                        depends_on_kind=DependencyKind.TASK,
                        depends_on_task_id=prior_job_last.task_id,
                    )
                )
            else:
                db.add(
                    JobDependency(
                        task_id=head.task_id,
                        depends_on_kind=DependencyKind.DATASET,
                        depends_on_table="gl.gl_posting",
                    )
                )
            for task in job_tasks[1:]:
                db.add(
                    JobDependency(
                        task_id=task.task_id,
                        depends_on_kind=DependencyKind.TASK,
                        depends_on_task_id=head.task_id,
                    )
                )
            prior_job_last = job_tasks[-1]

    return created
