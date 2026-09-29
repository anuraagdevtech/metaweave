INSERT INTO finance.nii_daily (as_of_date, product_sk, interest_income, interest_expense, net_interest_income)
WITH assets AS (
  SELECT l.as_of_date, l.product_sk, SUM(l.accrued_interest) AS interest_income
  FROM fact.loan_balance_daily l
  GROUP BY l.as_of_date, l.product_sk
),
liabilities AS (
  SELECT d.as_of_date, d.product_sk,
         SUM(d.average_daily_balance * d.contractual_rate / 365.0) AS interest_expense
  FROM fact.deposit_balance_daily d
  GROUP BY d.as_of_date, d.product_sk
)
SELECT
  a.as_of_date,
  a.product_sk,
  a.interest_income,
  l.interest_expense,
  a.interest_income - l.interest_expense AS net_interest_income
FROM assets a
JOIN liabilities l ON l.product_sk = a.product_sk AND l.as_of_date = a.as_of_date
JOIN dim.dim_product dp ON dp.product_sk = a.product_sk;
