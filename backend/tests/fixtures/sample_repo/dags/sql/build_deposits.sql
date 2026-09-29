INSERT INTO fact.deposit_balance_daily (as_of_date, account_sk, customer_sk, average_daily_balance, contractual_rate)
SELECT
  h.as_of_date,
  da.account_sk,
  dc.customer_sk,
  AVG(h.ledger_balance) AS average_daily_balance,
  rp.contractual_rate
FROM core.deposit_balance_history h
JOIN core.deposit_account a ON a.account_id = h.account_id
JOIN dim.dim_account da ON da.account_id = a.account_id
JOIN dim.dim_customer dc ON dc.customer_id = a.customer_id
JOIN ref.rate_plan rp ON rp.rate_plan_id = a.rate_plan_id
GROUP BY h.as_of_date, da.account_sk, dc.customer_sk, rp.contractual_rate;
