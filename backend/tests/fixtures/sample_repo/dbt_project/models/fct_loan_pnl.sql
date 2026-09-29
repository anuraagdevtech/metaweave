SELECT
  s.customer_id,
  SUM(s.outstanding_principal * s.contractual_rate) AS interest_income,
  COUNT(*) AS loan_count
FROM {{ ref('stg_loans') }} s
GROUP BY s.customer_id
