SELECT
  l.loan_id,
  l.customer_id,
  l.outstanding_principal,
  l.contractual_rate
FROM {{ source('core', 'loan_account') }} l
WHERE l.closed_date IS NULL
