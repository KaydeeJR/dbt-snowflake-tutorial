-- sums the amount of successful payments
-- 1st CTE
with payments as (
select * from {{ ref('stg_payments') }}
),

-- 2nd CTE 
total_successful_payments as (
select
sum(amount) as total_revenue
from payments
where status = 'success'
)

select * from total_successful_payments