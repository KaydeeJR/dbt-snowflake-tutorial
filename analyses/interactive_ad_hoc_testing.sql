-- SQL code is run using preview button
-- interactive/ad hoc test to check for duplicate customer ids in model stg_Customers
select customer_id
from {{ ref("stg_customers") }}
group by customer_id
having count(*) > 1
