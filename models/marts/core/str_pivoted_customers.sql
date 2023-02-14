-- 79 unique customer first names out of a total 100 customers
-- this pivoting model will find customers who have ordered more than once
with
    -- 1st CTE -> fetches all customer first names from model stg_customers
    cust_first_names as (select first_name, last_name from {{ ref("stg_customers") }}),

    -- 2nd CTE -> fetches all customer first names from model stg_customers
    pivoted_customers as (select first_name from cust_first_names group by 1)

select *
from pivoted_customers
