{% set order_status = [
    "placed",
    "shipped",
    "completed",
    "return_pending",
    "returned",
] %}

with  -- 1st CTE -> fetches all records from model stg_orders
    order_status as (select * from {{ ref("stg_orders") }}),

    pivoted_orders as (
        -- 2nd CTE -> fetches records from 1st CTE, query result set and produces a
        -- summary of the
        -- status of each customer order
        select
            order_id,
            {% for list_status in order_status %}
            sum(
                case when status = '{{ list_status }}' then 1 else 0 end
            ) as {{ list_status }}_order
            {% if not loop.last %}, {% endif %}
            {% endfor %}

        from order_status
        group by 1
    )
select *
from pivoted_orders

-- The results returned from this query remind me of one hot encoding where categorical variables
-- are converted into numerical values