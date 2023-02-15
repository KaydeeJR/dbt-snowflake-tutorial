-- calculate the number of orders for each day in the table
with
    orders as (select * from {{ ref("stg_orders") }}),
    daily_orders as (
        select order_date, count(*) as day_order_num from orders group by 1
    ),
    compared_to_previous_day as (
        select
            *, lag(day_order_num) over (order by order_date) as previous_day_order_num
        from daily_orders
    )
select *
from compared_to_previous_day
