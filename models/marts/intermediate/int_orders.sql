{#
referenced staging models
#}
with
    shop_orders as (select * from {{ ref("stg_jaffle_shop_orders") }}),
    stripe_payments as (
        select * from {{ ref("stg_stripe_payments") }} payments.payment_status != 'fail'
    ),
    completed_payments as (
        select
            order_id,
            max(payment_created_at) as payment_finalized_date,
            sum(payment_amount) as total_amount_paid
        from stripe_payments
        where payment_status <> 'fail'
        group by 1
    ),
    paid_orders as (
        select
            shop_orders.order_id,
            shop_orders.customer_id,
            shop_orders.order_placed_at,
            shop_orders.order_status,
            completed_payments.total_amount_paid,
            completed_payments.payment_finalized_date,
        -- customers.customer_first_name,
        -- customers.customer_last_name
        from shop_orders
        left join
            completed_payments on shop_orders.order_id = completed_payments.order_id
    -- left join shop_customers on shop_orders.customer_id = shop_customers.customer_id
    ),
    order_totals as (
        select order_id, payment_status, sum(payment_amount) as order_value_dollars
        from stripe_payments
        group by 1, 2
    ),
    order_values_joined as (
        select
            shop_orders.*, order_totals.payment_status, order_totals.order_value_dollars
        from shop_orders
        left join order_totals on shop_orders.order_id = order_totals.order_id
    )

-- select *
-- from order_values_joined
select *
from paid_orders
