{#
- replaced raw.stripe.payment with  {{ source('stripe', 'payment') }}
- replaced raw.jaffle_shop.orders with {{ source('jaffle_shop', 'orders') }}
- replaced raw.jaffle_shop.customers with {{ source('jaffle_shop', 'customers') }}
- moved all {{source()}} to the top of this file as import CTEs
- refactored 'from' and 'join' clauses to reference the import CTEs by name
    - from {{ source("jaffle_shop", "orders") }} -> from shop_orders
    - from {{ source("stripe", "payment") }} -> from stripe_payments
    - from {{ source("jaffle_shop", "customers") }} -> from shop_customers
- transformed to lower case and formatted the file
- created a CTE called completed_payments
- replaced single letter aliases such as c, p with fully qualified table names and references
- edited customer_lifetime_value subquery as a window function to avoid an unnecessary extra self join. Alternatively, this subquery could be written as a CTE. 
- removed the customer_orders CTE and its join with paid_orders in the final CTE
- created a first_value window function to replace a CTE that performs aggregation min(orders.order_date) as first_order_date
- changed the transformation logic for the following columns: nvsr & fdos
- wrapped the last select statement in a CTE called final_cte and used a select statement to fetch its results
- referenced the new staging models using the {{ ref('<your_model>') }} function:
    - from {{ source("jaffle_shop", "customers") }} -> {{ ref('stg_jaffle_shop__customers') }}
    - from {{ source("jaffle_shop", "orders") }} ->  {{ ref('stg_jaffle_shop__orders') }}
    - from {{ source("stripe", "payment") }} -> {{ ref('stg_stripe__payments') }}
- changed column names to reference appropriate columns e.g. the columns in staging models and paid_orders CTE
- moved paid_orders CTE and completed_payments CTE to an intermediate model called int_orders.sql
- imported paid_orders CTE from intermediate model. Referenced this import CTE for column names
- no use for payments import
#}
with
    -- Import CTEs
    shop_customers as (select * from {{ ref("stg_jaffle_shop_customers") }}),
    paid_orders as (select * from {{ ref("int_orders") }}),
    -- shop_orders as (select * from {{ ref("stg_jaffle_shop_orders") }}),
    -- stripe_payments as (select * from {{ ref("stg_stripe_payments") }}),
    -- MOVED TO intermediate model -> int_orders.sql
    -- completed_payments as (
    -- select
    -- order_id,
    -- max(payment_created_at) as payment_finalized_date,
    -- sum(payment_amount) as total_amount_paid
    -- -- orderid as order_id,
    -- -- max(created) as payment_finalized_date,
    -- -- sum(amount) / 100.0 as total_amount_paid
    -- from stripe_payments
    -- where payment_status <> 'fail'
    -- group by 1
    -- ),
    -- MOVED TO intermediate model -> int_orders.sql
    -- paid_orders as (
    -- select
    -- shop_orders.order_id,
    -- shop_orders.customer_id,
    -- shop_orders.order_placed_at,
    -- shop_orders.order_status,
    -- completed_payments.total_amount_paid,
    -- completed_payments.payment_finalized_date,
    -- customers.customer_first_name,
    -- customers.customer_last_name
    -- -- shop_orders.id as order_id,
    -- -- shop_orders.user_id as customer_id,
    -- -- shop_orders.order_date as order_placed_at,
    -- -- shop_orders.status as order_status,
    -- -- completed_payments.total_amount_paid,
    -- -- completed_payments.payment_finalized_date,
    -- -- shop_customers.first_name as customer_first_name,
    -- -- shop_customers.last_name as customer_last_name
    -- -- p.total_amount_paid,
    -- -- p.payment_finalized_date,
    -- -- c.first_name as customer_first_name,
    -- -- c.last_name as customer_last_name
    -- -- {{ source("jaffle_shop", "orders") }}
    -- from shop_orders
    -- {#  
    -- this select statement could be its own CTE called completed_payments       
    -- select
    -- orderid as order_id,
    -- max(created) as payment_finalized_date,
    -- sum(amount) / 100.0 as total_amount_paid
    -- from stripe_payments  -- {{ source("stripe", "payment") }}
    -- where status <> 'fail'
    -- group by 1
    -- #}
    -- -- left join completed_payments as p on shop_orders.id =
    -- -- p.order_id
    -- -- left join shop_customers as c on shop_orders.user_id = c.id
    -- -- replaced above joins with
    -- left join
    -- completed_payments on shop_orders.order_id = completed_payments.order_id
    -- left join shop_customers on shop_orders.customer_id = shop_customers.customer_id
    -- ),
    -- customer_orders as (
    -- select
    -- -- c.id as customer_id,
    -- -- min(order_date) as first_order_date,
    -- -- max(order_date) as most_recent_order_date,
    -- shop_customers.id as customer_id,
    -- min(shop_orders.order_date) as first_order_date,
    -- max(shop_orders.order_date) as most_recent_order_date,
    -- count(shop_orders.id) as number_of_orders
    -- -- {{ source("jaffle_shop", "customers") }}
    -- from shop_customers
    -- left join shop_orders on shop_orders.user_id = shop_customers.id
    -- group by 1
    -- ),
    final_cte as (
        select
            -- order_id,
            -- customer_id,
            -- order_placed_at,
            -- order_status,
            -- total_amount_paid,
            -- payment_finalized_date,
            -- customer_first_name,
            -- customer_last_name,
            paid_orders.order_id,
            paid_orders.customer_id,
            paid_orders.order_placed_at,
            paid_orders.order_status,
            paid_orders.total_amount_paid,
            paid_orders.payment_finalized_date,
            shop_customers.customer_first_name,
            shop_customers.customer_last_name,
            -- sum(t2.total_amount_paid) as clv_bad,
            -- p.*,
            -- > sales transaction sequence
            -- row_number() over (order by p.order_id) as
            -- transaction_seq,
            -- extra order by column (order_placed_at) to fix a future potential bug where if
            -- there are multiple
            -- orders placed on the same day for one customer ID, 
            -- there would occur an indeterminate ordering
            row_number() over (
                order by paid_orders.order_placed_at, paid_orders.order_id
            ) as transaction_seq,
            -- > customer sales sequence
            -- row_number() over (partition by customer_id order by
            -- p.order_id) as
            -- customer_sales_seq,
            row_number() over (
                partition by paid_orders.customer_id order by paid_orders.order_id
            ) as customer_sales_seq,
            -- > assigning new customers vs returning customer
            -- case
            -- -- when c.first_order_date = p.order_placed_at then
            -- 'new' else 'return'
            -- when customer_orders.first_order_date =
            -- paid_orders.order_placed_at
            -- then 'new'
            case
                when
                    (
                        rank() over (
                            partition by paid_orders.customer_id
                            order by paid_orders.order_placed_at, paid_orders.order_id
                        )
                        = 1
                    )
                then 'new'
                else 'returning'
            end as nvsr,
            -- previously -> x.clv_bad as customer_lifetime_value
            -- customer_lifetime_value
            -- added extra order by column -> order_id
            sum(paid_orders.total_amount_paid) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
            ) as customer_lifetime_value,
            -- first day of sale
            -- customer_orders.first_order_date as fdos
            -- added extra order by column -> order_id
            first_value(paid_orders.order_placed_at) over (
                partition by paid_orders.customer_id
                order by paid_orders.order_placed_at, paid_orders.order_id
            ) as fdos
        from paid_orders
        left join shop_customers on paid_orders.customer_id = shop_customers.customer_id
    -- left join
    -- customer_orders on paid_orders.customer_id =
    -- customer_orders.customer_id
    -- order by order_id
    -- c.first_order_date as fdos
    -- from paid_orders p
    -- left join customer_orders as c using (customer_id)
    -- left join
    -- (
    -- select p.order_id, sum(t2.total_amount_paid) as clv_bad
    -- from paid_orders p
    -- left join
    -- paid_orders t2
    -- on p.customer_id = t2.customer_id
    -- and p.order_id >= t2.order_id
    -- group by 1
    -- order by p.order_id
    -- ) x
    -- on x.order_id = p.order_id
    -- order by order_id
    )

-- final simple select statement
-- select *
-- from final_cte
select *
-- order by order_id
from final_cte
