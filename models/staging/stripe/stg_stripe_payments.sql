with
    source as (select * from {{ source("stripe", "payment") }}),
    transformed_payments as (
        select
            id as payment_id,
            orderid as order_id,
            status as payment_status,
            round(amount / 100.0, 2) as dollar_payment,
        from source
    )
select *
from transformed_payments
