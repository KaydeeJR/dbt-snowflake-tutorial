-- setting value of list variable payment_methods. Easily accessible since it is at
-- the top of the page.
{% set payment_methods = ["gift_card", "credit_card", "coupon", "bank_transfer"] %}

with  -- 1st CTE -> gets all records from model stg_payments
    stripe_payments as (select * from {{ ref("stg_payments") }}),

    pivoted_payments as (
        -- 2nd CTE -> fetches records from 1st CTE and produces a summary of the
        -- amount of money paid
        -- through a particular payment method
        select
            order_id,
            {% for payment in payment_methods %}
            sum(
                case when payment_method = '{{ payment }}' then amount else 0 end
            ) as {{ payment }}_amount
            {% if not loop.last %}, {% endif %}
            {% endfor %}

        from stripe_payments
        where status = 'success'
        group by 1
    )
select *
from pivoted_payments
