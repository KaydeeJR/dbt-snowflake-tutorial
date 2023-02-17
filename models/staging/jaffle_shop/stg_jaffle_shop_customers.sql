with
    customers_source as (select * from {{ source("jaffle_shop", "customers") }}),

    transformed_customer_names as (
        id as customer_id,
        last_name as surname,
        first_name as givenname,
        -- string concatenation
        select first_name || ' ' || last_name as full_name,
        from customers_source
    )
select *
from transformed_customer_names
