-- Ensure that stripe payment is successful. Status must be success.
-- Therefore return records where status is not success to make the test fail.
select amount as stripe_amount
from {{ ref("stg_payments") }}
group by 1
where status != "success"
