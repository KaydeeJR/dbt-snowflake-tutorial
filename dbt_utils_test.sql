-- formats current date and time
{{ dbt_utils.pretty_time(format='%Y-%m-%d %H:%M:%S') }}

-- LISTS ALL COLUMNS FROM THE MODEL
select
  {{ dbt_utils.star(ref('stg_customers')) }}
from {{ ref('stg_customers') }}
