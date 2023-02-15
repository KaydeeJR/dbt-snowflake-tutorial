-- unique_key ensures that no duplicate rows exist. it checks the primary key value
-- and uses it to
-- determine the uniqueness of the data
{# unique_key = 'page_view_id'#}
-- such models work well with real-time data or event data
{# -- with events as (
-- select * from {{ source('snowplow', 'events') }}
-- {% if is_incremental() %}
-- where collector_tstamp >= (select max(max_collector_tstamp) from {{ this }})
-- {% endif %}
-- ),
-- page_views as (
-- select * from events
-- where event = 'page_view'
-- ),
-- aggregated_page_events as (
-- select
-- page_view_id,
-- count(*) * 10 as approx_time_on_page,
-- min(derived_tstamp) as page_view_start,
-- max(collector_tstamp) as max_collector_tstamp
-- from events
-- group by 1
-- ),
-- joined as (
-- select
-- *
-- from page_views
-- left join aggregated_page_events using (page_view_id)
-- )
-- select * from joined
#}
{{ config(materialized="incremental") }}
