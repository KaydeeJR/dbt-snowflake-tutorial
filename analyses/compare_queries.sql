{# compare both legacy code files to see if they are similar#}
{# this code can be written and executed in a statement tab #}
{% set old_etl_relation = ref("customer_orders") %}
{# the dbt model that is created a new to replace the old model #}
{% set dbt_relation = ref("fct_customer_orders") %}
{# compares the primary key (order_id) of both tables#}
{#the percent of total field of the resulting relations table indicates how many similar records
exist (100 % means that both tables are similar). there is also a count display that indicates
the number of rows #}
{{
    audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key="order_id",
    )
}}
