{% macro grant_select(schema=target.schema, role=target.role) %}
-- grants a select priviledge to all models in a particular schema in the warehouse
-- dynamically generates permissions for users building in a particular schema and particular role
  {% set sql_query %}
-- set block to assign multiple values to multiple variables at once
-- 3 jinja functions for higher level macro writing
  grant usage on schema {{ schema }} to role {{ role }};
  grant select on all tables in schema {{ schema }} to role {{ role }};
  grant select on all views in schema {{ schema }} to role {{ role }};
  {% endset %}

{{
    log(
        "Granting select on all tables and views in schema "
        ~ target.schema
        ~ " to role "
        ~ role,
        info=True,
    )
}}
-- execute the query by running it against the warehouse models
{% do run_query(sql_query) %}
-- display query status
{{ log("Privileges granted", info=True) }}

{% endmacro %}
