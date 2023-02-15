{#  
    This macro:
    1. imports the get_relations_by_pattern() macro from dbt_utils package
    2. uses a set block to assign the tables fetched from a specified schema 
        and with the specified table name pattern to a list named tables
    3. loops through list of tables
    4. performs union all on all the tables and returns the resulting records
#}
{%- macro union_tables_by_pattern(schema_pat, table_pat) -%}
{%- set tables = dbt_utils.get_relations_by_pattern(
    schema_pattern=schema_pat, table_pattern=table_pat
) -%}

{% for table in tables %}
{%- if not loop.first -%}
union all
{%- endif %}
select *
from {{ table.database }}.{{ table.schema }}.{{ table.name }}

{% endfor -%}
{%- endmacro -%}
