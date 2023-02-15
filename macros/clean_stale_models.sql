{% macro clean_stale_models(
    database=target.database, schema=target.schema, days=7, dry_run=True
) %}
{#  
    This macro: 
    1. queries the information schema of a database
    2. finds objects that are more than 1 week old (no longer maintained)
    3. generates automated drop statements
    4. has the ability to execute those drop statements
#}
{# the query that should be run in the warehouse #}
{# set block#}
    {% set get_drop_commands_query %}
        select
            case 
            -- checks the table type column in the db information_schema
            -- if the table type is view then assign view to the record at column name drop type
            -- else assign table as a record value at column name drop type
                when table_type = 'VIEW'
                    then table_type
                else 
                    'TABLE'
            end as drop_type,
            -- perform string concatenation: drop command + drop type acquired above 
            -- + database name in uppercase + .schema + .table name + ;
            -- append a ; because this query will be run in Snowflake
            -- every SQL command in Snowflake ends with a ;
            'DROP ' || drop_type || ' {{ database | upper }}.' || table_schema || '.' || table_name || ';'
        from {{ database }}.information_schema.tables
        -- only affect the tables in the specified schema 
        where table_schema = upper('{{ schema }}')
        and last_altered <= current_date - {{ days }} 
    {% endset %}

{{ log("\nGenerating cleanup queries...\n", info=True) }}
-- macro called run_query macro which provides a convenient way to run queries and
-- fetch their results
-- accepts the SQL query as a parameter
{# Return the second column #}
{% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

{% for query in drop_queries %}
-- loop through the queries
-- if dry_run is set to True then show query that is being executed
{% if dry_run %} {{ log(query, info=True) }}
{% else %}
-- execute query using do statement
{{ log("Dropping object with command: " ~ query, info=True) }} {% do run_query(query) %}
{% endif %}
{% endfor %}

{% endmacro %}
