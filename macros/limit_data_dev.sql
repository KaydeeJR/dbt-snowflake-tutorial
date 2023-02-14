{% macro limit_data_in_dev(column_name, dev_days_data=3) %}
-- when working with Big Data. Works on source/raw data
-- limits the amount of data used during development
{% if target.name == "default" %}
-- dateadd and current_timestamp are SQL functions
-- gets last number of days
where {{ column_name }} >= dateadd('day', -{{ dev_days_data }}, current_timestamp)
{% endif %}

{% endmacro %}
