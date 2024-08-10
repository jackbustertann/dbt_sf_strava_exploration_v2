{% macro table_to_list_of_rows(table_name) %}
    {% set query_to_process %}
        SELECT *
        FROM {{ target.database }}.{{ target.schema }}.{{ table_name }}
    {% endset %}

    {% set results = run_query(query_to_process) %}

    {% if execute %}
    {% set results_list = results.rows %}
    {% else %}
    {% set results_list = [] %}
    {% endif %}

    {{ return(results_list) }}

{% endmacro %}
