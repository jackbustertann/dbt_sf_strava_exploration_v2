{% macro cast_column(column_name, data_type) -%}

    {%- if data_type == "str" -%}
    {{ column_name }}::string AS {{ column_name }}

    {%- elif data_type == "bool" -%}
    {{ column_name }}::boolean AS {{ column_name }}

    {%- elif data_type == "int" -%}
    {{ column_name }}::int AS {{ column_name }}

    {%- elif data_type == "float" -%}
    {{ column_name }}::float AS {{ column_name }}

    {%- elif data_type == "ntz" -%}
    TO_TIMESTAMP_NTZ({{ column_name }}) AS {{ column_name }}

    {%- elif data_type == "array" -%}
    TO_ARRAY({{ column_name }}) AS {{ column_name }}

    {%- endif -%}

{%- endmacro %}

