{% macro impute_column(column_name, impute_on, default_value) -%}

    {%- if impute_on == "null" -%}
        {%- if default_value == "null_ntz" -%}
    IFNULL({{ column_name }}, TIMESTAMP_NTZ_FROM_PARTS(1900, 1, 1, 00, 00, 00)) as {{ column_name }}
        {%- else -%}    
    IFNULL({{ column_name }}, {{ default_value }}) as {{ column_name }}
        {%- endif -%}

    {%- elif impute_on == "zero" -%}
    IFF({{ column_name }} = 0, {{ default_value }}, {{ column_name }}) AS {{ column_name }}

    {%- endif -%}

{%- endmacro %}