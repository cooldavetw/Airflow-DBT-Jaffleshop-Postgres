# macros/drop_table_in_schema_if_exists.sql
{%- macro drop_table_in_schema_if_exists(table_name) -%}
    {%- set drop_query -%}
        DROP TABLE IF EXISTS {{ target.schema }}.{{ table_name }} CASCADE
    {%- endset -%}
    {% do run_query(drop_query) %}
{%- endmacro -%}
