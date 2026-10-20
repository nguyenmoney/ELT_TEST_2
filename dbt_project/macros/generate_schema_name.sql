{#
  Mac dinh dbt ghep schema thanh <schema_cua_profile>_<schema_cua_model> (vd: silver_silver).
  Macro nay ghi de: neu model khai bao schema rieng thi dung dung ten do (vd: silver).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}