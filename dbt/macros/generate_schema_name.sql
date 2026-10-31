{#
  Use the configured schema literally (silver, gold, ...) instead of dbt's
  default "<target_schema>_<custom_schema>" concatenation. Layer names are part
  of the published contract and must not depend on the target.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
