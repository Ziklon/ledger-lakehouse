{#
  Iceberg time travel to the end-of-day tag created by the pipeline
  (ledger_pipeline.tags; name must match windows.eod_tag).
  Kept in its own file so .sqlfluff can override at_eod at lint time
  (sqlfluff's trino dialect can't parse FOR VERSION AS OF).
#}
{% macro eod_tag(batch_id) -%}
eod_{{ batch_id | replace('-', '_') }}
{%- endmacro %}

{# A model "as of" the end of a business day. #}
{% macro at_eod(model_name, batch_id) -%}
{{ ref(model_name) }} for version as of '{{ eod_tag(batch_id) }}'
{%- endmacro %}
