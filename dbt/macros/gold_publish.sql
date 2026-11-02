{#
  Gold publication macros: ONE pattern for every published entity.

    <entity>_full_daily   partition snapshot_date = batch: the complete EOD state
    <entity>_incremental  partition batch_date    = batch: net changes, op I/U/D

  Both read the SAME frozen input -- the Iceberg tag eod_YYYY_MM_DD on the silver
  *_current table, created by the pipeline right after silver ran for the batch.
  Same cut-off, same snapshot => FULL and INCREMENTAL always agree, and a rerun
  reads exactly what the first run read (docs/adr/0011).

  "Alive" = present in the snapshot and not soft-deleted. FULL keeps soft-deleted
  rows (is_deleted = true, last known values); consumers use gold views, which
  filter them. INCREMENTAL is expressed on ALIVE rows (docs/adr/0009):
    alive(N) and not alive(N-1)                 -> I  (new, or re-inserted)
    alive(N) and alive(N-1) and hash differs    -> U
    not alive(N) and alive(N-1)                 -> D
  so active(FULL N-1) + INCREMENTAL N = active(FULL N)   (checked in milestone e).

  Gold computes its OWN row_hash over the PUBLISHED columns (docs/adr/0008).
  Silver's hash includes bookkeeping columns (updated_at) because silver is an
  exact mirror; gold publishes business columns only. So a same-day flap
  (ACTIVE -> FROZEN -> ACTIVE, updated_at moved) changes silver but publishes
  nothing. Every published column is in the hash, so the equation above is
  exact on values, not just on keys: no published column may be left out.
#}

{#- Rerun idempotency: wipe this batch's partition before appending it again.
    Partition-aligned, so Iceberg drops whole files (metadata-only delete).
    Works even when the new result is empty (delete+insert would not). -#}
{% macro overwrite_partition(column) -%}
{%- if is_incremental() -%}
delete from {{ this }} where {{ column }} = date '{{ var("batch_id") }}'
{%- else -%}
select 1
{%- endif -%}
{%- endmacro %}

{% macro _require_batch() -%}
{%- if execute and var('batch_id') is none -%}
  {{ exceptions.raise_compiler_error("var batch_id is required") }}
{%- endif -%}
{%- endmacro %}

{#- Hash of exactly what is published (+ is_deleted for soft-delete entities). -#}
{% macro _gold_hash(columns, soft_delete) -%}
{{ dbt_utils.generate_surrogate_key(columns + (['is_deleted'] if soft_delete else [])) }}
{%- endmacro %}

{% macro _alive(alias, key_cols, soft_delete) -%}
({{ alias }}.{{ key_cols[0] }} is not null{% if soft_delete %} and not {{ alias }}.is_deleted{% endif %})
{%- endmacro %}


{% macro gold_full_daily(current_model, key, columns, soft_delete=true) %}
{{- _require_batch() -}}
{%- set key_cols = [key] if key is string else key -%}
select
    {% for k in key_cols %}{{ k }},
    {% endfor %}
    {%- for c in columns %}{{ c }},
    {% endfor %}
    {%- if soft_delete %}is_deleted,
    deleted_at,
    {% endif -%}
    {{ _gold_hash(columns, soft_delete) }} as row_hash,
    cast('{{ var("batch_id") }}' as date) as snapshot_date
from {{ at_eod(current_model, var('batch_id')) }}
{% endmacro %}


{% macro gold_incremental(current_model, key, columns, soft_delete=true) %}
{{- _require_batch() -}}
{%- set key_cols = [key] if key is string else key -%}
{%- set prev = var('prev_batch_id') -%}
{%- set src_cols = key_cols + columns + (['is_deleted'] if soft_delete else []) -%}
{%- set cols = src_cols + ['row_hash'] -%}

with curr as (
    select {{ src_cols | join(', ') }}, {{ _gold_hash(columns, soft_delete) }} as row_hash
    from {{ at_eod(current_model, var('batch_id')) }}
),

prev as (
    select {{ src_cols | join(', ') }}, {{ _gold_hash(columns, soft_delete) }} as row_hash
    {% if prev is none -%}
    from {{ ref(current_model) }} where false      -- first batch: nothing published before
    {%- else -%}
    from {{ at_eod(current_model, prev) }}
    {%- endif %}
),

compared as (
    select
        {% for c in cols %}
        {#- silver never hard-deletes, so curr exists whenever prev does; prev is a fallback only -#}
        case when c.{{ key_cols[0] }} is null then p.{{ c }} else c.{{ c }} end as {{ c }},
        {% endfor %}
        case
            when {{ _alive('c', key_cols, soft_delete) }} and not {{ _alive('p', key_cols, soft_delete) }} then 'I'
            when {{ _alive('c', key_cols, soft_delete) }} and {{ _alive('p', key_cols, soft_delete) }}
                 and c.row_hash <> p.row_hash then 'U'
            when not {{ _alive('c', key_cols, soft_delete) }} and {{ _alive('p', key_cols, soft_delete) }} then 'D'
        end as op
    from curr as c
    full outer join prev as p
        on {% for k in key_cols %}{% if not loop.first %} and {% endif %}c.{{ k }} = p.{{ k }}{% endfor %}
)

select
    {% for c in cols %}{{ c }},
    {% endfor -%}
    op,
    cast('{{ var("batch_id") }}' as date) as batch_date
from compared
where op is not null
{% endmacro %}
