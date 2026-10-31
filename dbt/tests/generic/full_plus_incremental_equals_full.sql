{#
  Invariant: active(FULL prev) + INCREMENTAL(N) = active(FULL N)
  on row count AND an order-independent checksum(row_hash) (docs/adr/0009).

  "prev" is the batch the INCREMENTAL was diffed against (var prev_batch_id),
  not the calendar day before: after a missed day both are the same gap.
  Also checks op semantics, which the equation alone can't see
  (an I mislabelled as U applies the same way):
    I -> key must NOT be alive in FULL prev;  U/D -> key MUST be alive in FULL prev.
  Returns one row per failure (dbt: test fails if any row).
#}
{% test full_plus_incremental_equals_full(model, incremental, key, soft_delete=true) %}
    {%- set key_cols = [key] if key is string else key -%}
    {%- set batch = var('batch_id') -%}
    {%- set prev = var('prev_batch_id') -%}
    {%- if execute and batch is none -%}
  {{ exceptions.raise_compiler_error("var batch_id is required") }}
{%- endif -%}
    {%- set active = 'not is_deleted' if soft_delete else 'true' -%}
    {%- set keys = key_cols | join(', ') -%}
    {%- set on_key %}{% for k in key_cols %}{% if not loop.first %} and {% endif %}i.{{ k }} = p.{{ k }}{% endfor %}{% endset -%}

with prev_full as (
    select {{ keys }}, row_hash
    from {{ model }}
{% if prev is none -%}
    where false                                   -- first batch: FULL prev is empty
    {%- else -%}
    where snapshot_date = date '{{ prev }}' and {{ active }}
    {%- endif %}
),

inc as (
    select {{ keys }}, row_hash, op
    from {{ incremental }}
    where batch_date = date '{{ batch }}'
),

applied as (
    select p.row_hash from prev_full as p
    where not exists (select 1 from inc as i where {{ on_key }})
    union all
    select row_hash from inc where op in ('I', 'U')
),

actual as (
    select row_hash from {{ model }}
    where snapshot_date = date '{{ batch }}' and {{ active }}
),

totals as (
    select
        (select count(*) from applied) as applied_rows,
        (select count(*) from actual) as full_rows,
        (select checksum(row_hash) from applied) as applied_checksum,
        (select checksum(row_hash) from actual) as full_checksum
),

wrong_ops as (
    select i.op, count(*) as n
    from inc as i
    left join prev_full as p on {{ on_key }}
    where (i.op = 'I' and p.{{ key_cols[0] }} is not null)
       or (i.op in ('U', 'D') and p.{{ key_cols[0] }} is null)
    group by i.op
)

select
    'count/checksum mismatch' as failure,
    cast(applied_rows as varchar) || ' applied vs ' || cast(full_rows as varchar) || ' full' as detail
from totals
where applied_rows <> full_rows or applied_checksum is distinct from full_checksum
union all
select
    'op ' || op || ' inconsistent with FULL prev' as failure,
    cast(n as varchar) || ' rows' as detail
from wrong_ops
{% endtest %}
