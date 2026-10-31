{#
  cdc_current: maintain a silver mirror ("*_current") of a source table from
  its staging model (output of cdc_latest_event). Incremental MERGE on key.

  Per key in the window:
    * business columns: from the window's latest live (non-delete) event if
      there is one, else carried forward from the existing silver row.
      Never from a delete event (docs/adr/0006). Row-level choice, not
      per-column coalesce (a coalesce would accept default-filled 0 / '').
    * is_deleted = latest event is a delete; deleted_at = its commit time
    * skipped: latest event is a delete AND the key is not in silver
      (created and deleted within the window: never existed at EOD)
    * LSN guard: applied only if newer than what silver already holds
    * emitted only if row_hash (business columns + is_deleted) changed;
      without is_deleted in the hash a pure delete would be invisible

  Cost scales with the window's change volume: existing rows are read only
  for keys present in the window.
#}
{% macro cdc_current(staging_model, key, columns) %}
{%- set key_cols = [key] if key is string else key -%}
{#- Only at execution: parse/ls/docs (and Cosmos DAG rendering) run without vars. -#}
{%- if execute and var('batch_id') is none -%}
  {{ exceptions.raise_compiler_error("var batch_id is required") }}
{%- endif -%}
{%- set on_key %}{% for k in key_cols %}{% if not loop.first %} and {% endif %}s.{{ k }} = e.{{ k }}{% endfor %}{% endset -%}

with staged as (
    select
        {% for k in key_cols %}{{ k }}, {% endfor %}
        {% for c in columns %}{{ c }}, {% endfor %}
        has_live_event, _op, _lsn, _source_commit_ts
    from {{ ref(staging_model) }}
),

{% if is_incremental() %}
existing as (
    select
        {% for k in key_cols %}{{ k }}, {% endfor %}
        {% for c in columns %}{{ c }}, {% endfor %}
        is_deleted, deleted_at, row_hash, _last_lsn
    from {{ this }}
    where ({{ key_cols | join(', ') }}) in (select {{ key_cols | join(', ') }} from staged)
),
{% else %}
existing as (
    -- first build: nothing exists yet (typed empty relation)
    select
        {% for k in key_cols %}{{ k }}, {% endfor %}
        {% for c in columns %}{{ c }}, {% endfor %}
        cast(null as boolean) as is_deleted,
        cast(null as timestamp(6) with time zone) as deleted_at,
        cast(null as varchar) as row_hash,
        cast(null as bigint) as _last_lsn
    from staged
    where false
),
{% endif %}

resolved as (
    select
        {% for k in key_cols %}s.{{ k }}, {% endfor %}
        {% for c in columns %}
        case when s.has_live_event then s.{{ c }} else e.{{ c }} end as {{ c }},
        {% endfor %}
        s._op = 'd' as is_deleted,
        case when s._op = 'd' then s._source_commit_ts end as deleted_at,
        s._lsn as _last_lsn,
        s._source_commit_ts as _last_commit_ts,
        e.row_hash as previous_row_hash
    from staged as s
    left join existing as e on {{ on_key }}
    where not (s._op = 'd' and e.{{ key_cols[0] }} is null)   -- created+deleted in window
      and s._lsn > coalesce(e._last_lsn, -1)                  -- never move backwards
),

hashed as (
    select
        {% for k in key_cols %}{{ k }}, {% endfor %}
        {% for c in columns %}{{ c }}, {% endfor %}
        is_deleted,
        deleted_at,
        {{ dbt_utils.generate_surrogate_key(columns + ['is_deleted']) }} as row_hash,
        _last_lsn,
        _last_commit_ts,
        previous_row_hash
    from resolved
)

select
    {% for k in key_cols %}{{ k }}, {% endfor %}
    {% for c in columns %}{{ c }}, {% endfor %}
    is_deleted,
    deleted_at,
    row_hash,
    _last_lsn,
    _last_commit_ts,
    cast('{{ var("batch_id") }}' as varchar) as _batch_id
from hashed
where previous_row_hash is null or previous_row_hash <> row_hash
{% endmacro %}
