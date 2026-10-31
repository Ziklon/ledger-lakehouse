{#
  cdc_latest_event: one row per key for the batch window [window_start, window_end).

  Returns, per key:
    * metadata of the LATEST event in the window (_op, _lsn, _source_commit_ts)
    * business columns of the latest NON-DELETE event in the window
      (NULL when the window contains only a delete for this key)
    * has_live_event: whether those business columns exist

  Why two rankings: delete events carry only the key. NOT NULL columns arrive
  as type defaults (0, '', 1970-01-01) -- never take business columns from a
  delete event (docs/adr/0006).

  Ordering is by _lsn (strict WAL order), never by _source_commit_ts (ms ties).
#}
{% macro cdc_latest_event(source_relation, key, columns) %}
{%- set key_cols = [key] if key is string else key -%}
{%- set window_start = var('window_start') -%}
{%- set window_end = var('window_end') -%}
{#- Only at execution: parse/ls/docs (and Cosmos DAG rendering) run without vars. -#}
{%- if execute and (window_start is none or window_end is none) -%}
  {{ exceptions.raise_compiler_error("vars window_start and window_end are required") }}
{%- endif -%}
with window_events as (
    select
        {% for k in key_cols %}{{ k }}, {% endfor %}
        {% for c in columns %}{{ c }}, {% endfor %}
        _op,
        _lsn,
        _source_commit_ts
    from {{ source_relation }}
    where _source_commit_ts >= from_iso8601_timestamp('{{ window_start }}')
      and _source_commit_ts <  from_iso8601_timestamp('{{ window_end }}')
),

latest as (
    select
        {% for k in key_cols %}{{ k }}, {% endfor %}
        _op,
        _lsn,
        _source_commit_ts
    from (
        select
            {% for k in key_cols %}{{ k }}, {% endfor %}
            _op, _lsn, _source_commit_ts,
            row_number() over (partition by {{ key_cols | join(', ') }} order by _lsn desc) as rn
        from window_events
    )
    where rn = 1
),

latest_live as (
    select
        {% for k in key_cols %}{{ k }}, {% endfor %}
        {{ columns | join(', ') }}
    from (
        select
            {% for k in key_cols %}{{ k }}, {% endfor %}
            {{ columns | join(', ') }},
            row_number() over (partition by {{ key_cols | join(', ') }} order by _lsn desc) as rn
        from window_events
        where _op <> 'd'
    )
    where rn = 1
)

select
    {% for k in key_cols %}l.{{ k }}, {% endfor %}
    {% for c in columns %}ll.{{ c }}, {% endfor %}
    ll.{{ key_cols[0] }} is not null as has_live_event,
    l._op,
    l._lsn,
    l._source_commit_ts
from latest as l
left join latest_live as ll
    on {% for k in key_cols %}{% if not loop.first %} and {% endif %}l.{{ k }} = ll.{{ k }}{% endfor %}
{% endmacro %}
