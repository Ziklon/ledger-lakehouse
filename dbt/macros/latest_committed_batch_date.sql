{# The single definition of "what consumers may see": the newest COMMITTED daily batch. #}
{% macro latest_committed_batch_date() -%}
select max(cast(batch_id as date))
from {{ source('control', 'batches') }}
where status = 'COMMITTED' and batch_type = 'daily'
{%- endmacro %}
