-- Latest event per asset in the batch window (see macros/cdc_latest_event.sql).
{{ cdc_latest_event(
    source('bronze', 'assets_cdc'),
    key='asset',
    columns=['name', 'precision', 'updated_at']
) }}
