-- Latest event per account in the batch window (see macros/cdc_latest_event.sql).
{{ cdc_latest_event(
    source('bronze', 'accounts_cdc'),
    key='account_id',
    columns=['customer_id', 'asset', 'status', 'opened_at', 'updated_at']
) }}
