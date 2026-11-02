-- Net daily changes of accounts (I/U/D), EOD(N) vs EOD(N-1) (macros/gold_publish.sql).
{{ config(
    properties={"partitioning": "ARRAY['batch_date']"},
    pre_hook="{{ overwrite_partition('batch_date') }}"
) }}
-- updated_at is source bookkeeping: mirrored in silver, not published (docs/adr/0008).
{{ gold_incremental(
    'accounts_current',
    key='account_id',
    columns=['customer_id', 'asset', 'status', 'opened_at']
) }}
