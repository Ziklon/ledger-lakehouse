-- Official EOD state of accounts, one partition per business day (macros/gold_publish.sql).
{{ config(
    properties={"partitioning": "ARRAY['snapshot_date']"},
    pre_hook="{{ overwrite_partition('snapshot_date') }}",
    post_hook="delete from {{ this }} where snapshot_date < date '{{ var(\"batch_id\") }}' - interval '{{ var(\"full_daily_retention_days\") }}' day"
) }}
-- updated_at is source bookkeeping: mirrored in silver, not published (docs/adr/0008).
{{ gold_full_daily(
    'accounts_current',
    key='account_id',
    columns=['customer_id', 'asset', 'status', 'opened_at']
) }}
