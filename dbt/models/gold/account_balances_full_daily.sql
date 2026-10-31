-- Official EOD balances per (account_id, asset) (macros/gold_publish.sql).
-- No soft deletes: a balance row exists once an account has had an entry.
{{ config(
    properties={"partitioning": "ARRAY['snapshot_date']"},
    pre_hook="{{ overwrite_partition('snapshot_date') }}",
    post_hook="delete from {{ this }} where snapshot_date < date '{{ var(\"batch_id\") }}' - interval '{{ var(\"full_daily_retention_days\") }}' day"
) }}
{{ gold_full_daily(
    'account_balances_current',
    key=['account_id', 'asset'],
    columns=['balance'],
    soft_delete=false
) }}
