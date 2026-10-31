-- Net daily balance changes (I/U), EOD(N) vs EOD(N-1) (macros/gold_publish.sql).
{{ config(
    properties={"partitioning": "ARRAY['batch_date']"},
    pre_hook="{{ overwrite_partition('batch_date') }}"
) }}
{{ gold_incremental(
    'account_balances_current',
    key=['account_id', 'asset'],
    columns=['balance'],
    soft_delete=false
) }}
