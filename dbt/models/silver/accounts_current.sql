-- Mirror of app.accounts with soft deletes (see macros/cdc_current.sql).
{{ config(unique_key='account_id') }}
{{ cdc_current(
    'stg_accounts_latest',
    key='account_id',
    columns=['customer_id', 'asset', 'status', 'opened_at', 'updated_at']
) }}
