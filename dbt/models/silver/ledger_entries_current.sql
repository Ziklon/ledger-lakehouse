-- Mirror of app.ledger_entries (immutable facts) (see macros/cdc_current.sql).
{{ config(unique_key='entry_id') }}
{{ cdc_current(
    'stg_ledger_entries_latest',
    key='entry_id',
    columns=['transaction_id', 'account_id', 'direction', 'amount', 'asset',
             'posting_date', 'effective_date', 'reverses_entry_id', 'created_at']
) }}
