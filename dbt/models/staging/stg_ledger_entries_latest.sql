-- Latest event per ledger entry in the batch window. Entries are immutable
-- (inserts only), but the same pattern applies so a stray UPDATE/DELETE in the
-- source would still be handled correctly instead of silently mis-modelled.
{{ cdc_latest_event(
    source('bronze', 'ledger_entries_cdc'),
    key='entry_id',
    columns=['transaction_id', 'account_id', 'direction', 'amount', 'asset',
             'posting_date', 'effective_date', 'reverses_entry_id', 'created_at']
) }}
