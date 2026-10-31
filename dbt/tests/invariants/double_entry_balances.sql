{#
  Invariant: every ledger transaction balances. Per transaction AND asset,
  sum(DEBIT) = sum(CREDIT), over all live entries in silver.

  All transactions, not only today's: one aggregation is cheap in Trino, and it
  also catches an entry LOST on the way (one leg missing), not just a bad
  generator. At very large scale, scope to transactions touched in the window
  (entries are immutable, so older transactions can't change).
#}
{{ config(tags=['invariant']) }}

select
    transaction_id,
    asset,
    sum(case when direction = 'DEBIT' then amount else 0 end) as debits,
    sum(case when direction = 'CREDIT' then amount else 0 end) as credits
from {{ ref('ledger_entries_current') }}
where not is_deleted
group by transaction_id, asset
having
    sum(case when direction = 'DEBIT' then amount else 0 end)
    <> sum(case when direction = 'CREDIT' then amount else 0 end)
