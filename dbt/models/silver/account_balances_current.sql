{#
  Current balance per (account_id, asset) = sum(CREDIT) - sum(DEBIT)
  (a liability to the customer; house accounts go negative by design).

  Affected-keys recompute (docs/adr/0007):
    * affected = accounts with ledger entries in THIS window
    * for those only, recompute the balance from ALL their entries in silver
      (self-correcting; no delta arithmetic that would compound errors)
    * merge only rows whose hash changed: an affected account whose entries
      net to zero in the window produces no output change.
#}
{{ config(unique_key=['account_id', 'asset']) }}

with affected as (
    select distinct account_id
    from {{ ref('stg_ledger_entries_latest') }}
),

recomputed as (
    select
        e.account_id,
        e.asset,
        sum(case when e.direction = 'CREDIT' then e.amount else -e.amount end) as balance
    from {{ ref('ledger_entries_current') }} as e
    where e.account_id in (select account_id from affected)
      and not e.is_deleted
    group by e.account_id, e.asset
),

hashed as (
    select
        account_id,
        asset,
        balance,
        {{ dbt_utils.generate_surrogate_key(['account_id', 'asset', 'balance']) }} as row_hash
    from recomputed
)

select
    h.account_id,
    h.asset,
    h.balance,
    h.row_hash,
    cast('{{ var("batch_id") }}' as varchar) as _batch_id
from hashed as h
{% if is_incremental() %}
left join {{ this }} as t
    on h.account_id = t.account_id and h.asset = t.asset
where t.row_hash is null or t.row_hash <> h.row_hash
{% endif %}
