{#
  Invariant: published EOD balances equal an INDEPENDENT recompute from bronze.

  Independent = does not reuse staging, silver or the affected-keys logic
  (docs/adr/0007): it rebuilds every balance from the raw change log, up to
  the batch cut-off. A bug in either path shows up as a difference.
  Compared with account_balances_current AS OF the batch's EOD tag (the input
  gold published), both directions (EXCEPT), so missing, extra and wrong rows
  all fail.
#}
{{ config(tags=['invariant']) }}

with events as (
    select
        entry_id,
        account_id,
        asset,
        direction,
        amount,
        _op,
        row_number() over (partition by entry_id order by _lsn desc) as rn
    from {{ source('bronze', 'ledger_entries_cdc') }}
    where _source_commit_ts < from_iso8601_timestamp('{{ var("window_end") }}')
),

recomputed as (
    select
        account_id,
        asset,
        sum(case when direction = 'CREDIT' then amount else -amount end) as balance
    from events
    where rn = 1 and _op <> 'd'
    group by account_id, asset
),

published as (
    select
        account_id,
        asset,
        balance
    from {{ at_eod('account_balances_current', var('batch_id')) }}
),

only_recomputed as (
    select
        account_id,
        asset,
        balance
    from recomputed
    except
    select
        account_id,
        asset,
        balance
    from published
),

only_published as (
    select
        account_id,
        asset,
        balance
    from published
    except
    select
        account_id,
        asset,
        balance
    from recomputed
)

select
    'recompute not in published' as failure,
    account_id,
    asset,
    balance
from only_recomputed
union all
select
    'published not in recompute' as failure,
    account_id,
    asset,
    balance
from only_published
