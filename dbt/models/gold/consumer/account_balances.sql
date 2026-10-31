-- Consumer view: balances as of the latest COMMITTED business day.
select
    account_id,
    asset,
    balance,
    snapshot_date
from {{ ref('account_balances_full_daily') }}
where snapshot_date = ({{ latest_committed_batch_date() }})
