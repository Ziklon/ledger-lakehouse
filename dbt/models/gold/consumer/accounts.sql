-- Consumer view: accounts as of the latest COMMITTED business day.
-- Consumers never filter soft deletes or pick snapshots themselves; a batch
-- that is RUNNING (or failed) is invisible here until commit_batch.
select
    account_id,
    customer_id,
    asset,
    status,
    opened_at,
    updated_at,
    snapshot_date
from {{ ref('accounts_full_daily') }}
where
    snapshot_date = ({{ latest_committed_batch_date() }})
    and not is_deleted
