"""Property tests for the pure planner: no database needed.

Each test runs several seeds over several chained business days (the state
returned by one day feeds the next, like the real DB would)."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal

import pytest

from ledger_gen.model import (
    ASSETS,
    DeleteAccount,
    InsertAccount,
    LedgerState,
    Operation,
    PostTransaction,
    UpdateAccountStatus,
)
from ledger_gen.planner import plan_day, plan_seed

SEEDS = [1, 7, 42, 1234, 99999]
START = date(2026, 1, 1)
DAYS = 4


def simulate(seed: int) -> list[list[Operation]]:
    """seed day + DAYS business days; returns ops per day."""
    ops, state = plan_seed(LedgerState(), seed, START)
    days = [ops]
    for d in range(1, DAYS + 1):
        ops, state = plan_day(state, seed, START + timedelta(days=d))
        days.append(ops)
    return days


@pytest.fixture(params=SEEDS, ids=lambda s: f"seed{s}")
def days(request) -> list[list[Operation]]:
    return simulate(request.param)


def txns(day_ops: list[Operation]) -> list[PostTransaction]:
    return [o for o in day_ops if isinstance(o, PostTransaction)]


def test_deterministic():
    assert simulate(42) == simulate(42)
    assert simulate(42) != simulate(43)


def test_every_transaction_is_balanced_per_asset(days):
    for day in days:
        for t in txns(day):
            per_asset: dict[str, Decimal] = defaultdict(Decimal)
            for e in t.entries:
                per_asset[e.asset] += e.amount if e.direction == "DEBIT" else -e.amount
            assert all(v == 0 for v in per_asset.values()), t


def test_amounts_positive_and_exact_at_asset_precision(days):
    for day in days:
        for t in txns(day):
            for e in t.entries:
                assert e.amount > 0
                assert -e.amount.as_tuple().exponent <= ASSETS[e.asset][1], e


def test_customer_balances_never_negative(days):
    balances: dict[int, Decimal] = defaultdict(Decimal)
    house: set[int] = set()
    for day in days:
        for op in day:
            if isinstance(op, InsertAccount) and op.customer_id == 0:
                house.add(op.account_id)
            if isinstance(op, PostTransaction):
                for e in op.entries:
                    balances[e.account_id] += e.amount if e.direction == "CREDIT" else -e.amount
                    if e.account_id not in house:
                        assert balances[e.account_id] >= 0, (op, e)


def test_effective_date_never_after_posting_date(days):
    for day in days:
        for t in txns(day):
            assert t.effective_date <= t.posting_date


def test_edge_cases_present_every_business_day(days):
    for day in days[1:]:
        reasons = Counter(getattr(o, "reason", None) for o in day)
        kinds = Counter(t.kind for t in txns(day))
        assert reasons["create_then_delete"] == 2  # the insert and the delete
        assert reasons["flap_freeze"] == 1 and reasons["flap_unfreeze"] == 1
        assert reasons["multi_update"] == 3
        assert kinds["reversal"] >= 1
        assert any(t.effective_date < t.posting_date for t in txns(day)), "no back-dated entry"


def test_create_then_delete_targets_same_key_with_no_entries_in_between(days):
    for day in days[1:]:
        created = {
            o.account_id
            for o in day
            if isinstance(o, InsertAccount) and o.reason == "create_then_delete"
        }
        deleted = {
            o.account_id
            for o in day
            if isinstance(o, DeleteAccount) and o.reason == "create_then_delete"
        }
        assert created == deleted and created
        touched = {e.account_id for t in txns(day) for e in t.entries}
        assert not created & touched


def test_deleted_key_is_reinserted_on_a_later_day(days):
    deleted_before: set[int] = set()
    reinserted = 0
    for day in days[1:]:
        for o in day:
            if isinstance(o, InsertAccount) and o.reason == "reinsert":
                assert o.account_id in deleted_before
                reinserted += 1
        deleted_before |= {o.account_id for o in day if isinstance(o, DeleteAccount)}
    assert reinserted >= DAYS - 1


def test_reversal_mirrors_original_entries(days):
    entries = {}
    for day in days:
        for t in txns(day):
            for e in t.entries:
                entries[e.entry_id] = e
                if e.reverses_entry_id is not None:
                    orig = entries[e.reverses_entry_id]
                    assert (orig.account_id, orig.amount, orig.asset) == (
                        e.account_id,
                        e.amount,
                        e.asset,
                    )
                    assert orig.direction != e.direction


def test_closed_accounts_have_no_later_activity(days):
    closed: set[int] = set()
    for day in days:
        for op in day:
            if isinstance(op, PostTransaction):
                assert not {e.account_id for e in op.entries} & closed, op
            if isinstance(op, UpdateAccountStatus):
                assert op.account_id not in closed, op
                if op.status == "CLOSED":
                    closed.add(op.account_id)
