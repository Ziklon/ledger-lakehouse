"""Pure traffic planner: (state, seed, business_date) -> operations.

No I/O, no clock. Same inputs -> same operations, so CI failures reproduce
exactly. Each business day interleaves:

* random activity: new accounts, deposits, withdrawals, transfers, trades
* guaranteed CDC edge cases (the cases silver/gold must get right):
    - several updates to one key in a day (incl. a net no-op flap)
    - create + delete of the same key in a day
    - delete of an unused account (leaves a gap...)
    - ...re-insert of a previously deleted key on a later day
    - reversal of an earlier transaction (immutable ledger: new entries)
    - back-dated entry (effective_date < posting_date)
    - account close-out (sweep balance to zero, then CLOSED)

Scenarios are generators that yield after each emitted operation; a seeded
scheduler interleaves them, preserving each scenario's internal order.
"""

from __future__ import annotations

import copy
import random
from collections.abc import Callable, Iterator
from datetime import date, timedelta
from decimal import Decimal

from ledger_gen.model import (
    ASSETS,
    HOUSE_CUSTOMER_ID,
    Account,
    DeleteAccount,
    Direction,
    EntrySpec,
    InsertAccount,
    InsertAsset,
    LedgerState,
    Operation,
    PostedEntry,
    PostTransaction,
    Status,
    UpdateAccountStatus,
)

# Reference prices (USD) used to size amounts and price trades.
PRICES_USD: dict[str, Decimal] = {
    "BTC": Decimal(60000),
    "ETH": Decimal(3000),
    "USDT": Decimal(1),
    "USD": Decimal(1),
}

Leg = tuple[Account, Direction, Decimal]
Scenario = Iterator[None]


class Planner:
    def __init__(self, state: LedgerState, seed: int, business_date: date) -> None:
        # Plan against a private copy: later operations see earlier ones' effects.
        self.state = copy.deepcopy(state)
        self.rng = random.Random(f"{seed}:{business_date.isoformat()}")
        self.date = business_date
        self.ops: list[Operation] = []
        # Accounts a running scenario is working on; other scenarios must not touch them.
        self.reserved: set[int] = set()
        # Keys deleted on PREVIOUS days only. Re-inserting a key deleted earlier
        # today would turn the "create+delete in one window" case into
        # "create+delete+create" -- a different edge case.
        self.prior_deleted: list[int] = sorted(state.deleted_account_ids)

    # ------------------------------------------------------------- primitives
    def _amount(self, asset: str, usd_lo: int, usd_hi: int) -> Decimal:
        """Exact random amount: an integer number of the asset's minor units."""
        precision = ASSETS[asset][1]
        scale = Decimal(10) ** precision
        lo = int(Decimal(usd_lo) / PRICES_USD[asset] * scale)
        hi = int(Decimal(usd_hi) / PRICES_USD[asset] * scale)
        return Decimal(self.rng.randint(max(lo, 1), max(hi, 2))).scaleb(-precision)

    def _portion(self, balance: Decimal, asset: str) -> Decimal:
        """A random amount in (0, balance], in minor units."""
        precision = ASSETS[asset][1]
        units = int(balance.scaleb(precision))
        return Decimal(self.rng.randint(1, units)).scaleb(-precision)

    def _new_account(
        self,
        customer_id: int | None = None,
        asset: str | None = None,
        account_id: int | None = None,
        reason: str = "new_account",
    ) -> Account:
        s = self.state
        if customer_id is None:
            customer_id = s.next_customer_id
            s.next_customer_id += 1
        if account_id is None:
            account_id = s.next_account_id
            s.next_account_id += 1
        asset = asset or self.rng.choice(sorted(ASSETS))
        acct = Account(account_id, customer_id, asset, "ACTIVE")
        s.accounts[account_id] = acct
        self.ops.append(InsertAccount(account_id, customer_id, asset, "ACTIVE", reason))
        return acct

    def _set_status(self, acct: Account, status: Status, reason: str) -> None:
        acct.status = status
        self.ops.append(UpdateAccountStatus(acct.account_id, status, reason))

    def _delete(self, acct: Account, reason: str) -> None:
        assert acct.entry_count == 0, "FK: accounts with entries cannot be deleted"
        del self.state.accounts[acct.account_id]
        self.state.deleted_account_ids.append(acct.account_id)
        self.ops.append(DeleteAccount(acct.account_id, reason))

    def _post(
        self,
        kind: str,
        legs: list[Leg],
        effective_date: date | None = None,
        reverses: list[int | None] | None = None,
    ) -> PostTransaction:
        s = self.state
        txn_id = s.next_transaction_id
        s.next_transaction_id += 1
        entries: list[EntrySpec] = []
        for i, (acct, direction, amount) in enumerate(legs):
            entries.append(
                EntrySpec(
                    s.next_entry_id,
                    acct.account_id,
                    direction,
                    amount,
                    acct.asset,
                    reverses[i] if reverses else None,
                )
            )
            s.next_entry_id += 1
            acct.balance += amount if direction == "CREDIT" else -amount
            acct.entry_count += 1
            assert acct.is_house or acct.balance >= 0, "customer balance went negative"
        txn = PostTransaction(txn_id, kind, self.date, effective_date or self.date, tuple(entries))
        assert txn.is_balanced(), f"unbalanced transaction {txn}"
        if kind != "reversal":
            s.reversible[txn_id] = [
                PostedEntry(e.entry_id, e.account_id, e.direction, e.amount, e.asset)
                for e in entries
            ]
        self.ops.append(txn)
        return txn

    # -------------------------------------------------------- business events
    def deposit(self, acct: Account, effective_date: date | None = None) -> None:
        amt = self._amount(acct.asset, 50, 5000)
        house = self.state.house_account(acct.asset)
        self._post("deposit", [(house, "DEBIT", amt), (acct, "CREDIT", amt)], effective_date)

    def withdrawal(
        self, acct: Account, amount: Decimal | None = None, kind: str = "withdrawal"
    ) -> None:
        amt = amount if amount is not None else self._portion(acct.balance, acct.asset)
        house = self.state.house_account(acct.asset)
        self._post(kind, [(acct, "DEBIT", amt), (house, "CREDIT", amt)])

    def transfer(self, src: Account, dst: Account) -> None:
        amt = self._portion(src.balance, src.asset)
        self._post("transfer", [(src, "DEBIT", amt), (dst, "CREDIT", amt)])

    def trade(self, sell: Account, buy: Account) -> None:
        """Customer sells `sell.asset` for `buy.asset`; the house is counterparty.
        Two legs, each balanced in its own asset."""
        sell_amt = self._portion(sell.balance, sell.asset)
        usd = sell_amt * PRICES_USD[sell.asset]
        precision = ASSETS[buy.asset][1]
        buy_amt = (usd / PRICES_USD[buy.asset]).quantize(Decimal(1).scaleb(-precision))
        if buy_amt <= 0:
            return
        hs, hb = self.state.house_account(sell.asset), self.state.house_account(buy.asset)
        self._post(
            "trade",
            [
                (sell, "DEBIT", sell_amt),
                (hs, "CREDIT", sell_amt),
                (hb, "DEBIT", buy_amt),
                (buy, "CREDIT", buy_amt),
            ],
        )

    def reverse(self, txn_id: int) -> bool:
        """Mirror an earlier transaction with directions swapped. Skipped if it
        would drive a customer balance negative (funds already spent)."""
        original = self.state.reversible[txn_id]
        legs: list[Leg] = []
        for e in original:
            acct = self.state.accounts.get(e.account_id)
            if acct is None or acct.status == "CLOSED":
                return False
            swapped: Direction = "CREDIT" if e.direction == "DEBIT" else "DEBIT"
            if not acct.is_house and swapped == "DEBIT" and acct.balance < e.amount:
                return False
            legs.append((acct, swapped, e.amount))
        del self.state.reversible[txn_id]
        self._post("reversal", legs, reverses=[e.entry_id for e in original])
        return True

    # --------------------------------------------------------------- scenarios
    def _available(self) -> list[Account]:
        """Active customer accounts not reserved by an in-flight scenario."""
        return [
            a for a in self.state.active_customer_accounts() if a.account_id not in self.reserved
        ]

    def _funded(self) -> list[Account]:
        return [a for a in self._available() if a.balance > 0]

    def sc_random_activity(self, n: int) -> Scenario:
        for _ in range(n):
            roll = self.rng.random()
            funded = self._funded()
            if roll < 0.15 or not funded:  # new account (+ deposit)
                acct = self._new_account()
                if self.rng.random() < 0.8:
                    self.deposit(acct)
            elif roll < 0.45:  # deposit
                self.deposit(self.rng.choice(self._available()))
            elif roll < 0.60:  # withdrawal
                self.withdrawal(self.rng.choice(funded))
            elif roll < 0.80:  # transfer, same asset
                src = self.rng.choice(funded)
                peers = [
                    a
                    for a in self._available()
                    if a.asset == src.asset and a.account_id != src.account_id
                ]
                if peers:
                    self.transfer(src, self.rng.choice(peers))
            else:  # trade, same customer
                sell = self.rng.choice(funded)
                buys = [
                    a
                    for a in self._available()
                    if a.customer_id == sell.customer_id and a.asset != sell.asset
                ]
                if not buys:
                    buys = [
                        self._new_account(
                            sell.customer_id,
                            self.rng.choice([x for x in sorted(ASSETS) if x != sell.asset]),
                        )
                    ]
                self.trade(sell, self.rng.choice(buys))
            yield

    def _reserve(self, acct: Account) -> Account:
        self.reserved.add(acct.account_id)
        return acct

    def _release(self, acct: Account) -> None:
        self.reserved.discard(acct.account_id)

    def sc_status_flap(self) -> Scenario:
        """ACTIVE -> FROZEN -> ACTIVE in one day: a net no-op for consumers.
        Silver mirrors it (updated_at moved); gold's hash covers published
        columns only, so it must not appear in the day's INCREMENTAL."""
        acct = self._reserve(self.rng.choice(self._available()))
        self._set_status(acct, "FROZEN", "flap_freeze")
        yield
        self._set_status(acct, "ACTIVE", "flap_unfreeze")
        self._release(acct)
        yield

    def sc_multi_update(self) -> Scenario:
        """FROZEN -> ACTIVE -> FROZEN: several updates, one net change."""
        acct = self._reserve(self.rng.choice(self._available()))
        for status in ("FROZEN", "ACTIVE", "FROZEN"):
            self._set_status(acct, status, "multi_update")  # type: ignore[arg-type]
            yield
        self._release(acct)

    def sc_create_then_delete(self) -> Scenario:
        """Insert then delete the same key in one day: silver must skip it."""
        acct = self._reserve(self._new_account(reason="create_then_delete"))
        yield
        self._delete(acct, "create_then_delete")
        self._release(acct)
        yield

    def sc_delete_unused(self) -> Scenario:
        """Delete an existing never-funded account opened on an earlier day."""
        unused = [a for a in self._available() if a.entry_count == 0]
        if unused:
            self._delete(self.rng.choice(unused), "delete_unused")
        yield

    def sc_reinsert(self) -> Scenario:
        """Re-insert a key deleted on an earlier day: latest event wins in silver."""
        if self.prior_deleted:
            account_id = self.prior_deleted.pop(0)
            self.state.deleted_account_ids.remove(account_id)
            acct = self._reserve(self._new_account(account_id=account_id, reason="reinsert"))
            yield
            self.deposit(acct)
            self._release(acct)
        yield

    def sc_reversal(self) -> Scenario:
        for txn_id in sorted(self.state.reversible):
            entries = self.state.reversible[txn_id]
            if any(e.account_id in self.reserved for e in entries):
                continue
            if self.reverse(txn_id):
                break
        yield

    def sc_back_dated(self) -> Scenario:
        """Deposit booked today that economically belongs to two days ago."""
        accts = self._available()
        if accts:
            self.deposit(self.rng.choice(accts), effective_date=self.date - timedelta(days=2))
        yield

    def sc_close_out(self) -> Scenario:
        """Sweep the balance to zero, then close."""
        funded = self._funded()
        if funded:
            acct = self._reserve(self.rng.choice(funded))
            self.withdrawal(acct, amount=acct.balance, kind="close_out")
            yield
            self._set_status(acct, "CLOSED", "close_out")
            self._release(acct)
        yield

    # ---------------------------------------------------------------- driver
    def _interleave(self, scenarios: list[Callable[[], Scenario]]) -> None:
        live = [s() for s in scenarios]
        while live:
            it = self.rng.choice(live)
            try:
                next(it)
            except StopIteration:
                live.remove(it)


def plan_seed(
    state: LedgerState, seed: int, business_date: date, customers: int = 30
) -> tuple[list[Operation], LedgerState]:
    """Initial load: assets, one house account per asset, customers + deposits."""
    if state.accounts:
        raise ValueError("source already seeded")
    p = Planner(state, seed, business_date)
    for asset, (name, precision) in sorted(ASSETS.items()):
        p.ops.append(InsertAsset(asset, name, precision))
    for asset in sorted(ASSETS):
        p._new_account(customer_id=HOUSE_CUSTOMER_ID, asset=asset, reason="house")
    p.state.next_customer_id = max(p.state.next_customer_id, 1)
    for _ in range(customers):
        acct = p._new_account()
        if p.rng.random() < 0.85:  # some accounts stay unfunded -> deletable later
            p.deposit(acct)
    return p.ops, p.state


def plan_day(
    state: LedgerState, seed: int, business_date: date, activity: int = 40
) -> tuple[list[Operation], LedgerState]:
    """One business day of traffic with every edge case present."""
    p = Planner(state, seed, business_date)
    p._interleave(
        [
            lambda: p.sc_random_activity(activity),
            p.sc_status_flap,
            p.sc_multi_update,
            p.sc_create_then_delete,
            p.sc_delete_unused,
            p.sc_reinsert,
            p.sc_reversal,
            p.sc_back_dated,
            p.sc_close_out,
        ]
    )
    return p.ops, p.state
