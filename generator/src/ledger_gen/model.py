"""Domain model: ledger state and the operations the planner emits.

Operations are plain immutable values. The planner (pure) produces them; the
db module (I/O) executes each one as its own database transaction, so every
operation becomes one atomic group of CDC events with one commit timestamp.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Literal

Direction = Literal["DEBIT", "CREDIT"]
Status = Literal["ACTIVE", "FROZEN", "CLOSED"]

HOUSE_CUSTOMER_ID = 0  # the exchange itself: counterparty for deposits/withdrawals/trades

# asset -> (display name, decimal precision)
ASSETS: dict[str, tuple[str, int]] = {
    "BTC": ("Bitcoin", 8),
    "ETH": ("Ether", 18),
    "USDT": ("Tether USD", 6),
    "USD": ("US Dollar", 2),
}


def quantize(amount: Decimal, asset: str) -> Decimal:
    """Round to the asset's precision. Money is Decimal end to end, never float."""
    return amount.quantize(Decimal(1).scaleb(-ASSETS[asset][1]))


# --------------------------------------------------------------------------- state
@dataclass
class Account:
    account_id: int
    customer_id: int
    asset: str
    status: Status
    balance: Decimal = Decimal(0)  # sum(CREDIT) - sum(DEBIT): a liability to the customer
    entry_count: int = 0

    @property
    def is_house(self) -> bool:
        return self.customer_id == HOUSE_CUSTOMER_ID


@dataclass(frozen=True)
class PostedEntry:
    entry_id: int
    account_id: int
    direction: Direction
    amount: Decimal
    asset: str


@dataclass
class LedgerState:
    """Everything the planner needs to know about the current source DB."""

    accounts: dict[int, Account] = field(default_factory=dict)
    # recent, reversible transactions: transaction_id -> its entries
    reversible: dict[int, list[PostedEntry]] = field(default_factory=dict)
    next_account_id: int = 1
    next_customer_id: int = 1
    next_entry_id: int = 1
    next_transaction_id: int = 1
    deleted_account_ids: list[int] = field(default_factory=list)  # gaps -> re-insert candidates

    def house_account(self, asset: str) -> Account:
        return next(a for a in self.accounts.values() if a.is_house and a.asset == asset)

    def active_customer_accounts(self) -> list[Account]:
        return sorted(
            (a for a in self.accounts.values() if not a.is_house and a.status == "ACTIVE"),
            key=lambda a: a.account_id,
        )


# ---------------------------------------------------------------------- operations
@dataclass(frozen=True)
class InsertAsset:
    asset: str
    name: str
    precision: int


@dataclass(frozen=True)
class InsertAccount:
    account_id: int
    customer_id: int
    asset: str
    status: Status = "ACTIVE"
    reason: str = "new_account"


@dataclass(frozen=True)
class UpdateAccountStatus:
    account_id: int
    status: Status
    reason: str = "status_change"


@dataclass(frozen=True)
class DeleteAccount:
    account_id: int
    reason: str = "delete"


@dataclass(frozen=True)
class EntrySpec:
    entry_id: int
    account_id: int
    direction: Direction
    amount: Decimal
    asset: str
    reverses_entry_id: int | None = None


@dataclass(frozen=True)
class PostTransaction:
    transaction_id: int
    kind: str  # deposit | withdrawal | transfer | trade | reversal | close_out
    posting_date: date
    effective_date: date
    entries: tuple[EntrySpec, ...]

    def is_balanced(self) -> bool:
        """Double entry: per asset, sum(DEBIT) == sum(CREDIT)."""
        per_asset: dict[str, Decimal] = {}
        for e in self.entries:
            signed = e.amount if e.direction == "DEBIT" else -e.amount
            per_asset[e.asset] = per_asset.get(e.asset, Decimal(0)) + signed
        return all(v == 0 for v in per_asset.values())


Operation = InsertAsset | InsertAccount | UpdateAccountStatus | DeleteAccount | PostTransaction
