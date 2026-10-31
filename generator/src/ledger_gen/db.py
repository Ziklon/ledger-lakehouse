"""Imperative shell: load LedgerState from Postgres, apply planned operations.

The database IS the generator's state (no state file): deleted keys are the
gaps in 1..max(account_id); balances come from the entries. Each operation runs
in its own transaction -> one atomic group of CDC events, one commit timestamp.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from decimal import Decimal

import psycopg

from ledger_gen.model import (
    Account,
    DeleteAccount,
    InsertAccount,
    InsertAsset,
    LedgerState,
    Operation,
    PostedEntry,
    PostTransaction,
    UpdateAccountStatus,
)

REVERSIBLE_LOOKBACK_TXNS = 200


def connect() -> psycopg.Connection:
    """Connection settings come from the environment (same .env as the stack).
    From the host, PG_HOST/PG_PORT are overridden to localhost + HOST_PG_PORT."""
    return psycopg.connect(
        host=os.environ.get("GEN_PG_HOST", "localhost"),
        port=int(os.environ.get("GEN_PG_PORT", os.environ.get("HOST_PG_PORT", "15432"))),
        dbname=os.environ["PG_DATABASE"],
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


def load_state(conn: psycopg.Connection) -> LedgerState:
    s = LedgerState()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT a.account_id, a.customer_id, a.asset, a.status,
                   coalesce(sum(CASE e.direction WHEN 'CREDIT' THEN e.amount
                                                 ELSE -e.amount END), 0) AS balance,
                   count(e.entry_id) AS entry_count
            FROM app.accounts a
            LEFT JOIN app.ledger_entries e USING (account_id)
            GROUP BY a.account_id, a.customer_id, a.asset, a.status
            ORDER BY a.account_id
        """)
        for account_id, customer_id, asset, status, balance, n in cur.fetchall():
            s.accounts[account_id] = Account(
                account_id, customer_id, asset, status, Decimal(balance), n
            )

        cur.execute("""
            SELECT coalesce(max(account_id), 0), coalesce(max(customer_id), 0)
            FROM app.accounts
        """)
        max_acct, max_cust = cur.fetchone()
        cur.execute("""
            SELECT coalesce(max(entry_id), 0), coalesce(max(transaction_id), 0)
            FROM app.ledger_entries
        """)
        max_entry, max_txn = cur.fetchone()
        s.next_account_id, s.next_customer_id = max_acct + 1, max_cust + 1
        s.next_entry_id, s.next_transaction_id = max_entry + 1, max_txn + 1

        # Deleted keys = gaps in the dense id sequence -> re-insert candidates.
        cur.execute(
            """
            SELECT g.id FROM generate_series(1, %s) AS g(id)
            WHERE NOT EXISTS (SELECT 1 FROM app.accounts a WHERE a.account_id = g.id)
            ORDER BY g.id
        """,
            (max_acct,),
        )
        s.deleted_account_ids = [r[0] for r in cur.fetchall()]

        # Recent transactions that are neither reversals nor already reversed.
        cur.execute(
            """
            WITH recent AS (
                SELECT transaction_id FROM app.ledger_entries
                GROUP BY transaction_id
                HAVING bool_and(reverses_entry_id IS NULL)
                ORDER BY transaction_id DESC LIMIT %s
            )
            SELECT e.transaction_id, e.entry_id, e.account_id, e.direction, e.amount, e.asset
            FROM app.ledger_entries e
            JOIN recent r USING (transaction_id)
            WHERE NOT EXISTS (SELECT 1 FROM app.ledger_entries rv
                              WHERE rv.reverses_entry_id = e.entry_id)
            ORDER BY e.transaction_id, e.entry_id
        """,
            (REVERSIBLE_LOOKBACK_TXNS,),
        )
        for txn_id, entry_id, account_id, direction, amount, asset in cur.fetchall():
            s.reversible.setdefault(txn_id, []).append(
                PostedEntry(entry_id, account_id, direction, Decimal(amount), asset)
            )
    return s


def _execute(cur: psycopg.Cursor, op: Operation) -> None:
    match op:
        case InsertAsset(asset, name, precision):
            cur.execute(
                "INSERT INTO app.assets (asset, name, precision) VALUES (%s, %s, %s)",
                (asset, name, precision),
            )
        case InsertAccount(account_id, customer_id, asset, status, _):
            cur.execute(
                """
                INSERT INTO app.accounts (account_id, customer_id, asset, status,
                                          opened_at, updated_at)
                VALUES (%s, %s, %s, %s, now(), now())
            """,
                (account_id, customer_id, asset, status),
            )
        case UpdateAccountStatus(account_id, status, _):
            cur.execute(
                "UPDATE app.accounts SET status = %s, updated_at = now() WHERE account_id = %s",
                (status, account_id),
            )
        case DeleteAccount(account_id, _):
            cur.execute("DELETE FROM app.accounts WHERE account_id = %s", (account_id,))
        case PostTransaction(txn_id, _, posting_date, effective_date, entries):
            cur.executemany(
                """
                INSERT INTO app.ledger_entries (entry_id, transaction_id, account_id, direction,
                    amount, asset, posting_date, effective_date, reverses_entry_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                [
                    (
                        e.entry_id,
                        txn_id,
                        e.account_id,
                        e.direction,
                        e.amount,
                        e.asset,
                        posting_date,
                        effective_date,
                        e.reverses_entry_id,
                    )
                    for e in entries
                ],
            )
        case _:
            raise TypeError(f"unknown operation {op!r}")


def apply(conn: psycopg.Connection, ops: list[Operation], pace_seconds: float = 0) -> Counter:
    """Apply each operation in its own transaction. pace_seconds spreads the
    operations evenly over wall-clock time (demo mode: a 'day' lasts minutes)."""
    delay = pace_seconds / len(ops) if ops and pace_seconds > 0 else 0
    stats: Counter = Counter()
    for op in ops:
        with conn.transaction(), conn.cursor() as cur:
            _execute(cur, op)
        stats[getattr(op, "kind", None) or getattr(op, "reason", None) or type(op).__name__] += 1
        if delay:
            time.sleep(delay)
    return stats
