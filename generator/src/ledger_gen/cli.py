"""ledger-gen CLI.

ledger-gen seed --date 2026-01-01                # initial load (once)
ledger-gen day  --date 2026-01-02                # one business day of traffic
ledger-gen day  --date 2026-01-03 --pace-seconds 120   # spread over 2 minutes
ledger-gen day  --date 2026-01-03 --dry-run      # print the plan, write nothing
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from ledger_gen import db
from ledger_gen.planner import plan_day, plan_seed


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="ledger-gen", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="command", required=True)
    for name in ("seed", "day"):
        p = sub.add_parser(name)
        p.add_argument(
            "--date",
            type=date.fromisoformat,
            required=True,
            help="business date (posting_date) of the generated traffic",
        )
        p.add_argument("--seed", type=int, default=42)
        p.add_argument(
            "--pace-seconds",
            type=float,
            default=0,
            help="spread operations over this many wall-clock seconds",
        )
        p.add_argument("--dry-run", action="store_true")
    sub.choices["seed"].add_argument("--customers", type=int, default=30)
    sub.choices["day"].add_argument(
        "--activity", type=int, default=40, help="number of random business events"
    )
    args = ap.parse_args(argv)

    with db.connect() as conn:
        state = db.load_state(conn)
        if args.command == "seed":
            ops, _ = plan_seed(state, args.seed, args.date, args.customers)
        else:
            if not state.accounts:
                print("source is empty: run `ledger-gen seed` first", file=sys.stderr)
                return 1
            ops, _ = plan_day(state, args.seed, args.date, args.activity)

        if args.dry_run:
            for op in ops:
                print(op)
            print(f"{len(ops)} operations (dry run)")
            return 0
        stats = db.apply(conn, ops, args.pace_seconds)

    print(f"{args.command} {args.date}: {len(ops)} operations committed")
    for kind, n in sorted(stats.items()):
        print(f"  {kind:<20} {n}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
