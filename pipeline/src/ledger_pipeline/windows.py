"""Batch window planning: pure, no I/O (unit-tested in tests/test_windows.py).

Rules (docs/adr/0012):
* window_start = window_end of the latest COMMITTED batch (epoch for the first),
  so a missed day is caught up by the next run: the window covers everything
  since the last committed watermark. Airflow dates never define the window.
* window_end = the cut-off: next 00:00 UTC after the business date, unless an
  explicit cut-off is given (demo mode: "now").
* Closed days are immutable: a batch older than the latest COMMITTED one is
  refused. Rerunning the latest COMMITTED batch reuses its exact window and
  predecessor, so the rerun reproduces the same result (idempotent).
* At most one batch in flight: a RUNNING batch with another id must be
  finished (rerun it) before a new one starts.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
COMMITTED = "COMMITTED"
RUNNING = "RUNNING"


class BatchRefused(Exception):
    """The requested batch must not run (closed day, bad cut-off, batch in flight)."""


@dataclass(frozen=True)
class BatchRecord:
    """A row of control.batches, as far as planning is concerned."""

    batch_id: date
    status: str
    window_start: datetime
    window_end: datetime
    prev_batch_id: date | None


@dataclass(frozen=True)
class BatchPlan:
    batch_id: date
    window_start: datetime
    window_end: datetime
    prev_batch_id: date | None
    is_rerun: bool  # rerun of the latest COMMITTED batch

    def to_dict(self) -> dict[str, str | bool | None]:
        """JSON-safe form, e.g. an Airflow XCom between tasks."""
        return {**self.dbt_vars(), "is_rerun": self.is_rerun}

    @classmethod
    def from_dict(cls, d: dict) -> BatchPlan:
        return cls(
            batch_id=date.fromisoformat(d["batch_id"]),
            window_start=datetime.fromisoformat(d["window_start"]),
            window_end=datetime.fromisoformat(d["window_end"]),
            prev_batch_id=date.fromisoformat(d["prev_batch_id"]) if d["prev_batch_id"] else None,
            is_rerun=bool(d["is_rerun"]),
        )

    def dbt_vars(self) -> dict[str, str | None]:
        """The ONLY batch identity dbt ever sees (never invocation_id)."""
        return {
            "batch_id": self.batch_id.isoformat(),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "prev_batch_id": self.prev_batch_id.isoformat() if self.prev_batch_id else None,
        }


def ceil_ms(ts: datetime) -> datetime:
    """Round UP to the millisecond -- the precision of _source_commit_ts
    (Debezium source.ts_ms) and of Trino's from_iso8601_timestamp.

    Up, never down: a commit's ms timestamp <= its true time < now <= ceil(now),
    so everything committed before "now" stays inside the window. Rounding
    down would push commits from the last instant into the next day.
    """
    extra = ts.microsecond % 1000
    return ts if extra == 0 else ts + timedelta(microseconds=1000 - extra)


def calendar_cutoff(batch_id: date) -> datetime:
    """Daily cut-off: 00:00 UTC at the end of the business date."""
    return datetime.combine(batch_id + timedelta(days=1), time(0), tzinfo=UTC)


def plan_batch(
    batch_id: date,
    batches: Sequence[BatchRecord],
    now: datetime,
    cutoff: datetime | None = None,
) -> BatchPlan:
    committed = [b for b in batches if b.status == COMMITTED]
    latest = max(committed, key=lambda b: b.batch_id, default=None)

    in_flight = [b for b in batches if b.status == RUNNING and b.batch_id != batch_id]
    if in_flight:
        ids = ", ".join(sorted(b.batch_id.isoformat() for b in in_flight))
        raise BatchRefused(f"batch {ids} is RUNNING; rerun it to completion first")

    if latest is not None and batch_id < latest.batch_id:
        raise BatchRefused(
            f"{batch_id} is a closed day (latest COMMITTED is {latest.batch_id}); "
            "closed days are immutable -- adjustments go into the next day"
        )

    if latest is not None and batch_id == latest.batch_id:
        return BatchPlan(
            batch_id=batch_id,
            window_start=latest.window_start,
            window_end=latest.window_end,
            prev_batch_id=latest.prev_batch_id,
            is_rerun=True,
        )

    window_start = latest.window_end if latest else EPOCH
    window_end = cutoff if cutoff is not None else calendar_cutoff(batch_id)
    if window_end.microsecond % 1000:
        # Stored and compared at ms precision: a sub-ms boundary would make
        # adjacent windows overlap after the round trip through control.batches.
        raise BatchRefused(f"cut-off {window_end} is not millisecond-aligned (use ceil_ms)")
    if window_end <= window_start:
        raise BatchRefused(f"cut-off {window_end} is not after window_start {window_start}")
    if window_end > now:
        raise BatchRefused(f"cut-off {window_end} is in the future (now {now})")

    return BatchPlan(
        batch_id=batch_id,
        window_start=window_start,
        window_end=window_end,
        prev_batch_id=latest.batch_id if latest else None,
        is_rerun=False,
    )


def eod_tag(batch_id: date) -> str:
    return f"eod_{batch_id:%Y_%m_%d}"


def close_tag(batch_id: date) -> str | None:
    """Month-end close tag, only on the last day of a month."""
    if (batch_id + timedelta(days=1)).day == 1:
        return f"close_{batch_id:%Y_%m}"
    return None
