"""Window rules of control.batches (ledger_pipeline.windows)."""

from datetime import UTC, date, datetime

import pytest

from ledger_pipeline.windows import (
    EPOCH,
    BatchRecord,
    BatchRefused,
    calendar_cutoff,
    ceil_ms,
    close_tag,
    eod_tag,
    plan_batch,
)

NOW = datetime(2026, 1, 10, 12, tzinfo=UTC)
D = date.fromisoformat


def committed(day: str, start: datetime, end: datetime, prev: str | None = None) -> BatchRecord:
    return BatchRecord(D(day), "COMMITTED", start, end, D(prev) if prev else None)


def test_first_batch_starts_at_epoch_and_cuts_at_next_midnight_utc():
    plan = plan_batch(D("2026-01-02"), [], now=NOW)
    assert plan.window_start == EPOCH
    assert plan.window_end == datetime(2026, 1, 3, tzinfo=UTC)
    assert plan.prev_batch_id is None and not plan.is_rerun


def test_next_window_starts_at_previous_committed_end():
    b1 = committed("2026-01-02", EPOCH, calendar_cutoff(D("2026-01-02")))
    plan = plan_batch(D("2026-01-03"), [b1], now=NOW)
    assert plan.window_start == b1.window_end  # no gap, no overlap
    assert plan.prev_batch_id == D("2026-01-02")


def test_missed_day_is_caught_up_by_the_next_run():
    b1 = committed("2026-01-02", EPOCH, calendar_cutoff(D("2026-01-02")))
    plan = plan_batch(D("2026-01-04"), [b1], now=NOW)  # 01-03 never ran
    assert plan.window_start == datetime(2026, 1, 3, tzinfo=UTC)
    assert plan.window_end == datetime(2026, 1, 5, tzinfo=UTC)
    assert plan.prev_batch_id == D("2026-01-02")


def test_rerun_of_latest_committed_reuses_its_exact_window():
    b1 = committed("2026-01-02", EPOCH, calendar_cutoff(D("2026-01-02")))
    b2 = committed("2026-01-03", b1.window_end, calendar_cutoff(D("2026-01-03")), "2026-01-02")
    plan = plan_batch(D("2026-01-03"), [b1, b2], now=NOW, cutoff=NOW)  # cutoff ignored
    assert (plan.window_start, plan.window_end) == (b2.window_start, b2.window_end)
    assert plan.prev_batch_id == D("2026-01-02") and plan.is_rerun


def test_closed_day_is_refused():
    b1 = committed("2026-01-02", EPOCH, calendar_cutoff(D("2026-01-02")))
    b2 = committed("2026-01-03", b1.window_end, calendar_cutoff(D("2026-01-03")), "2026-01-02")
    with pytest.raises(BatchRefused, match="closed day"):
        plan_batch(D("2026-01-02"), [b1, b2], now=NOW)


def test_running_batch_blocks_a_different_batch_but_not_its_own_retry():
    running = BatchRecord(D("2026-01-02"), "RUNNING", EPOCH, NOW, None)
    with pytest.raises(BatchRefused, match="RUNNING"):
        plan_batch(D("2026-01-03"), [running], now=NOW)
    assert plan_batch(D("2026-01-02"), [running], now=NOW, cutoff=NOW).window_end == NOW


def test_explicit_cutoff_demo_mode():
    plan = plan_batch(D("2026-01-02"), [], now=NOW, cutoff=NOW)
    assert plan.window_end == NOW


def test_future_cutoff_and_empty_window_are_refused():
    with pytest.raises(BatchRefused, match="future"):
        plan_batch(D("2026-01-20"), [], now=NOW)
    b1 = committed("2026-01-02", EPOCH, NOW)
    with pytest.raises(BatchRefused, match="not after"):
        plan_batch(D("2026-01-03"), [b1], now=NOW, cutoff=NOW)


def test_dbt_vars_are_the_batch_identity():
    v = plan_batch(D("2026-01-02"), [], now=NOW).dbt_vars()
    assert v == {
        "batch_id": "2026-01-02",
        "window_start": "1970-01-01T00:00:00+00:00",
        "window_end": "2026-01-03T00:00:00+00:00",
        "prev_batch_id": None,
    }


def test_tag_names():
    assert eod_tag(D("2026-01-02")) == "eod_2026_01_02"
    assert close_tag(D("2026-01-31")) == "close_2026_01"
    assert close_tag(D("2024-02-29")) == "close_2024_02"
    assert close_tag(D("2026-01-30")) is None


def test_cutoff_is_rounded_up_to_the_millisecond_and_must_be_aligned():
    t = datetime(2026, 1, 2, 3, 4, 5, 574680, tzinfo=UTC)
    assert ceil_ms(t) == datetime(2026, 1, 2, 3, 4, 5, 575000, tzinfo=UTC)
    assert ceil_ms(ceil_ms(t)) == ceil_ms(t)  # aligned values are unchanged
    with pytest.raises(BatchRefused, match="millisecond"):
        plan_batch(D("2026-01-02"), [], now=NOW, cutoff=t)
