"""Completeness gate (ledger_pipeline.gate) against a simulated Iceberg sink.

The fake follows the commit protocol of the Iceberg Kafka Connect sink 1.12.0
(kafka-connect/.../channel/Worker.java, Channel.java, Coordinator.java):

  worker_txn():     on START_COMMIT the worker closes its files and, in ONE Kafka
                    transaction, sends DATA_WRITTEN and commits the SOURCE offsets
                    (consumer group connect-bronze-sink).  -> offsets move
  iceberg_commit(): later, the coordinator appends those files, ONE COMMIT PER
                    TABLE; a failed table keeps its files for the next cycle.
                                                           -> rows become visible
Offsets therefore move BEFORE rows are visible, and tables can lag each other:
those gaps are what the gate must survive.
Each gate sleep() advances a scripted timeline, so races are deterministic.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

from ledger_pipeline.gate import Probes, run_gate

CUTOFF = datetime(2026, 1, 3, tzinfo=UTC)
HB = ("ledger.app.cdc_heartbeat", 0)
LE = ("ledger.app.ledger_entries", 0)


@dataclass
class Event:
    lsn: int
    ts: datetime
    offset: int  # Kafka offset in its partition


@dataclass
class FakeCdc:
    """Postgres WAL + Kafka topics + the Iceberg sink, reduced to what the gate sees."""

    wal_lsn: int = 0
    log: dict = field(default_factory=lambda: {HB: [], LE: []})  # Kafka partitions
    consumed: dict = field(default_factory=lambda: {HB: 0, LE: 0})  # in open files
    staged: list = field(default_factory=list)  # DATA_WRITTEN, not yet in Iceberg
    committed: dict = field(default_factory=lambda: {HB: -1, LE: -1})  # source group
    visible: dict = field(default_factory=lambda: {HB: [], LE: []})  # rows in bronze
    timeline: list[Callable[[], None]] = field(default_factory=list)  # scripted steps first
    debezium_up: bool = True  # then, each sleep: a heartbeat is emitted and read...
    iceberg_up: bool = True  # ...and committed to Iceberg
    failing_tables: set = field(default_factory=set)  # their Iceberg commits fail
    # bronze rows carry _kafka_offset only from this offset on (earlier rows were
    # written before the column existed: NULL)
    offsets_from: dict = field(default_factory=lambda: {HB: 0, LE: 0})
    now: float = 0.0

    # -- source side ---------------------------------------------------------
    def emit(self, tp, seconds_after_cutoff: float) -> Event:
        self.wal_lsn += 10
        ts = CUTOFF + timedelta(seconds=seconds_after_cutoff)
        ev = Event(self.wal_lsn, ts, offset=len(self.log[tp]))
        self.log[tp].append(ev)
        return ev

    # -- sink side -----------------------------------------------------------
    def consume(self, tp, upto: int | None = None) -> None:
        """The worker reads `tp` up to offset `upto` (default: everything)."""
        self.consumed[tp] = len(self.log[tp]) if upto is None else upto

    def worker_txn(self) -> None:
        batch = []
        for tp, upto in self.consumed.items():
            start = self.committed[tp] if self.committed[tp] >= 0 else 0
            batch += [(tp, ev) for ev in self.log[tp][start:upto]]
            if upto:
                self.committed[tp] = upto
        self.staged.append(batch)

    def iceberg_commit(self) -> None:
        if not self.iceberg_up:
            return
        kept = []
        for batch in self.staged:
            for tp, ev in batch:
                if tp in self.failing_tables:
                    kept.append((tp, ev))  # rolled into the next commit
                else:
                    self.visible[tp].append(ev)
        self.staged = [kept] if kept else []

    def cycle(self) -> None:
        """One full commit cycle for everything the worker has read."""
        self.worker_txn()
        self.iceberg_commit()

    # -- what the gate observes ---------------------------------------------
    def probes(self) -> Probes:
        return Probes(
            latest_heartbeat_ts=lambda: max((e.ts for e in self.visible[HB]), default=None),
            end_offsets=lambda: {tp: len(evs) for tp, evs in self.log.items()},
            committed_offsets=lambda parts: {tp: self.committed[tp] for tp in parts},
            # the migration recorded the first offset carrying _kafka_offset
            offset_baseline=lambda: {tp: o for tp, o in self.offsets_from.items() if o > 0},
            bronze_offsets=lambda wanted: {
                tp: o
                for tp, low in wanted.items()
                if (o := self._bronze_offsets(tp, low)) is not None
            },
            source_lsn=lambda: self.wal_lsn,
            latest_heartbeat_lsn=lambda since: max(
                (e.lsn for e in self.visible[HB] if e.ts >= since), default=None
            ),
        )

    def _bronze_offsets(self, tp, low: int) -> int | None:
        """max(_kafka_offset) in bronze for `tp` among rows >= low (NULL offsets excluded)."""
        offsets = [e.offset for e in self.visible[tp] if e.offset >= self.offsets_from[tp]]
        return max((o for o in offsets if o >= low), default=None)

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        if self.timeline:
            self.timeline.pop(0)()
        elif self.debezium_up:
            self.emit(HB, self.now)
            self.consume(HB)
            self.worker_txn()
            self.iceberg_commit()

    def gate(self, timeout_s: int = 60):
        return run_gate(
            CUTOFF, self.probes(), timeout_s=timeout_s, poll_s=5,
            clock=lambda: self.now, sleep=self.sleep,
        )  # fmt: skip


def test_passes_when_bronze_holds_everything_before_the_cutoff():
    cdc = FakeCdc()
    cdc.emit(LE, -1)
    cdc.emit(HB, +1)
    cdc.consume(LE)
    cdc.consume(HB)
    cdc.cycle()
    result = cdc.gate()
    assert result.heartbeat_ts == CUTOFF + timedelta(seconds=1)
    assert len(cdc.visible[LE]) == 1


def test_waits_for_a_heartbeat_after_the_cutoff():
    cdc = FakeCdc()
    cdc.emit(LE, -1)
    cdc.emit(HB, -0.5)  # heartbeat still before the cut-off
    cdc.consume(LE)
    cdc.consume(HB)
    cdc.cycle()
    cdc.timeline = [lambda: (cdc.emit(HB, +1), cdc.consume(HB), cdc.cycle())]
    assert cdc.gate().heartbeat_ts == CUTOFF + timedelta(seconds=1)


def test_times_out_when_no_heartbeat_reaches_bronze():
    cdc = FakeCdc(debezium_up=False)  # connector down: nothing ever arrives
    with pytest.raises(TimeoutError, match="latest heartbeat in bronze None"):
        cdc.gate(timeout_s=20)


def test_times_out_when_the_sink_reads_but_iceberg_commits_fail():
    """Catalog down: the worker keeps reading and committing offsets (checks 1-2
    would pass), but nothing new becomes visible in bronze."""
    cdc = FakeCdc()
    cdc.emit(HB, +1)
    cdc.consume(HB)
    cdc.cycle()
    cdc.emit(LE, +2)
    cdc.consume(LE)
    cdc.worker_txn()  # read, offsets committed, not in Iceberg
    cdc.iceberg_up = False
    with pytest.raises(TimeoutError, match=r"read but not in bronze yet: .*ledger_entries\[0\]"):
        cdc.gate(timeout_s=20)


def test_an_unsubscribed_partition_blocks():
    cdc = FakeCdc()
    cdc.emit(LE, -1)
    cdc.emit(HB, +1)
    cdc.consume(HB)  # the sink never subscribed to ledger_entries
    cdc.cycle()
    with pytest.raises(TimeoutError, match=r"ledger_entries\[0\] committed=-1 required=1"):
        cdc.gate(timeout_s=20)


def test_offsets_committed_before_the_iceberg_commit_do_not_pass_the_gate():
    """The race (backlog gate-iceberg-commit-race). After an outage the worker
    reads a big backlog; Kafka gives no order ACROSS partitions, so the new
    heartbeat can be read in an earlier cycle than older business events:

      cycle k   : heartbeat H (after cut-off)      -> committed, visible
      cycle k+1 : ledger entry E (BEFORE cut-off)  -> worker txn done (offsets moved),
                                                      Iceberg commit not yet
    Checks 1 and 2 both see "complete" while E is not in bronze. If silver ran now,
    E would miss day N, and day N+1's window starts after E's commit time: lost.
    """
    cdc = FakeCdc()
    entry = cdc.emit(LE, -1)  # committed in Postgres before the cut-off
    cdc.emit(HB, +1)
    cdc.consume(HB)  # cycle k: only the heartbeat partition was fetched
    cdc.cycle()
    cdc.consume(LE)  # cycle k+1: worker txn, but no Iceberg commit yet
    cdc.worker_txn()
    cdc.timeline = [
        cdc.iceberg_commit,  # cycle k+1 lands in Iceberg
        lambda: (cdc.emit(HB, +11), cdc.consume(HB), cdc.cycle()),  # next heartbeat, k+2
    ]

    cdc.gate()

    assert entry in cdc.visible[LE], "gate passed while a pre-cut-off event was not in bronze"


def _late_entry_with_failing_table(cdc: FakeCdc) -> Event:
    """A pre-cut-off ledger entry is read, but the ledger_entries table's Iceberg
    commits fail for two cycles while the heartbeat table keeps committing
    (e.g. a conflict with `optimize`). Then the failure clears."""
    entry = cdc.emit(LE, -1)
    cdc.emit(HB, +1)
    cdc.consume(LE)
    cdc.consume(HB)
    cdc.failing_tables = {LE}
    cdc.cycle()  # heartbeat visible, entry staged (failed)

    def heartbeat_cycle():
        cdc.emit(HB, cdc.now)
        cdc.consume(HB)
        cdc.cycle()

    cdc.timeline = [heartbeat_cycle, heartbeat_cycle, lambda: cdc.failing_tables.clear()]
    return entry


def test_a_table_whose_iceberg_commits_fail_blocks_the_gate():
    """The residual risk of the heartbeat-LSN check (ADR 0010): per-table commits."""
    cdc = FakeCdc()
    entry = _late_entry_with_failing_table(cdc)
    result = cdc.gate()
    assert entry in cdc.visible[LE], "gate passed while ledger_entries was a cycle behind"
    assert result.exact == [HB, LE] and result.fallback == []


def test_the_lsn_fallback_alone_misses_a_per_table_commit_failure():
    """Pins the known limit of the fallback, used only while a partition has no
    _kafka_offset in bronze yet: here every ledger_entries row predates the column."""
    cdc = FakeCdc(offsets_from={HB: 0, LE: 10**9})
    entry = _late_entry_with_failing_table(cdc)
    result = cdc.gate()
    assert result.fallback == [LE] and result.exact == [HB]
    assert entry not in cdc.visible[LE]  # the fallback opened the gate too early


def test_fallback_is_used_only_where_bronze_has_no_offsets():
    cdc = FakeCdc(offsets_from={HB: 0, LE: 10**9})  # ledger_entries: pre-migration rows only
    cdc.emit(LE, -1)
    cdc.emit(HB, +1)
    cdc.consume(LE)
    cdc.consume(HB)
    cdc.cycle()
    result = cdc.gate()
    assert result.exact == [HB]
    assert result.fallback == [LE]
    assert result.heartbeat_lsn > result.source_lsn
