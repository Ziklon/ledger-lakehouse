"""Completeness gate: a batch proceeds only when bronze provably holds every
source commit before the cut-off (docs/adr/0010). Three checks, in order:

1. Heartbeat: a heartbeat committed AFTER the cut-off is visible in bronze.
   Debezium (one task) emits in commit order, so every earlier commit has
   already been SENT to Kafka -- on some topic.
2. Sink caught up: snapshot the end offset of every partition of every CDC
   topic NOW, then wait until the sink's consumer group has committed at least
   that far on each one. This proves the sink has READ everything, not that it
   is in bronze: the Iceberg sink commits source offsets in the worker's Kafka
   transaction (with DATA_WRITTEN), BEFORE the coordinator's Iceberg commit.
3. Committed in Iceberg: read the source's current WAL position L, then wait
   until bronze holds a heartbeat with _lsn > L. That heartbeat was written
   after check 2 was observed, so it belongs to a later commit cycle than every
   offset checked; the coordinator commits cycles in order (a failed cycle's
   files roll into the next commit), so its visibility implies theirs.
   L comes from Postgres (pg_current_wal_lsn through the read-only federation
   catalog), on the same scale as Debezium's lsn: no clock is compared.
   Cost: up to one heartbeat interval + one sink commit interval.

Why not "lag == 0"? Heartbeats arrive every few seconds and, in production,
business traffic continues past midnight: lag is almost never zero. We only
need what existed when (1) proved the cut-off had passed.
A partition without a committed offset means the sink never subscribed to
that topic (e.g. a new table the regex hasn't picked up yet): it blocks too.

The decisions (`run_gate`) take their I/O as probes, so the sink's commit
protocol can be simulated in tests (pipeline/tests/test_gate.py).
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime

from .config import Settings
from .control import query

log = logging.getLogger(__name__)

Partition = tuple[str, int]


@dataclass(frozen=True)
class Probes:
    """Everything the gate observes. Real ones: `Probes.live`; tests: a fake sink."""

    latest_heartbeat_ts: Callable[[], datetime | None]  # max heartbeat commit time in bronze
    end_offsets: Callable[[], dict[Partition, int]]  # every CDC partition, right now
    committed_offsets: Callable[[Iterable[Partition]], dict[Partition, int]]  # sink group
    source_lsn: Callable[[], int]  # Postgres WAL position, right now
    latest_heartbeat_lsn: Callable[[datetime], int | None]  # max heartbeat _lsn in bronze

    @classmethod
    def live(cls, s: Settings, conn) -> Probes:
        return cls(
            latest_heartbeat_ts=lambda: query(
                conn, "select max(_source_commit_ts) from lakehouse.bronze.cdc_heartbeat_cdc"
            )[0][0],
            end_offsets=lambda: end_offsets(s),
            committed_offsets=lambda parts: committed_offsets(s, parts),
            source_lsn=lambda: query(conn, SOURCE_LSN_SQL)[0][0],
            # only heartbeats after the cut-off can satisfy check 3: prune to them
            latest_heartbeat_lsn=lambda since: query(
                conn,
                "select max(_lsn) from lakehouse.bronze.cdc_heartbeat_cdc "
                "where _source_commit_ts >= from_iso8601_timestamp(?)",
                [since.isoformat()],
            )[0][0],
        )


# pg_lsn as a number = Debezium's `lsn` (bronze _lsn). Pass-through query: Trino has no pg_lsn.
SOURCE_LSN_SQL = (
    "select current_lsn from table(ledger_pg.system.query(query => "
    "'select (pg_current_wal_lsn() - ''0/0''::pg_lsn)::bigint as current_lsn'))"
)


@dataclass(frozen=True)
class GateResult:
    heartbeat_ts: datetime
    partitions: dict[str, int]  # topic -> number of partitions checked
    source_lsn: int  # check 3: bronze holds a heartbeat beyond this WAL position
    heartbeat_lsn: int


def _poll(check: Callable[[], str | None], timeout_s, poll_s, clock, sleep) -> None:
    """Call `check` until it returns None (= satisfied). It returns the reason it is not
    satisfied yet; on timeout that reason becomes the error."""
    deadline = clock() + timeout_s
    while True:
        why_not = check()
        if why_not is None:
            return
        if clock() > deadline:
            raise TimeoutError(f"completeness gate: {why_not} after {timeout_s}s")
        log.info("gate: %s, waiting %ss", why_not, poll_s)
        sleep(poll_s)


def run_gate(
    cutoff: datetime,
    probes: Probes,
    timeout_s: int = 300,
    poll_s: int = 5,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> GateResult:
    seen: dict[str, datetime] = {}

    def heartbeat_after_cutoff() -> str | None:
        latest = probes.latest_heartbeat_ts()
        if latest is not None and latest >= cutoff:
            seen["hb"] = latest
            return None
        return (
            f"latest heartbeat in bronze {latest} < cut-off {cutoff} "
            f"(connector down? sink not committing?)"
        )

    _poll(heartbeat_after_cutoff, timeout_s, poll_s, clock, sleep)
    log.info("gate 1/3: heartbeat in bronze at %s >= cut-off %s", seen["hb"], cutoff)

    required = probes.end_offsets()
    if not required:
        raise RuntimeError("no CDC topics found: wrong regex or cluster?")

    def sink_caught_up() -> str | None:
        committed = probes.committed_offsets(required)
        behind = {
            tp: (committed.get(tp, -1), end)
            for tp, end in required.items()
            if end > 0 and committed.get(tp, -1) < end
        }
        if not behind:
            return None
        detail = ", ".join(
            f"{t}[{p}] committed={c} required={e}" for (t, p), (c, e) in sorted(behind.items())
        )
        return f"sink behind: {detail} (sink down or slow? topic not subscribed?)"

    _poll(sink_caught_up, timeout_s, poll_s, clock, sleep)
    topics: dict[str, int] = {}
    for t, _ in required:
        topics[t] = topics.get(t, 0) + 1
    log.info("gate 2/3: sink read everything on %s", topics)

    # Read L only AFTER check 2 was observed: that ordering is the whole argument.
    source_lsn = probes.source_lsn()
    hb_lsn: dict[str, int] = {}

    def iceberg_committed() -> str | None:
        latest = probes.latest_heartbeat_lsn(cutoff)
        if latest is not None and latest > source_lsn:
            hb_lsn["lsn"] = latest
            return None
        return (
            f"no heartbeat beyond source lsn {source_lsn} in bronze yet (latest {latest}): "
            f"sink read but not committed to Iceberg (catalog down? commits failing?)"
        )

    _poll(iceberg_committed, timeout_s, poll_s, clock, sleep)
    log.info("gate 3/3: heartbeat lsn %s > source lsn %s in bronze", hb_lsn["lsn"], source_lsn)
    return GateResult(
        heartbeat_ts=seen["hb"],
        partitions=topics,
        source_lsn=source_lsn,
        heartbeat_lsn=hb_lsn["lsn"],
    )


# ---- live probes (Kafka) -----------------------------------------------------


def end_offsets(s: Settings) -> dict[Partition, int]:
    """End offset of every partition of every CDC topic, right now."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({**s.kafka_config(), "group.id": "ledger-pipeline-gate"})
    try:
        topics = consumer.list_topics(timeout=10).topics
        pattern = re.compile(s.cdc_topic_regex)
        ends = {}
        for name, meta in topics.items():
            if not pattern.match(name):
                continue
            for p in meta.partitions:
                _, high = consumer.get_watermark_offsets(TopicPartition(name, p), timeout=10)
                ends[(name, p)] = high
    finally:
        consumer.close()
    if not ends:
        raise RuntimeError(f"no topics match {s.cdc_topic_regex!r}: wrong regex or cluster?")
    return ends


def committed_offsets(s: Settings, partitions) -> dict[Partition, int]:
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(s.kafka_config())
    req = ConsumerGroupTopicPartitions(s.sink_group, [TopicPartition(t, p) for t, p in partitions])
    result = admin.list_consumer_group_offsets([req])[s.sink_group].result()
    # offset < 0 = nothing committed for that partition
    return {(tp.topic, tp.partition): tp.offset for tp in result.topic_partitions}
