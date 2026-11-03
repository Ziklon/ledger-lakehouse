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
3. In bronze, per table: for every partition of (2), its bronze table holds the
   record at offset end - 1 (`_kafka_partition`, `_kafka_offset`, filled by the
   sink's InsertField SMT). Exact, per table, and independent of the sink's
   internals: the coordinator commits each table separately, so one table can
   lag another after a failed commit.
   Fallback, only for a partition whose last record predates the migration
   that added the columns (control.bronze_offset_baseline: end - 1 < first
   offset that carries one; ends at the partition's next event): read the
   source WAL position L after (2), then wait for a bronze heartbeat with
   _lsn > L. It proves a later commit cycle reached the heartbeat table, which
   covers the cross-partition race but not a per-table commit failure.
   "No offsets in bronze" is NOT a fallback criterion: it is also what a table
   whose commits fail looks like -- exactly the case the exact check is for.

Why not "lag == 0"? Heartbeats arrive every few seconds and, in production,
business traffic continues past midnight: lag is almost never zero. We only
need what existed when (1) proved the cut-off had passed.
A partition without a committed offset means the sink never subscribed to
that topic (e.g. a new table the regex hasn't picked up yet): it blocks too.
Assumes CDC topics are written without Kafka transactions (no exactly-once
source): a transaction marker takes an offset, so end - 1 would never arrive.

The decisions (`run_gate`) take their I/O as probes, so the sink's commit
protocol can be simulated in tests (pipeline/tests/test_gate.py).
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime

from .config import Settings
from .control import query

log = logging.getLogger(__name__)

Partition = tuple[str, int]


def bronze_table(topic: str) -> str:
    """Bronze table of a CDC topic: the sink's RegexRouter `ledger\\.app\\.(.*)` ->
    `bronze.$1_cdc` (infra/connect/connectors/bronze-sink.json)."""
    name = topic.rsplit(".", 1)[-1]
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise ValueError(f"unexpected CDC topic name {topic!r}")
    return f"lakehouse.bronze.{name}_cdc"


@dataclass(frozen=True)
class Probes:
    """Everything the gate observes. Real ones: `Probes.live`; tests: a fake sink."""

    latest_heartbeat_ts: Callable[[], datetime | None]  # max heartbeat commit time in bronze
    end_offsets: Callable[[], dict[Partition, int]]  # every CDC partition, right now
    committed_offsets: Callable[[Iterable[Partition]], dict[Partition, int]]  # sink group
    # migration boundary: first offset whose bronze row carries _kafka_offset
    offset_baseline: Callable[[], dict[Partition, int]]
    # max _kafka_offset in bronze per partition, among rows >= the given offset
    bronze_offsets: Callable[[dict[Partition, int]], dict[Partition, int]]
    source_lsn: Callable[[], int]  # fallback: Postgres WAL position, right now
    latest_heartbeat_lsn: Callable[[datetime], int | None]  # fallback: max heartbeat _lsn

    @classmethod
    def live(cls, s: Settings, conn) -> Probes:
        return cls(
            latest_heartbeat_ts=lambda: query(
                conn, "select max(_source_commit_ts) from lakehouse.bronze.cdc_heartbeat_cdc"
            )[0][0],
            end_offsets=lambda: end_offsets(s),
            committed_offsets=lambda parts: committed_offsets(s, parts),
            offset_baseline=lambda: {
                (t, p): o
                for t, p, o in query(
                    conn, f"select topic, kafka_partition, first_offset from {BASELINE}"
                )
            },
            bronze_offsets=lambda wanted: _bronze_offsets(conn, wanted),
            source_lsn=lambda: query(conn, SOURCE_LSN_SQL)[0][0],
            # only heartbeats after the cut-off can satisfy the fallback: prune to them
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
    exact: list[Partition] = field(default_factory=list)  # check 3 by offset
    fallback: list[Partition] = field(default_factory=list)  # check 3 by heartbeat LSN
    source_lsn: int | None = None  # fallback only
    heartbeat_lsn: int | None = None


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


def _fmt(tp: Partition) -> str:
    return f"{tp[0]}[{tp[1]}]"


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
    nonempty = {tp: end for tp, end in required.items() if end > 0}

    def sink_caught_up() -> str | None:
        committed = probes.committed_offsets(required)
        behind = {
            tp: (committed.get(tp, -1), end)
            for tp, end in nonempty.items()
            if committed.get(tp, -1) < end
        }
        if not behind:
            return None
        detail = ", ".join(
            f"{_fmt(tp)} committed={c} required={e}" for tp, (c, e) in sorted(behind.items())
        )
        return f"sink behind: {detail} (sink down or slow? topic not subscribed?)"

    _poll(sink_caught_up, timeout_s, poll_s, clock, sleep)
    topics: dict[str, int] = {}
    for t, _ in required:
        topics[t] = topics.get(t, 0) + 1
    log.info("gate 2/3: sink read everything on %s", topics)

    # The fallback's L must be read AFTER (2) was observed (any time after is safe).
    baseline = probes.offset_baseline()
    before_migration = {tp for tp, end in nonempty.items() if end - 1 < baseline.get(tp, 0)}
    exact = {tp: end - 1 for tp, end in nonempty.items() if tp not in before_migration}
    fallback = sorted(before_migration)
    source_lsn = probes.source_lsn() if fallback else None

    def all_offsets_in_bronze() -> str | None:
        got = probes.bronze_offsets(exact)
        missing = {tp: (got.get(tp), last) for tp, last in exact.items() if got.get(tp, -1) < last}
        if not missing:
            return None
        detail = ", ".join(
            f"{_fmt(tp)} bronze={b} required={r}" for tp, (b, r) in sorted(missing.items())
        )
        return f"read but not in bronze yet: {detail} (catalog down? commits failing?)"

    _poll(all_offsets_in_bronze, timeout_s, poll_s, clock, sleep)

    hb_lsn: dict[str, int] = {}
    if fallback:
        log.warning(
            "gate 3/3: no event since the _kafka_offset migration on %s: falling back to the "
            "heartbeat-LSN check for them",
            ", ".join(_fmt(tp) for tp in fallback),
        )

        def heartbeat_beyond_source_lsn() -> str | None:
            latest = probes.latest_heartbeat_lsn(cutoff)
            if latest is not None and latest > source_lsn:
                hb_lsn["lsn"] = latest
                return None
            return (
                f"no heartbeat beyond source lsn {source_lsn} in bronze yet (latest {latest}): "
                f"sink read but not committed to Iceberg (catalog down? commits failing?)"
            )

        _poll(heartbeat_beyond_source_lsn, timeout_s, poll_s, clock, sleep)

    log.info(
        "gate 3/3: in bronze: %d partition(s) by offset, %d by heartbeat lsn",
        len(exact),
        len(fallback),
    )
    return GateResult(
        heartbeat_ts=seen["hb"],
        partitions=topics,
        exact=sorted(exact),
        fallback=fallback,
        source_lsn=source_lsn,
        heartbeat_lsn=hb_lsn.get("lsn"),
    )


# ---- live probes (Trino) -----------------------------------------------------


def _by_table(parts: Iterable[Partition]) -> dict[str, list[Partition]]:
    tables: dict[str, list[Partition]] = {}
    for tp in parts:
        tables.setdefault(bronze_table(tp[0]), []).append(tp)
    return tables


BASELINE = "lakehouse.control.bronze_offset_baseline"


def record_offset_baseline(
    s: Settings,
    conn,
    timeout_s: int = 180,
    poll_s: int = 5,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[Partition, int]:
    """Migration step for bronze _kafka_partition/_kafka_offset (scripts/bootstrap.sh,
    after the sink config that fills them). Idempotent and insert-only.

    For each partition of a bronze table that holds rows WITHOUT an offset, record
    the first offset guaranteed to carry one. Safe boundary: wait until a bronze row
    with an offset is visible (the single sink task runs the new config, the old
    task is gone), THEN read the end offsets: every later record is read by the
    new task. Fresh stacks (no offset-less rows) record nothing."""
    from .control import tables_in

    legacy = [
        t
        for t in tables_in(conn, "bronze", "_cdc")
        if query(conn, f"select count(*) from lakehouse.bronze.{t} where _kafka_offset is null")[0][
            0
        ]
    ]
    if not legacy:
        log.info("offset baseline: no bronze rows without _kafka_offset, nothing to record")
        return {}

    def new_config_live() -> str | None:
        n = query(
            conn,
            "select count(*) from lakehouse.bronze.cdc_heartbeat_cdc "
            "where _kafka_offset is not null",
        )[0][0]
        return None if n else "no bronze row with _kafka_offset yet (new sink config not live?)"

    _poll(new_config_live, timeout_s, poll_s, clock, sleep)
    done = {(t, p) for t, p in query(conn, f"select topic, kafka_partition from {BASELINE}")}
    legacy_tables = {f"lakehouse.bronze.{t}" for t in legacy}
    new = {
        tp: end
        for tp, end in end_offsets(s).items()
        if tp not in done and bronze_table(tp[0]) in legacy_tables
    }
    for (topic, partition), end in sorted(new.items()):
        query(
            conn,
            f"insert into {BASELINE} values (?, ?, ?, current_timestamp)",
            [topic, partition, end],
        )
    log.info("offset baseline: recorded %s (already recorded: %d)", new, len(done))
    return new


def _bronze_offsets(conn, wanted: dict[Partition, int]) -> dict[Partition, int]:
    """Max _kafka_offset per partition, scanning only rows at or past the lowest offset
    wanted in that table (file pruning on the column's min/max stats)."""
    got = {}
    for table, tps in _by_table(wanted).items():
        lowest = min(wanted[tp] for tp in tps)
        rows = query(
            conn,
            f"select _kafka_partition, max(_kafka_offset) from {table} "
            "where _kafka_offset >= ? group by _kafka_partition",
            [lowest],
        )
        by_partition = dict(rows)
        for tp in tps:
            if tp[1] in by_partition:
                got[tp] = by_partition[tp[1]]
    return got


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
