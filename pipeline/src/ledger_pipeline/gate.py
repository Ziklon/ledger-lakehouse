"""Completeness gate: a batch proceeds only when bronze provably holds every
source commit before the cut-off (docs/adr/0010). Two checks, in order:

1. Heartbeat: a heartbeat committed AFTER the cut-off is visible in bronze.
   Debezium (one task) emits in commit order, so every earlier commit has
   already been SENT to Kafka -- on some topic.
2. Sink caught up: snapshot the end offset of every partition of every CDC
   topic NOW, then wait until the sink's consumer group has committed at least
   that far on each one. The Iceberg sink commits offsets only after the data
   is committed to Iceberg, so "committed >= snapshot" means "in bronze".

Why not "lag == 0"? Heartbeats arrive every few seconds and, in production,
business traffic continues past midnight: lag is almost never zero. We only
need what existed when (1) proved the cut-off had passed.
A partition without a committed offset means the sink never subscribed to
that topic (e.g. a new table the regex hasn't picked up yet): it blocks too.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime

from .config import Settings
from .control import query

log = logging.getLogger(__name__)


def wait_for_heartbeat(conn, cutoff: datetime, timeout_s: int = 300, poll_s: int = 5) -> datetime:
    deadline = time.monotonic() + timeout_s
    while True:
        latest = query(
            conn, "select max(_source_commit_ts) from lakehouse.bronze.cdc_heartbeat_cdc"
        )[0][0]
        if latest is not None and latest >= cutoff:
            return latest
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"completeness gate: latest heartbeat in bronze {latest} < cut-off {cutoff} "
                f"after {timeout_s}s (connector down? sink not committing?)"
            )
        log.info("gate: heartbeat %s < cut-off %s, waiting %ss", latest, cutoff, poll_s)
        time.sleep(poll_s)


def end_offsets(s: Settings) -> dict[tuple[str, int], int]:
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


def committed_offsets(s: Settings, partitions) -> dict[tuple[str, int], int]:
    from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(s.kafka_config())
    req = ConsumerGroupTopicPartitions(s.sink_group, [TopicPartition(t, p) for t, p in partitions])
    result = admin.list_consumer_group_offsets([req])[s.sink_group].result()
    # offset < 0 = nothing committed for that partition
    return {(tp.topic, tp.partition): tp.offset for tp in result.topic_partitions}


def wait_for_sink(s: Settings, timeout_s: int = 300, poll_s: int = 5) -> dict[str, int]:
    """Wait until the sink has committed everything present at call time.
    Returns topic -> number of partitions checked."""
    required = end_offsets(s)
    deadline = time.monotonic() + timeout_s
    while True:
        committed = committed_offsets(s, required)
        behind = {
            tp: (committed.get(tp, -1), end)
            for tp, end in required.items()
            if end > 0 and committed.get(tp, -1) < end
        }
        if not behind:
            topics: dict[str, int] = {}
            for t, _ in required:
                topics[t] = topics.get(t, 0) + 1
            return topics
        if time.monotonic() > deadline:
            detail = ", ".join(
                f"{t}[{p}] committed={c} required={e}" for (t, p), (c, e) in sorted(behind.items())
            )
            raise TimeoutError(
                f"completeness gate: sink group {s.sink_group} behind after {timeout_s}s: "
                f"{detail} (sink down or slow? topic not subscribed?)"
            )
        log.info("gate: sink behind on %d partition(s), waiting %ss", len(behind), poll_s)
        time.sleep(poll_s)
