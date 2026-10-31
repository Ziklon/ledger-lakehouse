"""Completeness gate: proceed only when bronze provably holds every commit
before the cut-off (docs/adr/0010).

Milestone (d) implements the heartbeat half: a heartbeat committed AFTER the
cut-off is visible in bronze. Logical decoding emits in commit order, so
everything earlier on the heartbeat's stream has been delivered.
Milestone (f) adds the other half: sink consumer lag ~0 on EVERY CDC topic
(the heartbeat is its own topic, so on its own it doesn't prove the others).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

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
