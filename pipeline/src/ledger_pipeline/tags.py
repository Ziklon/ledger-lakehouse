"""EOD tags on every silver *_current table, via pyiceberg (docs/adr/0011).

A tag is a named, immutable pointer to a snapshot: "the state at the end of
business day N". Gold reads `FOR VERSION AS OF 'eod_YYYY_MM_DD'` so FULL and
INCREMENTAL are computed from the same frozen input.

Retention is stored on the ref (max_ref_age_ms) and enforced by snapshot
expiry (Phase 2 retention): eod_* 90 days, close_YYYY_MM 7 years.

Rerun policy: a tag of a COMMITTED day is never moved -- silver may already
contain later changes, so re-pointing it would rewrite history. An attempt of
a not-yet-committed batch (re)creates its tag on the current snapshot.
"""

from __future__ import annotations

import logging
from datetime import date

from pyiceberg.catalog import load_catalog

from .config import Settings
from .windows import close_tag, eod_tag

log = logging.getLogger(__name__)
DAY_MS = 24 * 3600 * 1000
EOD_RETENTION_MS = 90 * DAY_MS
CLOSE_RETENTION_MS = 7 * 366 * DAY_MS


def tag_eod(s: Settings, batch_id: date, committed: bool) -> dict[str, int]:
    """Tag the current snapshot of each silver *_current table. Returns table -> snapshot id."""
    catalog = load_catalog("lakehouse", **s.iceberg_catalog_properties())
    names = [(eod_tag(batch_id), EOD_RETENTION_MS)]
    if close := close_tag(batch_id):
        names.append((close, CLOSE_RETENTION_MS))

    tagged: dict[str, int] = {}
    for ident in sorted(catalog.list_tables("silver")):
        name = ident[-1]
        if not name.endswith("_current"):
            continue
        table = catalog.load_table(ident)
        snapshot = table.current_snapshot()
        if snapshot is None:
            raise RuntimeError(f"silver.{name} has no snapshot to tag")
        for tag, max_age in names:
            existing = table.metadata.refs.get(tag)
            if existing is not None and committed:
                log.info("tag %s on silver.%s kept (COMMITTED day is immutable)", tag, name)
                tagged[name] = existing.snapshot_id
                continue
            ops = table.manage_snapshots()
            if existing is not None:
                ops = ops.remove_tag(tag)
            ops.create_tag(snapshot.snapshot_id, tag, max_ref_age_ms=max_age).commit()
            table = catalog.load_table(ident)
            tagged[name] = snapshot.snapshot_id
    return tagged
