"""Upstream resume cursor (decision B): the SSE `id` is a JSON list of per-topic positions.

Example, as observed:
  [{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1790479264469},
   {"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1}]

Every Kafka message carries the cursor it was read at (header `upstream_cursor`). On
restart the producer merges the cursors found at the tail of each partition, taking the
furthest position per upstream (topic, partition). Resuming by timestamp can re-send
events at the boundary, which is why this hop is at-least-once (decision A).
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any


def _parse(cursor: str) -> list[dict[str, Any]]:
    entries = json.loads(cursor)
    if not isinstance(entries, list) or not all(
        isinstance(e, dict) and "topic" in e and "partition" in e for e in entries
    ):
        raise ValueError(f"not an EventStreams cursor: {cursor[:200]}")
    return entries


def _rank(entry: dict[str, Any]) -> tuple[int, int]:
    # A timestamp position outranks an offset-only one (offset -1 means "nothing seen yet").
    if "timestamp" in entry:
        return (1, int(entry["timestamp"]))
    return (0, int(entry.get("offset", -1)))


def merge_cursors(cursors: Iterable[str | None]) -> str | None:
    """Furthest position per upstream (topic, partition), as a canonical cursor string."""
    best: dict[tuple[str, int], dict[str, Any]] = {}
    for c in cursors:
        if not c:
            continue
        for e in _parse(c):
            k = (e["topic"], int(e["partition"]))
            if k not in best or _rank(e) > _rank(best[k]):
                best[k] = e
    if not best:
        return None
    ordered = [best[k] for k in sorted(best)]
    return json.dumps(ordered, sort_keys=True, separators=(",", ":"))
