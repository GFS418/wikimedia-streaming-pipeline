"""Upstream resume cursor (decision B): the SSE `id` is a JSON list of per-topic positions.

Example, as observed:
  [{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1790479264469},
   {"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1}]

Every Kafka message carries the cursor it was read at (header `upstream_cursor`). On
restart the producer resumes from the slowest partition tail's cursor (D29).

A timestamp in an id is the event's own timestamp PLUS 1 ms (2,620 of 2,620 live events,
2026-10-10), and a resume starts at the first event stamped at or after it. So resuming from
an id as given skips every event not yet received that is stamped at or before the last
one's millisecond: same-millisecond neighbours (2 events lost overnight 2026-10-10) and
events whose timestamps run backwards (0.76% of 5.7M eqiad events; at most 30.5 s). The
original belief here, that a timestamp resume could only re-send, was wrong. D31: resume
1 s early (`rewind`), ignore what is re-sent by upstream offset, and check that each
upstream partition continues at exactly the next offset, re-reading further back if not
(`ResumeGuard`).
"""

from __future__ import annotations

import json
from collections import Counter
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


def cursor_newest_ms(cursor: str) -> int | None:
    """Newest upstream timestamp in a cursor: roughly when the producer last wrote."""
    stamps = [int(e["timestamp"]) for e in _parse(cursor) if "timestamp" in e]
    return max(stamps) if stamps else None


def advance(sticky: str | None, server_id: str | None) -> str | None:
    """The producer's own cursor, which never regresses (decision D22).

    After every reconnect, EventStreams reports a quiet topic's position as
    {"offset": -1} until that topic sends an event (observed 2026-10-08 13:33:45, codfw
    13:21:23.667 -> -1 right after reconnect #5). Resuming from such an id can re-send the
    quiet topic's events (11 duplicates on 2026-10-09) or skip them. Merging each server id
    into the previous cursor keeps the last real position of every topic instead.
    """
    return merge_cursors([sticky, server_id])


def without_timestamp(cursor: str | None) -> set[tuple[str, int]]:
    """Upstream (topic, partition) entries that carry no timestamp position."""
    if not cursor:
        return set()
    return {(e["topic"], int(e["partition"])) for e in _parse(cursor) if "timestamp" not in e}


def slowest(cursors: Iterable[str | None]) -> str | None:
    """Per upstream (topic, partition), the EARLIEST timestamp position across cursors
    (decision D29). One cursor per partition of our topic: each partition is delivered in
    order, so anything a crash lost sits after that partition's tail, and resuming from the
    slowest tail re-fetches all of it (re-sending what faster partitions already had:
    duplicates, which silver removes). Entries without a timestamp are skipped; a topic no
    cursor has a timestamp for keeps an offset-only entry."""
    best: dict[tuple[str, int], dict[str, Any]] = {}
    fallback: dict[tuple[str, int], dict[str, Any]] = {}
    for c in cursors:
        if not c:
            continue
        for e in _parse(c):
            k = (e["topic"], int(e["partition"]))
            if "timestamp" not in e:
                fallback.setdefault(k, e)
            elif k not in best or int(e["timestamp"]) < int(best[k]["timestamp"]):
                best[k] = e
    merged = fallback | best
    if not merged:
        return None
    return json.dumps([merged[k] for k in sorted(merged)], sort_keys=True, separators=(",", ":"))


# ------------------------------------------------------------------ D31: resume, then verify

Position = tuple[str, int]  # upstream (topic, partition)


def rewind(cursor: str | None, ms: int) -> str | None:
    """The cursor with every timestamp moved `ms` earlier; offset-only entries unchanged."""
    if not cursor or ms <= 0:
        return cursor
    entries = [
        e | {"timestamp": int(e["timestamp"]) - ms} if "timestamp" in e else e
        for e in _parse(cursor)
    ]
    return json.dumps(entries, sort_keys=True, separators=(",", ":"))


def offsets_header(last: dict[Position, int]) -> str:
    """Newest upstream offset handled per upstream partition (header `upstream_offsets`)."""
    entries = [{"topic": t, "partition": p, "offset": o} for (t, p), o in sorted(last.items())]
    return json.dumps(entries, sort_keys=True, separators=(",", ":"))


def parse_offsets(header: str) -> dict[Position, int]:
    return {(e["topic"], int(e["partition"])): int(e["offset"]) for e in json.loads(header)}


def floors(tail_headers: Iterable[str | None]) -> dict[Position, int]:
    """Per upstream partition, an offset at or below which every event is already in Kafka:
    the minimum over our partitions' tails of the newest offset handled when that tail was
    written. Each of our partitions is delivered in order, so everything handled before a
    tail and routed to that partition is there; dead letters and drops are flushed before
    the producer moves on (D29). Empty (nothing known) if any tail lacks the header, and an
    upstream partition missing from any tail is left out."""
    result: dict[Position, int] | None = None
    for header in tail_headers:
        if not header:
            return {}
        last = parse_offsets(header)
        result = (
            last
            if result is None
            else {k: min(result[k], last[k]) for k in result.keys() & last.keys()}
        )
    return result or {}


class ResumeGuard:
    """Resume by timestamp, verify by offset (D31).

    Tracks the newest upstream offset handled per upstream partition. After each (re)connect,
    every known partition must continue at exactly the next offset: an event at or below the
    last one is a re-send from the rewound resume and is ignored; one beyond the next means
    events were skipped, so the rewind doubles and the caller re-reads from further back. At
    `max_ms` the missing offsets are taken to be gone upstream and the gap is accepted (and
    counted). This relies on upstream offsets being dense: in the 5.7M events retained, every
    offset is accounted for once the audit adds dead letters and dropped canaries.
    """

    NEW, RESENT, REREAD, GAP = "new", "resent", "reread", "gap_accepted"

    def __init__(self, base_ms: int, max_ms: int, floor: dict[Position, int] | None = None):
        self.base_ms, self.max_ms = base_ms, max_ms
        self.rewind_ms = base_ms
        self.last: dict[Position, int] = dict(floor or {})
        self.unverified: set[Position] = set()
        self.counts: Counter[str] = Counter()

    def on_connect(self) -> None:
        self.unverified = set(self.last)

    def admit(self, key: Position, offset: int) -> str:
        last = self.last.get(key)
        if last is not None and offset <= last:
            self.counts["resume.resent_ignored"] += 1
            return self.RESENT
        verdict = self.NEW
        if key in self.unverified:
            self.unverified.discard(key)
            if offset == last + 1:
                self.counts["resume.verified"] += 1
                self.rewind_ms = self.base_ms
            elif self.rewind_ms < self.max_ms:
                self.rewind_ms = min(max(1, 2 * self.rewind_ms), self.max_ms)
                self.counts["resume.reread"] += 1
                return self.REREAD  # not admitted: the re-read must deliver last + 1 first
            else:
                self.counts["resume.gap_accepted"] += 1
                self.rewind_ms = self.base_ms
                verdict = self.GAP
        elif last is not None and offset > last + 1:
            # Mid-connection: re-reading cannot help (the server sent what it has), but a hole
            # in upstream offsets should be visible before the next audit finds it.
            self.counts["upstream.offset_gap"] += offset - last - 1
        self.last[key] = offset
        return verdict

    def header(self) -> str:
        return offsets_header(self.last)
