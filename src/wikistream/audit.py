"""Reconcile our three topics against Wikimedia's upstream offsets (decision D8).

Every upstream offset in the audited range must be accounted for exactly once:
delivered (main topic) + dead-lettered (DLQ) + deliberately dropped (drops ledger).
`reconcile` is pure; `collect` reads the topics; `main` is the CLI.

The range per upstream partition is what the live capture wrote in the window. Events repaired
later (header `backfill`, D31) count towards it, but do not stretch it: they were written
hours after their neighbours, so a window around the repair would otherwise span back to
them and report everything in between as missing.

    uv run python scripts/audit_topic.py [--since 2026-09-30T23:30:00Z]
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.config import Settings
from wikistream.contract import UpstreamPosition, upstream_position
from wikistream.producer import H_BACKFILL, H_UP_OFFSET, H_UP_PARTITION, H_UP_TOPIC

SOURCES = ("main", "dlq", "drops")


@dataclass(frozen=True)
class Seen:
    source: str  # main | dlq | drops
    upstream: UpstreamPosition | None
    event_id: str | None = None
    backfill: bool = False  # re-fetched later from upstream history (D31)


def _gap_runs(offsets: list[int]) -> list[tuple[int, int]]:
    """Missing ranges as inclusive (first_missing, last_missing)."""
    return [(a + 1, b - 1) for a, b in zip(offsets, offsets[1:], strict=False) if b > a + 1]


def reconcile(seen: Iterable[Seen]) -> dict:
    seen = list(seen)
    by_source = Counter(s.source for s in seen)
    unattributed = Counter(s.source for s in seen if s.upstream is None)

    ids = Counter(s.event_id for s in seen if s.source == "main" and s.event_id)
    dup_ids = sum(c - 1 for c in ids.values() if c > 1)

    where: dict[tuple[str, int], dict[int, Counter]] = defaultdict(lambda: defaultdict(Counter))
    live: dict[tuple[str, int], list[int]] = defaultdict(list)
    for s in seen:
        if s.upstream:
            u = s.upstream
            where[(u.topic, u.partition)][u.offset][s.source] += 1
            if not s.backfill:
                live[(u.topic, u.partition)].append(u.offset)

    upstream = {}
    total_missing = 0
    for (topic, part), all_offsets in sorted(where.items()):
        if not live[(topic, part)]:
            upstream[f"{topic}[{part}]"] = {"backfilled_outside_window": len(all_offsets)}
            continue
        lo, hi = min(live[(topic, part)]), max(live[(topic, part)])
        offsets = {o: c for o, c in all_offsets.items() if lo <= o <= hi}
        ordered = sorted(offsets)
        gaps = _gap_runs(ordered)
        missing = sum(b - a + 1 for a, b in gaps)
        total_missing += missing
        repeated = sum(sum(c.values()) - 1 for c in offsets.values() if sum(c.values()) > 1)
        conflicts = sum(1 for c in offsets.values() if len(c) > 1)  # e.g. in main AND dlq
        upstream[f"{topic}[{part}]"] = {
            "first": ordered[0],
            "last": ordered[-1],
            "expected": ordered[-1] - ordered[0] + 1,
            "accounted": len(ordered),
            "by_source": dict(sum((c for c in offsets.values()), Counter())),
            "missing": missing,
            "gap_runs": gaps[:10],
            "repeated_offsets": repeated,
            "cross_source_conflicts": conflicts,
            "backfilled_outside_window": len(all_offsets) - len(offsets),
        }

    unattributed_dlq = unattributed["dlq"]
    if total_missing == 0:
        verdict = "reconciled"
    elif total_missing <= unattributed_dlq:
        verdict = "explainable_by_unattributed_dlq"
    else:
        verdict = "unexplained_gaps"
    return {
        "verdict": verdict,
        "messages": dict(by_source),
        "unattributed": dict(unattributed),
        "main_distinct_event_ids": len(ids),
        "main_duplicate_event_ids": dup_ids,
        "missing_upstream_offsets": total_missing,
        "upstream": upstream,
    }


def _start_offsets(c: Consumer, topic: str, since_ms: int | None) -> dict[int, tuple[int, int]]:
    parts = list(c.list_topics(topic, timeout=10).topics[topic].partitions)
    ranges = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in parts}
    if since_ms is None:
        return ranges
    found = c.offsets_for_times([TopicPartition(topic, p, since_ms) for p in parts], timeout=10)
    out = {}
    for tp in found:
        lo, hi = ranges[tp.partition]
        start = hi if tp.offset < 0 else max(tp.offset, lo)  # -1: nothing that recent
        out[tp.partition] = (start, hi)
    return out


def collect(settings: Settings, since_ms: int | None = None) -> list[Seen]:
    c = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "wikistream-audit",  # assign() only; never commits
            "enable.auto.commit": False,
        }
    )
    deser = AvroDeserializer(SchemaRegistryClient({"url": settings.schema_registry_url}))
    ctx = SerializationContext(settings.main_topic.name, MessageField.VALUE)
    names = dict(zip(SOURCES, (t.name for t in settings.all_topics), strict=True))
    seen: list[Seen] = []
    try:
        # One snapshot of every topic's range before reading any. Per-topic snapshots let a
        # canary that reached the drops ledger mid-audit extend the upstream range past the
        # main topic's end: a false trailing gap (2026-10-10, 535 "missing" offsets).
        snapshot = {source: _start_offsets(c, topic, since_ms) for source, topic in names.items()}
        for source, topic in names.items():
            ranges = snapshot[source]
            want = sum(hi - lo for lo, hi in ranges.values())
            if want == 0:
                continue
            c.assign([TopicPartition(topic, p, lo) for p, (lo, _) in ranges.items()])
            got = 0
            while got < want:
                m = c.poll(10)
                if m is None:
                    raise RuntimeError(f"audit timed out on {topic} after {got}/{want}")
                if m.error():
                    raise RuntimeError(str(m.error()))
                if m.offset() >= ranges[m.partition()][1]:
                    continue  # arrived after the audit started; out of scope
                got += 1
                h = {k: v.decode() for k, v in (m.headers() or [])}
                repaired = H_BACKFILL in h
                if source == "main":
                    meta = deser(m.value(), ctx)["meta"]
                    seen.append(Seen(source, upstream_position(meta), meta["id"], repaired))
                else:
                    pos = None
                    if H_UP_TOPIC in h:
                        pos = UpstreamPosition(
                            h[H_UP_TOPIC], int(h[H_UP_PARTITION]), int(h[H_UP_OFFSET])
                        )
                    seen.append(Seen(source, pos, backfill=repaired))
            c.unassign()
    finally:
        c.close()
    return seen


def main() -> None:
    ap = argparse.ArgumentParser(description="Reconcile topics against upstream offsets.")
    ap.add_argument("--since", help="ISO-8601 time; audit only messages produced after it")
    args = ap.parse_args()
    since_ms = None
    if args.since:
        since_ms = int(datetime.fromisoformat(args.since.replace("Z", "+00:00")).timestamp() * 1000)
    report = reconcile(collect(Settings(), since_ms))
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["verdict"] == "reconciled" else 1)
