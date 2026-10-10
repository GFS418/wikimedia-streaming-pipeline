"""Repair: re-fetch named upstream events from Wikimedia's 7-day history and produce them.

For events the capture missed, e.g. the two a resume skipped overnight 2026-10-10 (D31).
Stop the producer first (docs/runbook_pause_resume.md, "Backfill"), so partition tails are
still while their resume headers are copied.

    uv run python scripts/backfill_upstream.py \\
        'eqiad.mediawiki.recentchange[0]@6598017959=2026-10-10T04:06:56.735Z'

Each target is UPSTREAM_TOPIC[PARTITION]@OFFSET=APPROXIMATE_TIME. For each one:
1. skip it if the main topic already holds it (scanned from `margin` before its time to the
   end), so a second run adds nothing;
2. stream from `margin` before its time until that upstream partition passes its offset,
   keeping the exact bytes the service sends;
3. route it through the producer's Router (same contract, serializer and topics), with the
   header `backfill`, and with the resume headers (upstream_cursor, upstream_offsets) copied
   from the tail of the partition it lands in, so a later restart resumes as if it were not
   there. Downstream it is an ordinary row, except that gold drops it: it arrives hours
   after its minute closed (scripts/verify_gold.py accounts for that).
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
import zlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import httpx
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.config import Settings
from wikistream.producer import (
    H_BACKFILL,
    H_CURSOR,
    H_OFFSETS,
    Router,
    make_value_serializer,
    producer_config,
)
from wikistream.sse import SSEParser

log = logging.getLogger("wikistream.backfill")

TARGET_RE = re.compile(r"^(?P<topic>[^\[\]]+)\[(?P<partition>\d+)\]@(?P<offset>\d+)=(?P<at>.+)$")


@dataclass(frozen=True)
class Target:
    topic: str
    partition: int
    offset: int
    at: datetime

    @classmethod
    def parse(cls, spec: str) -> Target:
        m = TARGET_RE.match(spec)
        if not m:
            raise ValueError(f"expected TOPIC[PARTITION]@OFFSET=TIME, got {spec!r}")
        at = datetime.fromisoformat(m["at"].replace("Z", "+00:00"))
        return cls(m["topic"], int(m["partition"]), int(m["offset"]), at)

    def label(self) -> str:
        return f"{self.topic}[{self.partition}]@{self.offset}"


def _consumer(s: Settings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": s.bootstrap_servers,
            "group.id": "wikistream-backfill",  # assign() only; never commits
            "enable.auto.commit": False,
        }
    )


def already_present(s: Settings, targets: list[Target], margin: timedelta) -> set[Target]:
    """Targets the main topic already holds, from `margin` before the earliest one to the end
    (which also covers anything an earlier backfill appended)."""
    topic, n = s.main_topic.name, s.main_topic.partitions
    since_ms = int((min(t.at for t in targets) - margin).timestamp() * 1000)
    want = {(t.topic, t.partition, t.offset): t for t in targets}
    deser = AvroDeserializer(SchemaRegistryClient({"url": s.schema_registry_url}))
    ctx = SerializationContext(topic, MessageField.VALUE)
    c = _consumer(s)
    found: set[Target] = set()
    try:
        starts = c.offsets_for_times([TopicPartition(topic, p, since_ms) for p in range(n)], 10)
        ends = {
            p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1] for p in range(n)
        }
        todo = {tp.partition: tp.offset for tp in starts if 0 <= tp.offset < ends[tp.partition]}
        if not todo:
            return found
        c.assign([TopicPartition(topic, p, o) for p, o in todo.items()])
        left = sum(ends[p] - o for p, o in todo.items())
        while left:
            m = c.poll(10)
            if m is None:
                raise RuntimeError(f"scan of {topic} timed out with {left} messages left")
            if m.error() or m.offset() >= ends[m.partition()]:
                continue
            left -= 1
            meta = deser(m.value(), ctx)["meta"]
            if t := want.get((meta["topic"], meta["partition"], meta["offset"])):
                found.add(t)
    finally:
        c.close()
    return found


def fetch(s: Settings, target: Target, margin: timedelta, deadline_s: float = 300) -> str | None:
    """The target's event exactly as the service sends it, or None if it never appears."""
    since = (target.at - margin).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    headers = {"User-Agent": s.user_agent, "Accept": "text/event-stream"}
    stop_at = time.monotonic() + deadline_s
    timeout = httpx.Timeout(10.0, read=60.0)
    with httpx.stream(
        "GET", f"{s.stream_url}?since={since}", headers=headers, timeout=timeout
    ) as r:
        r.raise_for_status()
        for ev in SSEParser().parse(r.iter_lines()):
            meta = json.loads(ev.data).get("meta") or {}
            if (meta.get("topic"), meta.get("partition")) == (target.topic, target.partition):
                if meta.get("offset") == target.offset:
                    return ev.data
                if isinstance(meta.get("offset"), int) and meta["offset"] > target.offset:
                    return None  # passed it: the service no longer has it
            if time.monotonic() > stop_at:
                return None
    return None


def tail_resume_headers(s: Settings, partition: int) -> tuple[str | None, str | None]:
    """upstream_cursor and upstream_offsets of the partition's last message."""
    topic = s.main_topic.name
    c = _consumer(s)
    try:
        low, high = c.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        if high <= low:
            return None, None
        c.assign([TopicPartition(topic, partition, high - 1)])
        m = c.poll(10)
        if m is None or m.error():
            raise RuntimeError(f"could not read the tail of {topic}[{partition}]")
        h = {k: v.decode() for k, v in (m.headers() or [])}
        return h.get(H_CURSOR), h.get(H_OFFSETS)
    finally:
        c.close()


def backfill(
    s: Settings, targets: list[Target], margin: timedelta = timedelta(minutes=10), label=None
) -> dict:
    label = label or f"{datetime.now(UTC):%Y-%m-%dT%H:%M:%SZ} upstream repair"
    present = already_present(s, targets, margin)
    producer = Producer(producer_config(s))
    router = Router(producer, s, make_value_serializer(s))
    report = {"label": label, "targets": {}}
    for t in sorted(targets, key=lambda t: t.at):
        if t in present:
            report["targets"][t.label()] = "already in Kafka: skipped"
            continue
        data = fetch(s, t, margin)
        if data is None:
            report["targets"][t.label()] = "not found upstream"
            continue
        key = json.loads(data)["meta"]["id"]
        # Where librdkafka's default partitioner puts it: CRC32 of the key (see test helpers).
        partition = zlib.crc32(key.encode()) % s.main_topic.partitions
        cursor, offsets = tail_resume_headers(s, partition)
        router.handle(data, cursor, offsets, extra_headers=((H_BACKFILL, label),))
        if producer.flush(30) != 0 or router.fatal:
            raise RuntimeError(f"backfill of {t.label()} was not delivered")
        report["targets"][t.label()] = {"meta_id": key, "partition": partition, "bytes": len(data)}
    report["counts"] = dict(router.counts)
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("targets", nargs="+", help="TOPIC[PARTITION]@OFFSET=APPROXIMATE_TIME")
    ap.add_argument("--margin-minutes", type=float, default=10)
    ap.add_argument(
        "--label", help="value of the backfill header (default: now + 'upstream repair')"
    )
    args = ap.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    targets = [Target.parse(t) for t in args.targets]
    margin = timedelta(minutes=args.margin_minutes)
    print(json.dumps(backfill(Settings(), targets, margin, args.label), indent=2))
