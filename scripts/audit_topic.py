"""Audit the main topic: duplicates by meta.id and upstream offset continuity.

Reads every message currently in the topic, decodes it, and reports, per upstream topic
(eqiad / codfw), the offset range, gaps and duplicates. Duplicates here are the
at-least-once cost of the SSE hop (decision A); gaps would be data loss.

    uv run python scripts/audit_topic.py
"""

import json
from collections import Counter, defaultdict

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.config import Settings


def main() -> None:
    s = Settings()
    topic = s.main_topic.name
    c = Consumer(
        {"bootstrap.servers": s.bootstrap_servers, "group.id": "audit", "enable.auto.commit": False}
    )
    parts = list(c.list_topics(topic, timeout=10).topics[topic].partitions)
    ends = {p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10) for p in parts}
    c.assign([TopicPartition(topic, p, ends[p][0]) for p in parts])
    deser = AvroDeserializer(SchemaRegistryClient({"url": s.schema_registry_url}))
    ctx = SerializationContext(topic, MessageField.VALUE)

    want = sum(hi - lo for lo, hi in ends.values())
    ids: Counter[str] = Counter()
    upstream: dict[str, list[int]] = defaultdict(list)
    extra: Counter[str] = Counter()
    n = 0
    while n < want:
        m = c.poll(10)
        if m is None:
            raise SystemExit(f"timed out after {n}/{want}")
        if m.error():
            raise SystemExit(str(m.error()))
        rec = deser(m.value(), ctx)
        ids[rec["meta"]["id"]] += 1
        if rec["meta"]["topic"] is not None and rec["meta"]["offset"] is not None:
            upstream[rec["meta"]["topic"]].append(rec["meta"]["offset"])
        if rec["_extra_json"]:
            extra.update(json.loads(rec["_extra_json"]).keys())
        n += 1
    c.close()

    dup_events = sum(v - 1 for v in ids.values() if v > 1)
    report = {
        "messages": n,
        "distinct_meta_id": len(ids),
        "duplicate_messages": dup_events,
        "duplicate_rate": round(dup_events / n, 6) if n else 0,
        "extra_fields_seen": dict(extra),
        "upstream": {},
    }
    for t, offs in sorted(upstream.items()):
        uniq = sorted(set(offs))
        gaps = [(a, b) for a, b in zip(uniq, uniq[1:], strict=False) if b > a + 1]
        report["upstream"][t] = {
            "min": uniq[0],
            "max": uniq[-1],
            "expected": uniq[-1] - uniq[0] + 1,
            "distinct": len(uniq),
            "missing": sum(b - a - 1 for a, b in gaps),
            "gap_runs": gaps[:10],
        }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
