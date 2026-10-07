"""Verify the landing zone against Kafka (decisions D9-D12). Read-only.

For every landed file:
- integrity: MD5 of the downloaded body equals the S3 ETag (single-part PUT, SSE-S3);
- structure: gzip mtime is 0; offsets are contiguous and exactly cover the key's range
  (the first file of a partition may start mid-range if Kafka's low watermark did);
  partition and key fields are consistent;
- history: exactly one S3 version per key (a clean run never rewrites);
- determinism: if Kafka still retains the range, re-derive the file from Kafka with the
  consumer's own encoding and compare MD5s. Same offsets -> same bytes, in a separate
  process and at a different time. (Same code path, so this proves determinism and
  integrity, not an independent reimplementation.)
Per partition: ranges are contiguous with no overlaps, and the unlanded tail is reported.

    uv run python scripts/verify_landing.py
"""

from __future__ import annotations

import gzip
import hashlib
import json
from collections import defaultdict

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.config import Settings
from wikistream.consumer import encode_file, envelope, parse_key


def _rederive(c: Consumer, deser, ctx, topic: str, p: int, first: int, end: int, level: int):
    c.assign([TopicPartition(topic, p, first)])
    rows = []
    while len(rows) < end - first + 1:
        m = c.poll(10)
        if m is None or m.error():
            raise RuntimeError(f"re-derive {topic}[{p}] {first}..{end}: {m and m.error()}")
        rows.append(envelope(m, deser(m.value(), ctx)))
    c.unassign()
    return encode_file(rows, level)


def verify(settings: Settings, s3) -> dict:
    s = settings
    c = Consumer(
        {
            "bootstrap.servers": s.bootstrap_servers,
            "group.id": "wikistream-verify",  # assign() only; never commits
            "enable.auto.commit": False,
        }
    )
    deser = AvroDeserializer(SchemaRegistryClient({"url": s.schema_registry_url}))
    ctx = SerializationContext(s.main_topic.name, MessageField.VALUE)
    topic = s.main_topic.name

    versions: dict[str, int] = defaultdict(int)
    for page in s3.get_paginator("list_object_versions").paginate(
        Bucket=s.landing_bucket, Prefix=s.landing_prefix
    ):
        for v in page.get("Versions", []):
            versions[v["Key"]] += 1

    report: dict = {"partitions": {}, "failures": []}
    fail = report["failures"].append
    try:
        n_parts = len(c.list_topics(topic, timeout=10).topics[topic].partitions)
        for p in range(n_parts):
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            objs = []
            for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=s.landing_bucket, Prefix=f"{s.landing_prefix}p{p:02d}/"
            ):
                for o in page.get("Contents", []):
                    parsed = parse_key(o["Key"])
                    if parsed is None:
                        fail(f"foreign object in landing prefix: {o['Key']}")
                        continue
                    objs.append((parsed, o["Key"], o["ETag"].strip('"')))
            objs.sort()
            part = {
                "kafka_low": low,
                "kafka_high": high,
                "files": len(objs),
                "rows": 0,
                "rederived_identical": 0,
                "not_rederivable_expired": 0,
            }
            prev_end = None
            for i, ((pp, start, end), key, etag) in enumerate(objs):
                if pp != p:
                    fail(f"{key}: partition {pp} under p{p:02d}/")
                if prev_end is not None and start != prev_end + 1:
                    fail(f"p{p}: range gap or overlap before {key} (previous end {prev_end})")
                prev_end = end
                if versions[key] != 1:
                    fail(f"{key}: {versions[key]} versions (expected 1 in a clean run)")

                body = s3.get_object(Bucket=s.landing_bucket, Key=key)["Body"].read()
                if hashlib.md5(body).hexdigest() != etag:
                    fail(f"{key}: body MD5 != ETag")
                if body[4:8] != b"\x00\x00\x00\x00":
                    fail(f"{key}: gzip mtime is not 0")
                rows = [json.loads(r) for r in gzip.decompress(body).decode().splitlines()]
                offsets = [r["kafka"]["offset"] for r in rows]
                first = offsets[0] if offsets else None
                head_ok = i == 0 and first is not None and start <= first
                if offsets != list(range(first if head_ok else start, end + 1)):
                    fail(f"{key}: offsets do not exactly cover the range")
                if any(r["kafka"]["partition"] != p for r in rows):
                    fail(f"{key}: row from another partition")
                if any(r["kafka"]["key"] != r["value"]["meta"]["id"] for r in rows):
                    fail(f"{key}: Kafka key != meta.id")
                part["rows"] += len(rows)

                if first is not None and first >= low:
                    again = _rederive(c, deser, ctx, topic, p, first, end, s.gzip_level)
                    if hashlib.md5(again).hexdigest() == etag:
                        part["rederived_identical"] += 1
                    else:
                        fail(f"{key}: re-derived bytes differ from landed bytes")
                else:
                    part["not_rederivable_expired"] += 1

            part["landed_through"] = prev_end
            nxt = 0 if prev_end is None else prev_end + 1
            part["unlanded_tail_in_kafka"] = max(0, high - max(nxt, low))
            report["partitions"][f"p{p:02d}"] = part
    finally:
        c.close()

    parts = report["partitions"].values()
    report["files"] = sum(x["files"] for x in parts)
    report["rows"] = sum(x["rows"] for x in parts)
    report["rederived_identical"] = sum(x["rederived_identical"] for x in parts)
    report["verdict"] = "ok" if not report["failures"] else "FAILED"
    return report


def main() -> None:
    import boto3

    # Admin (default) profile: the consumer role deliberately cannot read objects.
    report = verify(Settings(), boto3.client("s3"))
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["verdict"] == "ok" else 1)
