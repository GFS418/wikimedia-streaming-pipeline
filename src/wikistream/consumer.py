"""Kafka main topic -> gzipped JSONL files on S3, one file per fixed offset range.

Guarantee for this hop: exactly-once per offset into the landing zone.
- D9: file k of a partition covers offsets k*N .. k*N+N-1, so a range has one name forever.
- D10: file bytes are a pure function of the messages in the range (canonical JSON rows,
  gzip with mtime=0), so any rewrite is identical.
- D11: S3 is the record of progress. A PUT is atomic; on assignment the consumer lists its
  prefix and resumes after the highest range written. No Kafka offsets are committed.

A partial range is never written: on shutdown or rebalance it is discarded and re-read.
"""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
import logging
import re
import signal
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.config import Settings
from wikistream.contract import canonical_json

log = logging.getLogger("wikistream.consumer")

KEY_RE = re.compile(r"p(\d{2})/(\d{20})-(\d{20})\.jsonl\.gz$")


# ------------------------------------------------------------------ pure pieces


def range_for(offset: int, n: int) -> tuple[int, int]:
    start = (offset // n) * n
    return start, start + n - 1


def object_key(prefix: str, partition: int, start: int, end: int) -> str:
    return f"{prefix}p{partition:02d}/{start:020d}-{end:020d}.jsonl.gz"


def parse_key(key: str) -> tuple[int, int, int] | None:
    m = KEY_RE.search(key)
    return (int(m[1]), int(m[2]), int(m[3])) if m else None


def envelope(msg: Any, record: dict[str, Any]) -> str:
    """One landing row. Every field is immutable for a given offset (no write time)."""
    value = msg.value()
    return canonical_json(
        {
            "kafka": {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp_ms": msg.timestamp()[1],
                "key": msg.key().decode() if msg.key() is not None else None,
                "schema_id": int.from_bytes(value[1:5], "big"),  # Confluent wire format
                "headers": {k: v.decode() for k, v in (msg.headers() or [])},
            },
            "value": record,
        }
    )


def encode_file(rows: list[str], level: int) -> bytes:
    """Gzipped JSONL. mtime=0 so identical rows always give identical bytes (D10)."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=level, mtime=0) as f:
        f.write(("\n".join(rows) + "\n").encode())
    return buf.getvalue()


class OffsetGap(RuntimeError):
    """Kafka offsets in this topic are dense; a gap means something is badly wrong."""


@dataclass
class RangeBuffer:
    partition: int
    start: int
    end: int
    next_offset: int  # first offset this buffer expects; > start only for a partial head
    rows: list[str] = field(default_factory=list)

    def add(self, offset: int, row: str) -> bool:
        """Append the next row; returns True when the range is complete."""
        if offset != self.next_offset:
            raise OffsetGap(f"p{self.partition}: expected offset {self.next_offset}, got {offset}")
        self.rows.append(row)
        self.next_offset += 1
        return offset == self.end


# ------------------------------------------------------------------ S3


def last_written_end(s3, bucket: str, prefix: str, partition: int) -> int | None:
    """Highest range end already landed for this partition (the D11 record)."""
    best = None
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{prefix}p{partition:02d}/"
    )
    for page in pages:
        for obj in page.get("Contents", []):
            parsed = parse_key(obj["Key"])
            if parsed and parsed[0] == partition and (best is None or parsed[2] > best):
                best = parsed[2]
    return best


def put_file(s3, bucket: str, key: str, body: bytes) -> None:
    # Content-MD5 makes S3 reject a corrupted upload instead of storing it.
    md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentMD5=md5, ContentType="application/gzip")


# ------------------------------------------------------------------ the loop


class LandingConsumer:
    def __init__(self, settings: Settings, s3, stop: threading.Event | None = None) -> None:
        self.s = settings
        self.s3 = s3
        self.stop = stop or threading.Event()
        self.counts: Counter[str] = Counter()
        self.buffers: dict[int, RangeBuffer] = {}
        self._closing = False
        self.consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.consumer_group,
                "enable.auto.commit": False,  # D11: S3, not Kafka, records progress
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",  # unused: every partition is seeked
            }
        )
        self.deser = AvroDeserializer(SchemaRegistryClient({"url": settings.schema_registry_url}))
        self.ctx = SerializationContext(settings.main_topic.name, MessageField.VALUE)

    def _new_buffer(self, partition: int, next_offset: int) -> RangeBuffer:
        start, end = range_for(next_offset, self.s.offsets_per_file)
        return RangeBuffer(partition, start, end, next_offset)

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        for tp in partitions:
            low, _ = consumer.get_watermark_offsets(tp, timeout=10)
            end = last_written_end(
                self.s3, self.s.landing_bucket, self.s.landing_prefix, tp.partition
            )
            nxt = low if end is None else end + 1
            if nxt < low:
                # Only if the consumer was down longer than Kafka kept the data: at least
                # retention.ms (7 days), at most retention.ms + segment.ms (14 days).
                self.counts["expired_before_landing"] += low - nxt
                log.error(
                    "p%d: offsets %d..%d expired from Kafka before landing",
                    tp.partition,
                    nxt,
                    low - 1,
                )
                nxt = low
            tp.offset = nxt
            self.buffers[tp.partition] = self._new_buffer(tp.partition, nxt)
            log.info("p%d: resume at offset %d (last landed end: %s)", tp.partition, nxt, end)
        consumer.assign(partitions)

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        for tp in partitions:
            dropped = self.buffers.pop(tp.partition, None)
            # Shutdown also revokes; those rows are already counted as partial_rows_at_exit.
            if dropped and dropped.rows and not self._closing:
                self.counts["partial_rows_discarded_on_rebalance"] += len(dropped.rows)
        consumer.unassign()

    def _land(self, buf: RangeBuffer) -> None:
        key = object_key(self.s.landing_prefix, buf.partition, buf.start, buf.end)
        body = encode_file(buf.rows, self.s.gzip_level)
        put_file(self.s3, self.s.landing_bucket, key, body)
        self.counts["files"] += 1
        self.counts["rows"] += len(buf.rows)
        self.counts["bytes"] += len(body)
        log.info("landed %s (%d rows, %d bytes)", key, len(buf.rows), len(body))

    def run(self, max_files: int | None = None, max_seconds: float | None = None) -> Counter[str]:
        self.consumer.subscribe(
            [self.s.main_topic.name], on_assign=self._on_assign, on_revoke=self._on_revoke
        )
        started = time.monotonic()
        try:
            while not self.stop.is_set():
                if max_seconds and time.monotonic() - started >= max_seconds:
                    break
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                buf = self.buffers[msg.partition()]
                row = envelope(msg, self.deser(msg.value(), self.ctx))
                if buf.add(msg.offset(), row):
                    self._land(buf)
                    self.buffers[msg.partition()] = self._new_buffer(msg.partition(), buf.end + 1)
                    if max_files and self.counts["files"] >= max_files:
                        break
        finally:
            partial = sum(len(b.rows) for b in self.buffers.values())
            self.counts["partial_rows_at_exit"] = partial  # re-read next run, never written
            self._closing = True
            self.consumer.close()
            log.info("final %s", json.dumps(dict(self.counts), sort_keys=True))
        return self.counts


def main() -> None:
    import boto3

    ap = argparse.ArgumentParser(description="Land the main topic on S3 (decisions D9-D12).")
    ap.add_argument("--max-files", type=int)
    ap.add_argument("--max-seconds", type=float)
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    s = Settings()
    s3 = boto3.Session(profile_name=s.aws_profile).client("s3")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    LandingConsumer(s, s3, stop).run(args.max_files, args.max_seconds)


if __name__ == "__main__":
    main()
