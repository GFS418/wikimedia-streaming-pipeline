"""Wikimedia SSE -> contract -> Kafka (main topic or dead-letter topic).

Guarantee for this hop (decision A): at-least-once. The upstream cursor is a timestamp,
so a restart can re-send events at the boundary; silver dedups on meta.id. Within Kafka,
the idempotent producer (acks=all, enable.idempotence) prevents duplicates from retries.

Resume (decision B): no local state. On start, read the last message of every partition
of both topics and resume from the furthest `upstream_cursor` header found.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
import time
from collections import Counter
from datetime import UTC, datetime

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, SerializationError

from wikistream import __version__
from wikistream.config import Settings
from wikistream.contract import SCHEMA_PATH, DeadLetter, Drop, Valid, classify
from wikistream.cursor import merge_cursors
from wikistream.sse import SSEClient

log = logging.getLogger("wikistream.producer")

H_CURSOR = "upstream_cursor"
H_VERSION = "producer_version"


def producer_config(settings: Settings) -> dict[str, object]:
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": f"wikistream-producer-{__version__}",
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
        "linger.ms": 50,
    }


def make_value_serializer(settings: Settings) -> AvroSerializer:
    sr = SchemaRegistryClient({"url": settings.schema_registry_url})
    # The producer never registers schemas: a new version must pass the FULL_TRANSITIVE
    # check through bootstrap/CI first, so an unreviewed schema cannot reach the topic.
    return AvroSerializer(sr, SCHEMA_PATH.read_text(), conf={"auto.register.schemas": False})


class Router:
    """Classify each raw event and produce it to the right place. Counts everything."""

    def __init__(self, producer: Producer, settings: Settings, value_serializer) -> None:
        self.producer = producer
        self.main = settings.main_topic.name
        self.dlq = settings.dlq_topic.name
        self.serialize = value_serializer
        self.counts: Counter[str] = Counter()
        self.fatal: KafkaException | None = None

    def _on_delivery(self, err, msg) -> None:
        kind = "dlq" if msg.topic() == self.dlq else "main"
        if err is None:
            self.counts[f"delivered.{kind}"] += 1
            return
        self.counts[f"delivery_failed.{kind}"] += 1
        log.error("delivery failed (%s): %s", kind, err)
        if err.fatal():
            self.fatal = KafkaException(err)

    def _produce(self, topic: str, key: bytes | None, value: bytes, headers: list) -> None:
        while True:
            try:
                self.producer.produce(
                    topic, key=key, value=value, headers=headers, on_delivery=self._on_delivery
                )
                break
            except BufferError:  # local queue full: let deliveries drain, then retry
                self.producer.poll(0.5)
        self.producer.poll(0)
        if self.fatal:
            raise self.fatal

    def _dead_letter(self, raw: bytes, cursor: str | None, dl: DeadLetter) -> None:
        headers = [
            ("error_class", dl.error_class),
            ("error_detail", dl.detail[:1000]),
            ("failed_at", datetime.now(UTC).isoformat()),
            (H_VERSION, __version__),
        ]
        if cursor:
            headers.append((H_CURSOR, cursor))
        self._produce(self.dlq, dl.key.encode() if dl.key else None, raw, headers)
        self.counts["dlq"] += 1
        self.counts[f"dlq.{dl.error_class}"] += 1

    def handle(self, data: str | bytes, cursor: str | None) -> None:
        raw = data.encode() if isinstance(data, str) else data
        self.counts["received"] += 1
        outcome = classify(raw)
        if isinstance(outcome, Drop):
            self.counts[f"dropped.{outcome.reason}"] += 1
            return
        if isinstance(outcome, DeadLetter):
            self._dead_letter(raw, cursor, outcome)
            return
        assert isinstance(outcome, Valid)
        try:
            value = self.serialize(
                outcome.record, SerializationContext(self.main, MessageField.VALUE)
            )
        except SerializationError as exc:  # should be unreachable if contract.py is right
            self._dead_letter(
                raw, cursor, DeadLetter("serialization_failed", str(exc), outcome.key)
            )
            return
        headers = [(H_VERSION, __version__)]
        if cursor:
            headers.append((H_CURSOR, cursor))
        self._produce(self.main, outcome.key.encode(), value, headers)
        self.counts["main"] += 1
        for f in outcome.extra_fields:
            self.counts[f"extra_field.{f}"] += 1


def resume_cursor(settings: Settings, timeout_s: float = 10.0) -> str | None:
    """Furthest upstream cursor found at the tail of every partition of both topics."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "wikistream-producer-resume",  # never commits; assign() only
            "enable.auto.commit": False,
        }
    )
    found: list[str] = []
    try:
        md = consumer.list_topics(timeout=timeout_s)
        for spec in (settings.main_topic, settings.dlq_topic):
            if spec.name not in md.topics:
                continue
            for p in md.topics[spec.name].partitions:
                low, high = consumer.get_watermark_offsets(
                    TopicPartition(spec.name, p), timeout=timeout_s
                )
                if high <= low:
                    continue
                consumer.assign([TopicPartition(spec.name, p, high - 1)])
                msg = consumer.poll(timeout_s)
                if msg is None or msg.error():
                    raise RuntimeError(f"could not read tail of {spec.name}[{p}] at {high - 1}")
                headers = dict(msg.headers() or [])
                if H_CURSOR in headers:
                    found.append(headers[H_CURSOR].decode())
    finally:
        consumer.close()
    return merge_cursors(found)


def run(
    settings: Settings,
    max_events: int | None = None,
    max_seconds: float | None = None,
    from_now: bool = False,
    stats_every_s: float = 60.0,
) -> Counter[str]:
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    cursor = None if from_now else resume_cursor(settings)
    log.info("starting from %s", f"cursor {cursor}" if cursor else "now (no cursor in Kafka)")

    producer = Producer(producer_config(settings))
    router = Router(producer, settings, make_value_serializer(settings))
    client = SSEClient(settings.stream_url, settings.user_agent, last_event_id=cursor, stop=stop)

    started = last_stats = time.monotonic()
    try:
        for ev in client.events():
            router.handle(ev.data, ev.id)
            now = time.monotonic()
            if now - last_stats >= stats_every_s:
                log.info(
                    "stats %s", json.dumps(dict(router.counts) | {"reconnects": client.reconnects})
                )
                last_stats = now
            if (max_events and router.counts["received"] >= max_events) or (
                max_seconds and now - started >= max_seconds
            ):
                stop.set()
                break
    finally:
        remaining = producer.flush(30)
        router.counts["undelivered_at_exit"] = remaining
        router.counts["reconnects"] = client.reconnects
        log.info("final %s", json.dumps(dict(router.counts), sort_keys=True))
    return router.counts


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--max-events", type=int)
    ap.add_argument("--max-seconds", type=float)
    ap.add_argument("--from-now", action="store_true", help="ignore the cursor stored in Kafka")
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    counts = run(Settings(), args.max_events, args.max_seconds, args.from_now)
    if counts["undelivered_at_exit"] or any(k.startswith("delivery_failed") for k in counts):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
