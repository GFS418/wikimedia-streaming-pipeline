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
from wikistream.contract import (
    SCHEMA_PATH,
    DeadLetter,
    Drop,
    UpstreamPosition,
    Valid,
    canonical_json,
    classify,
    upstream_position,
)
from wikistream.cursor import cursor_newest_ms, merge_cursors
from wikistream.sse import SSEClient

log = logging.getLogger("wikistream.producer")

H_CURSOR = "upstream_cursor"
H_VERSION = "producer_version"
H_UP_TOPIC, H_UP_PARTITION, H_UP_OFFSET = "upstream_topic", "upstream_partition", "upstream_offset"


def upstream_headers(pos: UpstreamPosition | None) -> list[tuple[str, str]]:
    if pos is None:
        return []
    return [
        (H_UP_TOPIC, pos.topic),
        (H_UP_PARTITION, str(pos.partition)),
        (H_UP_OFFSET, str(pos.offset)),
    ]


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
        self.drops = settings.drops_topic.name
        self.serialize = value_serializer
        self.counts: Counter[str] = Counter()
        self.fatal: KafkaException | None = None

    def _on_delivery(self, err, msg) -> None:
        kind = {self.dlq: "dlq", self.drops: "drops"}.get(msg.topic(), "main")
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
            *upstream_headers(dl.upstream),
        ]
        if cursor:
            headers.append((H_CURSOR, cursor))
        self._produce(self.dlq, dl.key.encode() if dl.key else None, raw, headers)
        self.counts["dlq"] += 1
        self.counts[f"dlq.{dl.error_class}"] += 1

    def _record_drop(self, cursor: str | None, drop: Drop) -> None:
        # The ledger (decision D8) holds positions and reasons, not the event itself.
        pos = drop.upstream
        entry = {
            "reason": drop.reason,
            "upstream_topic": pos.topic if pos else None,
            "upstream_partition": pos.partition if pos else None,
            "upstream_offset": pos.offset if pos else None,
        }
        headers = [(H_VERSION, __version__), *upstream_headers(pos)]
        if cursor:
            headers.append((H_CURSOR, cursor))
        self._produce(self.drops, None, canonical_json(entry).encode(), headers)
        self.counts[f"dropped.{drop.reason}"] += 1

    def handle(self, data: str | bytes, cursor: str | None) -> None:
        raw = data.encode() if isinstance(data, str) else data
        self.counts["received"] += 1
        outcome = classify(raw)
        if isinstance(outcome, Drop):
            self._record_drop(cursor, outcome)
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
                raw,
                cursor,
                DeadLetter(
                    "serialization_failed",
                    str(exc),
                    outcome.key,
                    upstream_position(outcome.record["meta"]),
                ),
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
    """Furthest upstream cursor found at the tail of every partition of all our topics."""
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
        for spec in settings.all_topics:
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


DEFAULT_MAX_CURSOR_AGE_S = 30 * 60


class StaleCursor(RuntimeError):
    """Resuming would replay more upstream history than allowed without an explicit choice."""

    def __init__(self, age_s: float | None, max_age_s: float):
        self.age_s, self.max_age_s = age_s, max_age_s
        if age_s is None:
            age = "of unknown age"
        elif age_s < 2 * 3600:
            age = f"{age_s / 60:.0f} min old"
        else:
            age = f"{age_s / 3600:.1f} h old"
        super().__init__(
            f"resume cursor is {age} (limit {max_age_s / 60:.0f} min); resuming would replay "
            "that much upstream history. Re-run with --catch-up to replay it, or --from-now "
            "to skip it (the audit will then show the gap)."
        )


def choose_start(
    cursor: str | None,
    now_ms: int,
    max_age_s: float = DEFAULT_MAX_CURSOR_AGE_S,
    from_now: bool = False,
    catch_up: bool = False,
) -> str | None:
    """Where to start (decision D13): the cursor to resume from, or None for "now".

    A stale cursor (the producer was down longer than max_age_s) is never resumed
    silently: replaying days of upstream history must be a deliberate choice.
    """
    if from_now and catch_up:
        raise ValueError("--from-now and --catch-up are mutually exclusive")
    if from_now or cursor is None:
        return None
    newest = cursor_newest_ms(cursor)
    age_s = None if newest is None else (now_ms - newest) / 1000
    if catch_up or (age_s is not None and age_s <= max_age_s):
        return cursor
    raise StaleCursor(age_s, max_age_s)


def run(
    settings: Settings,
    max_events: int | None = None,
    max_seconds: float | None = None,
    from_now: bool = False,
    stats_every_s: float = 60.0,
    catch_up: bool = False,
    max_cursor_age_s: float = DEFAULT_MAX_CURSOR_AGE_S,
) -> Counter[str]:
    # Decide where to start before touching anything else, so a refusal has no side effects.
    stored = None if from_now else resume_cursor(settings)
    cursor = choose_start(stored, int(time.time() * 1000), max_cursor_age_s, from_now, catch_up)
    if cursor:
        log.info("starting from cursor %s%s", cursor, " (--catch-up)" if catch_up else "")
    else:
        log.info("starting from now (%s)", "--from-now" if from_now else "no cursor in Kafka")

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

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
    start = ap.add_mutually_exclusive_group()
    start.add_argument("--from-now", action="store_true", help="ignore the cursor stored in Kafka")
    start.add_argument(
        "--catch-up", action="store_true", help="resume even from a stale cursor (replay backlog)"
    )
    ap.add_argument(
        "--max-cursor-age-minutes",
        type=float,
        default=DEFAULT_MAX_CURSOR_AGE_S / 60,
        help="refuse to resume silently from an older cursor (default: %(default)s)",
    )
    ap.add_argument("--log-level", default="INFO")
    args = ap.parse_args()
    logging.basicConfig(
        level=args.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    try:
        counts = run(
            Settings(),
            args.max_events,
            args.max_seconds,
            args.from_now,
            catch_up=args.catch_up,
            max_cursor_age_s=args.max_cursor_age_minutes * 60,
        )
    except StaleCursor as exc:
        log.error("%s", exc)
        raise SystemExit(2) from None
    if counts["undelivered_at_exit"] or any(k.startswith("delivery_failed") for k in counts):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
