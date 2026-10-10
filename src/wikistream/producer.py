"""Wikimedia SSE -> contract -> Kafka (main topic or dead-letter topic).

Guarantee for this hop (decision A): at-least-once, verified (D31). The upstream cursor is
a timestamp that Wikimedia sets 1 ms past each event, so resuming from it as given could skip
events (see cursor.py). Every resume starts 1 s early, events at or below the newest upstream
offset already handled are ignored, and each upstream partition must continue at exactly the
next offset, or the producer re-reads from further back. A restart can still re-send what a
crash left half-delivered; silver dedups on meta.id. Within Kafka, the idempotent producer
(acks=all, enable.idempotence) prevents duplicates from retries.

Resume (decisions B, D29, D31): no local state. On start, read the last message of every
partition of the main topic: resume from the slowest `upstream_cursor` header, and take the
lowest `upstream_offsets` header as what is already in Kafka.
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
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
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
from wikistream.cursor import (
    ResumeGuard,
    advance,
    cursor_newest_ms,
    floors,
    merge_cursors,
    rewind,
    slowest,
)
from wikistream.faults import crash_point
from wikistream.sse import SSEClient

log = logging.getLogger("wikistream.producer")

H_CURSOR = "upstream_cursor"
H_OFFSETS = "upstream_offsets"  # D31: newest upstream offset handled per upstream partition
H_VERSION = "producer_version"
H_BACKFILL = "backfill"  # D31 repair: set on events re-fetched later from upstream history

# D31, from the disorder of 5.7M eqiad events: a 1 s rewind covers all but 9 of them; the
# worst seen was 30.5 s. Past the base, the rewind doubles per re-read up to 10 minutes.
RESUME_REWIND_MS = 1_000
MAX_RESUME_REWIND_MS = 600_000
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
        # D29: never carry on past a lost message; stop, restart, resume from the slowest
        # partition (re-fetching it), instead of letting later deliveries move the cursor on.
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
        crash_point("producer.after_produce")  # Phase 4 drills; inert unless configured
        self.producer.poll(0)
        if self.fatal:
            raise self.fatal

    def _flush_rare(self) -> None:
        """Dead letters and drops-ledger entries are rare (about one canary an hour), so make
        each durable before moving on (D29): their sparse tails cannot take part in the
        slowest-partition resume, so they must never be lost in flight instead."""
        if self.producer.flush(30) != 0:
            self.fatal = self.fatal or KafkaException("dead-letter/drops write not delivered")
        if self.fatal:
            raise self.fatal

    def _dead_letter(self, raw: bytes, cursor: str | None, dl: DeadLetter, extra=()) -> None:
        headers = [
            ("error_class", dl.error_class),
            ("error_detail", dl.detail[:1000]),
            ("failed_at", datetime.now(UTC).isoformat()),
            (H_VERSION, __version__),
            *upstream_headers(dl.upstream),
            *extra,
        ]
        if cursor:
            headers.append((H_CURSOR, cursor))
        self._produce(self.dlq, dl.key.encode() if dl.key else None, raw, headers)
        self._flush_rare()  # D29
        self.counts["dlq"] += 1
        self.counts[f"dlq.{dl.error_class}"] += 1

    def _record_drop(self, cursor: str | None, drop: Drop, extra=()) -> None:
        # The ledger (decision D8) holds positions and reasons, not the event itself.
        pos = drop.upstream
        entry = {
            "reason": drop.reason,
            "upstream_topic": pos.topic if pos else None,
            "upstream_partition": pos.partition if pos else None,
            "upstream_offset": pos.offset if pos else None,
        }
        headers = [(H_VERSION, __version__), *upstream_headers(pos), *extra]
        if cursor:
            headers.append((H_CURSOR, cursor))
        self._produce(self.drops, None, canonical_json(entry).encode(), headers)
        self._flush_rare()  # D29
        self.counts[f"dropped.{drop.reason}"] += 1

    def handle(
        self,
        data: str | bytes,
        cursor: str | None,
        offsets: str | None = None,
        extra_headers: tuple[tuple[str, str], ...] = (),  # e.g. the backfill marker
    ) -> None:
        raw = data.encode() if isinstance(data, str) else data
        self.counts["received"] += 1
        outcome = classify(raw)
        if isinstance(outcome, Drop):
            self._record_drop(cursor, outcome, extra_headers)
            return
        if isinstance(outcome, DeadLetter):
            self._dead_letter(raw, cursor, outcome, extra_headers)
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
                extra_headers,
            )
            return
        headers = [(H_VERSION, __version__), *extra_headers]
        if cursor:
            headers.append((H_CURSOR, cursor))
        if offsets:
            headers.append((H_OFFSETS, offsets))
        self._produce(self.main, outcome.key.encode(), value, headers)
        self.counts["main"] += 1
        for f in outcome.extra_fields:
            self.counts[f"extra_field.{f}"] += 1


def _cursor_header(msg) -> str | None:
    raw = dict(msg.headers() or []).get(H_CURSOR)
    return raw.decode() if raw else None


def _latest_positions_for(
    consumer: Consumer,
    topic: str,
    partition: int,
    low: int,
    high: int,
    missing: set[tuple[str, int]],
    timeout_s: float,
    chunk: int = 5000,
) -> list[str]:
    """Scan one partition backwards, chunk by chunk, for the latest stored cursors that
    carry a timestamp for each `missing` upstream topic. Headers only; no decoding."""
    found: list[str] = []
    wanted = set(missing)
    end = high
    while end > low and wanted:
        start = max(low, end - chunk)
        consumer.assign([TopicPartition(topic, partition, start)])
        best: dict[tuple[str, int], dict] = {}
        for _ in range(end - start):
            msg = consumer.poll(timeout_s)
            if msg is None or msg.error():
                raise RuntimeError(f"could not scan {topic}[{partition}] at {start}..{end}")
            cur = _cursor_header(msg)
            for e in json.loads(cur) if cur else []:
                key = (e["topic"], int(e["partition"]))
                if key in wanted and "timestamp" in e:
                    if key not in best or e["timestamp"] > best[key]["timestamp"]:
                        best[key] = e
        if best:
            found.append(json.dumps(list(best.values())))
            wanted -= set(best)
        end = start
    return found


def resume_cursor(settings: Settings, timeout_s: float = 10.0) -> str | None:
    """Where to resume upstream (decisions B, D22, D29).

    One cursor per partition of the main topic: the one stored on its last message, with any
    upstream topic it lacks a timestamp for (cursors written before D22 could say
    {"offset": -1}) filled by scanning that partition back. Then take, per upstream topic,
    the SLOWEST of those positions (D29): each partition is delivered in order, so a crash can
    only lose what follows that partition's tail. The furthest tail (the pre-D29 rule) skipped
    the slower partitions' lost messages: 104 missing in the 2026-10-10 producer crash drill.
    Dead letters and drops are written synchronously, so their tails need no part in this.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "wikistream-producer-resume",  # never commits; assign() only
            "enable.auto.commit": False,
        }
    )
    spec = settings.main_topic
    per_partition: list[str] = []
    try:
        md = consumer.list_topics(timeout=timeout_s)
        if spec.name not in md.topics:
            return None
        tails = []
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
            tails.append((p, low, high, _cursor_header(msg)))
        upstream = {
            (e["topic"], int(e["partition"])) for *_, cur in tails if cur for e in json.loads(cur)
        }
        for p, low, high, cur in tails:
            have = (
                {(e["topic"], int(e["partition"])) for e in json.loads(cur) if "timestamp" in e}
                if cur
                else set()
            )
            found = [cur] if cur else []
            if missing := upstream - have:
                log.info(
                    "%s[%d] tail has no position for %s; scanning back",
                    spec.name,
                    p,
                    sorted(missing),
                )
                found += _latest_positions_for(
                    consumer, spec.name, p, low, high, missing, timeout_s
                )
            per_partition.append(merge_cursors(found))
    finally:
        consumer.close()
    return slowest(per_partition)


def resume_floors(settings: Settings, timeout_s: float = 10.0) -> dict[tuple[str, int], int]:
    """What is already in Kafka, per upstream partition (D31; see cursor.floors), read from the
    `upstream_offsets` header of every main partition's last message. Empty when any
    partition is empty or its tail predates the header: the next resume is then unverified."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "wikistream-producer-resume",  # never commits; assign() only
            "enable.auto.commit": False,
        }
    )
    spec = settings.main_topic
    headers: list[str | None] = []
    try:
        md = consumer.list_topics(timeout=timeout_s)
        if spec.name not in md.topics:
            return {}
        for p in md.topics[spec.name].partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(spec.name, p), timeout=timeout_s
            )
            if high <= low:
                return {}
            consumer.assign([TopicPartition(spec.name, p, high - 1)])
            msg = consumer.poll(timeout_s)
            if msg is None or msg.error():
                raise RuntimeError(f"could not read tail of {spec.name}[{p}] at {high - 1}")
            raw = dict(msg.headers() or []).get(H_OFFSETS)
            headers.append(raw.decode() if raw else None)
    finally:
        consumer.close()
    return floors(headers)


def _admit(guard: ResumeGuard, data: str | bytes) -> str:
    """The guard's verdict on one raw event; events too broken to place are always new."""
    try:
        meta = json.loads(data).get("meta")
    except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
        return ResumeGuard.NEW
    pos = upstream_position(meta)
    return ResumeGuard.NEW if pos is None else guard.admit((pos.topic, pos.partition), pos.offset)


class ContractNotRegistered(RuntimeError):
    """The producer's schema is not registered under the subject (Phase 4 negative drill)."""


def ensure_contract_registered(settings: Settings) -> int:
    """Refuse to start unless this exact schema is registered (it got there only by passing
    the subject's FULL_TRANSITIVE check in bootstrap). Without this, an unregistered schema
    would fail on the first serialization, after upstream was already connected."""
    sr = SchemaRegistryClient({"url": settings.schema_registry_url})
    try:
        found = sr.lookup_schema(settings.value_subject, Schema(SCHEMA_PATH.read_text(), "AVRO"))
    except SchemaRegistryError as exc:
        if exc.http_status_code == 404:
            raise ContractNotRegistered(
                f"{SCHEMA_PATH.name} is not registered under {settings.value_subject}; "
                "run scripts/bootstrap_kafka.py (which applies the compatibility check)"
            ) from exc
        raise
    return found.schema_id


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
    rewind_ms: int = RESUME_REWIND_MS,
    max_rewind_ms: int = MAX_RESUME_REWIND_MS,
    stop: threading.Event | None = None,
) -> Counter[str]:
    # Decide where to start before touching anything else, so a refusal has no side effects.
    stored = None if from_now else resume_cursor(settings)
    cursor = choose_start(stored, int(time.time() * 1000), max_cursor_age_s, from_now, catch_up)
    guard = ResumeGuard(rewind_ms, max_rewind_ms, resume_floors(settings) if cursor else None)
    schema_id = ensure_contract_registered(settings)
    log.info("contract %s registered as schema id %d", SCHEMA_PATH.name, schema_id)
    if cursor:
        log.info("starting from cursor %s%s", cursor, " (--catch-up)" if catch_up else "")
        log.info("already in Kafka: %s", guard.header() if guard.last else "unknown (unverified)")
    else:
        log.info("starting from now (%s)", "--from-now" if from_now else "no cursor in Kafka")

    stop = stop or threading.Event()
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stop.set())

    producer = Producer(producer_config(settings))
    router = Router(producer, settings, make_value_serializer(settings))
    client = SSEClient(
        settings.stream_url,
        settings.user_agent,
        last_event_id=cursor,
        stop=stop,
        resume_from=lambda c: rewind(c, guard.rewind_ms),  # D31: always resume early
    )

    started = last_stats = time.monotonic()
    connections = 0
    try:
        sticky = cursor
        for ev in client.events():
            if client.connections != connections:  # a (re)connect: verify what follows
                connections = client.connections
                guard.on_connect()
            verdict = _admit(guard, ev.data)
            if verdict == ResumeGuard.REREAD:
                log.warning(
                    "resume skipped upstream events; re-reading %d ms further back",
                    guard.rewind_ms,
                )
                client.reread()
            elif verdict != ResumeGuard.RESENT:
                if verdict == ResumeGuard.GAP:
                    log.error(
                        "upstream gap accepted: offsets missing even %d ms back", max_rewind_ms
                    )
                sticky = advance(sticky, ev.id)
                router.handle(ev.data, sticky, guard.header())
            client.last_event_id = sticky  # reconnects resume from the sticky cursor (D22)
            now = time.monotonic()
            if now - last_stats >= stats_every_s:
                log.info(
                    "stats %s",
                    json.dumps(
                        dict(router.counts)
                        | dict(guard.counts)
                        | {"reconnects": client.reconnects, "rewind_ms": guard.rewind_ms}
                    ),
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
        router.counts.update(guard.counts)
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
    except ContractNotRegistered as exc:
        log.error("%s", exc)
        raise SystemExit(3) from None
    if counts["undelivered_at_exit"] or any(k.startswith("delivery_failed") for k in counts):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
