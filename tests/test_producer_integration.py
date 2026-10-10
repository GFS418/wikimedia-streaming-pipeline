"""End to end on the local stack: bootstrap -> Router -> Kafka -> read back.

Feeds the 39 real fixture events plus injected faults through the real producer path
(serializer, idempotent producer, both topics), then reads everything back and checks
every event landed in exactly one place, byte-for-byte where it matters, with the
headers the resume logic relies on.
"""

import json
import zlib
from collections import Counter
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.admin import bootstrap
from wikistream.config import DAY_MS, Settings, TopicSpec
from wikistream.contract import classify
from wikistream.cursor import merge_cursors
from wikistream.producer import (
    H_CURSOR,
    H_OFFSETS,
    Router,
    make_value_serializer,
    producer_config,
    resume_cursor,
)

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.jsonl"
EQIAD = "eqiad.mediawiki.recentchange"


def partition_of(data: str, n: int = 3) -> int:
    """Where our producer puts an event: librdkafka's default partitioner, CRC32 of the key
    (verified on 6,000 live messages; Java's murmur2 is NOT what we use)."""
    return zlib.crc32(json.loads(data)["meta"]["id"].encode()) % n


def slowest_tail(indexed_main_inputs) -> int:
    """Index of the input on the slowest partition's tail: per partition the last input
    routed to it, then the earliest of those (decision D29, computed independently)."""
    tails = {}
    for i, data in indexed_main_inputs:
        tails[partition_of(data)] = i
    return min(tails.values())


def cursor_at(ts: int) -> str:
    return json.dumps([{"topic": EQIAD, "partition": 0, "timestamp": ts}])


@pytest.fixture
def settings(stack, run_id):
    s = Settings(
        main_topic=TopicSpec(f"it-{run_id}.recentchange", 3, DAY_MS),
        dlq_topic=TopicSpec(f"it-{run_id}.recentchange.dlq", 1, DAY_MS),
        drops_topic=TopicSpec(f"it-{run_id}.recentchange.drops", 1, DAY_MS),
    )
    yield s
    AdminClient({"bootstrap.servers": s.bootstrap_servers}).delete_topics(
        [t.name for t in s.all_topics]
    )
    sr = SchemaRegistryClient({"url": s.schema_registry_url})
    try:
        sr.delete_subject(s.value_subject, permanent=False)
        sr.delete_subject(s.value_subject, permanent=True)
    except SchemaRegistryError as exc:  # a test may never have registered it
        if exc.http_status_code != 404:
            raise


def read_all(settings: Settings, topic: str, partitions: int) -> list:
    c = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"read-{topic}",
            "enable.auto.commit": False,
        }
    )
    ends = {
        p: c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1]
        for p in range(partitions)
    }
    c.assign([TopicPartition(topic, p, 0) for p in range(partitions)])
    msgs, want = [], sum(ends.values())
    while len(msgs) < want:
        m = c.poll(10)
        assert m is not None, f"timed out after {len(msgs)}/{want} messages"
        assert m.error() is None, m.error()
        msgs.append(m)
    c.close()
    return msgs


def test_end_to_end_routing_and_resume(settings):
    result = bootstrap(settings)
    assert result["compatibility"] == "FULL_TRANSITIVE"
    again = bootstrap(settings)  # idempotent: nothing recreated, same schema id
    assert all(again[t.name] == "exists" for t in settings.all_topics)
    assert again["schema_id"] == result["schema_id"]

    real = FIXTURE.read_text().splitlines()
    base = json.loads(real[0])
    faults = {
        "invalid_json": '{"meta": {"id": "broken"',
        "missing_required": json.dumps({k: v for k, v in base.items() if k != "wiki"}),
        "type_mismatch": json.dumps(base | {"timestamp": "not-a-number"}),
    }
    canary = json.dumps(base | {"meta": base["meta"] | {"domain": "canary"}})
    with_extra = json.dumps(
        base | {"meta": base["meta"] | {"id": "extra-0001"}, "brand_new_field": [1, 2]}
    )
    inputs = real + list(faults.values()) + [canary, with_extra]

    producer = Producer(producer_config(settings))
    router = Router(producer, settings, make_value_serializer(settings))
    for i, data in enumerate(inputs):
        router.handle(data, cursor_at(1_000_000 + i))
    assert producer.flush(30) == 0

    n_valid = len(real) + 1
    assert router.counts["main"] == router.counts["delivered.main"] == n_valid
    assert router.counts["dlq"] == router.counts["delivered.dlq"] == len(faults)
    assert router.counts["dropped.canary"] == router.counts["delivered.drops"] == 1
    assert router.counts["extra_field.brand_new_field"] == 1
    assert router.counts["received"] == len(inputs)

    # Main topic: every valid event exactly once, keyed by meta.id, decoding to exactly
    # what the contract produced.
    deser = AvroDeserializer(SchemaRegistryClient({"url": settings.schema_registry_url}))
    ctx = SerializationContext(settings.main_topic.name, MessageField.VALUE)
    main = read_all(settings, settings.main_topic.name, 3)
    expected = {c.key: c.record for c in (classify(d) for d in real + [with_extra])}
    got = {m.key().decode(): deser(m.value(), ctx) for m in main}
    assert len(main) == len(got) == n_valid  # no duplicates
    assert got == expected
    assert json.loads(got["extra-0001"]["_extra_json"]) == {"brand_new_field": [1, 2]}
    assert len({m.partition() for m in main}) == 3  # meta.id keys spread over partitions
    assert all(dict(m.headers())[H_CURSOR] for m in main)

    # DLQ: raw bytes preserved, reason in headers.
    dlq = read_all(settings, settings.dlq_topic.name, 1)
    by_class = {dict(m.headers())["error_class"].decode(): m for m in dlq}
    assert set(by_class) == set(faults)
    for cls, data in faults.items():
        assert by_class[cls].value() == data.encode()
    assert by_class["type_mismatch"].key().decode() == base["meta"]["id"]
    assert by_class["invalid_json"].key() is None
    # Dead letters with intact meta carry their upstream position; broken JSON cannot.
    tm = {k: v.decode() for k, v in by_class["type_mismatch"].headers()}
    assert (tm["upstream_topic"], int(tm["upstream_offset"])) == (
        base["meta"]["topic"],
        base["meta"]["offset"],
    )
    assert "upstream_offset" not in dict(by_class["invalid_json"].headers())

    # Drops ledger (decision D8): position and reason only, never the event body.
    (drop,) = read_all(settings, settings.drops_topic.name, 1)
    assert json.loads(drop.value()) == {
        "reason": "canary",
        "upstream_topic": base["meta"]["topic"],
        "upstream_partition": base["meta"]["partition"],
        "upstream_offset": base["meta"]["offset"],
    }

    # Resume (decisions B, D29): the slowest of the main partitions' tails. Dead letters and
    # drops are written synchronously and take no part.
    tails = Counter(m.partition() for m in main)
    assert sum(tails.values()) == n_valid
    main_inputs = [(i, d) for i, d in enumerate(inputs) if i < len(real) or i == len(inputs) - 1]
    expected = merge_cursors([cursor_at(1_000_000 + slowest_tail(main_inputs))])
    assert resume_cursor(settings) == expected


def test_run_refuses_a_stale_cursor_before_connecting_anywhere(settings, monkeypatch):
    import time as _time

    from wikistream import producer as producer_mod

    bootstrap(settings)
    week_ago_ms = int(_time.time() * 1000) - 8 * 24 * 3600 * 1000
    p = Producer(producer_config(settings))
    router = Router(p, settings, make_value_serializer(settings))
    router.handle(FIXTURE.read_text().splitlines()[0], cursor_at(week_ago_ms))
    assert p.flush(30) == 0

    def no_network(*args, **kwargs):  # the refusal must come before any upstream connection
        raise AssertionError("SSEClient was constructed despite a stale cursor")

    monkeypatch.setattr(producer_mod, "SSEClient", no_network)
    with pytest.raises(producer_mod.StaleCursor) as exc:
        producer_mod.run(settings, max_seconds=1)
    assert exc.value.age_s > 7 * 24 * 3600


CODFW = "codfw.mediawiki.recentchange"


def two_dc_cursor(eqiad_ts: int, codfw):
    codfw_entry = {"topic": CODFW, "partition": 0}
    codfw_entry |= {"timestamp": codfw} if codfw is not None else {"offset": -1}
    return json.dumps([{"topic": EQIAD, "partition": 0, "timestamp": eqiad_ts}, codfw_entry])


def test_resume_scans_back_past_reset_tails_for_a_quiet_topic(settings):
    # Cursors written before D22: one early message knows codfw's position, every later
    # one (after a reconnect) says -1. Resume must recover codfw's real position.
    bootstrap(settings)
    real = FIXTURE.read_text().splitlines()
    p = Producer(producer_config(settings))
    router = Router(p, settings, make_value_serializer(settings))
    router.handle(real[0], two_dc_cursor(1_000, 777))
    for i, data in enumerate(real[1:20], start=1):
        router.handle(data, two_dc_cursor(1_000 + i, None))
    # As on 2026-10-09: the drops-ledger tail (a canary) offers an OLDER codfw timestamp.
    base = json.loads(real[0])
    canary = json.dumps(base | {"meta": base["meta"] | {"domain": "canary", "id": "c-1"}})
    router.handle(canary, two_dc_cursor(1_020, 700))
    assert p.flush(30) == 0
    positions = {e["topic"]: e for e in json.loads(resume_cursor(settings))}
    assert positions[CODFW] == {"topic": CODFW, "partition": 0, "timestamp": 777}  # not 700
    main_inputs = list(enumerate(real[:20]))
    assert positions[EQIAD]["timestamp"] == 1_000 + slowest_tail(main_inputs)


def test_running_producer_stamps_and_reconnects_with_the_sticky_cursor(settings, monkeypatch):
    from wikistream import producer as producer_mod
    from wikistream.sse import SSEEvent

    bootstrap(settings)
    real = FIXTURE.read_text().splitlines()
    ids = [two_dc_cursor(10, None), two_dc_cursor(20, 555), two_dc_cursor(30, None)]
    clients = []

    class FakeClient:  # stands in for SSEClient: no network
        def __init__(self, url, user_agent, last_event_id=None, stop=None, resume_from=None):
            self.last_event_id, self.reconnects, self.connections = last_event_id, 0, 1
            clients.append(self)

        def events(self):
            for data, sid in zip(real[:3], ids, strict=True):
                yield SSEEvent(data=data, id=sid)

    monkeypatch.setattr(producer_mod, "SSEClient", FakeClient)
    producer_mod.run(settings, max_events=3, from_now=True)

    # Select the third event by key (meta.id): timestamps can tie across partitions.
    third_id = json.loads(real[2])["meta"]["id"]
    by_key = {
        m.key().decode(): dict(m.headers())[H_CURSOR].decode()
        for m in read_all(settings, settings.main_topic.name, 3)
    }
    last = {e["topic"]: e for e in json.loads(by_key[third_id])}
    assert last[CODFW] == {"topic": CODFW, "partition": 0, "timestamp": 555}  # not -1
    assert {e["topic"]: e for e in json.loads(clients[0].last_event_id)}[CODFW]["timestamp"] == 555


def test_producer_refuses_to_start_without_a_registered_contract(settings, monkeypatch):
    """Phase 4 negative drill, in miniature: no registered schema -> no upstream connection."""
    from wikistream import producer as producer_mod

    def no_network(*args, **kwargs):
        raise AssertionError("SSEClient was constructed without a registered contract")

    monkeypatch.setattr(producer_mod, "SSEClient", no_network)
    with pytest.raises(producer_mod.ContractNotRegistered):
        producer_mod.run(settings, max_seconds=1, from_now=True)
    bootstrap(settings)  # registered through the compatibility check: now it may start
    assert producer_mod.ensure_contract_registered(settings) > 0


def test_resume_starts_from_the_slowest_partition_so_lost_tails_are_refetched(settings):
    """The 2026-10-10 producer crash drill: partitions stopped at upstream 820, 983 and 798.
    Resuming from the furthest (983) skipped 104 events lost in p0's and p2's unflushed
    tails; the slowest (798) re-fetches them."""
    bootstrap(settings)
    base = json.loads(FIXTURE.read_text().splitlines()[0])

    def event_on(p: int, n: int) -> str:
        i = 0
        while True:
            e = json.dumps(base | {"meta": base["meta"] | {"id": f"d29-{p}-{n}-{i}"}})
            if partition_of(e) == p:
                return e
            i += 1

    producer = Producer(producer_config(settings))
    router = Router(producer, settings, make_value_serializer(settings))
    for p, last in [(0, 820), (1, 983), (2, 798)]:
        for n, ts in enumerate([last - 10, last]):
            router.handle(event_on(p, n), cursor_at(ts))
    assert producer.flush(30) == 0
    positions = {e["topic"]: e for e in json.loads(resume_cursor(settings))}
    assert positions[EQIAD]["timestamp"] == 798


# ------------------------------------------------------- D31: resume by timestamp, verify by offset

T0 = 1_791_600_000_000  # 2026-10-10, epoch ms
SAME_MS_AFTER, REGRESSED_AFTER = 10, 25  # cut points: the events right after them are at risk


def risky_stream(missing=(), n=40):
    """Dense eqiad offsets 5000.. stamped 10 ms apart, plus two codfw events, in delivery order.
    The event after 5010 shares 5010's millisecond (as both overnight losses did); the event
    after 5025 is stamped 50 ms before it (out of order, as 0.76% of live events are)."""
    from fake_eventstreams import make_event

    base = json.loads(FIXTURE.read_text().splitlines()[0])
    events, codfw = [], 701  # upstream offsets are dense in every partition
    for i in range(n):
        if i in (1, 30):
            events.append(make_event(base, CODFW, codfw, T0 + 10 * i - 5))
            codfw += 1
        ts = T0 + 10 * i
        if i == SAME_MS_AFTER + 1:
            ts = T0 + 10 * SAME_MS_AFTER
        elif i == REGRESSED_AFTER + 1:
            ts = T0 + 10 * REGRESSED_AFTER - 50
        if 5000 + i not in missing:
            events.append(make_event(base, EQIAD, 5000 + i, ts))
    return events


CUTS = {(EQIAD, 5000 + SAME_MS_AFTER), (EQIAD, 5000 + REGRESSED_AFTER)}


def run_on_fake(settings, fake, timeout_s: float = 60, **kw):
    """The real producer against the fake; a broken resume must fail the test, not hang it
    (lost events mean max_events is never reached)."""
    import threading
    from dataclasses import replace

    from wikistream import producer as producer_mod

    stop = threading.Event()
    deadline = threading.Timer(timeout_s, stop.set)
    deadline.start()
    try:
        return producer_mod.run(replace(settings, stream_url=fake.url), stop=stop, **kw)
    finally:
        deadline.cancel()


def upstream_in_kafka(settings) -> Counter:
    deser = AvroDeserializer(SchemaRegistryClient({"url": settings.schema_registry_url}))
    ctx = SerializationContext(settings.main_topic.name, MessageField.VALUE)
    metas = (deser(m.value(), ctx)["meta"] for m in read_all(settings, settings.main_topic.name, 3))
    return Counter((meta["topic"], meta["offset"]) for meta in metas)


def test_resuming_from_the_id_as_given_loses_same_ms_and_out_of_order_events(settings):
    """The pre-D31 behaviour, reproduced: no rewind, and a skip is merely accepted."""
    from fake_eventstreams import FakeEventStreams

    bootstrap(settings)
    events = risky_stream()
    with FakeEventStreams(events, cut_at=CUTS) as fake:
        counts = run_on_fake(
            settings, fake, from_now=True, max_events=len(events) - 2, rewind_ms=0, max_rewind_ms=0
        )
    got = upstream_in_kafka(settings)
    lost = {(e["topic"], e["offset"]) for e in events} - set(got)
    assert lost == {(EQIAD, 5000 + SAME_MS_AFTER + 1), (EQIAD, 5000 + REGRESSED_AFTER + 1)}
    assert counts["resume.gap_accepted"] == 2


@pytest.mark.parametrize("rewind_ms", [1, 1_000])
def test_resuming_early_loses_nothing_and_resends_are_ignored(settings, rewind_ms):
    """1 ms is already enough (the running maximum's event is at or before the last one
    received); the 1 s default adds margin. Either way: every offset exactly once."""
    from fake_eventstreams import FakeEventStreams

    bootstrap(settings)
    events = risky_stream()
    with FakeEventStreams(events, cut_at=CUTS) as fake:
        counts = run_on_fake(
            settings, fake, from_now=True, max_events=len(events), rewind_ms=rewind_ms
        )
    got = upstream_in_kafka(settings)
    assert set(got) == {(e["topic"], e["offset"]) for e in events}
    assert max(got.values()) == 1  # nothing re-sent reached Kafka
    # Verified: eqiad after each cut, and quiet codfw once its next event arrived.
    assert counts["resume.verified"] == 3 and counts["resume.resent_ignored"] >= 2
    assert counts["resume.reread"] == counts["resume.gap_accepted"] == 0


def test_verification_heals_a_resume_rule_change(settings):
    """If the service ever stamped ids further past the event (here +8 ms instead of +1), a
    1 ms rewind would skip again; the offset check catches it and re-reads further back."""
    from fake_eventstreams import FakeEventStreams

    bootstrap(settings)
    events = risky_stream()
    with FakeEventStreams(events, cut_at=CUTS, id_offset_ms=8) as fake:
        counts = run_on_fake(
            settings, fake, from_now=True, max_events=len(events), rewind_ms=1, max_rewind_ms=64
        )
    got = upstream_in_kafka(settings)
    assert set(got) == {(e["topic"], e["offset"]) for e in events} and max(got.values()) == 1
    assert counts["resume.reread"] == 6  # per cut: 1 -> 2 -> 4 -> 8 ms, then verified
    assert counts["resume.verified"] == 3 and counts["resume.gap_accepted"] == 0


def test_a_gap_that_is_really_upstream_is_accepted_at_the_cap(settings):
    from fake_eventstreams import FakeEventStreams

    bootstrap(settings)
    gone = 5000 + SAME_MS_AFTER + 1
    events = risky_stream(missing={gone})
    with FakeEventStreams(events, cut_at=CUTS) as fake:
        counts = run_on_fake(
            settings, fake, from_now=True, max_events=len(events), rewind_ms=1, max_rewind_ms=8
        )
    got = upstream_in_kafka(settings)
    assert set(got) == {(e["topic"], e["offset"]) for e in events} and max(got.values()) == 1
    assert counts["resume.reread"] == 3 and counts["resume.gap_accepted"] == 1  # 2, 4, 8 ms


def test_restart_resumes_from_kafka_ignoring_what_is_already_there(settings):
    """Clean stop, then restart from the partition tails: the lowest `upstream_offsets` header
    is what is surely in Kafka. Only events between it and the newest one handled are sent
    again (each partition's tail was written at a different moment)."""
    from fake_eventstreams import FakeEventStreams

    from wikistream.producer import resume_floors

    bootstrap(settings)
    events = risky_stream()
    with FakeEventStreams(events, cut_at=CUTS) as fake:
        run_on_fake(settings, fake, from_now=True, max_events=20)
    first = upstream_in_kafka(settings)
    newest = max(o for t, o in first if t == EQIAD)
    floor = resume_floors(settings)[(EQIAD, 0)]
    assert resume_floors(settings)[(CODFW, 0)] == 701 and floor <= newest

    rest = sum(1 for e in events if e["topic"] == EQIAD and e["offset"] > floor) + 1  # codfw 702
    with FakeEventStreams(events) as fake:
        counts = run_on_fake(settings, fake, catch_up=True, max_events=rest)
    got = upstream_in_kafka(settings)
    assert set(got) == {(e["topic"], e["offset"]) for e in events}
    dups = {k for k, n in got.items() if n > 1}
    assert dups == {(EQIAD, o) for o in range(floor + 1, newest + 1)}
    assert counts["resume.verified"] >= 1 and counts["resume.gap_accepted"] == 0


def test_backfill_repairs_skipped_events_once_without_disturbing_resume(settings):
    from dataclasses import replace
    from datetime import UTC, datetime, timedelta

    from fake_eventstreams import FakeEventStreams

    from wikistream.backfill import H_BACKFILL, Target, backfill

    bootstrap(settings)
    events = risky_stream()
    with FakeEventStreams(events, cut_at=CUTS) as fake:  # the pre-D31 resume loses two
        run_on_fake(
            settings, fake, from_now=True, max_events=len(events) - 2, rewind_ms=0, max_rewind_ms=0
        )
    every = {(e["topic"], e["offset"]): e for e in events}
    lost = sorted(set(every) - set(upstream_in_kafka(settings)))
    assert len(lost) == 2
    tails = {}
    for m in read_all(settings, settings.main_topic.name, 3):
        tails[m.partition()] = dict(m.headers())  # read in offset order: the last one wins
    targets = [
        Target(t, 0, o, datetime.fromtimestamp(every[(t, o)]["ts"] / 1000, UTC)) for t, o in lost
    ]
    with FakeEventStreams(events) as fake:
        s = replace(settings, stream_url=fake.url)
        first = backfill(s, targets, timedelta(seconds=1), label="test repair")
        second = backfill(s, targets, timedelta(seconds=1), label="test repair")

    got = upstream_in_kafka(settings)
    assert set(got) == set(every) and max(got.values()) == 1  # repaired, and only once
    assert set(second["targets"].values()) == {"already in Kafka: skipped"}
    repaired = [
        m
        for m in read_all(settings, settings.main_topic.name, 3)
        if H_BACKFILL in dict(m.headers())
    ]
    assert len(repaired) == 2 == first["counts"]["main"]
    for m in repaired:
        h = dict(m.headers())
        assert h[H_BACKFILL] == b"test repair"
        before = tails[m.partition()]  # resume headers copied from the tail it joined
        assert (h[H_CURSOR], h.get(H_OFFSETS)) == (before[H_CURSOR], before.get(H_OFFSETS))
