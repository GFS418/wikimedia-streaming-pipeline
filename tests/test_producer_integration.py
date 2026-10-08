"""End to end on the local stack: bootstrap -> Router -> Kafka -> read back.

Feeds the 39 real fixture events plus injected faults through the real producer path
(serializer, idempotent producer, both topics), then reads everything back and checks
every event landed in exactly one place, byte-for-byte where it matters, with the
headers the resume logic relies on.
"""

import json
from collections import Counter
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from wikistream.admin import bootstrap
from wikistream.config import DAY_MS, Settings, TopicSpec
from wikistream.contract import classify
from wikistream.cursor import merge_cursors
from wikistream.producer import (
    H_CURSOR,
    Router,
    make_value_serializer,
    producer_config,
    resume_cursor,
)

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.jsonl"
EQIAD = "eqiad.mediawiki.recentchange"


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
    sr.delete_subject(s.value_subject, permanent=False)
    sr.delete_subject(s.value_subject, permanent=True)


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

    # Resume (decision B): furthest cursor across the tails of all 4 partitions. The
    # last input (index len-1) was valid, so it is the furthest position overall.
    tails = Counter(m.partition() for m in main)
    assert sum(tails.values()) == n_valid
    assert resume_cursor(settings) == merge_cursors([cursor_at(1_000_000 + len(inputs) - 1)])


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
