"""Real Kafka + mocked S3 (moto): the consumer's exactly-once-per-offset claims.

Uses N=10 offsets per file so a few hundred events exercise many ranges.
"""

import gzip
import json
from pathlib import Path

import boto3
import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from moto import mock_aws

from wikistream.admin import bootstrap
from wikistream.config import DAY_MS, Settings, TopicSpec
from wikistream.consumer import LandingConsumer, parse_key
from wikistream.producer import Router, make_value_serializer, producer_config

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.jsonl"
N = 10
BUCKET = "it-landing"


def events(count: int, tag: str):
    base = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
    for i in range(count):
        e = base[i % len(base)]
        yield json.dumps(e | {"meta": e["meta"] | {"id": f"{tag}-{i:05d}"}})


@pytest.fixture
def env(stack, run_id):
    s = Settings(
        main_topic=TopicSpec(f"itc-{run_id}.recentchange", 3, DAY_MS),
        dlq_topic=TopicSpec(f"itc-{run_id}.recentchange.dlq", 1, DAY_MS),
        drops_topic=TopicSpec(f"itc-{run_id}.recentchange.drops", 1, DAY_MS),
        offsets_per_file=N,
        consumer_group=f"itc-{run_id}",
        landing_bucket=BUCKET,
    )
    bootstrap(s)
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "us-east-2"}
        )
        yield s, s3
    AdminClient({"bootstrap.servers": s.bootstrap_servers}).delete_topics(
        [t.name for t in s.all_topics]
    )
    sr = SchemaRegistryClient({"url": s.schema_registry_url})
    sr.delete_subject(s.value_subject, permanent=False)
    sr.delete_subject(s.value_subject, permanent=True)


def produce(s: Settings, payloads) -> None:
    p = Producer(producer_config(s))
    router = Router(p, s, make_value_serializer(s))
    for i, data in enumerate(payloads):
        router.handle(data, json.dumps([{"topic": "eqiad", "partition": 0, "timestamp": i}]))
    assert p.flush(30) == 0


def landed(s3) -> dict[str, bytes]:
    objs = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    return {o["Key"]: s3.get_object(Bucket=BUCKET, Key=o["Key"])["Body"].read() for o in objs}


def expected_ranges(s: Settings) -> set[tuple[int, int, int]]:
    from confluent_kafka import Consumer, TopicPartition

    c = Consumer({"bootstrap.servers": s.bootstrap_servers, "group.id": "probe"})
    out = set()
    for p in range(3):
        _, hi = c.get_watermark_offsets(TopicPartition(s.main_topic.name, p), timeout=10)
        out |= {(p, k * N, k * N + N - 1) for k in range(hi // N)}
    c.close()
    return out


def test_lands_every_full_range_once_with_exact_contents(env):
    s, s3 = env
    produce(s, events(200, "a"))
    counts = LandingConsumer(s, s3).run(max_seconds=8)

    files = landed(s3)
    ranges = {parse_key(k) for k in files}
    assert ranges == expected_ranges(s)  # every full range, nothing partial
    assert counts["files"] == len(files)
    assert counts["rows"] + counts["partial_rows_at_exit"] == 200
    # Shutdown revokes partitions too; those rows must not be counted a second time.
    assert counts["partial_rows_discarded_on_rebalance"] == 0

    for key, body in files.items():
        p, start, end = parse_key(key)
        rows = [json.loads(r) for r in gzip.decompress(body).decode().splitlines()]
        assert [r["kafka"]["offset"] for r in rows] == list(range(start, end + 1))
        assert all(r["kafka"]["partition"] == p for r in rows)
        assert all(r["kafka"]["key"] == r["value"]["meta"]["id"] for r in rows)


def test_resume_from_s3_writes_nothing_twice_and_continues(env):
    s, s3 = env
    produce(s, events(150, "b"))
    LandingConsumer(s, s3).run(max_seconds=8)
    first = landed(s3)

    # Restart with nothing new: position comes from S3, so no file is written again.
    again = LandingConsumer(s, s3).run(max_seconds=6)
    assert again["files"] == 0
    assert landed(s3) == first

    # New events: only the new ranges land; earlier files untouched.
    produce(s, events(150, "c"))
    LandingConsumer(s, s3).run(max_seconds=8)
    after = landed(s3)
    assert {k: after[k] for k in first} == first
    assert {parse_key(k) for k in after} == expected_ranges(s)


def test_interrupted_run_produces_byte_identical_files_to_an_uninterrupted_one(env):
    s, s3 = env
    produce(s, events(240, "d"))

    # Interrupted: stop after 2 files (partial buffers discarded), then resume twice.
    LandingConsumer(s, s3).run(max_files=2, max_seconds=8)
    LandingConsumer(s, s3).run(max_files=3, max_seconds=8)
    LandingConsumer(s, s3).run(max_seconds=8)
    interrupted = landed(s3)

    # Reference: same topic, fresh bucket, fresh group, one uninterrupted run.
    for key in interrupted:
        s3.delete_object(Bucket=BUCKET, Key=key)
    ref = Settings(**{**s.__dict__, "consumer_group": s.consumer_group + "-ref"})
    LandingConsumer(ref, s3).run(max_seconds=8)
    uninterrupted = landed(s3)

    assert interrupted == uninterrupted  # same keys, same bytes


def test_verifier_passes_a_clean_landing_and_catches_tampering(env):
    from wikistream.consumer import encode_file
    from wikistream.verify_landing import verify

    s, s3 = env
    produce(s, events(90, "e"))
    LandingConsumer(s, s3).run(max_seconds=8)

    clean = verify(s, s3)
    assert clean["verdict"] == "ok", clean["failures"]
    assert clean["files"] == clean["rederived_identical"] == len(landed(s3)) > 0

    # Tamper: same key, valid gzip, one row's content changed. This is exactly the
    # "different content under a processed path" case that Auto Loader would ignore.
    key = sorted(landed(s3))[0]
    rows = gzip.decompress(landed(s3)[key]).decode().splitlines()
    doctored = json.loads(rows[0])
    doctored["value"]["wiki"] = "tamperedwiki"
    rows[0] = json.dumps(doctored, sort_keys=True, separators=(",", ":"))
    s3.put_object(Bucket=BUCKET, Key=key, Body=encode_file(rows, s.gzip_level))

    bad = verify(s, s3)
    assert bad["verdict"] == "FAILED"
    assert any("re-derived bytes differ" in f and key in f for f in bad["failures"])
