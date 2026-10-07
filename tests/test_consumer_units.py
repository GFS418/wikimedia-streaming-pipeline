import gzip
import json

import boto3
import pytest
from moto import mock_aws

from wikistream.consumer import (
    OffsetGap,
    RangeBuffer,
    encode_file,
    last_written_end,
    object_key,
    parse_key,
    range_for,
)

PREFIX = "landing/recentchange/"


@pytest.mark.parametrize(
    ("offset", "expected"),
    [(0, (0, 4999)), (4999, (0, 4999)), (5000, (5000, 9999)), (12345, (10000, 14999))],
)
def test_range_for_is_aligned(offset, expected):
    assert range_for(offset, 5000) == expected


def test_key_round_trip_and_lexical_order_equals_numeric_order():
    keys = [object_key(PREFIX, 2, s, s + 4999) for s in (0, 5000, 10000, 995000, 1000000)]
    assert keys == sorted(keys)
    assert keys[0] == "landing/recentchange/p02/00000000000000000000-00000000000000004999.jsonl.gz"
    assert [parse_key(k) for k in keys][1] == (2, 5000, 9999)


@pytest.mark.parametrize(
    "key",
    [
        "landing/recentchange/p00/0-4999.jsonl.gz",  # not zero-padded
        "landing/_spike/recentchange/spike-000.jsonl",
        "landing/recentchange/p00/00000000000000000000-00000000000000004999.jsonl",
    ],
)
def test_parse_key_rejects_foreign_keys(key):
    assert parse_key(key) is None


def test_range_buffer_completes_exactly_at_range_end():
    b = RangeBuffer(0, 0, 2, 0)
    assert [b.add(o, f"r{o}") for o in range(3)] == [False, False, True]
    assert b.rows == ["r0", "r1", "r2"]


def test_range_buffer_partial_head_starts_mid_range():
    b = RangeBuffer(0, 0, 4, 3)  # Kafka low watermark was 3
    assert b.add(3, "a") is False and b.add(4, "b") is True


@pytest.mark.parametrize("bad", [1, 2, 0])
def test_range_buffer_rejects_gaps_and_repeats(bad):
    b = RangeBuffer(0, 0, 9, 0)
    b.add(0, "r0")
    if bad == 2:
        with pytest.raises(OffsetGap):
            b.add(2, "skip")
    elif bad == 0:
        with pytest.raises(OffsetGap):
            b.add(0, "repeat")
    else:
        assert b.add(1, "ok") is False


def test_encode_file_is_deterministic_and_round_trips():
    rows = [json.dumps({"i": i}) for i in range(100)]
    a, b = encode_file(rows, 5), encode_file(rows, 5)
    assert a == b
    assert a[4:8] == b"\x00\x00\x00\x00"  # gzip header mtime field is zero
    assert gzip.decompress(a).decode().splitlines() == rows


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-2")
        client.create_bucket(
            Bucket="unit-bucket", CreateBucketConfiguration={"LocationConstraint": "us-east-2"}
        )
        yield client


def test_last_written_end_none_when_empty(s3):
    assert last_written_end(s3, "unit-bucket", PREFIX, 0) is None


def test_last_written_end_takes_highest_range_of_that_partition_only(s3):
    for p, s in [(0, 0), (0, 5000), (0, 10000), (1, 995000)]:
        s3.put_object(Bucket="unit-bucket", Key=object_key(PREFIX, p, s, s + 4999), Body=b"x")
    s3.put_object(Bucket="unit-bucket", Key=f"{PREFIX}p00/not-a-range.txt", Body=b"x")
    assert last_written_end(s3, "unit-bucket", PREFIX, 0) == 14999
    assert last_written_end(s3, "unit-bucket", PREFIX, 1) == 999999
    assert last_written_end(s3, "unit-bucket", PREFIX, 2) is None


def test_last_written_end_paginates(s3):
    for s in range(0, 1100 * 10, 10):  # 1,100 keys > one 1,000-key page
        s3.put_object(Bucket="unit-bucket", Key=object_key(PREFIX, 0, s, s + 9), Body=b"")
    assert last_written_end(s3, "unit-bucket", PREFIX, 0) == 10999
