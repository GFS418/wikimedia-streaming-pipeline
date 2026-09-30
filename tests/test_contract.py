import copy
import io
import json
from pathlib import Path

import fastavro
import pytest

from wikistream.contract import (
    DeadLetter,
    Drop,
    UpstreamPosition,
    Valid,
    canonical_json,
    classify,
    load_schema,
    upstream_position,
)

FIXTURE = Path(__file__).parent / "fixtures" / "recentchange_sample.jsonl"
EVENTS = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
PARSED_SCHEMA = fastavro.parse_schema(load_schema())


def base_event():
    return copy.deepcopy(next(e for e in EVENTS if e["type"] == "edit"))


def raw(event):
    return json.dumps(event)


# --------------------------------------------------------------------------- tier 1: valid


def test_fixture_covers_the_edge_cases_we_saw_upstream():
    assert {e["type"] for e in EVENTS} == {"edit", "new", "log", "categorize"}
    assert any(isinstance(e.get("log_params"), list) for e in EVENTS)
    assert any(isinstance(e.get("log_params"), dict) for e in EVENTS)
    assert any("id" not in e for e in EVENTS)


@pytest.mark.parametrize("event", EVENTS, ids=lambda e: f"{e['type']}-{e['meta']['id'][:8]}")
def test_every_real_event_is_valid_and_round_trips_through_avro(event):
    out = classify(raw(event))
    assert isinstance(out, Valid), out
    assert out.key == event["meta"]["id"]
    assert out.extra_fields == [] and out.record["_extra_json"] is None

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, PARSED_SCHEMA, out.record)
    buf.seek(0)
    back = fastavro.schemaless_reader(buf, PARSED_SCHEMA)
    assert back == out.record


def test_log_params_list_and_dict_become_canonical_json_strings():
    for e in EVENTS:
        if "log_params" in e:
            rec = classify(raw(e)).record
            assert rec["log_params_json"] == canonical_json(e["log_params"])
            assert json.loads(rec["log_params_json"]) == e["log_params"]


def test_log_params_encoding_is_key_order_independent():
    a = base_event() | {"log_params": {"b": 1, "a": [2, {"d": 3, "c": 4}]}}
    b = base_event() | {"log_params": {"a": [2, {"c": 4, "d": 3}], "b": 1}}
    assert classify(raw(a)).record["log_params_json"] == classify(raw(b)).record["log_params_json"]


def test_dollar_schema_is_carried_as_upstream_schema():
    rec = classify(raw(base_event())).record
    assert rec["upstream_schema"] == "/mediawiki/recentchange/1.0.0"


def test_missing_optional_id_is_null_not_dead_letter():
    e = base_event()
    del e["id"]
    out = classify(raw(e))
    assert isinstance(out, Valid) and out.record["id"] is None


def test_classify_accepts_bytes():
    assert isinstance(classify(raw(base_event()).encode()), Valid)


# ----------------------------------------------------------- unknown fields -> _extra_json


def test_unknown_top_level_and_nested_fields_are_kept_in_extra_json():
    e = base_event()
    e["new_upstream_field"] = {"x": 1}
    e["meta"]["new_meta_field"] = "hello"
    e["length"]["delta"] = 5
    out = classify(raw(e))
    assert isinstance(out, Valid)
    assert out.extra_fields == ["length.delta", "meta.new_meta_field", "new_upstream_field"]
    assert json.loads(out.record["_extra_json"]) == {
        "length.delta": 5,
        "meta.new_meta_field": "hello",
        "new_upstream_field": {"x": 1},
    }


def test_upstream_field_named_extra_json_does_not_collide_with_ours():
    e = base_event() | {"_extra_json": 123}
    out = classify(raw(e))
    assert isinstance(out, Valid)
    assert json.loads(out.record["_extra_json"]) == {"_extra_json": 123}


# ------------------------------------------------------------------ tier 2: drop and count


def test_canary_events_are_dropped_with_their_upstream_position():
    e = base_event()
    e["meta"]["domain"] = "canary"
    m = e["meta"]
    assert classify(raw(e)) == Drop(
        "canary", UpstreamPosition(m["topic"], m["partition"], m["offset"])
    )


def test_canary_is_dropped_even_if_otherwise_invalid():
    e = {"meta": {"domain": "canary"}}
    assert classify(raw(e)) == Drop("canary", None)


# --------------------------------------------------------------------- tier 3: dead letter


def test_invalid_json():
    out = classify(b'{"meta": {"id": "x"')
    assert isinstance(out, DeadLetter) and out.error_class == "invalid_json" and out.key is None


def test_invalid_utf8():
    out = classify(b"\xff\xfe{}")
    assert isinstance(out, DeadLetter) and out.error_class == "invalid_json"


@pytest.mark.parametrize("payload", ["[]", "42", '"text"', "null"])
def test_top_level_must_be_an_object(payload):
    out = classify(payload)
    assert isinstance(out, DeadLetter) and out.error_class == "not_an_object"


@pytest.mark.parametrize("path", ["meta.id", "meta.dt", "type", "wiki", "timestamp", "bot", "meta"])
def test_missing_required_field(path):
    e = base_event()
    *parents, leaf = path.split(".")
    target = e
    for p in parents:
        target = target[p]
    del target[leaf]
    out = classify(raw(e))
    assert isinstance(out, DeadLetter), out
    assert out.error_class == "missing_required"
    assert out.detail.startswith(path)


def test_explicit_null_in_required_field():
    e = base_event() | {"wiki": None}
    out = classify(raw(e))
    assert isinstance(out, DeadLetter) and out.error_class == "missing_required"


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("timestamp", "1790479264", "timestamp: expected long, got str"),
        ("timestamp", True, "timestamp: expected long, got bool"),  # bool is an int in Python
        ("timestamp", 1790479264.5, "timestamp: expected long, got float"),
        ("bot", "false", "bot: expected boolean, got str"),
        ("bot", 0, "bot: expected boolean, got int"),
        ("namespace", 2**31, "namespace: expected int, got int"),  # overflows Avro int
        ("length", [1, 2], "length: expected object, got list"),
    ],
)
def test_type_mismatch(field, value, expected):
    e = base_event() | {field: value}
    out = classify(raw(e))
    assert isinstance(out, DeadLetter), out
    assert out.error_class == "type_mismatch"
    assert out.detail == expected


def test_dead_letter_keeps_meta_id_as_key_when_available():
    e = base_event() | {"timestamp": "bad"}
    out = classify(raw(e))
    assert out.key == e["meta"]["id"]


def test_dead_letter_key_is_none_when_meta_id_unusable():
    e = base_event()
    e["meta"]["id"] = 12345
    out = classify(raw(e))
    assert isinstance(out, DeadLetter) and out.key is None
    assert out.detail == "meta.id: expected string, got int"


# ------------------------------------------------------------- upstream position for audits


def test_dead_letter_carries_upstream_position_when_meta_is_intact():
    e = base_event() | {"timestamp": "bad"}
    m = e["meta"]
    out = classify(raw(e))
    assert out.upstream == UpstreamPosition(m["topic"], m["partition"], m["offset"])


def test_unparseable_dead_letter_has_no_position():
    assert classify(b"{not json").upstream is None


@pytest.mark.parametrize(
    "meta",
    [
        None,
        "text",
        {"topic": "t", "partition": 0},
        {"topic": "t", "partition": 0, "offset": "5"},
        {"topic": "t", "partition": True, "offset": 5},
        {"topic": 7, "partition": 0, "offset": 5},
    ],
)
def test_upstream_position_is_none_unless_well_typed(meta):
    assert upstream_position(meta) is None
