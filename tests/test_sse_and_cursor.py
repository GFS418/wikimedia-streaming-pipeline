import json

import pytest

from wikistream.cursor import merge_cursors
from wikistream.sse import SSEEvent, SSEParser

# --------------------------------------------------------------------------------- parser


def parse(text: str) -> list[SSEEvent]:
    return list(SSEParser().parse(text.split("\n")))


def test_basic_event_with_id():
    assert parse('id: [1]\ndata: {"a":1}\n\n') == [SSEEvent(data='{"a":1}', id="[1]")]


def test_multiline_data_is_joined_with_newlines():
    assert parse("data: line1\ndata: line2\n\n")[0].data == "line1\nline2"


def test_only_one_leading_space_is_stripped():
    assert parse("data:  two spaces\n\n")[0].data == " two spaces"
    assert parse("data:nospace\n\n")[0].data == "nospace"


def test_comments_and_unknown_fields_are_ignored():
    evs = parse(": keepalive\nretry: 5000\nfoo: bar\ndata: x\n\n")
    assert evs == [SSEEvent(data="x", id=None)]


def test_last_event_id_persists_across_events_until_changed():
    evs = parse("id: A\ndata: 1\n\ndata: 2\n\nid: B\ndata: 3\n\n")
    assert [(e.data, e.id) for e in evs] == [("1", "A"), ("2", "A"), ("3", "B")]


def test_id_containing_null_is_ignored():
    evs = parse("id: A\ndata: 1\n\nid: bad\0id\ndata: 2\n\n")
    assert [e.id for e in evs] == ["A", "A"]


def test_blank_lines_without_data_dispatch_nothing():
    assert parse("\n\nid: A\n\n\n") == []


def test_event_type_resets_after_dispatch():
    evs = parse("event: message\ndata: 1\n\nevent: other\ndata: 2\n\ndata: 3\n\n")
    assert [e.event for e in evs] == ["message", "other", "message"]


def test_incomplete_trailing_event_is_not_emitted():
    assert parse("data: 1\n\ndata: partial") == [SSEEvent(data="1", id=None)]


def test_field_without_colon_has_empty_value():
    assert parse("data\n\n") == [SSEEvent(data="", id=None)]


# --------------------------------------------------------------------------------- cursor

EQIAD, CODFW = "eqiad.mediawiki.recentchange", "codfw.mediawiki.recentchange"


def cur(*entries):
    return json.dumps(list(entries))


def test_merge_takes_the_furthest_timestamp_per_upstream_topic():
    a = cur(
        {"topic": EQIAD, "partition": 0, "timestamp": 100},
        {"topic": CODFW, "partition": 0, "timestamp": 900},
    )
    b = cur(
        {"topic": EQIAD, "partition": 0, "timestamp": 200},
        {"topic": CODFW, "partition": 0, "timestamp": 800},
    )
    merged = json.loads(merge_cursors([a, b]))
    assert {e["topic"]: e["timestamp"] for e in merged} == {EQIAD: 200, CODFW: 900}


def test_timestamp_outranks_offset_minus_one_as_observed_upstream():
    a = cur(
        {"topic": EQIAD, "partition": 0, "timestamp": 100},
        {"topic": CODFW, "partition": 0, "offset": -1},
    )
    b = cur({"topic": CODFW, "partition": 0, "timestamp": 5})
    merged = {e["topic"]: e for e in json.loads(merge_cursors([a, b]))}
    assert merged[CODFW] == {"topic": CODFW, "partition": 0, "timestamp": 5}


def test_merge_is_order_independent_and_canonical():
    a = cur({"topic": EQIAD, "partition": 0, "timestamp": 1})
    b = cur({"topic": CODFW, "partition": 0, "timestamp": 2})
    assert merge_cursors([a, b]) == merge_cursors([b, a])


def test_merge_of_nothing_is_none():
    assert merge_cursors([]) is None
    assert merge_cursors([None, ""]) is None


def test_merge_rejects_malformed_cursor():
    with pytest.raises(ValueError):
        merge_cursors(['{"not": "a list"}'])


# ------------------------------------------------------------- sticky cursor (decision D22)


def test_advance_keeps_a_quiet_topics_position_through_a_reconnect_reset():
    # The exact sequence observed 2026-10-08: codfw at 13:21:23.667, then -1 after
    # reconnect #5 (13:33:45.162) although no codfw event had been consumed since.
    from wikistream.cursor import advance

    before = cur(
        {"topic": EQIAD, "partition": 0, "timestamp": 1791465684675},
        {"topic": CODFW, "partition": 0, "timestamp": 1791465683667},
    )
    after_reconnect = cur(
        {"topic": EQIAD, "partition": 0, "timestamp": 1791466425162},
        {"topic": CODFW, "partition": 0, "offset": -1},
    )
    sticky = advance(advance(None, before), after_reconnect)
    positions = {e["topic"]: e for e in json.loads(sticky)}
    assert positions[CODFW] == {"topic": CODFW, "partition": 0, "timestamp": 1791465683667}
    assert positions[EQIAD]["timestamp"] == 1791466425162


def test_advance_from_nothing_is_the_server_id():
    from wikistream.cursor import advance

    sid = cur({"topic": EQIAD, "partition": 0, "timestamp": 5})
    assert json.loads(advance(None, sid)) == json.loads(sid)


def test_without_timestamp_lists_reset_topics():
    from wikistream.cursor import without_timestamp

    c = cur(
        {"topic": EQIAD, "partition": 0, "timestamp": 5},
        {"topic": CODFW, "partition": 0, "offset": -1},
    )
    assert without_timestamp(c) == {(CODFW, 0)}
    assert without_timestamp(None) == set()
