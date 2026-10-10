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


# ------------------------------------------------- slowest-partition resume (decision D29)


def test_slowest_takes_the_earliest_position_per_upstream_topic():
    from wikistream.cursor import slowest

    tails = [  # one cursor per partition of our topic, as at the 2026-10-10 crash drill
        cur({"topic": EQIAD, "partition": 0, "timestamp": 820}),
        cur({"topic": EQIAD, "partition": 0, "timestamp": 983}),
        cur({"topic": EQIAD, "partition": 0, "timestamp": 798}),
    ]
    assert json.loads(slowest(tails)) == [{"topic": EQIAD, "partition": 0, "timestamp": 798}]


def test_slowest_ignores_entries_without_a_timestamp_but_keeps_unknown_topics():
    from wikistream.cursor import slowest

    tails = [
        cur(
            {"topic": EQIAD, "partition": 0, "timestamp": 5},
            {"topic": CODFW, "partition": 0, "offset": -1},
        ),
        cur(
            {"topic": EQIAD, "partition": 0, "timestamp": 9},
            {"topic": CODFW, "partition": 0, "timestamp": 7},
        ),
    ]
    got = {e["topic"]: e for e in json.loads(slowest(tails))}
    assert got[EQIAD]["timestamp"] == 5 and got[CODFW]["timestamp"] == 7
    only_reset = [cur({"topic": CODFW, "partition": 0, "offset": -1})]
    assert json.loads(slowest(only_reset)) == [{"topic": CODFW, "partition": 0, "offset": -1}]
    assert slowest([None]) is None


# ------------------------------------------------------------------ D31: resume, then verify


def test_rewind_moves_timestamps_earlier_and_leaves_offset_entries():
    from wikistream.cursor import rewind

    c = cur(
        {"topic": EQIAD, "partition": 0, "timestamp": 1_000},
        {"topic": CODFW, "partition": 0, "offset": -1},
    )
    got = {e["topic"]: e for e in json.loads(rewind(c, 250))}
    assert got[EQIAD]["timestamp"] == 750
    assert got[CODFW] == {"topic": CODFW, "partition": 0, "offset": -1}
    assert rewind(c, 0) == c and rewind(None, 250) is None


def test_offsets_header_round_trips():
    from wikistream.cursor import offsets_header, parse_offsets

    last = {(EQIAD, 0): 6598017958, (CODFW, 0): 2086018072}
    assert parse_offsets(offsets_header(last)) == last


def test_floors_take_the_lowest_tail_and_know_nothing_without_every_header():
    from wikistream.cursor import floors, offsets_header

    tails = [
        offsets_header({(EQIAD, 0): 120, (CODFW, 0): 7}),
        offsets_header({(EQIAD, 0): 117, (CODFW, 0): 9}),
        offsets_header({(EQIAD, 0): 119}),  # written before codfw was ever seen
    ]
    assert floors(tails) == {(EQIAD, 0): 117}  # codfw: unknown in one tail, so unknown
    assert floors([tails[0], None]) == {}  # a tail from before the header: nothing known
    assert floors([]) == {}


def test_guard_ignores_resends_and_verifies_the_next_offset_after_a_connect():
    from wikistream.cursor import ResumeGuard

    g = ResumeGuard(base_ms=1_000, max_ms=8_000, floor={(EQIAD, 0): 100})
    g.on_connect()
    assert g.admit((EQIAD, 0), 99) == g.RESENT  # the rewound resume re-sends
    assert g.admit((EQIAD, 0), 100) == g.RESENT
    assert g.admit((EQIAD, 0), 101) == g.NEW  # exactly the next offset: verified
    assert g.admit((CODFW, 0), 5) == g.NEW  # never seen: nothing to verify against
    assert g.counts == {"resume.resent_ignored": 2, "resume.verified": 1}
    assert json.loads(g.header()) == [
        {"topic": CODFW, "partition": 0, "offset": 5},
        {"topic": EQIAD, "partition": 0, "offset": 101},
    ]


def test_guard_rereads_further_back_on_a_skip_and_resets_once_verified():
    from wikistream.cursor import ResumeGuard

    g = ResumeGuard(base_ms=1_000, max_ms=8_000, floor={(EQIAD, 0): 100})
    for expected_rewind in (2_000, 4_000):
        g.on_connect()
        assert g.admit((EQIAD, 0), 103) == g.REREAD  # 101 and 102 were skipped
        assert g.rewind_ms == expected_rewind and g.last[(EQIAD, 0)] == 100  # not admitted
    g.on_connect()
    assert g.admit((EQIAD, 0), 101) == g.NEW
    assert g.rewind_ms == 1_000  # back to base after a verified resume
    assert g.counts["resume.reread"] == 2 and g.counts["resume.verified"] == 1


def test_guard_accepts_a_gap_at_the_cap_and_counts_mid_connection_gaps():
    from wikistream.cursor import ResumeGuard

    g = ResumeGuard(base_ms=1_000, max_ms=4_000, floor={(EQIAD, 0): 100})
    verdicts = []
    for _ in range(3):
        g.on_connect()
        verdicts.append(g.admit((EQIAD, 0), 102))
    assert verdicts == [g.REREAD, g.REREAD, g.GAP]  # 2 s, 4 s (the cap), then accepted
    assert g.last[(EQIAD, 0)] == 102 and g.rewind_ms == 1_000
    assert g.admit((EQIAD, 0), 105) == g.NEW  # within a connection: counted, not re-read
    assert g.counts["upstream.offset_gap"] == 2 and g.counts["resume.gap_accepted"] == 1


def test_guard_with_no_margin_accepts_every_skip_as_before_d31():
    from wikistream.cursor import ResumeGuard

    g = ResumeGuard(base_ms=0, max_ms=0, floor={(EQIAD, 0): 100})
    g.on_connect()
    assert g.admit((EQIAD, 0), 102) == g.GAP
