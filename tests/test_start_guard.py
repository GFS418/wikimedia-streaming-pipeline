"""Decision D13: never resume silently from a stale upstream cursor."""

import json

import pytest

from wikistream.cursor import cursor_newest_ms
from wikistream.producer import StaleCursor, choose_start

EQ, CO = "eqiad.mediawiki.recentchange", "codfw.mediawiki.recentchange"
NOW = 1_791_414_789_811  # 2026-10-07T23:13:09.811Z
MIN = 60_000


def cursor(*entries):
    return json.dumps(list(entries))


def fresh(age_ms=5 * MIN):
    return cursor(
        {"topic": CO, "partition": 0, "offset": -1},
        {"topic": EQ, "partition": 0, "timestamp": NOW - age_ms},
    )


def test_newest_timestamp_ignores_offset_only_entries():
    assert cursor_newest_ms(fresh()) == NOW - 5 * MIN
    assert cursor_newest_ms(cursor({"topic": CO, "partition": 0, "offset": -1})) is None


def test_newest_timestamp_is_the_max_across_upstream_topics():
    c = cursor(
        {"topic": CO, "partition": 0, "timestamp": NOW - 7 * 24 * 60 * MIN},  # quiet codfw
        {"topic": EQ, "partition": 0, "timestamp": NOW - MIN},
    )
    assert cursor_newest_ms(c) == NOW - MIN
    assert choose_start(c, NOW) == c  # a quiet datacenter is not downtime


def test_fresh_cursor_resumes():
    assert choose_start(fresh(), NOW) == fresh()


def test_cursor_exactly_at_the_limit_resumes():
    assert choose_start(fresh(30 * MIN), NOW, max_age_s=30 * 60) == fresh(30 * MIN)


def test_stale_cursor_refuses_with_its_age():
    week = 7 * 24 * 60 * MIN
    with pytest.raises(StaleCursor) as exc:
        choose_start(fresh(week), NOW)
    assert exc.value.age_s == pytest.approx(week / 1000)
    assert "--catch-up" in str(exc.value) and "--from-now" in str(exc.value)


def test_stale_cursor_resumes_with_catch_up():
    old = fresh(7 * 24 * 60 * MIN)
    assert choose_start(old, NOW, catch_up=True) == old


def test_from_now_ignores_any_cursor():
    assert choose_start(fresh(7 * 24 * 60 * MIN), NOW, from_now=True) is None
    assert choose_start(fresh(), NOW, from_now=True) is None


def test_no_cursor_starts_now():
    assert choose_start(None, NOW) is None


def test_cursor_of_unknown_age_is_treated_as_stale():
    c = cursor({"topic": EQ, "partition": 0, "offset": 123})
    with pytest.raises(StaleCursor) as exc:
        choose_start(c, NOW)
    assert exc.value.age_s is None
    assert choose_start(c, NOW, catch_up=True) == c


def test_from_now_and_catch_up_are_mutually_exclusive():
    with pytest.raises(ValueError):
        choose_start(fresh(), NOW, from_now=True, catch_up=True)
