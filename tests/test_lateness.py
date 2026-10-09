"""The lateness simulation on synthetic data where the right answer is known by hand."""

import pytest

from wikistream.lateness import (
    Row,
    batch_of,
    drop_curve,
    dropped,
    file_of,
    landing_times,
    live_file_assignment,
    row_lateness,
)

MIN = 60_000


def test_file_of_uses_fixed_ranges():
    assert file_of(Row(2, 12_345, 0, 0, 0), 5000) == (2, 2)


def test_a_file_lands_when_its_last_row_arrives():
    rows = [Row(0, 0, 0, 1_000, 0), Row(0, 1, 0, 9_000, 0), Row(0, 2, 0, 4_000, 0)]
    assert landing_times(rows, lambda r: file_of(r, 3), lambda r: r.emit_ms) == {(0, 0): 9_000}


@pytest.mark.parametrize(
    ("landing_min", "expected_slot_min"),
    [(0, 0), (0.9, 0), (1.5, 30), (29, 30), (31, 30), (32, 60)],
)
def test_batch_is_first_slot_whose_ingestion_moment_follows_landing(landing_min, expected_slot_min):
    # Runs every 30 min; each ingests files landed up to 1 minute after its slot.
    assert batch_of(landing_min * MIN, 30 * MIN, 1 * MIN) == expected_slot_min * MIN


def test_lateness_is_measured_against_earlier_batches_only():
    # Batch 0: events at 100 and 200. Batch 1: events at 150 (late by 50) and 300.
    rows = [Row(0, i, e, 0, 0) for i, e in enumerate([100, 200, 150, 300])]
    batches = {0: 0, 1: 0, 2: 1, 3: 1}
    lat = row_lateness(rows, lambda r: batches[r.offset])
    assert lat == [None, None, 50, -100]  # first batch has no watermark yet


def test_rows_in_the_same_batch_never_drop_each_other():
    rows = [Row(0, i, e, 0, 0) for i, e in enumerate([1000, 10])]
    assert row_lateness(rows, lambda r: 0) == [None, None]


def test_drop_rule_is_strictly_later_than_the_watermark():
    lat = [None, 59.0, 60.0, 61.0, -5.0]
    assert dropped(lat, 60) == 1  # only 61 s is beyond a 60 s watermark
    assert drop_curve(lat, [0, 60, 120]) == {0: 3, 60: 1, 120: 0}


def test_cross_partition_file_skew_is_the_lateness_mechanism():
    """Two partitions fill at the same rate; p0's file lands before the 30-minute run's
    ingestion moment (30.5 min), p1's just after it, so p1's whole file goes one batch
    later and its oldest row trails p0's newest by nearly the fill time."""
    fill_s = 600  # each file spans 10 minutes of events
    rows = []
    for p, lands_min in [(0, 29.5), (1, 31.0)]:
        for i in range(10):
            e = 1_000 + i * 60  # events 1000..1540 s, one per minute
            arrival = (lands_min - (9 - i) * fill_s / 600) * MIN
            rows.append(Row(p, i, e, arrival, arrival))
    land = landing_times(rows, lambda r: file_of(r, 10), lambda r: r.emit_ms)
    lat = row_lateness(rows, lambda r: batch_of(land[file_of(r, 10)], 30 * MIN, 0.5 * MIN))
    p1 = [x for r, x in zip(rows, lat, strict=True) if r.partition == 1]
    assert max(p1) == 540  # the p0 file's newest event (1540) minus p1's oldest (1000)
    assert drop_curve(lat, [120, 600]) == {120: 7, 600: 0}


def test_live_files_follow_arrival_order_not_catch_up_offset_order():
    """The 2026-10-09 catch-up artifact: one live codfw event (arrival 18:14) interleaved at
    offset 2 among backlog events (07:08..). Real file (0, 0) would 'land' at 18:14; the
    live assignment moves that event to the end, so the backlog file lands at 07:08:xx."""
    h = 3_600_000
    arrivals = [7 * h, 7 * h + 1, 18 * h, 7 * h + 2, 7 * h + 3, 7 * h + 4]
    rows = [Row(0, 10 + i, 0, a, 0) for i, a in enumerate(arrivals)]
    real = landing_times(rows, lambda r: file_of(r, 3), lambda r: r.emit_ms)
    assert real[(0, 4)] == 18 * h  # offsets 12..14: the late event lands the whole file
    assign = live_file_assignment(rows, 3, lambda r: r.emit_ms)
    sim = landing_times(rows, lambda r: assign[(r.partition, r.offset)], lambda r: r.emit_ms)
    assert sorted(sim.values()) == [7 * h + 1, 7 * h + 4, 18 * h]  # late event moved to the end
    assert assign[(0, 12)] == (0, 5)  # base 10 + rank 5 -> range 15..17
