"""Lateness as the Databricks pipeline experiences it (decision D19, amendment 2).

The pipeline ingests whole landed files, one micro-batch per scheduled update. Spark's
watermark for a micro-batch is the highest event time seen in EARLIER batches minus the
delay W, and a row whose event time is below it is dropped from windowed aggregates. So a
row's lateness is: max event time of all earlier batches - its own event time.

Pure functions (no I/O), unit-tested in tests/test_lateness.py; scripts/lateness_sim.py
loads real data, measures the live parameters and validates the simulation against them.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from collections.abc import Callable, Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)  # millions of rows: keep them small
class Row:
    partition: int
    offset: int
    event_s: int  # value.timestamp: the watermark column
    emit_ms: int  # meta.dt: when upstream emitted the event
    produced_ms: int  # Kafka timestamp: when our producer wrote it


FileKey = tuple[int, int]  # (partition, offset // offsets_per_file)


def file_of(row: Row, offsets_per_file: int) -> FileKey:
    return row.partition, row.offset // offsets_per_file


def landing_times(
    rows: Iterable[Row], file_key: Callable[[Row], FileKey], arrival_ms: Callable[[Row], float]
) -> dict[FileKey, float]:
    """A file lands when its last row arrives (D9: a range is written only when complete)."""
    out: dict[FileKey, float] = {}
    for r in rows:
        k = file_key(r)
        a = arrival_ms(r)
        if k not in out or a > out[k]:
            out[k] = a
    return out


def live_file_assignment(
    rows: Iterable[Row], offsets_per_file: int, arrival_ms: Callable[[Row], float]
) -> dict[tuple[int, int], FileKey]:
    """The files that live operation would have cut: per partition, rows in arrival order,
    in fixed ranges. Needed because during a catch-up the real offset order is NOT the
    arrival order a live run would see: a quiet upstream topic (codfw) catches up to live
    within seconds and its events interleave with hours-old backlog (observed: one 18:14
    event inside a file of 07:08 events), so real files mix times a live file never would.
    Ranges start at each partition's first real offset, aligned to the file size."""
    by_part: dict[int, list[Row]] = defaultdict(list)
    for r in rows:
        by_part[r.partition].append(r)
    out: dict[tuple[int, int], FileKey] = {}
    for p, rs in by_part.items():
        base = min(r.offset for r in rs)
        for rank, r in enumerate(sorted(rs, key=lambda r: (arrival_ms(r), r.offset))):
            out[(p, r.offset)] = (p, (base + rank) // offsets_per_file)
    return out


def batch_of(landing_ms: float, slot_ms: int, ingest_offset_ms: float) -> int:
    """Start of the first scheduled slot whose ingestion moment (slot + offset) is at or
    after the file's landing: the run that picks the file up."""
    n = -(-(landing_ms - ingest_offset_ms) // slot_ms)  # ceil division
    return int(n * slot_ms)


def row_lateness(rows: list[Row], batch_key: Callable[[Row], int]) -> list[float | None]:
    """Seconds by which each row trails the max event time of all EARLIER batches
    (None for the first batch, which has no watermark yet). Negative = not late."""
    by_batch: dict[int, list[int]] = defaultdict(list)
    for r in rows:
        by_batch[batch_key(r)].append(r.event_s)
    prior_max: dict[int, float | None] = {}
    running: float | None = None
    for b in sorted(by_batch):
        prior_max[b] = running
        newest = max(by_batch[b])
        running = newest if running is None else max(running, newest)
    out: list[float | None] = []
    for r in rows:
        m = prior_max[batch_key(r)]
        out.append(None if m is None else m - r.event_s)
    return out


def dropped(lateness: list[float | None], watermark_s: float) -> int:
    """Rows Spark would drop at watermark delay W: event time < (prior max - W)."""
    return sum(1 for x in lateness if x is not None and x > watermark_s)


def drop_curve(lateness: list[float | None], watermarks_s: Iterable[float]) -> dict[float, int]:
    late = sorted(x for x in lateness if x is not None)
    return {w: len(late) - bisect.bisect_right(late, w) for w in watermarks_s}
