"""Choose the gold watermark from data (decision D19, amendment 2). Read-only.

1. Load every row produced since --since from local Kafka (partition, offset, event time,
   emit time, produce time); S3 landing times per file; bronze ingestion moment per file.
2. Measure live parameters on rows produced live (produce - emit <= 2 min): delivery delay,
   consumer landing delay (S3 LastModified - last produce time in the file), and the
   scheduled runs' ingestion offset (bronze _ingested_at - :00/:30 slot).
3. Simulate the whole span as if live: arrival = emit + delay; a file lands when its last row
   arrives; each scheduled run ingests the files landed by its ingestion moment; lateness =
   max event time of earlier batches - event time. Historical rows (D18) are excluded, as gold
   excludes them.
4. Validate on live rows that the scheduled runs really ingested: simulated vs actual landing
   time, batch, and drop counts. Report drop rates per watermark, overall and per UTC hour.

    uv run python scripts/lateness_sim.py --since 2026-10-08T12:34:00Z
"""

import argparse
import io
import json
import statistics as st
from collections import defaultdict
from datetime import UTC, datetime

import boto3
import fastavro
from confluent_kafka import Consumer, TopicPartition

from wikistream.config import Settings
from wikistream.consumer import parse_key
from wikistream.contract import load_schema
from wikistream.dbsql import query
from wikistream.lateness import (
    Row,
    batch_of,
    drop_curve,
    file_of,
    landing_times,
    live_file_assignment,
    row_lateness,
)

LIVE_MS = 120_000
SLOT_MS = 30 * 60_000
WATERMARKS_S = [30, 60, 120, 300, 600, 900, 1200, 1800]


def iso_ms(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def load_rows(s: Settings, since_ms: int) -> list[Row]:
    schema = fastavro.parse_schema(load_schema())
    c = Consumer(
        {
            "bootstrap.servers": s.bootstrap_servers,
            "group.id": "lateness-sim",
            "enable.auto.commit": False,
        }
    )
    t = s.main_topic.name
    starts = c.offsets_for_times([TopicPartition(t, p, since_ms) for p in range(3)], timeout=10)
    ends = {p: c.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1] for p in range(3)}
    c.assign([tp for tp in starts if tp.offset >= 0])
    want = sum(ends[tp.partition] - tp.offset for tp in starts if tp.offset >= 0)
    rows, seen = [], 0
    while seen < want:
        m = c.poll(10)
        if m is None or m.error():
            raise RuntimeError(f"scan stopped after {seen}/{want}: {m and m.error()}")
        if m.offset() >= ends[m.partition()]:
            continue
        seen += 1
        v = fastavro.schemaless_reader(io.BytesIO(m.value()[5:]), schema)
        rows.append(
            Row(
                m.partition(), m.offset(), v["timestamp"], iso_ms(v["meta"]["dt"]), m.timestamp()[1]
            )
        )
    c.close()
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True)
    args = ap.parse_args()
    s = Settings()
    n = s.offsets_per_file
    rows = load_rows(s, iso_ms(args.since))

    # Only complete files exist downstream (D9); drop rows of the still-filling ranges.
    s3 = boto3.client("s3")
    landed_at = {}
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=s.landing_bucket, Prefix=s.landing_prefix
    ):
        for o in page.get("Contents", []):
            if parsed := parse_key(o["Key"]):
                p, start, _ = parsed
                landed_at[(p, start // n)] = o["LastModified"].timestamp() * 1000
    rows = [r for r in rows if file_of(r, n) in landed_at]

    ingested = {}
    for r in query("""SELECT _source_file AS f, min(_ingested_at) AS t
                      FROM workspace.wikistream.bronze_recentchange GROUP BY 1"""):
        if parsed := parse_key(r["f"]):
            p, start, _ = parsed
            ingested[(p, start // n)] = iso_ms(r["t"])

    live = [r for r in rows if r.produced_ms - r.emit_ms <= LIVE_MS]
    historical = {(r.partition, r.offset) for r in rows if r.emit_ms / 1000 - r.event_s > 3600}

    # ---- measured live parameters
    delay_ms = st.median(r.produced_ms - r.emit_ms for r in live)
    by_file = defaultdict(list)
    for r in rows:
        by_file[file_of(r, n)].append(r)
    live_files = [
        k for k, rs in by_file.items() if all(r.produced_ms - r.emit_ms <= LIVE_MS for r in rs)
    ]
    consumer_ms = st.median(
        landed_at[k] - max(r.produced_ms for r in by_file[k]) for k in live_files
    )
    offsets = [t - (t // SLOT_MS) * SLOT_MS for t in set(ingested.values())]
    sched = [o for o in offsets if o < 5 * 60_000]  # scheduled runs; manual ones are off-slot
    ingest_offset_ms = st.median(sched)

    # ---- simulation over everything, as if live
    arrival = lambda r: r.emit_ms + delay_ms  # noqa: E731
    assign = live_file_assignment(rows, n, arrival)
    sim_file = lambda r: assign[(r.partition, r.offset)]  # noqa: E731
    sim_land = {k: v + consumer_ms for k, v in landing_times(rows, sim_file, arrival).items()}
    sim_lat = row_lateness(
        rows, lambda r: batch_of(sim_land[sim_file(r)], SLOT_MS, ingest_offset_ms)
    )
    keep = [
        (r, x)
        for r, x in zip(rows, sim_lat, strict=True)
        if (r.partition, r.offset) not in historical
    ]
    total = len(keep)
    curve = drop_curve([x for _, x in keep], WATERMARKS_S)

    hourly = defaultdict(list)
    for r, x in keep:
        hourly[datetime.fromtimestamp(r.emit_ms / 1000, UTC).hour].append(x)
    fill = defaultdict(list)
    for rs in by_file.values():
        h = datetime.fromtimestamp(max(r.emit_ms for r in rs) / 1000, UTC).hour
        fill[h].append((max(r.emit_ms for r in rs) - min(r.emit_ms for r in rs)) / 60_000)
    span_h = (max(r.emit_ms for r in rows) - min(r.emit_ms for r in rows)) / 3.6e6
    per_hour = {
        h: {
            "rows": len(hourly[h]),
            "events_per_min": round(len(hourly[h]) / (60 * max(1, span_h / 24)), 0),
            "median_file_fill_min": round(st.median(fill[h]), 1) if fill[h] else None,
            "drop_pct": {
                w: round(100 * c / len(hourly[h]), 4)
                for w, c in drop_curve(hourly[h], WATERMARKS_S).items()
            },
        }
        for h in sorted(hourly)
    }

    # ---- validation: live rows actually ingested by scheduled runs
    act_lat = row_lateness(
        [r for r in rows if file_of(r, n) in ingested], lambda r: ingested[file_of(r, n)]
    )
    act = {
        (r.partition, r.offset): x
        for r, x in zip([r for r in rows if file_of(r, n) in ingested], act_lat, strict=True)
    }
    val_files = [
        k
        for k in live_files
        if k in ingested and ingested[k] - (ingested[k] // SLOT_MS) * SLOT_MS < 5 * 60_000
    ]
    val_rows = [
        r for k in val_files for r in by_file[k] if (r.partition, r.offset) not in historical
    ]
    sim_by_key = {(r.partition, r.offset): x for r, x in zip(rows, sim_lat, strict=True)}
    # In live files real order == arrival order, so the real file's simulated landing is
    # comparable to its S3 LastModified and its run.
    real_land = {
        k: v + consumer_ms for k, v in landing_times(rows, lambda r: file_of(r, n), arrival).items()
    }
    land_err_s = [(real_land[k] - landed_at[k]) / 1000 for k in val_files]
    batch_agree = sum(
        batch_of(real_land[k], SLOT_MS, ingest_offset_ms) == (ingested[k] // SLOT_MS) * SLOT_MS
        for k in val_files
    )

    report = {
        "rows": len(rows),
        "rows_excluded_historical": len(historical),
        "span_hours": round(span_h, 1),
        "live_rows": len(live),
        "measured": {
            "delivery_delay_s_median": round(delay_ms / 1000, 2),
            "consumer_landing_delay_s_median": round(consumer_ms / 1000, 2),
            "run_ingest_offset_s_median": round(ingest_offset_ms / 1000, 1),
            "scheduled_batches_seen": len(sched),
        },
        "simulated_drop_pct": {w: round(100 * c / total, 4) for w, c in curve.items()},
        "per_utc_hour": per_hour,
        "validation": {
            "live_files_ingested_by_scheduled_runs": len(val_files),
            "rows": len(val_rows),
            "landing_error_s": {
                "median": round(st.median(land_err_s), 1) if land_err_s else None,
                "p95": round(sorted(land_err_s)[int(0.95 * len(land_err_s))], 1)
                if land_err_s
                else None,
                "max_abs": round(max(abs(x) for x in land_err_s), 1) if land_err_s else None,
            },
            "batch_agreement": f"{batch_agree}/{len(val_files)}",
            "drops_sim_vs_actual": {
                w: [
                    sum(
                        1
                        for r in val_rows
                        if (x := sim_by_key[(r.partition, r.offset)]) is not None and x > w
                    ),
                    sum(
                        1
                        for r in val_rows
                        if (x := act[(r.partition, r.offset)]) is not None and x > w
                    ),
                ]
                for w in WATERMARKS_S
            },
        },
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
