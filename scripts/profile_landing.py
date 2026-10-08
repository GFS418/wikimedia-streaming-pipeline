"""Phase 3 evidence from the landed files (read-only, admin profile).

A. Data-quality profile of every landed row: candidate silver expectations.
B. Lateness induced by fixed-range files (D9/D10). A Databricks run ingests whatever files
   have landed; a file lands only when its 5,000-offset range fills. Rows in a file that
   lands after another partition's newer file arrive "late" relative to the newest event
   time already ingested. For each file we take the newest event time among files landed
   strictly before it (the worst case: a run fires just before it lands) and measure how far
   behind that each of its rows is. Restricted to rows produced since --since, so a backlog
   (e.g. week-old rows landing for the first time) does not masquerade as lateness.

    uv run python scripts/profile_landing.py --since 2026-10-07T23:07:09Z
"""

import argparse
import gzip
import json
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime

import boto3

from wikistream.config import Settings
from wikistream.consumer import parse_key

TYPES = {"edit", "new", "log", "categorize", "external"}
WIKI_RE = re.compile(r"^[a-z0-9_]+$")


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def dt_s(iso: str) -> float:
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()


def load(s: Settings):
    s3 = boto3.client("s3")
    files = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=s.landing_bucket, Prefix=s.landing_prefix
    ):
        for o in page.get("Contents", []):
            parsed = parse_key(o["Key"])
            if parsed:
                body = s3.get_object(Bucket=s.landing_bucket, Key=o["Key"])["Body"].read()
                rows = [json.loads(r) for r in gzip.decompress(body).decode().splitlines()]
                files.append((o["LastModified"], o["Key"], parsed, rows))
    files.sort(key=lambda f: f[0])
    return files


def profile(rows) -> dict:
    v = [r["value"] for r in rows]
    by_type = Counter(x["type"] for x in v)
    present = defaultdict(Counter)
    for x in v:
        t = x["type"]
        for f in ("title", "user", "namespace", "comment", "log_type", "log_action"):
            present[t][f] += x.get(f) is not None
        for f in ("length", "revision"):
            rec = x.get(f) or {}
            present[t][f"{f}.old"] += rec.get("old") is not None
            present[t][f"{f}.new"] += rec.get("new") is not None
    emit_delay = defaultdict(list)  # meta.dt - timestamp, seconds
    for x in v:
        emit_delay[x["type"]].append(dt_s(x["meta"]["dt"]) - x["timestamp"])
    neg_len = sum(
        1
        for x in v
        for side in ("old", "new")
        if (x.get("length") or {}).get(side) is not None and x["length"][side] < 0
    )
    ns = [x["namespace"] for x in v if x.get("namespace") is not None]
    ids = Counter(x["meta"]["id"] for x in v)
    return {
        "rows": len(v),
        "types": dict(by_type.most_common()),
        "unknown_types": sum(c for t, c in by_type.items() if t not in TYPES),
        "bot_share": round(sum(x["bot"] for x in v) / len(v), 4),
        "presence_by_type": {
            t: {f: f"{c}/{by_type[t]}" for f, c in sorted(present[t].items())} for t in by_type
        },
        "negative_lengths": neg_len,
        "namespace_min_max": [min(ns), max(ns)] if ns else None,
        "namespace_negative": sum(n < 0 for n in ns),
        "wiki_name_mismatches": sum(not WIKI_RE.match(x["wiki"]) for x in v),
        "extra_json_rows": sum(x.get("_extra_json") is not None for x in v),
        "duplicate_meta_ids": sum(c - 1 for c in ids.values() if c > 1),
        "emit_delay_s_by_type": {
            t: {
                "min": round(min(d), 1),
                "p50": round(pct(d, 0.5), 1),
                "p99": round(pct(d, 0.99), 1),
                "p99.9": round(pct(d, 0.999), 1),
                "max": round(max(d), 1),
                "over_1h": sum(x > 3600 for x in d),
            }
            for t, d in emit_delay.items()
        },
    }


def file_lateness(files, since_ms: int) -> dict:
    prior_max = None
    per_file, lateness = [], []
    for landed_at, key, _range, rows in files:
        recent = [r for r in rows if r["kafka"]["timestamp_ms"] >= since_ms]
        times = [r["value"]["timestamp"] for r in recent]
        if recent:
            late = [max(0, prior_max - t) for t in times] if prior_max is not None else []
            lateness += late
            ks = [r["kafka"]["timestamp_ms"] for r in recent]
            per_file.append(
                {
                    "key": key.split("/", 2)[-1],
                    "landed_at": landed_at.astimezone(UTC).strftime("%H:%M:%S"),
                    "recent_rows": len(recent),
                    "event_time_span_s": max(times) - min(times),
                    "kafka_fill_span_s": round((max(ks) - min(ks)) / 1000),
                    "max_lateness_s": max(late) if late else 0,
                }
            )
        all_times = [r["value"]["timestamp"] for r in rows]
        newest = max(all_times)
        prior_max = newest if prior_max is None else max(prior_max, newest)
    n = len(lateness)
    return {
        "files": per_file,
        "rows_measured": n,
        "late_by_more_than": {
            f"{w}s": f"{sum(x > w for x in lateness)} ({sum(x > w for x in lateness) / n:.1%})"
            for w in (30, 60, 120, 300, 600, 900)
        }
        if n
        else {},
        "max_lateness_s": max(lateness) if lateness else None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True, help="ISO time; part B uses rows produced after it")
    args = ap.parse_args()
    since_ms = int(dt_s(args.since) * 1000)
    files = load(Settings())
    rows = [r for f in files for r in f[3]]
    print(
        json.dumps(
            {"A_profile": profile(rows), "B_file_lateness": file_lateness(files, since_ms)},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
