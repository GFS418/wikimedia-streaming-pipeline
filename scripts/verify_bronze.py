"""Verify bronze against the landing zone: every landed file exactly once, exact offsets.

For each landed file that existed when the latest pipeline update started, bronze must
hold exactly its rows (count and offset range), once; nothing may come from anywhere else;
no Kafka offset may appear twice (exactly-once per offset, Kafka -> bronze); and nothing
may sit in _rescued_data. Producer-side duplicates (same meta.id at different offsets,
the at-least-once cost of decision A) are reported separately, not treated as failures.

    uv run python scripts/verify_bronze.py --landed-before 2026-10-08T13:06:00Z
"""

import argparse
import json
from datetime import datetime

import boto3

from wikistream.config import Settings
from wikistream.consumer import parse_key
from wikistream.dbsql import query

TABLE = "workspace.wikistream.bronze_recentchange"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--landed-before",
        required=True,
        help="ISO time the update started; files landed after it are not expected yet",
    )
    args = ap.parse_args()
    cutoff = datetime.fromisoformat(args.landed_before.replace("Z", "+00:00"))
    s = Settings()

    landed = {}
    for page in (
        boto3.client("s3")
        .get_paginator("list_objects_v2")
        .paginate(Bucket=s.landing_bucket, Prefix=s.landing_prefix)
    ):
        for o in page.get("Contents", []):
            parsed = parse_key(o["Key"])
            if parsed:
                landed[f"s3://{s.landing_bucket}/{o['Key']}"] = (parsed, o["LastModified"])
    expected = {k: v for k, v in landed.items() if v[1] < cutoff}

    per_file = query(f"""
        SELECT _source_file AS file, count(*) AS n, min(kafka.offset) AS lo,
               max(kafka.offset) AS hi, count(_rescued_data) AS rescued,
               count(DISTINCT kafka.partition) AS parts
        FROM {TABLE} GROUP BY _source_file""")
    totals = query(f"""
        SELECT count(*) AS rows,
               count(DISTINCT kafka.partition, kafka.offset) AS distinct_offsets,
               count(DISTINCT value.meta.id) AS distinct_event_ids,
               count(_rescued_data) AS rescued
        FROM {TABLE}""")[0]

    failures = []
    got = {r["file"]: r for r in per_file}
    for f in sorted(set(expected) - set(got)):
        failures.append(f"landed before cutoff but missing from bronze: {f}")
    for f in sorted(set(got) - set(landed)):
        failures.append(f"in bronze but not a landed range file: {f}")
    for f, r in got.items():
        if f not in landed:
            continue
        (p, start, end), _ = landed[f]
        n, lo, hi = int(r["n"]), int(r["lo"]), int(r["hi"])
        if (n, hi) != (end - lo + 1, end) or lo < start or int(r["parts"]) != 1:
            failures.append(f"{f}: {n} rows, offsets {lo}..{hi} (expected ..{end})")
        if int(r["rescued"]):
            failures.append(f"{f}: {r['rescued']} rows with _rescued_data")

    rows = int(totals["rows"])
    report = {
        "files_expected": len(expected),
        "files_in_bronze": len(got),
        "rows": rows,
        "rows_expected_from_files": sum(
            v[0][2] - v[0][1] + 1 for k, v in landed.items() if k in got
        ),
        "duplicate_offsets": rows - int(totals["distinct_offsets"]),
        "producer_duplicate_event_ids": rows - int(totals["distinct_event_ids"]),
        "rescued_rows": int(totals["rescued"]),
        "failures": failures,
        "verdict": "ok" if not failures and rows == int(totals["distinct_offsets"]) else "FAILED",
    }
    print(json.dumps(report, indent=2))
    raise SystemExit(0 if report["verdict"] == "ok" else 1)


if __name__ == "__main__":
    main()
