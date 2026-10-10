"""Lag and health monitor (Phase 4): one JSON line per interval, with threshold alerts.

Runs as the `monitor` compose service next to the capture. It measures the local half of the
pipeline (Kafka and the S3 landing zone); the Databricks half is checked by the verifiers.
Thresholds come from measured behavior (reports/phase3/watermark.md, Phase 2/3 runs):

    producer_stalled   newest Kafka message older than 120 s     (live: under 1 s;
                                                                   reconnects pause ~5 s)
    upstream_lag       newest eqiad position more than 300 s behind live (catch-up or stuck)
    landing_backlog    a partition has 2+ files' worth of offsets not landed (normal: < 1)
    landing_stalled    newest landed file older than 30 min       (overnight fill <= ~13 min)
    dead_letters       any dead letter since the previous tick

    wikistream-monitor --interval-s 60
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import threading
import time
from dataclasses import dataclass

from confluent_kafka import Consumer, TopicPartition

from wikistream.config import Settings
from wikistream.consumer import parse_key
from wikistream.producer import H_CURSOR

log = logging.getLogger("wikistream.monitor")


@dataclass(frozen=True)
class Thresholds:
    producer_stalled_s: float = 120
    upstream_lag_s: float = 300
    landing_backlog_files: float = 2
    landing_stalled_s: float = 30 * 60


def evaluate(m: dict, t: Thresholds, offsets_per_file: int, prev_dlq: int | None) -> list[str]:
    """Pure: which alerts a metrics snapshot raises."""
    alerts = []
    if m["producer_freshness_s"] is not None and m["producer_freshness_s"] > t.producer_stalled_s:
        alerts.append(f"producer_stalled: no new message for {m['producer_freshness_s']:.0f} s")
    if m["upstream_lag_s"] is not None and m["upstream_lag_s"] > t.upstream_lag_s:
        alerts.append(f"upstream_lag: {m['upstream_lag_s']:.0f} s behind live")
    for p, unlanded in m["unlanded"].items():
        if unlanded >= t.landing_backlog_files * offsets_per_file:
            alerts.append(f"landing_backlog: p{p} has {unlanded} offsets not landed")
    if m["landing_freshness_s"] is not None and m["landing_freshness_s"] > t.landing_stalled_s:
        alerts.append(
            f"landing_stalled: newest file is {m['landing_freshness_s'] / 60:.0f} min old"
        )
    if prev_dlq is not None and m["dlq_total"] > prev_dlq:
        alerts.append(f"dead_letters: {m['dlq_total'] - prev_dlq} new")
    return alerts


def snapshot(s: Settings, s3, consumer: Consumer) -> dict:
    now_ms = time.time() * 1000
    newest_msg_ms, newest_eqiad_ms, highs = 0, 0, {}
    for spec in (s.main_topic, s.drops_topic):
        for p in range(spec.partitions):
            lo, hi = consumer.get_watermark_offsets(TopicPartition(spec.name, p), timeout=10)
            if spec is s.main_topic:
                highs[p] = hi
            if hi <= lo:
                continue
            consumer.assign([TopicPartition(spec.name, p, hi - 1)])
            m = consumer.poll(10)
            if m is None or m.error():
                continue
            newest_msg_ms = max(newest_msg_ms, m.timestamp()[1])
            raw = dict(m.headers() or []).get(H_CURSOR)
            for e in json.loads(raw.decode()) if raw else []:
                if e["topic"].startswith("eqiad") and "timestamp" in e:
                    newest_eqiad_ms = max(newest_eqiad_ms, e["timestamp"])
    dlq_lo, dlq_hi = consumer.get_watermark_offsets(TopicPartition(s.dlq_topic.name, 0), timeout=10)
    drops_lo, drops_hi = consumer.get_watermark_offsets(
        TopicPartition(s.drops_topic.name, 0), timeout=10
    )

    ends: dict[int, int] = {}
    newest_file_s = 0.0
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=s.landing_bucket, Prefix=s.landing_prefix
    ):
        for o in page.get("Contents", []):
            if parsed := parse_key(o["Key"]):
                p, _, end = parsed
                ends[p] = max(ends.get(p, -1), end)
                newest_file_s = max(newest_file_s, o["LastModified"].timestamp())
    return {
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ms / 1000)),
        "producer_freshness_s": round((now_ms - newest_msg_ms) / 1000, 1)
        if newest_msg_ms
        else None,
        "upstream_lag_s": round((now_ms - newest_eqiad_ms) / 1000, 1) if newest_eqiad_ms else None,
        "unlanded": {p: hi - (ends.get(p, -1) + 1) for p, hi in highs.items()},
        "landing_freshness_s": round(now_ms / 1000 - newest_file_s, 1) if newest_file_s else None,
        "dlq_total": dlq_hi - dlq_lo,
        "drops_total": drops_hi - drops_lo,
    }


def main() -> None:
    import boto3

    ap = argparse.ArgumentParser(description="Lag and health monitor for the capture.")
    ap.add_argument("--interval-s", type=float, default=60)
    args = ap.parse_args()
    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = Settings()
    s3 = boto3.Session(profile_name=s.aws_profile).client("s3")  # list-only use of the role
    consumer = Consumer(
        {
            "bootstrap.servers": s.bootstrap_servers,
            "group.id": "wikistream-monitor",
            "enable.auto.commit": False,
        }
    )
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    thresholds, prev_dlq, active = Thresholds(), None, set()
    try:
        while not stop.is_set():
            m = snapshot(s, s3, consumer)
            alerts = evaluate(m, thresholds, s.offsets_per_file, prev_dlq)
            prev_dlq = m["dlq_total"]
            names = {a.split(":")[0] for a in alerts}
            m["alerts"] = alerts
            log.info("health %s", json.dumps(m, sort_keys=True))
            for a in alerts:
                if a.split(":")[0] not in active:
                    log.warning("ALERT %s", a)
            for cleared in active - names:
                log.info("CLEARED %s", cleared)
            active = names
            stop.wait(args.interval_s)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
