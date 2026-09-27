# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = []
# ///
"""Summarize a probe_sse.py JSONL capture: rate, continuity, lateness, key skew, size."""

import json
import statistics as st
import sys
from collections import Counter
from datetime import datetime


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def murmur2(data: bytes) -> int:
    """Kafka's default partitioner hash (org.apache.kafka.common.utils.Utils.murmur2)."""
    m, r, seed = 0x5BD1E995, 24, 0x9747B28C
    length = len(data)
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(0, length - length % 4, 4):
        k = int.from_bytes(data[i : i + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    tail = length % 4
    base = length - tail
    if tail == 3:
        h ^= data[base + 2] << 16
    if tail >= 2:
        h ^= data[base + 1] << 8
    if tail >= 1:
        h ^= data[base]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return h


def kafka_partition(key: str, n: int) -> int:
    return (murmur2(key.encode()) & 0x7FFFFFFF) % n


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def main(path):
    rows = [json.loads(line) for line in open(path)]
    ev = [r for r in rows if not r.get("bad_json") and r.get("domain") != "canary"]
    n = len(ev)
    recv = [ts(r["recv"]) for r in ev]
    span_min = (recv[-1] - recv[0]) / 60
    print(
        f"events={len(rows)} usable={n} bad_json={sum(bool(r.get('bad_json')) for r in rows)} "
        f"canary={sum(r.get('domain') == 'canary' for r in rows)} span={span_min:.1f} min"
    )

    # --- rate
    per_min = Counter(int((t - recv[0]) // 60) for t in recv)
    full = [c for m, c in per_min.items() if m < int(span_min)]  # drop partial last minute
    if full:
        print(
            f"rate/min: mean={st.mean(full):.0f} min={min(full)} max={max(full)} "
            f"(n_minutes={len(full)})"
        )

    # --- continuity: offsets on the upstream Kafka partition, and meta.id duplicates
    # The public stream merges two upstream topics (one per datacenter), each with its
    # own offset sequence. The SSE id is a per-topic cursor; the entry that changed
    # since the previous event tells us which topic this event came from.
    prev = None
    for r in ev:
        cur = {p["topic"]: p.get("timestamp", p.get("offset")) for p in json.loads(r["sse_id"])}
        changed = [t for t in cur if prev is None or cur[t] != prev.get(t)]
        r["topic"] = changed[0] if len(changed) == 1 else None
        prev = cur
    # Fallback for ambiguous rows: nearest offset cluster among attributed rows.
    last = {}
    for r in ev:
        if r["topic"]:
            last[r["topic"]] = r["offset"]
        elif last:
            r["topic"] = min(last, key=lambda t: abs(last[t] - r["offset"]))
    print(f"events per upstream topic: {dict(Counter(r['topic'] for r in ev))}")
    for topic in sorted({r["topic"] for r in ev if r["topic"]}):
        offs = [r["offset"] for r in ev if r["topic"] == topic]
        back = sum(1 for a, b in zip(offs, offs[1:], strict=False) if b <= a)
        gaps = [(a, b) for a, b in zip(offs, offs[1:], strict=False) if b > a + 1]
        missing = sum(b - a - 1 for a, b in gaps)
        dup_off = sum(c - 1 for c in Counter(offs).values() if c > 1)
        print(
            f"  {topic}: offsets {offs[0]}..{offs[-1]} ({offs[-1] - offs[0] + 1} expected), "
            f"received {len(offs)}; non-increasing={back}; gap runs={len(gaps)} missing={missing}; "
            f"dup offsets={dup_off}"
        )
        if gaps[:5]:
            print(f"    first gaps: {gaps[:5]}")
    dup_ids = sum(c - 1 for c in Counter(r["meta_id"] for r in ev).values() if c > 1)
    print(f"dup meta.id overall={dup_ids}")

    # --- clock skew bound: meta.dt is stamped upstream before we receive it, so
    # recv - meta.dt < 0 means our clock is behind theirs by at least that much.
    ingest = [ts(r["recv"]) - ts(r["meta_dt"]) for r in ev]
    print(
        f"recv - meta.dt (s): min={min(ingest):.3f} p50={pct(ingest, 0.5):.3f} "
        f"p99={pct(ingest, 0.99):.3f} max={max(ingest):.3f}"
    )

    # --- event-time lateness. Two candidate event times:
    #   ts      = when the edit happened in MediaWiki (1 s resolution)
    #   meta.dt = when the event was emitted to the event bus
    for name, et in [
        ("timestamp (edit time)", [float(r["ts"]) for r in ev]),
        ("meta.dt (emit time)", [ts(r["meta_dt"]) for r in ev]),
    ]:
        delay = [ts(r["recv"]) - e for r, e in zip(ev, et, strict=True)]
        running, disorder = float("-inf"), []
        for e in et:
            running = max(running, e)
            disorder.append(running - e)  # how far behind the max event time seen so far
        late = [d for d in disorder if d > 0]
        print(f"\n[{name}]")
        print(
            f"  recv delay (s):   p50={pct(delay, 0.5):.1f} p99={pct(delay, 0.99):.1f} "
            f"p99.9={pct(delay, 0.999):.1f} max={max(delay):.1f}"
        )
        print(f"  out-of-order: {len(late)}/{n} ({len(late) / n:.1%}) behind running max")
        print(
            f"  disorder (s):     p50={pct(disorder, 0.5):.1f} p99={pct(disorder, 0.99):.1f} "
            f"p99.9={pct(disorder, 0.999):.1f} p99.99={pct(disorder, 0.9999):.1f} "
            f"max={max(disorder):.1f}"
        )
        for bound in (5, 30, 60, 120, 300, 600):
            dropped = sum(d > bound for d in disorder)
            print(f"    watermark {bound:>4}s would drop {dropped} ({dropped / n:.3%})")

    # --- mix
    print(f"\ntypes: {dict(Counter(r['type'] for r in ev).most_common())}")
    print(f"bot share: {sum(bool(r['bot']) for r in ev if r['bot'] is not None) / n:.1%}")
    key_sets = Counter(tuple(r["keys"]) for r in ev)
    print(f"distinct top-level field sets: {len(key_sets)}")
    for k, c in key_sets.most_common(4):
        print(f"  {c:>6}  {len(k)} fields")

    # --- partition-key skew under Kafka's default partitioner
    wikis = Counter(r["wiki"] for r in ev)
    top = wikis.most_common(5)
    print(f"\ndistinct wikis={len(wikis)}; top5: " + ", ".join(f"{w} {c / n:.1%}" for w, c in top))
    for P in (3, 6, 12):
        by_wiki = Counter(kafka_partition(r["wiki"], P) for r in ev)
        by_id = Counter(kafka_partition(r["meta_id"], P) for r in ev)
        print(
            f"  {P:>2} partitions: max partition share key=wiki {max(by_wiki.values()) / n:.1%}, "
            f"key=meta.id {max(by_id.values()) / n:.1%} (ideal {1 / P:.1%})"
        )

    # --- volume
    b = [r["bytes"] for r in ev]
    per_hour_mb = sum(b) / (recv[-1] - recv[0]) * 3600 / 1e6
    print(
        f"\nbytes/event: mean={st.mean(b):.0f} p99={pct(b, 0.99)} max={max(b)}; "
        f"~{per_hour_mb:.0f} MB/hour, ~{per_hour_mb * 24 / 1000:.1f} GB/day uncompressed JSON"
    )


if __name__ == "__main__":
    main(sys.argv[1])
