# Feed probe: Wikimedia EventStreams `recentchange`

**Capture:** 2026-09-27 03:21:30 to 03:38:30 UTC (17.0 min), read-only SSE client
(`scripts/probe_sse.py 1020 ...`), analyzed with `scripts/analyze_probe.py`.
Full output: [`summary_20260927.txt`](summary_20260927.txt). The raw capture (27 MB JSONL) is
gitignored; rerunning the probe reproduces the method, not the exact numbers.

## Numbers

| Measure | Value |
|---|---|
| Events | 39,621 (0 malformed JSON, 0 canary) |
| Rate | mean 2,329/min (about 39/s); per-minute min 2,062, max 3,399 |
| Size | mean 1,343 B, p99 2,195 B, max 7,807 B; about 188 MB/hour uncompressed |
| Types | categorize 60.9%, edit 29.9%, log 5.0%, new 4.2% |
| Bot share | 60.4% |
| Distinct wikis | 120; top: commonswiki 30.9%, enwiki 17.5%, arwiktionary 12.9% |
| Top-level field sets | 5 (categorize 17 fields; edit/new/log 20 to 21) |

## Continuity and reconnects

- The public stream merges two upstream Kafka topics, `eqiad.mediawiki.recentchange` and
  `codfw.mediawiki.recentchange`, each with its own offsets. The SSE `id` (resume cursor) is a
  per-topic **timestamp** in ms, not an offset.
- Two server disconnects: one at +15.00 min (the documented 15-minute cutoff) and one
  unexplained at +15.61 min. Both were resumed with `Last-Event-ID`.
- eqiad offsets 6554818550..6554858169: 39,620 expected, 39,620 received, 0 gaps, 0 duplicates,
  0 duplicate `meta.id`. codfw carried 0 events in this window (2 in an earlier 20 s smoke
  test), so its continuity is **untested**.

## Event-time disorder

Disorder = (max event time seen so far) - (this event's time), in arrival order. This is what
an event-time watermark must absorb.

| Event-time column | out of order | p99 | p99.9 | p99.99 | max | dropped at 30 s | at 60 s |
|---|---|---|---|---|---|---|---|
| `timestamp` (edit time, 1 s resolution) | 69.6% | 6 s | 18 s | 33 s | 51 s | 5 (0.013%) | 0 |
| `meta.dt` (emit time, ms) | 1.4% | under 0.05 s | | | under 0.05 s | 0 | 0 |

Clock skew: `recv - meta.dt` has a minimum of -0.084 s, so the local clock runs at least 84 ms
behind Wikimedia's. This affects only the receive-delay figures, not disorder, which uses event
times alone.

## Limitations

- A 17-minute window at a low-traffic hour (about 03:30 UTC). It says nothing about
  peak-hour rates, backlog bursts, or rare multi-minute delays in the job queue.
- One sample of reconnect behavior (2 events), and none for codfw.
- The watermark bound drawn from this capture is a starting point, to be re-measured on a
  24-hour capture from our own Kafka topic.
