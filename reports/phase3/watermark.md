# Choosing the gold watermark (decision D19, amendment 2)

**Data:** 3,105,438 rows over 29.9 hours (2026-10-08 12:34 to 2026-10-09 18:20 UTC), continuous
thanks to two backfills from Wikimedia's history; every upstream offset accounted for
(`audit_topic.py`: eqiad 3,096,791 and codfw 360, 0 gaps). Method and code:
`scripts/lateness_sim.py`, `src/wikistream/lateness.py` (13 unit tests). Raw output:
[`lateness_sim_20261009.json`](lateness_sim_20261009.json).

## Measured live parameters (rows produced within 2 minutes of emission: 272,151)

| Parameter | Median |
|---|---|
| Delivery delay, upstream emit to our Kafka | 0.07 s |
| Consumer landing delay, last row produced to S3 LastModified | 0.98 s |
| Scheduled run's ingestion moment after its :00/:30 slot | 86.8 s (6 scheduled batches) |

## Validation (35 live files ingested by scheduled runs, 174,995 rows)

| Check | Result |
|---|---|
| Simulated vs actual S3 landing time | median 0.2 s, p95 0.6 s, max 4.9 s |
| Same run as reality | 32 of 35 files |
| Rows dropped, simulated vs actual, per watermark 30 s / 1 / 2 / 5 / 10 / 15+ min | 16,401 vs 16,403 / 15,047 vs 15,047 / 12,308 vs 12,308 / 4,341 vs 4,341 / 1,040 vs 1,040 / 0 vs 0 |

## Result over the full day

| Watermark | 30 s | 1 min | 2 min | 5 min | 10 min | **15 min** | 20 min | 30 min |
|---|---|---|---|---|---|---|---|---|
| Rows dropped | 5.98% | 5.12% | 3.69% | 0.82% | 0.034% | **0%** | 0% | 0% |

Lateness tail: p99 283 s, p99.9 446 s, p99.99 719 s, **max 771 s (12.8 min)**, at 02:48 UTC in
the overnight low (about 1,000 events/min, files filling in 10 to 13 minutes). At 10 minutes
every hour is clean except 02 UTC (1.2%); at 15 minutes every hour is 0%. The probe-based
provisional watermark of 2 minutes (D3) would have dropped 3.7% of all rows, 7 to 9% overnight.

## A modelling error found and fixed on the way

The first run showed a floor of 2.42% dropped at every watermark from 15 to 30 minutes. Cause:
during a catch-up, quiet codfw reaches live within seconds and its events interleave with
hours-old eqiad backlog, so a real file could hold 4,999 rows from 07:08 and one from 18:14;
"a file lands when its last row arrives" then made all 4,999 look 11 hours late. Live
operation never forms such files, so the simulation now cuts files from each partition's
rows in arrival order (`live_file_assignment`, with a test reproducing the artifact).
Upstream emit delays were ruled out first: only 59 of 3.1 M rows were emitted more than
5 minutes after their edit.

## Limitations

- One weekday. Lateness tracks file fill time, which grows as traffic falls: a day 35% quieter
  than this one's overnight low would fill files in about 20 minutes.
- Validation covers 35 live files from one overnight window; the simulation is exact only up to
  the measured parameters (sub-second delivery and landing, 86.8 s run start-up).
