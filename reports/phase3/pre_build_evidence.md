# Phase 3 evidence, gathered before building (2026-10-07)

Source: the 6 landed files (30,000 rows; offsets 0..9999 of each partition) profiled by
`scripts/profile_landing.py --since 2026-10-07T23:07:09Z`. Part B uses only the 9,805 rows
produced on 2026-10-07 (23:07:09 onward), so week-old backlog rows do not count as lateness.

## A. Data-quality profile (candidate silver expectations)

| Measure | Value |
|---|---|
| Types | categorize 15,925 (53.1%), edit 11,166, log 1,877, new 1,032; unknown types 0 |
| Bot share (all events) | 55.8% |
| Field presence by type | edit: revision.old/new and length.old/new on 11,166/11,166. new: revision.new and length.new on 1,032/1,032, revision.old and length.old on 0/1,032. log: log_type and log_action on 1,877/1,877, no revision/length. categorize: no revision/length (0/15,925). title, user, namespace, comment present on every row. |
| Negative lengths | 0 |
| Namespace range | -1 to 1,728. The 12 rows with namespace -1 are all log events (abusefilter 11, translationreview 1) on localized Special pages, i.e. legitimate: a naive `namespace >= 0` rule would quarantine real data |
| Wiki names not matching `^[a-z0-9_]+$` | 0 |
| Rows with `_extra_json` (unknown upstream fields) | 0 |
| Duplicate `meta.id` | 0 |
| Emit delay `meta.dt - timestamp` (s), p99.9 / max | edit 26.7 / 115.7; categorize 16.0 / 23.2; new 7.1 / 10.5; log 5.9 / 10.7. Minimum 0.0 (never negative beyond the 1 s resolution of `timestamp`). None over 1 hour |

## B. Lateness induced by fixed-range files

A Databricks run ingests whatever files have landed, and a file lands only when its 5,000-offset
range fills. For each file, lateness of a row = newest event time among files landed strictly
before it minus the row's event time: the worst case, where a run fires just before the file lands.

| File (10/07 rows only) | Landed (UTC) | Rows | Kafka fill span | Max lateness |
|---|---|---|---|---|
| p02 5000..9999 | 23:13:03 | 3,211 | 352 s | 0 s (landed first) |
| p00 5000..9999 | 23:13:09 | 3,323 | 358 s | 355 s |
| p01 5000..9999 | 23:13:09 | 3,271 | 358 s | 364 s |

Of the 9,805 rows, the share later than: 30 s 61.0%, 60 s 55.6%, **120 s 44.9%**, 300 s 11.0%,
600 s 0%. Maximum 364 s, which is the file fill time.

**Reading:** in the worst-case run placement, the D3 provisional watermark of 2 minutes would drop
about 45% of a late file's rows from windowed aggregates. Lateness at bronze is dominated by the
pipeline's own batching (file fill time, 6 to 9 minutes at the observed 2,330 and 1,650
events/min), not by upstream disorder (emit delay at most 115.7 s here; probe disorder at most
51 s). The watermark must exceed the longest fill time plus upstream disorder.

## Limitations

- Three files of one evening; the worst-case placement assumes a run fires inside the few
  seconds between landings, which becomes likelier as partition offsets drift apart over time.
  The 24 h capture will measure lateness exactly as Spark sees it (per micro-batch), under the
  real schedule and the overnight traffic minimum.
