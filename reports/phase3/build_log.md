# Phase 3 build log: bronze and silver on Lakeflow (2026-10-08)

Pipeline `wikistream` (serverless, Databricks Runtime 18.3, CURRENT channel; DBR 18 is the
Apache Spark 4.1 line, so local tests use PySpark 4.1.3), deployed by `databricks.yml`.

## Bronze

First manual update 13:04:59 to 13:06:16 UTC: 57 s of serverless start-up, 14 s of ingest.
`scripts/verify_bronze.py`:

| Check | After update 1 (cutoff 13:06:00) | After update 2 (cutoff 13:18:41) |
|---|---|---|
| Landed files expected / in bronze | 18 / 18 | 21 / 21 |
| Rows (= sum of the files' offset ranges) | 90,000 | 105,000 |
| Duplicate Kafka offsets | 0 | 0 |
| Producer duplicate `meta.id` | 0 | 0 |
| Rows in `_rescued_data` | 0 | 0 |

The schema generated from the Avro contract (decision D16) fit every real row; Auto Loader
picked up the nested `pNN/` folders. Job `wikistream-refresh` was unpaused after update 1.

## Silver

Second update 13:17:34 to 13:19:24 UTC. `scripts/verify_silver.py`:

| Measure | Value |
|---|---|
| Bronze rows | 105,000 |
| Silver rows (one per `meta_id`) | 104,998 |
| Quarantined | 2 (`emit_delay_window`) |
| Later duplicates dropped by AUTO CDC | 0 |
| Reconciliation: silver + quarantine + duplicates = bronze | 104,998 + 2 + 0 = 105,000 |
| `meta_id` in both silver and quarantine | 0 |
| Silver rows that are not the earliest copy | 0 |
| Expectation metrics (event log) | emit_delay_window 104,998 passed / 2 failed; the other three rules 105,000 / 0 |
| `dq_flags` set | none |

### The two quarantined rows

Both are Commons `log/upload` events for *File:King Features retro.png*, emitted 0.36 s apart
at 2026-10-08 12:54:50 UTC with `timestamp` values from 2018-01-10 (14:26 and 22:23): an emit
delay of about 8.7 years. They look like a file-history import replaying original upload
times. The rows are legitimate, not corrupt: the rule caught a real event-time anomaly, but its
premise (a large gap means a broken timestamp) does not hold here. 2 of 105,000 rows (0.002%).
See the open question below.

## First scheduled run

Job `wikistream-refresh`, trigger PERIODIC, 13:30:03 to 13:31:56 UTC, TERMINATED SUCCESS.
Verified afterwards: bronze 27 of 27 landed files, 135,000 rows exact, 0 duplicate offsets,
0 rescued; silver 134,993 + quarantine 7 + duplicates 0 = 135,000, one row per `meta_id`.

All 7 quarantined rows by then were Commons `log/upload` events for 2 file titles, with
`timestamp` values from 2007-08-25 to 2023-08-11 (3.2 to 19.1 years before emission): the
same historical-import pattern, so the 1-hour rule is catching legitimate events.

## Bugs caught by the local Spark 4.1 tests before deployment

1. **Malformed `meta.dt` would have failed the whole update.** Spark 4 (and DBR 18) run in ANSI
   mode, where `to_timestamp` throws on malformed input instead of returning NULL. Fixed with
   `try_to_timestamp`; `test_unparseable_meta_dt_fails_the_emit_delay_rule` covers it.
2. **An absurd epoch value would have failed the whole update.** `timestamp_seconds` overflows a
   long near 10^15 s (`ArithmeticException: long overflow`, job aborted). Event times outside
   years 1 to 9999 now become NULL and the row is quarantined;
   `test_absurd_epoch_is_quarantined_not_a_pipeline_failure` fails if the guard is removed
   (checked by removing it).
3. `F.not_` does not exist in PySpark 4.1, and module-level Column objects need an active
   session in classic PySpark; rules and flags are now SQL strings compiled lazily.

## Open question

Should events whose edit time is more than an hour before emission be quarantined (current
rule) or kept in silver with a `historical_event_time` flag, leaving quarantine for truly
broken timestamps (unparseable, in the future, or out of range)?

## Overnight capture, 2026-10-09: what ran, what stopped it, what the data shows

**Timeline (UTC).** 02:19 rule split deployed and silver fully refreshed from bronze (135,000 =
135,000, 0 quarantine, 7 flagged `historical_event_time`: the rebuild reproduced silver exactly).
02:20 Kafka restarted after the forced stop: watermarks 51,219 / 51,195 / 51,283, unchanged.
02:21:16 producer resumed with `--catch-up` from 2026-10-08 13:37:37; backfill of about 1.18 M
events at about 4,000/s; eqiad caught up to live at 02:26:55. Live until **04:12**, when the
**Claude desktop app's "stealth update after idle timeout" relaunched the app** (its log,
00:12:29 local: `[stealth-update] Triggering stealth update after idle timeout`), closing its
Terminal panel and killing the producer, the consumer and the user's `caffeinate` with SIGHUP.
No reboot (uptime 12 days), Kafka and the registry kept running. Lesson: long runs must not
live in an application's terminal.

**Audit since 2026-10-08 12:34 (`audit_topic.py`): reconciled.** eqiad 1,561,949 upstream
offsets, all accounted (1,561,934 delivered + 15 canaries), 0 gaps; codfw 193 accounted, with
11 offsets delivered twice. Databricks: 31 scheduled runs since 02:00, all SUCCESS (the 02:30
run absorbed the backfill in 175 s).

**Bronze** (cutoff 17:30:03): 319 of 319 files, 1,595,000 rows exact, 0 duplicate Kafka offsets,
11 producer-duplicate `meta.id`, 0 rescued. **Silver:** 1,594,989 = 1,595,000 - 11 duplicates
dropped, 0 quarantine, one row per `meta_id`, `not_first_copy` 0: on the first real duplicates,
AUTO CDC kept the earliest copy as designed (D17). 14 rows flagged `historical_event_time`.

**The 11 duplicates, explained.** All are consecutive codfw upstream offsets 2086017512..522
(emitted 13:17 to 13:21 on 10-08); both copies of each sit in the same partition; every second
copy was produced at 02:21:16, the first instant of the backfill. Cause: after each SSE
reconnect, EventStreams reports a quiet topic's position as `offset: -1` until that topic sends
an event (observed: codfw went from 13:21:23.667 to -1 at p0@48371, 13:33:45.162, right after
reconnect #5 at 13:33:43). Every tail message at the pause carried codfw = -1, so the resume
merge fell back to an older codfw timestamp (13:15:00.001, a canary's cursor in the drops-ledger
tail) and re-sent 13:15 to 13:21. Duplicates are the benign outcome; had no older codfw
timestamp existed, the resume would have sent codfw = -1, which can skip a quiet topic's events.
No loss occurred (codfw 193 of 193 accounted), but the cursor logic should not depend on luck.
