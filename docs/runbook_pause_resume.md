# Runbook: pausing and resuming the capture

**Since 2026-10-09 (D23) the capture runs as Docker services:**

```bash
docker compose --profile capture up -d --build   # start, or resume after any stop (auto catch-up)
docker compose --profile capture stop            # pause cleanly (60 s grace: flush, then exit)
docker compose logs -f producer consumer         # watch
```

Keep the Mac awake from macOS **Terminal.app** (`caffeinate -ims`), never from the Claude app's
Terminal panel: the app can silently relaunch to update (it did at 04:12 UTC on 2026-10-09)
and closing its panel kills whatever runs there. The sections below describe the manual
procedure used before the services existed.

The pipeline can stop for hours and resume with no data loss:

- **Producer:** resumes from the cursor read off Kafka's partition tails (decision B).
  Wikimedia keeps 7 to 31 days of history, so `--catch-up` backfills the whole gap.
  The stale-cursor guard (D13) refuses a resume older than 30 minutes unless you choose.
- **Consumer:** resumes after the last range recorded in S3 (D11); partial ranges are re-read.
- **Kafka:** keeps at least 7 days (D14), so nothing waiting in it expires during a pause.

**Resume within 7 days**, or Wikimedia's history may no longer cover the gap.

## Pause (done 2026-10-08, 13:38 UTC)

1. Ctrl-C the producer and consumer. **Use `tee -ai`**, not `tee -a`: Ctrl-C reaches the
   whole pipeline, and a plain `tee` dies first and loses the final log lines (it happened
   on 2026-10-08; the data was verified intact by the audit instead).
2. Audit the window: `uv run python scripts/audit_topic.py --since <capture start>`.
3. Pause the Databricks schedule: `databricks bundle deploy --var="schedule_pause_status=PAUSED"`.
4. Stop the stack with a generous grace period: `docker compose stop -t 60`. (On
   2026-10-08 the default 10 s was too short and Kafka was killed with exit code 137; a
   process kill, not a power loss, so its files are intact, but the next start runs log
   recovery.)

State at the 2026-10-08 pause:

| Item | Value |
|---|---|
| Capture window audited | 12:34:00 to 13:37:37 UTC: 114,135 messages + 2 canaries, 0 gaps, 0 duplicates, reconciled |
| Resume cursor (newest) | eqiad 1791466657056 ms = 2026-10-08 13:37:37.056 UTC |
| Kafka high watermarks (main topic p0/p1/p2) | 51,219 / 51,195 / 51,283 |
| Landed in S3 | through offset 49,999 on every partition |
| Databricks job `wikistream-refresh` | PAUSED |

## Resume

1. Start Docker Desktop, then `docker compose up -d --wait`.
2. Check Kafka recovered intact: high watermarks must equal the table above.
3. Producer, replaying the gap from Wikimedia's history:
   `uv run wikistream-producer --catch-up --max-seconds 86400 2>&1 | tee -ai logs/capture_producer.log`
4. Consumer: `uv run wikistream-consumer --max-seconds 90000 2>&1 | tee -ai logs/capture_consumer.log`
5. Unpause Databricks: `databricks bundle deploy` (the default is UNPAUSED).
6. After the backfill, audit from the original capture start; the gap must reconcile to
   0 missing offsets.

For the lateness measurement (D19), catch-up periods are not normal operation: rows whose
Kafka timestamp lags `meta.dt` by more than 2 minutes are replayed backlog, excluded from the
watermark analysis and reported separately as an outage-recovery scenario.

## Pending at the 2026-10-08 pause

The D18 amendment (rule split) is coded and its tests updated, but **not yet tested or
deployed**. On resume, before restarting the capture: run `tests/test_silver_rules.py`, then
`databricks bundle deploy --var="schedule_pause_status=PAUSED"`, then
`databricks bundle run wikistream --full-refresh silver_checked --full-refresh silver_recentchange --full-refresh silver_quarantine`,
then `scripts/verify_silver.py` (expect the historical rows in silver, flagged).
