# Decision log

Each entry: the decision, the evidence it rests on, and what it costs. Newest last.

## D1. Architecture: Kafka → S3 → Databricks Auto Loader (2026-09-26)

- **Decision:** local Kafka in Docker; our own Python consumer writes offset-range files to
  S3; Databricks Auto Loader streams them into Delta on a schedule with
  `Trigger.AvailableNow`.
- **Evidence:** a cloud workspace cannot reach a laptop broker; serverless compute
  (including Free Edition) rejects `ProcessingTime` and `Continuous` triggers
  (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`); Free Edition restricts outbound internet,
  which makes a managed-Kafka direct read risky.
- **Cost:** minute-level latency, not seconds. Two systems to operate instead of one.

## D2. Own consumer, not Kafka Connect S3 sink (2026-09-26)

- **Decision:** a hand-written consumer: offset-range object keys, manual commit only after
  a successful PUT.
- **Why:** the commit-after-durable-write logic is the guarantee; owning it makes it
  explainable and testable. Connect is the industry default and the README says so.

## D3. Event time, watermark, key, partitions (2026-09-27)

- **Decision:** event time = `timestamp` (edit time); provisional watermark 2 min; Kafka key
  = `meta.id`; 3 partitions; producer stamps upstream topic/offset in message headers.
- **Evidence:** [`reports/probe/findings.md`](../reports/probe/findings.md). Disorder on
  `timestamp`: p99.99 33 s, max 51 s, 0 drops at 60 s (17-min low-traffic sample).
  Key = `wiki` puts 36.2% of events on one of 6 partitions (ideal 16.7%); `meta.id` gives
  17.1%.
- **Cost:** no per-wiki ordering in Kafka (nothing downstream needs it). Watermark must be
  re-measured on a 24 h capture.

## D4. Databricks Free Edition (2026-09-28)

- **Decision:** Free Edition, us-east-2, read-only external location on `landing/`.
- **Evidence:** [`reports/spike/findings.md`](../reports/spike/findings.md): credential,
  external location, and Auto Loader all pass; rerun and same-key rewrite add 0 rows.
- **Consequence:** Auto Loader skips a processed path regardless of content, so consumer
  output per offset range **must be byte-deterministic**, or a rewrite silently loses data.

## D5. Avro with our own schema (2026-09-30)

- **Decision:** Avro in Kafka via Confluent Schema Registry. `log_params` (array, object or
  string upstream) is carried as a JSON string. The consumer decodes and writes canonical
  JSON lines (sorted keys, fixed separators) to S3, with the writer `schema_id` per row.
- **Evidence:** on 500 real events, Avro is 35% smaller per message but only 6% smaller
  after zstd batch compression (76.4 vs 81.4 KB), so size did not decide it. Upstream
  Wikimedia schema is JSON Schema draft-07 with 3 required fields and an open content
  model, so it is not a usable contract. Confluent's JSON Schema rules make "add an optional
  field" backward compatible only for closed schemas, which then reject new upstream
  fields; Avro's "add a field with a default" is compatible in both directions. Avro
  round-trip of all 500 events, `log_params` included: 0 mismatches.
- **Cost:** we maintain the schema; `log_params` loses inner typing; binary messages need
  tooling to inspect; Avro container files are not byte-deterministic (random sync marker),
  hence JSONL on S3.

## D6. Compatibility mode FULL_TRANSITIVE (2026-09-30)

- **Decision:** `FULL_TRANSITIVE` on the value subject.
- **Why:** replays from Kafka or S3 mean new readers read old data across many versions
  (backward, transitive); the producer notices upstream drift and is upgraded first while
  the old consumer runs (forward).
- **Cost:** only optional fields with defaults can be added or removed; no required
  additions, no in-place renames.
- **Verified on the registry** (`tests/test_registry_compat.py`): add/remove optional field
  accepted; add required, remove required, narrow long to int rejected. A v3 that re-adds
  a removed field with a different type is accepted under FULL but rejected under
  FULL_TRANSITIVE. Caveat: renaming an optional field is **accepted** (it is structurally
  an add plus a remove) and old data would silently read as null, so renames follow an
  add-then-deprecate policy; the registry check is structural, not semantic.

## D7. Three-tier bad-data handling + `_extra_json` (2026-09-30)

| Tier | Events | Action |
|---|---|---|
| Dead-letter topic | not JSON; missing or mistyped required fields (`meta.id`, `meta.dt`, `type`, `wiki`, `timestamp` int, `bot` bool); any other Avro serialization failure | raw bytes + headers: error class, detail, upstream topic/offset, producer version |
| Drop and count | canary events (`meta.domain == "canary"`, per Wikimedia docs) | metric only |
| Main topic, silver expectations | unknown `type`, `timestamp` far from `meta.dt`, negative lengths, ... | rows kept and counted in silver |

- Unknown upstream fields go into `_extra_json` (a JSON string) and are counted per field
  name, so a loose upstream never loses data silently; the count is the signal to evolve
  the schema.
- **Evidence:** 0 malformed and 0 canary events in 39,621 probed, so the dead-letter path
  must be exercised with injected faults. `id` is absent on some log events
  (`abusefilter`, `thanks`, `patrol`), which is legitimate, so it is nullable.

## D8. Drops ledger and offset reconciliation (2026-09-30)

- **Decision:** every deliberately dropped event (canaries) writes a small JSON record
  (reason + upstream topic/partition/offset) to `wikistream.recentchange.drops`
  (1 partition, 30-day retention); dead letters carry the upstream position in headers
  when their `meta` is intact. `wikistream/audit.py` then reconciles every upstream offset:
  delivered + dead-lettered + dropped, with duplicates and cross-source conflicts reported
  separately.
- **Evidence:** the first live run left one unexplained upstream gap (6570336730) that
  took a manual replay of upstream history to identify as a canary. With the ledger, the
  audit closes such gaps on its own (`tests/test_audit.py` reproduces that exact case).
- **Cost:** a third topic; JSON rather than Avro for the ledger (an internal operations
  log, so a second registry subject would be process without benefit). Dead letters whose
  JSON is unparseable have no position; the audit can only bound them
  (`explainable_by_unattributed_dlq`).

## D9. Files cut at fixed offset boundaries (2026-09-30)

- **Decision:** file *k* of a partition holds offsets `k*N .. k*N+N-1`, whatever the time.
  No time-based flush.
- **Why:** a crash can never produce two differently named files covering the same
  offsets, so Kafka -> bronze stays exactly-once per offset. With a time flush, a range
  written as 100-149 before a crash could be rewritten as 100-199 after it; Auto Loader
  would ingest 100-149 twice.
- **Cost:** when traffic stops (e.g. the producer is stopped), the last partial range per
  partition waits in Kafka until more events arrive. Delayed, never lost.

## D10. 5,000 offsets per file, gzip level 5 (2026-09-30)

- **Evidence** (5,000 real rows from the topic): canonical JSONL rows average 1,948 B;
  gzip-5 gives 8.04x in a median 44 ms per file vs gzip-6 8.24x in 53 ms. At about 771
  events/min per partition a file fills in about 6.5 min; about 666 files/day. 30-day
  storage if run continuously: about 24 GB, $0.56/month (uncompressed would be about
  195 GB, about $4.50/month, nearly the whole $5 budget).
- **Determinism:** gzip with `mtime=0` produced identical bytes across runs; with the
  default mtime it did not. The correctness requirement is identical *decompressed*
  content per key (Auto Loader skips a processed path); identical bytes are what the
  drills check via ETags, and hold for the same zlib (1.2.12 here).

## D11. S3 is the consumer's record of progress (2026-09-30)

- **Decision:** no Kafka offset commits. On partition assignment the consumer lists its
  S3 prefix for that partition and resumes after the highest range written. An S3 PUT is
  atomic, so the write *is* the commit; there is no "written but not committed" window.
- **Why:** self-contained; one source of truth for what reached the landing zone.
- **Costs and invariants:** the consumer needs `s3:ListBucket` on its prefix. S3
  retention (30 days) must exceed Kafka's *worst-case* retention, or expired files would make
  the consumer re-ingest offsets Kafka still holds; enforced by a Terraform validation. Kafka's
  standard lag tooling sees no committed offsets, so Phase 4 computes lag as
  high watermark minus the S3-derived position.
- **Correction (2026-10-07):** the bound was first stated as Kafka's 7-day `retention.ms`.
  Kafka deletes whole segments once their newest record passes retention, and rolls segments
  every `segment.ms` (7 days), so a record can survive up to 14 days; observed live when
  2026-09-30 records shared a segment with 2026-10-07 records
  ([`reports/phase2/live_run.md`](../reports/phase2/live_run.md)). The validation now
  requires more than 14 days.

## D12. Key layout, row envelope, upload-only role (2026-09-30)

- **Keys:** `landing/recentchange/p{partition:02d}/{start:020d}-{end:020d}.jsonl.gz`.
  Zero-padded so lexical order is numeric; no `key=value` directories, which Auto Loader
  would infer as partition columns.
- **Rows:** `{"kafka": {topic, partition, offset, timestamp_ms, key, schema_id, headers},
  "value": {...Avro record...}}`, canonical JSON. Every field is immutable per offset;
  deliberately no write time.
- **Credentials:** role `wikistream-consumer` (Terraform): `s3:PutObject` on
  `landing/recentchange/*` and `s3:ListBucket` on that prefix only. No read, no delete.

## D13. Never resume silently from a stale upstream cursor (2026-10-07)

- **Decision:** before connecting upstream, the producer compares the newest timestamp in
  its Kafka-derived cursor with the clock. Older than 30 minutes (`--max-cursor-age-minutes`)
  means it refuses (exit code 2) unless the operator chooses `--catch-up` (replay the
  backlog) or `--from-now` (skip it; the audit then shows the gap). A cursor with no
  timestamps counts as stale.
- **Evidence:** on 2026-10-07 the stored cursor was 7 days old; a plain start would have
  replayed a week of Wikimedia history (millions of events at 1,650 to 2,300/min). Live
  demo: refusal with exit code 2 and zero upstream connections
  ([`reports/phase2/live_run.md`](../reports/phase2/live_run.md)). 30 minutes is at most
  about 70,000 events, which the producer catches up in a minute or two.
- **Cost:** one more flag in operations. The newest timestamp across upstream datacenters
  is used, so a quiet datacenter (codfw often carries only canaries) is not mistaken for
  downtime.

## D14. One-day segments and config reconciliation (2026-10-07)

- **Decision:** `segment.ms` = 1 day on all three topics, and `bootstrap` now reconciles the
  configs of existing topics (incremental alter) instead of only creating missing ones.
- **Why:** Kafka retains whole segments until their newest record passes `retention.ms`, so
  records live up to `retention.ms + segment.ms`; with the 7-day broker default that was
  14 days for a 7-day topic. One-day segments bound it at 8. Without reconciliation the
  live topics would have kept 7-day segments silently.
- **Evidence:** live topics reported `updated (segment.ms: 604800000 -> 86400000)` on the
  first run and `exists` on the second; `tests/test_admin_integration.py` covers both and
  checks that a partition-count mismatch is refused, never "repaired".

## D15. Lakeflow Declarative Pipelines for bronze, silver and gold (2026-10-08)

- **Decision:** one serverless Lakeflow pipeline holds all three layers; rows rejected by
  silver checks are kept in a separate quarantine table, not dropped.
- **Why:** the user's call, for the Lakeflow / Delta Live Tables résumé keyword, accepting
  the costs below.
- **Costs, stated precisely:** run time is not the cost; development speed and control are.
  Pipeline code only runs inside a Databricks pipeline update (about 1 to 2 minutes of
  start-up per test run), so transformation logic lives in plain PySpark helpers under
  `pipeline/wikistream_lake/` that are unit-tested locally, and the pipeline files only wire
  them together. Free Edition allows one active pipeline, which holds every layer; there is
  no side-by-side dev copy. The pipeline owns checkpoints, so the Phase 4 drills use stop and
  restart, full refresh and `reset_checkpoint_selection` instead of deleting checkpoint
  folders. Windowed aggregates are emitted only once the watermark passes (append
  semantics), so gold has no provisional numbers.

## D16. Bronze schema generated from the Avro contract (2026-10-08)

- **Decision:** `wikistream/lake_schema.py` turns the `.avsc` into the Spark schema of a
  landing row; `scripts/generate_landing_schema.py` writes it to
  `pipeline/wikistream_lake/landing_schema.py`, committed; `tests/test_lake_schema.py`
  fails if the copy is stale and checks real landed rows fit it with nothing to rescue.
  Auto Loader reads with that schema and `rescuedDataColumn`.
- **Why:** one source of truth, and schema evolution only through a registry-checked change.

## D17. Silver deduplication with AUTO CDC on meta.id (2026-10-08)

- **Decision:** the MERGE of decision P3-3, expressed declaratively: an AUTO CDC flow into
  silver keyed on `meta.id`, SCD type 1, sequenced by `(kafka.timestamp_ms,
  kafka.partition, kafka.offset)` so ties are deterministic.
- **Why:** correct under any replay, including a full refresh from bronze.
- **Consequence:** an AUTO CDC target receives updates, so it cannot be a plain streaming
  source; per the docs, downstream streaming must read its change feed. How gold reads
  silver is settled with the watermark (D19).

## D18. Silver checks: quarantine rules and dq_flags (2026-10-08)

- **Quarantine** (row moved to `silver_quarantine` with the names of every failed rule):
  1. `structure_by_type`: edit has revision.old/new and length.old/new; new has
     revision.new and length.new and no old values; log has log_type and log_action;
     categorize has no revision or length. Unknown types have no structure rule.
  2. `non_negative_lengths`: length.old and length.new are not negative.
  3. `emit_delay_window`: `meta.dt - timestamp` is between -1 s and 1 hour (observed 0.0 to
     115.7 s); an unparseable `meta.dt` fails it.
  4. `wiki_name_format`: `wiki` matches `^[a-z0-9_]+$`.
- **Flag but keep** (`dq_flags` array on silver rows, so data stays usable and is easy to
  exclude with one filter): `unknown_type` (a `type` outside edit/new/log/categorize/
  external), `extra_fields` (`_extra_json` present: upstream fields not yet in the
  contract), `rescued_data` (bronze put something in `_rescued_data`). Gold counts flagged
  types in an `other` column so totals still reconcile.
- **Not a rule:** `namespace >= 0`; the 12 namespace -1 rows in the profile are legitimate
  log events on Special pages.
- **Evidence:** every rule passes on all 30,000 profiled rows
  ([`reports/phase3/pre_build_evidence.md`](../reports/phase3/pre_build_evidence.md)), so the
  quarantine path is exercised with injected bad rows in tests.

## D19. Watermark measured over a 24-hour capture (2026-10-08)

- **Decision:** the gold watermark is the smallest value that drops at most 0.01% of rows,
  measured over 24 hours of real traffic, including the overnight low (the longest
  file-fill times).
- **Capture:** started 2026-10-08 12:34:00 UTC, producer `--from-now --max-seconds 86400`,
  consumer resumed from S3 at offset 10000 on every partition; logs in `logs/`.

## D20. Bot share as a view over gold (2026-10-08)

- **Decision:** bot share per minute is a view summing `gold_wiki_minute`; no second
  streaming aggregation or state.

## D21. Databricks Asset Bundle: pipeline, schema, scheduled job (2026-10-08)

- **Decision:** `databricks.yml` defines the UC schema `workspace.wikistream`, the
  pipeline, and job `wikistream-refresh` that triggers a pipeline update every 30 minutes.
  The three layers are tables inside the one pipeline, so the job has one pipeline task
  (decision P3-7's "three tasks" maps onto the pipeline's own dependency graph). The
  schedule is deployed paused and unpaused only after a manual run is validated. Only
  `pipeline/` is uploaded (`sync.paths`).

## D18 amendment and D19 amendment (2026-10-08)

- **D18:** the `emit_delay_window` rule (edit time within 1 hour of emission) quarantined
  legitimate events: all 7 rows it caught were Commons upload logs replaying 2007-2023
  upload times. Split into a quarantine rule `event_time_valid` (meta.dt parses, event time in
  range, edit not more than 1 s after its own emission) and a flag `historical_event_time`
  (edit more than 1 hour before emission; kept in silver, excluded from gold explicitly).
  Silver is fully refreshed from bronze to re-route the existing rows.
- **D19:** the user's laptop cannot stay awake for 24 hours, so the capture becomes several
  live windows, at least one overnight on power to cover the global traffic low. Catch-up
  periods after a pause (Kafka timestamp lagging `meta.dt` by more than 2 minutes) are
  excluded from the lateness measurement and reported separately as an outage-recovery
  scenario, which doubles as a Phase 4 drill.

## D22. Sticky upstream positions in the cursor (2026-10-09)

- **Decision:** the producer keeps its own cursor, merging each server-sent SSE id into the
  previous cursor so no topic's position can regress (`cursor.advance`); that cursor is what
  each message stores and what reconnects resume from. On resume, any partition whose tail
  cursor lacks a timestamp for an upstream topic is scanned back (headers only) for that
  topic's latest stored position.
- **Evidence:** after every reconnect, EventStreams reports a quiet topic's position as
  `offset: -1` until it sends an event (codfw 13:21:23.667 -> -1 at 13:33:45.162, right after
  reconnect #5). On 2026-10-09 the resume fell back to an older codfw timestamp from the
  drops-ledger tail and re-sent 11 events (absorbed by silver). With no older timestamp, it
  could have skipped codfw events instead. On the live topics the repaired resume found codfw
  at 04:09:54.401 instead of the stale 03:15:00.001.
- **Tests:** the observed sequence (`test_advance_keeps_a_quiet_topics_position_...`), the
  back-scan behind reset tails with an older drops-ledger timestamp present, and the running
  producer stamping and reconnecting with the sticky cursor (fails if reverted, checked).
- **Cost:** resuming by timestamp can still re-send an event at the exact boundary; silver
  removes it (D17).

## D23. The capture runs as Docker services (2026-10-09)

- **Decision:** producer and consumer are `docker compose` services (profile `capture`,
  `restart: unless-stopped`, 60 s stop grace, rotated json logs), built from the project's
  Dockerfile (Python 3.12.14, `uv sync --no-dev`, uid 501). The unattended producer always
  uses `--catch-up` within 7 days. The consumer assumes its upload-only role in-container
  from a read-only `~/.aws` mount (verified: `assumed-role/wikistream-consumer`).
- **Why:** on 2026-10-09 at 04:12 UTC the Claude desktop app's silent auto-update relaunched
  the app and closed its Terminal panel, killing the capture (and `caffeinate`) mid-night.
  Services are independent of any terminal, restart after crashes, and stop cleanly.
- **Cost:** a 527 MB image; the broad source credentials are readable inside the consumer
  container (read-only), the same exposure the host process already had. Keeping the Mac
  awake stays the user's step, in macOS Terminal.app.

## D19 amendment 2. Lateness simulated over the full day, validated on live windows (2026-10-09)

- **Decision:** backfill all gaps from Wikimedia's history so upstream data is continuous from
  2026-10-08 12:34 UTC; simulate the pipeline's lateness for every hour from it (file
  boundaries from Kafka offsets, landing times from each range's last event, the 30-minute
  schedule); accept the simulation only where it matches measured lateness in the live
  windows (10-08 12:34-13:37 and 10-09 02:27-04:12, plus any live time after the backfill).
- **Why:** the laptop cannot run unattended overnight reliably; the backfill covers every hour
  of the day, including the global traffic low that sets the watermark.
