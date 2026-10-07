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
