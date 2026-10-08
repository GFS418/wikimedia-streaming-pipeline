# Phase 2: S3 landing consumer, first live run

**Date:** 2026-10-07, 23:05 to 23:20 UTC. Local Kafka stack; bucket
`wikistream-landing-060647991548` (us-east-2); consumer runs as role `wikistream-consumer`
(CLI profile of the same name, assumed from the default profile).

## 1. The consumer role can upload and list, nothing else

`scripts/check_consumer_iam.py`: 13 of 13 checks pass.

| Check | Expected | Result |
|---|---|---|
| Simulator: PutObject `landing/recentchange/p00/...` | allowed | allowed |
| Simulator: PutObject `landing/_spike/...`, `other/...` | denied | implicitDeny (both) |
| Simulator: GetObject, DeleteObject, DeleteObjectVersion on `landing/recentchange/...` | denied | implicitDeny (all three) |
| Simulator: PutBucketPolicy | denied | implicitDeny |
| Simulator: ListBucket, prefix `landing/recentchange/p00/` | allowed | allowed |
| Simulator: ListBucket, prefix `landing/` or `landing/_spike/` | denied | implicitDeny (both) |
| Real call as role: list `landing/recentchange/p00/` | allowed | allowed |
| Real call as role: list `landing/_spike/` | denied | AccessDenied |
| Real call as role: read `landing/_spike/recentchange/spike-000.jsonl` | denied | AccessDenied |

Destructive actions were checked only in the simulator; real calls were limited to list and read.

## 2. Runs

| Run | Window (UTC) | What | Result |
|---|---|---|---|
| Consumer | 23:06:45 to 23:15:45 | `wikistream-consumer --max-seconds 540`; S3 empty, so resumed at offset 0 on all 3 partitions | 6 files, 30,000 rows, 7,103,970 bytes; 5,364 rows of incomplete ranges left in Kafka, never written |
| Producer A | 23:07:09 to 23:13:09 | `wikistream-producer --from-now --max-seconds 360` | 9,907 delivered (about 1,650/min); 0 DLQ, 0 drops, 0 undelivered |
| Producer B | 23:13:45 to 23:17:45 | `wikistream-producer --max-seconds 240`, resumed from the Kafka-derived cursor (eqiad 1791414789811 ms = 23:13:09.811) | 9,271 delivered, **2 canaries dropped to the ledger**, 0 undelivered |

Files landed:

| Partition | 0..4999 (2026-09-30 data, landed 23:06:47-48) | 5000..9999 (mixed 09-30 and 10-07 data, landed 23:13:03-09) |
|---|---|---|
| p00 | 1,194,506 B | 1,172,776 B |
| p01 | 1,197,195 B | 1,174,315 B |
| p02 | 1,200,620 B | 1,164,558 B |

Mean 1.18 MB per 5,000-row file (D10 estimated 1.21 MB). p00's second file landed 0.85 s
before producer A stopped; that partition ended run A at offset 10,014, 15 past the range end.

## 3. Landing verification (`scripts/verify_landing.py`)

| Check | Result |
|---|---|
| Files / rows | 6 / 30,000 |
| Body MD5 == S3 ETag | 6 of 6 |
| gzip mtime 0; offsets exactly cover each key's range; partition and key consistent | 6 of 6 |
| Ranges contiguous per partition, no overlaps | yes (each partition landed through 9,999) |
| S3 versions per key | 1 for every key (no rewrites) |
| **Re-derived from Kafka in a separate process, byte-identical** | **6 of 6** |
| Failures | 0; verdict `ok` |

The re-derivation uses the consumer's own encoding functions, so it proves determinism and
integrity (same offsets give the same bytes at a different time, in a different process),
not an independent reimplementation. The verifier's ability to fail is tested:
`test_verifier_passes_a_clean_landing_and_catches_tampering` rewrites one landed file with one
field changed and the verifier reports it.

## 4. Upstream reconciliation across the producer restart

`scripts/audit_topic.py --since 2026-10-07T23:07:09Z`: verdict **reconciled**.

| Upstream | Range | Expected | Accounted |
|---|---|---|---|
| eqiad | 6592256935..6592276113 | 19,179 | 19,178 main + 1 drops ledger |
| codfw | 2086017385 | 1 | 1 drops ledger |

0 duplicate `meta.id` and 0 missing offsets across the resume boundary; 19,178 = 9,907 + 9,271.
The drops ledger's first live use: both canaries (one per datacenter) were dropped between
23:14:45 and 23:15:45, and the audit closed their gaps with no manual replay (on 2026-09-30
the same situation needed a hand replay of upstream history).

## 5. Finding: Kafka retention is per segment, so the D11 bound was wrong

Each partition's first segment holds offsets 0 to about 10,014: the 2026-09-30 records and
the first 10-07 run. Kafka rolled to a new segment (base offsets 10014 / 10015 / 10073) at the
first append after the segment's oldest record turned 7 days old (`segment.ms` = 7 days).
Kafka deletes a segment when its *newest* record passes `retention.ms`, so the 09-30 records
will be deleted around 2026-10-14 23:13, not 10-07 as first estimated. A record can live up to
`retention.ms + segment.ms` = 14 days. The Terraform guard on S3 landing retention was
corrected from `> 7` to `> 14` days (no infrastructure change; 30 days still satisfies it).

## Limitations

- One clean consumer run. The hard-kill drill (SIGKILL mid-range and mid-PUT) and a
  multi-consumer rebalance are Phase 4; until then the crash evidence is the moto-backed test
  in which an interrupted run produces files byte-identical to an uninterrupted one.
- Evening traffic (about 1,650/min) was 30% below the 2026-09-30 rate; file fill times scale
  accordingly.

## 6. Follow-ups applied the same evening (decisions D13, D14)

- **Topic configs reconciled:** `scripts/bootstrap_kafka.py` reported
  `updated (segment.ms: 604800000 -> 86400000)` for all three topics, then `exists` on a
  second run.
- **Stale-cursor guard, live:** a plain `wikistream-producer --max-seconds 1` at 23:46:11
  found a cursor 28.4 minutes old (newest position 23:17:45.449), under the 30-minute limit,
  so it correctly resumed and replayed 189 events in its 1-second run. With
  `--max-cursor-age-minutes 1` it refused: exit code 2, zero connections to Wikimedia.
- **Test suite:** 145 tests; consumer integration tests now stop on a condition
  (`until_caught_up`) instead of a fixed 8-second window, halving suite time to about 37 s.
  One unexplained failure occurred in 14 suite runs (the output was truncated before the
  failure summary); the 13 runs since passed, and full output is now kept for every run.
