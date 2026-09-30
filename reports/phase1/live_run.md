# Phase 1: first live producer run

**Date:** 2026-09-30, about 23:13 to 23:17 UTC. Local stack from `docker-compose.yml`;
topics and schema created by `scripts/bootstrap_kafka.py` (schema id 53, FULL_TRANSITIVE).

## Runs

| Run | Command | Received | To main | DLQ | Dropped (canary) | SSE reconnects | Undelivered at exit |
|---|---|---|---|---|---|---|---|
| 1 | `wikistream-producer --max-seconds 180 --from-now` | 7,952 | 7,950 | 0 | 2 | 1 (server disconnect mid-run) | 0 |
| 2 | `wikistream-producer --max-seconds 60` (resume from Kafka) | 2,992 | 2,992 | 0 | 0 | 0 | 0 |

Run 2 resumed from the merged cursor read off the partition tails, covering both upstream
datacenters: codfw at 1790810100001 ms and eqiad at 1790810189785 ms.

## Audit (`scripts/audit_topic.py`)

| Measure | Value |
|---|---|
| Messages in topic | 10,942 (= 7,950 + 2,992) |
| Distinct `meta.id` | 10,942 |
| Duplicates | **0**, across one SSE reconnect and one process restart |
| eqiad upstream offsets | 6570331421..6570342363: 10,943 expected, 10,942 present, 1 missing |
| Unknown upstream fields (`_extra_json`) | none |

**The one missing offset, 6570336730, is a canary event**, verified by replaying upstream
history with `?since=2026-09-30T23:15:28.000Z`: `meta.domain = canary`,
`meta.dt = 23:15:00.000Z` (canaries are stamped on the minute), between two real edits.
The second dropped canary came from codfw, which carried no other events in this window;
that is why the codfw position appears in the resume cursor.

## Limitations

- Zero duplicates at one restart is one observation, not a guarantee; the at-least-once
  claim (decision A) stands, and the Phase 4 kill drill measures it under a hard kill
  rather than a clean stop.
- The audit could not explain the canary gap on its own; it took a manual upstream replay.

## Run 3: after adding the drops ledger (decision D8)

`wikistream-producer --from-now --max-seconds 240`, 2026-09-30 23:27:24 to 23:31:24 UTC,
then `scripts/audit_topic.py --since 2026-09-30T23:27:24Z`:

| Measure | Value |
|---|---|
| Delivered to main | 9,253 (about 2,313/min) |
| Dead letters / drops | 0 / 0 |
| Distinct `meta.id` | 9,253; duplicates 0 |
| eqiad offsets 6570366884..6570376135 | 9,252 expected, 9,252 accounted, 0 missing |
| codfw | 1 event, accounted |
| Verdict | **reconciled** (exit code 0) |

No canary arrived in this window, so the ledger's live path is not yet observed; it is
covered by `tests/test_producer_integration.py` and will be exercised by the 24 h capture.
