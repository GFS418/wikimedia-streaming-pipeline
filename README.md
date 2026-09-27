# Wikimedia Streaming Pipeline

Streaming pipeline over live Wikimedia edits (EventStreams `recentchange`):
Kafka with a schema registry and dead-letter topic → S3 landing with offset-range files →
Databricks Auto Loader → Delta bronze/silver/gold, with every delivery guarantee proven by a
failure drill.

**Status:** Phase 0 (architecture decided, feed probed, local stack scaffolded).

## Architecture (Option A)

```
Wikimedia SSE ──> producer ──> Kafka (local, Docker) ──> consumer ──> S3 (offset-range files)
                     │            + Schema Registry         │                 │
                     └──> DLQ topic               commit after PUT    Databricks Auto Loader
                                                                     (Trigger.AvailableNow, scheduled)
                                                                              │
                                                                  Delta bronze → silver → gold
```

Why this shape: a cloud Databricks workspace cannot reach a broker on a laptop, and serverless
compute (including Free Edition) only supports `Trigger.AvailableNow`, not always-on triggers.
Decoupling through S3 makes both constraints irrelevant and makes exactly-once a mechanism
(deterministic file names + Auto Loader's checkpoint) rather than a config flag.

## Local setup

```bash
uv sync
docker compose up -d
```

Kafka: `localhost:9092`. Schema Registry: `http://localhost:8081`.

## Layout

```
src/wikistream/   producer, consumer, shared code
scripts/          probes and one-off tools (probe_sse.py, analyze_probe.py)
databricks/       Auto Loader / Delta notebooks and job definitions
infra/            Terraform (S3, IAM, budget alarm)
reports/          measured results; every headline number traces here
tests/
```
