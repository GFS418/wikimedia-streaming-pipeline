"""Shared settings. Defaults match docker-compose.yml; override with WIKISTREAM_* env vars."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from wikistream import __version__

DAY_MS = 24 * 3600 * 1000


def _env(name: str, default: str) -> str:
    return os.environ.get(f"WIKISTREAM_{name}", default)


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int
    # Decision D14: Kafka deletes a segment only when its NEWEST record passes retention.ms,
    # and rolls segments every segment.ms, so records live up to retention.ms + segment.ms.
    # The broker default (7 days) let records live 14 days; 1 day bounds it at 8.
    segment_ms: int = DAY_MS

    @property
    def config(self) -> dict[str, str]:
        return {
            "retention.ms": str(self.retention_ms),
            "segment.ms": str(self.segment_ms),
            "cleanup.policy": "delete",
        }


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = field(default_factory=lambda: _env("BOOTSTRAP", "localhost:9092"))
    schema_registry_url: str = field(
        default_factory=lambda: _env("SCHEMA_REGISTRY", "http://localhost:8081")
    )
    stream_url: str = field(
        default_factory=lambda: _env(
            "STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"
        )
    )
    user_agent: str = (
        f"wikistream/{__version__} (https://github.com/GFS418/wikimedia-streaming-pipeline)"
    )
    # Decision D3: 3 partitions keyed by meta.id; 7-day replay window (~3.8 GB compressed).
    main_topic: TopicSpec = field(
        default_factory=lambda: TopicSpec(_env("TOPIC", "wikistream.recentchange"), 3, 7 * DAY_MS)
    )
    # Decision D7: dead letters are rare and need investigating, so keep them longer.
    dlq_topic: TopicSpec = field(
        default_factory=lambda: TopicSpec(
            _env("DLQ_TOPIC", "wikistream.recentchange.dlq"), 1, 30 * DAY_MS
        )
    )
    # Decision D8: one small JSON record per deliberately dropped event (canaries), so the
    # audit can account for every upstream offset: delivered + dead-lettered + dropped.
    drops_topic: TopicSpec = field(
        default_factory=lambda: TopicSpec(
            _env("DROPS_TOPIC", "wikistream.recentchange.drops"), 1, 30 * DAY_MS
        )
    )
    compatibility: str = "FULL_TRANSITIVE"  # decision D6

    # Landing zone (decisions D9-D12). Bucket name is a Terraform output, not a secret.
    landing_bucket: str = field(
        default_factory=lambda: _env("LANDING_BUCKET", "wikistream-landing-060647991548")
    )
    landing_prefix: str = "landing/recentchange/"
    offsets_per_file: int = 5000
    gzip_level: int = 5
    consumer_group: str = "wikistream-landing"
    aws_profile: str = field(default_factory=lambda: _env("AWS_PROFILE", "wikistream-consumer"))

    @property
    def all_topics(self) -> tuple[TopicSpec, ...]:
        return (self.main_topic, self.dlq_topic, self.drops_topic)

    @property
    def value_subject(self) -> str:
        # Confluent's default TopicNameStrategy.
        return f"{self.main_topic.name}-value"
