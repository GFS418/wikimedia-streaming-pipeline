"""Idempotent setup: topics, schema registration, compatibility mode. Safe to re-run."""

from __future__ import annotations

import logging

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from wikistream.config import Settings, TopicSpec
from wikistream.contract import SCHEMA_PATH

log = logging.getLogger(__name__)


def ensure_topic(admin: AdminClient, spec: TopicSpec) -> str:
    existing = admin.list_topics(timeout=10).topics
    if spec.name in existing:
        have = len(existing[spec.name].partitions)
        if have != spec.partitions:
            # Never silently repartition: it changes key->partition mapping for meta.id.
            raise RuntimeError(
                f"topic {spec.name} has {have} partitions, expected {spec.partitions}"
            )
        return "exists"
    topic = NewTopic(
        spec.name,
        num_partitions=spec.partitions,
        replication_factor=1,  # single local broker
        config={"retention.ms": str(spec.retention_ms), "cleanup.policy": "delete"},
    )
    admin.create_topics([topic])[spec.name].result(timeout=30)
    return "created"


def ensure_schema(
    sr: SchemaRegistryClient, subject: str, schema_str: str, compatibility: str
) -> int:
    """Set the subject's compatibility, then register (a no-op if already registered)."""
    sr.set_compatibility(subject_name=subject, level=compatibility)
    return sr.register_schema(subject, Schema(schema_str, "AVRO"))


def bootstrap(settings: Settings | None = None) -> dict[str, object]:
    s = settings or Settings()
    admin = AdminClient({"bootstrap.servers": s.bootstrap_servers})
    sr = SchemaRegistryClient({"url": s.schema_registry_url})
    result: dict[str, object] = {
        s.main_topic.name: ensure_topic(admin, s.main_topic),
        s.dlq_topic.name: ensure_topic(admin, s.dlq_topic),
    }
    schema_id = ensure_schema(sr, s.value_subject, SCHEMA_PATH.read_text(), s.compatibility)
    result["schema_id"] = schema_id
    result["compatibility"] = sr.get_compatibility(s.value_subject)
    return result
