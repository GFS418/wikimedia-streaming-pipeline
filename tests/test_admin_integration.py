"""Decision D14: ensure_topic reconciles configs of existing topics, not just creates them."""

import pytest
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, ResourceType

from wikistream.admin import ensure_topic
from wikistream.config import DAY_MS, TopicSpec

pytestmark = pytest.mark.integration


def describe(admin: AdminClient, name: str) -> dict[str, str]:
    r = ConfigResource(ResourceType.TOPIC, name)
    return {k: v.value for k, v in admin.describe_configs([r])[r].result(timeout=30).items()}


def test_existing_topic_with_broker_default_segments_is_corrected_once(stack, run_id):
    admin = AdminClient({"bootstrap.servers": stack.bootstrap_servers})
    name = f"ita-{run_id}.segments"
    # A topic created like the live ones were before D14: no segment.ms, broker default.
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result(30)
    try:
        assert describe(admin, name)["segment.ms"] == str(7 * DAY_MS)
        spec = TopicSpec(name, 1, 7 * DAY_MS)  # retention matches the broker default

        first = ensure_topic(admin, spec)
        assert first == f"updated (segment.ms: {7 * DAY_MS} -> {DAY_MS})"
        assert describe(admin, name)["segment.ms"] == str(DAY_MS)
        assert ensure_topic(admin, spec) == "exists"  # idempotent
    finally:
        admin.delete_topics([name])


def test_partition_mismatch_is_refused_not_repaired(stack, run_id):
    admin = AdminClient({"bootstrap.servers": stack.bootstrap_servers})
    name = f"ita-{run_id}.partitions"
    admin.create_topics([NewTopic(name, num_partitions=2, replication_factor=1)])[name].result(30)
    try:
        with pytest.raises(RuntimeError, match="has 2 partitions, expected 3"):
            ensure_topic(admin, TopicSpec(name, 3, DAY_MS))
    finally:
        admin.delete_topics([name])
