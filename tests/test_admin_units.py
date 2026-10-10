"""ensure_topic under metadata lag, with a fake admin client (no Kafka needed)."""

from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaError, KafkaException

from wikistream.admin import ensure_topic
from wikistream.config import DAY_MS, TopicSpec


class Done:
    def __init__(self, value=None, exc=None):
        self.value, self.exc = value, exc

    def result(self, timeout=None):
        if self.exc:
            raise self.exc
        return self.value


class LaggingAdmin:
    """The topic already exists, but metadata only lists it from the n-th call on."""

    def __init__(self, partitions: int, visible_after: int, config: dict):
        self.partitions, self.visible_after, self.config = partitions, visible_after, config
        self.list_calls = 0
        self.altered = []

    def list_topics(self, timeout=None):
        self.list_calls += 1
        topics = {}
        if self.list_calls >= self.visible_after:
            topics["t"] = SimpleNamespace(partitions={i: None for i in range(self.partitions)})
        return SimpleNamespace(topics=topics)

    def create_topics(self, topics):
        err = KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS))
        return {topics[0].topic: Done(exc=err)}

    def describe_configs(self, resources):
        entries = {k: SimpleNamespace(value=v) for k, v in self.config.items()}
        return {resources[0]: Done(entries)}

    def incremental_alter_configs(self, resources):
        self.altered.append(resources[0])
        return {resources[0]: Done()}


SPEC = TopicSpec("t", 3, DAY_MS)


def test_lagging_metadata_is_waited_for_not_turned_into_a_crash():
    admin = LaggingAdmin(partitions=3, visible_after=3, config=SPEC.config)
    assert ensure_topic(admin, SPEC) == "exists"
    assert admin.list_calls >= 3


def test_after_the_wait_a_partition_mismatch_is_still_refused():
    admin = LaggingAdmin(partitions=2, visible_after=2, config=SPEC.config)
    with pytest.raises(RuntimeError, match="has 2 partitions, expected 3"):
        ensure_topic(admin, SPEC)


def test_after_the_wait_configs_are_still_reconciled():
    stale = SPEC.config | {"segment.ms": str(7 * DAY_MS)}
    admin = LaggingAdmin(partitions=3, visible_after=2, config=stale)
    assert ensure_topic(admin, SPEC) == f"updated (segment.ms: {7 * DAY_MS} -> {DAY_MS})"
