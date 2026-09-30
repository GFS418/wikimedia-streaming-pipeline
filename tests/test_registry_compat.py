"""Evidence for decision D6: what FULL_TRANSITIVE accepts and rejects, on the real registry.

Each case registers v1 on a fresh subject, then tries to register a candidate. The
registry enforces the subject's compatibility level at registration (HTTP 409 on failure),
including the transitive check against every earlier version.
"""

import copy
import json

import pytest
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from wikistream.contract import load_schema

pytestmark = pytest.mark.integration

V1 = load_schema()


def variant(mutate):
    s = copy.deepcopy(V1)
    mutate(s["fields"])
    return s


def field_index(fields, name):
    return next(i for i, f in enumerate(fields) if f["name"] == name)


def add_optional(fields):
    fields.append({"name": "new_field", "type": ["null", "string"], "default": None})


def add_required(fields):
    fields.append({"name": "new_field", "type": "string"})


def remove_optional(fields):
    del fields[field_index(fields, "patrolled")]


def remove_required(fields):
    del fields[field_index(fields, "wiki")]


def narrow_type(fields):
    fields[field_index(fields, "timestamp")]["type"] = "int"


def rename_optional(fields):
    fields[field_index(fields, "title")]["name"] = "page_title"


@pytest.fixture
def sr(stack):
    client = SchemaRegistryClient({"url": stack.schema_registry_url})
    created = []
    yield client, created
    for subject in created:
        client.delete_subject(subject, permanent=False)
        client.delete_subject(subject, permanent=True)


def register(client, subject, schema) -> bool:
    try:
        client.register_schema(subject, Schema(json.dumps(schema), "AVRO"))
        return True
    except SchemaRegistryError as exc:
        assert exc.http_status_code == 409, exc
        return False


@pytest.mark.parametrize(
    ("change", "mutate", "accepted"),
    [
        ("add optional field with default", add_optional, True),
        ("remove optional field with default", remove_optional, True),
        ("add required field", add_required, False),
        ("remove required field", remove_required, False),
        ("narrow long to int", narrow_type, False),
        # Structural check only: a rename of an optional field is add + remove, both
        # compatible, so it passes -- and old data silently reads as null. Renames need
        # an add-then-deprecate policy, not trust in the registry.
        ("rename optional field", rename_optional, True),
    ],
)
def test_full_transitive_single_change(sr, run_id, change, mutate, accepted):
    client, created = sr
    subject = f"compat-{run_id}-value"
    created.append(subject)
    client.set_compatibility(subject_name=subject, level="FULL_TRANSITIVE")
    assert register(client, subject, V1)
    assert register(client, subject, variant(mutate)) is accepted, change


def test_transitive_catches_what_non_transitive_misses(sr, run_id):
    """v2 drops `patrolled`; v3 re-adds it as a string. v3 is fine against v2 alone,
    but conflicts with v1 (boolean). Only the transitive mode sees v1."""
    client, created = sr
    v2 = variant(remove_optional)

    def readd_as_string(fields):
        fields.append({"name": "patrolled", "type": ["null", "string"], "default": None})

    v3 = copy.deepcopy(v2)
    readd_as_string(v3["fields"])

    outcomes = {}
    for level in ("FULL", "FULL_TRANSITIVE"):
        subject = f"compat-{run_id}-{level.lower()}-value"
        created.append(subject)
        client.set_compatibility(subject_name=subject, level=level)
        assert register(client, subject, V1)
        assert register(client, subject, v2)
        outcomes[level] = register(client, subject, v3)

    assert outcomes == {"FULL": True, "FULL_TRANSITIVE": False}
