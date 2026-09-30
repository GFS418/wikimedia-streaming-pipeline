"""Map one raw upstream event to our Avro contract (decision D7).

`classify(raw)` is pure: bytes/str in, exactly one of Valid / DeadLetter / Drop out.
Validation is driven by the .avsc itself, so the schema file is the single source of
truth for which fields are required and what their types are. Checks are strict
(a JSON `true` is not a long, a float is not an int) so that anything reaching the
serializer is guaranteed to encode; failures carry a precise reason for the DLQ.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas" / "recentchange-v1.avsc"

INT_MIN, INT_MAX = -(2**31), 2**31 - 1
LONG_MIN, LONG_MAX = -(2**63), 2**63 - 1

# Upstream names that cannot be Avro field names, or whose type we deliberately change.
RENAMED = {"$schema": "upstream_schema"}
JSON_ENCODED = {"log_params": "log_params_json"}


def canonical_json(value: Any) -> str:
    """Deterministic JSON: sorted keys, no whitespace, UTF-8 preserved."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@dataclass(frozen=True)
class Valid:
    record: dict[str, Any]
    key: str
    extra_fields: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DeadLetter:
    error_class: str  # invalid_json | not_an_object | missing_required | type_mismatch
    detail: str
    key: str | None = None


@dataclass(frozen=True)
class Drop:
    reason: str  # canary


Outcome = Valid | DeadLetter | Drop


class _Invalid(Exception):
    def __init__(self, error_class: str, detail: str):
        super().__init__(detail)
        self.error_class = error_class
        self.detail = detail


def load_schema(path: Path = SCHEMA_PATH) -> dict[str, Any]:
    return json.loads(path.read_text())


def _check_primitive(t: str, v: Any, path: str) -> Any:
    ok = {
        "string": isinstance(v, str),
        "boolean": isinstance(v, bool),
        # bool is a subclass of int in Python; exclude it explicitly.
        "int": isinstance(v, int) and not isinstance(v, bool) and INT_MIN <= v <= INT_MAX,
        "long": isinstance(v, int) and not isinstance(v, bool) and LONG_MIN <= v <= LONG_MAX,
    }.get(t)
    if ok is None:
        raise ValueError(f"unsupported Avro type in schema: {t}")
    if not ok:
        raise _Invalid("type_mismatch", f"{path}: expected {t}, got {type(v).__name__}")
    return v


def _conform(avro_type: Any, v: Any, path: str, extras: dict[str, Any]) -> Any:
    """Return v conformed to avro_type, collecting unknown keys into extras."""
    if isinstance(avro_type, list):  # union; our schema only uses ["null", T]
        non_null = [t for t in avro_type if t != "null"]
        if v is None and "null" in avro_type:
            return None
        if len(non_null) != 1:
            raise ValueError(f"unsupported union at {path}: {avro_type}")
        return _conform(non_null[0], v, path, extras)
    if v is None:
        raise _Invalid("missing_required", f"{path}: required, got null")
    if isinstance(avro_type, dict) and avro_type.get("type") == "record":
        if not isinstance(v, dict):
            raise _Invalid("type_mismatch", f"{path}: expected object, got {type(v).__name__}")
        return _conform_record(avro_type, v, path, extras)
    if isinstance(avro_type, str):
        return _check_primitive(avro_type, v, path)
    raise ValueError(f"unsupported schema node at {path}: {avro_type}")


def _conform_record(
    schema: dict[str, Any], obj: dict[str, Any], prefix: str, extras: dict[str, Any]
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    known = set()
    for f in schema["fields"]:
        name = f["name"]
        known.add(name)
        path = f"{prefix}.{name}" if prefix else name
        if name not in obj:
            nullable = isinstance(f["type"], list) and "null" in f["type"]
            if not nullable:
                raise _Invalid("missing_required", f"{path}: required field absent")
            out[name] = None
            continue
        out[name] = _conform(f["type"], obj[name], path, extras)
    for k, v in obj.items():
        if k not in known:
            extras[f"{prefix}.{k}" if prefix else k] = v
    return out


def classify(raw: bytes | str, schema: dict[str, Any] | None = None) -> Outcome:
    schema = schema or _SCHEMA
    try:
        event = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        return DeadLetter("invalid_json", str(exc)[:500])
    if not isinstance(event, dict):
        return DeadLetter("not_an_object", f"top-level JSON is {type(event).__name__}")

    meta = event.get("meta")
    key = meta.get("id") if isinstance(meta, dict) and isinstance(meta.get("id"), str) else None
    if isinstance(meta, dict) and meta.get("domain") == "canary":
        return Drop("canary")

    # Apply the two deliberate upstream-to-contract mappings before validation.
    mapped = dict(event)
    for up, ours in RENAMED.items():
        if up in mapped:
            mapped[ours] = mapped.pop(up)
    for up, ours in JSON_ENCODED.items():
        if up in mapped:
            val = mapped.pop(up)
            mapped[ours] = None if val is None else canonical_json(val)

    extras: dict[str, Any] = {}
    # _extra_json is ours; an upstream field of that name is just another unknown field.
    if "_extra_json" in mapped:
        extras["_extra_json"] = mapped.pop("_extra_json")
    try:
        record = _conform_record(schema, mapped, "", extras)
    except _Invalid as exc:
        return DeadLetter(exc.error_class, exc.detail, key)
    record["_extra_json"] = canonical_json(extras) if extras else None
    return Valid(record=record, key=record["meta"]["id"], extra_fields=sorted(extras))


_SCHEMA = load_schema()
