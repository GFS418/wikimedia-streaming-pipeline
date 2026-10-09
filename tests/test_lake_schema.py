"""Decision P3-2: the bronze schema is generated from the Avro contract and cannot drift."""

import copy
import gzip
import json
from pathlib import Path

import pytest
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
)

from wikistream.contract import load_schema
from wikistream.lake_schema import avro_to_spark, landing_schema, render_module

ROOT = Path(__file__).resolve().parents[1]
GENERATED = ROOT / "pipeline" / "wikistream_lake" / "landing_schema.py"
LANDING_FIXTURE = Path(__file__).parent / "fixtures" / "landing_sample.jsonl.gz"


def test_committed_pipeline_schema_matches_the_avro_contract():
    assert GENERATED.read_text() == render_module(), (
        "pipeline/wikistream_lake/landing_schema.py is stale: run "
        "`uv run python scripts/generate_landing_schema.py`"
    )


def test_avro_types_map_as_intended():
    assert avro_to_spark("string") == StringType()
    assert avro_to_spark(["null", "long"]) == LongType()
    assert avro_to_spark(["null", "int"]) == IntegerType()
    assert avro_to_spark("boolean") == BooleanType()
    with pytest.raises(ValueError):
        avro_to_spark(["null", "string", "long"])  # multi-type unions are not in our contract
    with pytest.raises(ValueError):
        avro_to_spark("double")


def test_envelope_shape():
    s = landing_schema()
    assert s.fieldNames() == ["kafka", "value"]
    assert isinstance(s["kafka"].dataType["headers"].dataType, MapType)
    value = s["value"].dataType
    assert value.fieldNames()[:5] == ["meta", "type", "wiki", "timestamp", "bot"]
    assert value["timestamp"].dataType == LongType()
    assert isinstance(value["meta"].dataType, StructType)


def test_a_new_optional_avro_field_appears_in_the_generated_schema():
    v2 = copy.deepcopy(load_schema())
    v2["fields"].append({"name": "new_field", "type": ["null", "string"], "default": None})
    assert "new_field" in landing_schema(v2)["value"].dataType.fieldNames()
    assert render_module(v2) != render_module()  # so the drift test would catch a stale copy


def conforms(value, dtype, path=""):
    """Every key and value of a JSON row fits the Spark type (else Auto Loader rescues it)."""
    if value is None:
        return []
    if isinstance(dtype, StructType):
        if not isinstance(value, dict):
            return [f"{path}: expected object"]
        problems = [f"{path}.{k}: not in schema" for k in value if k not in dtype.fieldNames()]
        for f in dtype.fields:
            problems += conforms(value.get(f.name), f.dataType, f"{path}.{f.name}")
        return problems
    if isinstance(dtype, MapType):
        ok = isinstance(value, dict) and all(isinstance(v, str) for v in value.values())
        return [] if ok else [f"{path}: expected map<string,string>"]
    checks = {
        StringType: lambda v: isinstance(v, str),
        BooleanType: lambda v: isinstance(v, bool),
        IntegerType: lambda v: (
            isinstance(v, int) and not isinstance(v, bool) and -(2**31) <= v < 2**31
        ),
        LongType: lambda v: isinstance(v, int) and not isinstance(v, bool),
    }
    return [] if checks[type(dtype)](value) else [f"{path}: {value!r} is not {dtype}"]


def test_real_landed_rows_fit_the_schema_with_nothing_to_rescue():
    rows = [json.loads(r) for r in gzip.decompress(LANDING_FIXTURE.read_bytes()).splitlines()]
    assert len(rows) >= 200
    schema = landing_schema()
    problems = [p for r in rows for p in conforms(r, schema)]
    assert problems == []


def test_conformance_check_catches_unknown_fields_and_type_drift():
    rows = [json.loads(r) for r in gzip.decompress(LANDING_FIXTURE.read_bytes()).splitlines()]
    bad = copy.deepcopy(rows[0])
    bad["value"]["surprise"] = 1
    bad["value"]["timestamp"] = "1791414789"
    problems = conforms(bad, landing_schema())
    assert ".value.surprise: not in schema" in problems
    assert any(p.startswith(".value.timestamp:") for p in problems)


def test_generated_module_imports_as_the_pipeline_imports_it():
    # pipeline/ is the Lakeflow root (on sys.path there; pytest pythonpath here).
    from wikistream_lake.landing_schema import LANDING_SCHEMA

    assert LANDING_SCHEMA == landing_schema()
