"""Decision D18 on a local Spark 4.1 session (the DBR 18.3 line): rules, flags, routing.

Real landed rows must pass every rule (as the profile found); each rule must catch an
injected violation of exactly that rule; flags must mark rows without quarantining them.
"""

import copy
import gzip
import json
import shutil
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("local Spark needs Java", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from wikistream_lake.landing_schema import LANDING_SCHEMA  # noqa: E402
from wikistream_lake.silver_rules import RULES, flatten, with_checks  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "landing_sample.jsonl.gz"
ROWS = [json.loads(r) for r in gzip.decompress(FIXTURE.read_bytes()).splitlines()]


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("wikistream-silver-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def bronze_like(spark, rows, rescued=None):
    """What bronze holds: the landing row plus Auto Loader's metadata columns."""
    df = spark.read.schema(LANDING_SCHEMA).json(
        spark.sparkContext.parallelize([json.dumps(r) for r in rows])
    )
    return df.select(
        "*",
        F.lit(rescued).cast("string").alias("_rescued_data"),
        F.lit("s3://test/landing/recentchange/p00/x.jsonl.gz").alias("_source_file"),
        F.current_timestamp().alias("_file_landed_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


def checked(spark, rows, rescued=None):
    return {
        r["meta_id"]: r for r in with_checks(flatten(bronze_like(spark, rows, rescued))).collect()
    }


def first(type_, **changes):
    """A copy of the first real fixture row of this type with value fields changed.

    `revision__old=None` sets value.revision.old; a missing or null parent becomes {}.
    """
    row = copy.deepcopy(next(r for r in ROWS if r["value"]["type"] == type_))
    row["value"]["meta"]["id"] = f"injected-{type_}-{sorted(changes.items())}"
    for path, val in changes.items():
        target = row["value"]
        *parents, leaf = path.split("__")
        for p in parents:
            if target.get(p) is None:
                target[p] = {}
            target = target[p]
        target[leaf] = val
    return row


def test_every_real_landed_row_passes_every_rule_with_no_flags(spark):
    out = checked(spark, ROWS)
    assert len(out) == len(ROWS) == 300
    assert {k: r["failed_rules"] for k, r in out.items() if r["failed_rules"]} == {}
    assert {k: r["dq_flags"] for k, r in out.items() if r["dq_flags"]} == {}


def test_flatten_types_and_derived_columns(spark):
    r = next(iter(checked(spark, [ROWS[-1]]).values()))
    v = ROWS[-1]["value"]
    assert r["meta_id"] == v["meta"]["id"]
    assert int(r["event_time"].timestamp()) == v["timestamp"]
    assert r["event_date"].isoformat() == r["event_time"].date().isoformat()
    assert r["kafka_offset"] == ROWS[-1]["kafka"]["offset"]
    assert 0 <= r["emit_delay_s"] < 3600


@pytest.mark.parametrize(
    ("row", "rule"),
    [
        (lambda: first("edit", revision__old=None), "structure_by_type"),
        (lambda: first("edit", length__new=None), "structure_by_type"),
        (lambda: first("new", length__old=10), "structure_by_type"),
        (lambda: first("log", log_type=None), "structure_by_type"),
        (lambda: first("categorize", revision__new=5), "structure_by_type"),
        (lambda: first("edit", length__new=-1), "non_negative_lengths"),
        (lambda: first("edit", wiki="En-Wiki"), "wiki_name_format"),
    ],
    ids=[
        "edit-no-old-rev",
        "edit-no-new-len",
        "new-with-old-len",
        "log-no-type",
        "categorize-with-rev",
        "negative-length",
        "wiki-format",
    ],
)
def test_each_rule_catches_its_violation_and_only_its_own(spark, row, rule):
    r = next(iter(checked(spark, [row()]).values()))
    assert r["failed_rules"] == [rule]


def emit_delay_row(spark, delay_s):
    row = first("edit")
    ts = row["value"]["timestamp"]
    from datetime import UTC, datetime

    dt = datetime.fromtimestamp(ts + delay_s, UTC).isoformat(timespec="milliseconds")
    row["value"]["meta"]["dt"] = dt.replace("+00:00", "Z")
    return next(iter(checked(spark, [row]).values()))


@pytest.mark.parametrize(
    ("delay_s", "quarantined", "flags"),
    [
        (0, False, []),
        (-1, False, []),
        (-1.5, True, []),  # edit stamped after its own emission: broken
        (115.7, False, []),  # the largest live delay observed
        (3600, False, []),
        (3601, False, ["historical_event_time"]),  # old, but real: kept and flagged
    ],
)
def test_event_time_rule_and_historical_flag_boundaries(spark, delay_s, quarantined, flags):
    r = emit_delay_row(spark, delay_s)
    assert ("event_time_valid" in r["failed_rules"]) is quarantined
    assert r["dq_flags"] == flags
    assert r["emit_delay_s"] == pytest.approx(delay_s, abs=0.001)


def test_commons_file_history_import_is_kept_and_flagged(spark):
    # The real case from 2026-10-08: an upload log stamped 2018-01-10, emitted 2026-10-08.
    row = first("log", timestamp=1515594378)
    row["value"]["meta"]["dt"] = "2026-10-08T12:54:50.022Z"
    r = next(iter(checked(spark, [row]).values()))
    assert r["failed_rules"] == [] and r["dq_flags"] == ["historical_event_time"]


def test_unparseable_meta_dt_fails_the_event_time_rule(spark):
    row = first("edit")
    row["value"]["meta"]["dt"] = "not-a-time"
    r = next(iter(checked(spark, [row]).values()))
    assert r["meta_dt"] is None and r["failed_rules"] == ["event_time_valid"]


def test_several_failures_are_all_reported(spark):
    r = next(iter(checked(spark, [first("edit", revision__old=None, wiki="BAD!")]).values()))
    assert r["failed_rules"] == ["structure_by_type", "wiki_name_format"]


def test_unknown_type_is_flagged_not_quarantined(spark):
    r = next(iter(checked(spark, [first("edit", type="wikidata_magic")]).values()))
    assert r["failed_rules"] == [] and r["dq_flags"] == ["unknown_type"]


def test_documented_external_type_is_neither_flagged_nor_quarantined(spark):
    r = next(iter(checked(spark, [first("log", type="external")]).values()))
    assert r["failed_rules"] == [] and r["dq_flags"] == []


def test_extra_fields_and_rescued_data_are_flagged(spark):
    row = first("edit", _extra_json='{"brand_new":1}')
    r = next(iter(checked(spark, [row], rescued='{"x":1}').values()))
    assert r["failed_rules"] == [] and r["dq_flags"] == ["extra_fields", "rescued_data"]


def test_special_page_namespace_minus_one_is_legitimate(spark):
    r = next(iter(checked(spark, [first("log", namespace=-1)]).values()))
    assert r["failed_rules"] == [] and r["namespace"] == -1


def test_rule_names_are_stable():
    # Renaming a rule changes quarantine contents and dashboards; make it deliberate.
    assert list(RULES) == [
        "structure_by_type",
        "non_negative_lengths",
        "event_time_valid",
        "wiki_name_format",
    ]


@pytest.mark.parametrize("absurd", [10**15, 2**62, -(10**15)])
def test_absurd_epoch_is_quarantined_not_a_pipeline_failure(spark, absurd):
    # Unguarded, timestamp_seconds overflows a long here and ANSI mode fails the update.
    r = next(iter(checked(spark, [first("edit", timestamp=absurd)]).values()))
    assert r["event_time"] is None and r["event_date"] is None
    assert r["failed_rules"] == ["event_time_valid"]
