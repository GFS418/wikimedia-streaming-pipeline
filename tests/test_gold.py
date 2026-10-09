"""Gold metrics (decisions D20, D24) on a local Spark 4.1 session, with known answers."""

import shutil
from datetime import UTC, datetime

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("local Spark needs Java", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from wikistream_lake.gold import METRIC_NAMES, eligible, per_wiki_minute  # noqa: E402

SILVER = StructType(
    [
        StructField("event_time", TimestampType()),
        StructField("wiki", StringType()),
        StructField("type", StringType()),
        StructField("bot", BooleanType()),
        StructField("length_old", LongType()),
        StructField("length_new", LongType()),
        StructField("dq_flags", ArrayType(StringType())),
    ]
)


def at(seconds: float) -> datetime:
    return datetime.fromtimestamp(1_791_499_980 + seconds, UTC).replace(tzinfo=None)  # whole minute


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("wikistream-gold-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def gold(spark, rows):
    df = spark.createDataFrame(rows, SILVER)
    return {
        (r["window_start"], r["wiki"]): r.asDict() for r in per_wiki_minute(eligible(df)).collect()
    }


def test_each_type_counted_once_and_bot_share_over_edits_and_new_pages_only(spark):
    t = at(20)  # 20 s into the first window
    rows = [
        (t, "enwiki", "edit", False, 100, 150, []),  # human edit, +50 bytes
        (t, "enwiki", "edit", True, 300, 200, []),  # bot edit, -100 bytes
        (t, "enwiki", "new", False, None, 40, []),  # human new page, +40 bytes
        (t, "enwiki", "log", True, None, None, []),  # log: not a "change"
        (t, "enwiki", "categorize", True, None, None, []),  # categorize: not a "change"
        (t, "enwiki", "external", False, None, None, []),  # documented but rare
        (t, "enwiki", "wikidata_magic", False, None, None, ["unknown_type"]),
    ]
    g = gold(spark, rows)[(at(0).replace(second=0), "enwiki")]
    assert {k: g[k] for k in METRIC_NAMES} == {
        "events": 7,
        "edits": 2,
        "new_pages": 1,
        "log_events": 1,
        "categorize_events": 1,
        "other_events": 2,
        "bot_changes": 1,
        "human_changes": 2,
        "bytes_added": 90,
        "bytes_removed": 100,
    }
    # Identities the reconciliation also checks.
    assert g["events"] == sum(
        g[k] for k in ("edits", "new_pages", "log_events", "categorize_events", "other_events")
    )
    assert g["bot_changes"] + g["human_changes"] == g["edits"] + g["new_pages"]


def test_historical_rows_are_excluded_but_other_flags_are_counted(spark):
    t = at(5)
    rows = [
        (t, "commonswiki", "log", False, None, None, ["historical_event_time"]),
        (t, "commonswiki", "edit", False, 1, 2, ["extra_fields"]),
    ]
    g = gold(spark, rows)
    assert len(g) == 1 and next(iter(g.values()))["events"] == 1


def test_minute_windows_split_at_exactly_sixty_seconds(spark):
    rows = [
        (at(59), "dewiki", "edit", False, 1, 1, []),
        (at(60), "dewiki", "edit", False, 1, 1, []),
        (at(60), "frwiki", "edit", False, 1, 1, []),
    ]
    g = gold(spark, rows)
    assert sorted((k[0].minute, k[1], v["events"]) for k, v in g.items()) == [
        (at(0).minute, "dewiki", 1),
        (at(60).minute, "dewiki", 1),
        (at(60).minute, "frwiki", 1),
    ]
    first = g[(at(0).replace(second=0), "dewiki")]
    assert (first["window_end"] - first["window_start"]).total_seconds() == 60
