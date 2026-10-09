"""Gold: activity per wiki per minute of event time (decisions D19, D20, D24).

Pure PySpark, no pipeline decorators: unit-tested locally (tests/test_gold.py). The same
functions run on a batch DataFrame (tests, reconciliation) and on the streaming source.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# D24: over a simulated, live-validated full day the smallest watermark dropping nothing
# was 15 min (worst lateness 12.8 min, in the overnight low); 20 min leaves room for a day
# about 35% quieter, since lateness tracks file fill time (reports/phase3/watermark.md).
WATERMARK = "20 minutes"
WINDOW = "1 minute"

CHANGE_TYPES = ("edit", "new")  # what "edits" and bot share are measured over
KNOWN_TYPES = ("edit", "new", "log", "categorize")

METRIC_NAMES = [
    "events",
    "edits",
    "new_pages",
    "log_events",
    "categorize_events",
    "other_events",
    "bot_changes",
    "human_changes",
    "bytes_added",
    "bytes_removed",
]


def eligible(silver: DataFrame) -> DataFrame:
    """Live activity only: rows flagged historical_event_time (D18) are excluded explicitly,
    before the watermark, so they neither count nor register as pipeline lateness."""
    return silver.where(~F.array_contains("dq_flags", "historical_event_time"))


def _metrics():
    t = F.col("type")
    change = t.isin(*CHANGE_TYPES)
    delta = F.coalesce(F.col("length_new"), F.lit(0)) - F.coalesce(F.col("length_old"), F.lit(0))

    def n(cond):
        return F.sum(F.when(cond, 1).otherwise(0)).cast("long")

    return [
        F.count(F.lit(1)).cast("long").alias("events"),
        n(t == "edit").alias("edits"),
        n(t == "new").alias("new_pages"),
        n(t == "log").alias("log_events"),
        # Generated automatically when an edit changes a page's categories (53% of all
        # events in the profile): reported separately, never counted as edits.
        n(t == "categorize").alias("categorize_events"),
        n(~t.isin(*KNOWN_TYPES)).alias("other_events"),  # 'external' and unknown types
        n(change & F.col("bot")).alias("bot_changes"),
        n(change & ~F.col("bot")).alias("human_changes"),
        F.sum(F.when(change, F.greatest(delta, F.lit(0))).otherwise(0))
        .cast("long")
        .alias("bytes_added"),
        F.sum(F.when(change, F.greatest(-delta, F.lit(0))).otherwise(0))
        .cast("long")
        .alias("bytes_removed"),
    ]


def per_wiki_minute(events: DataFrame) -> DataFrame:
    """One row per (1-minute event-time window, wiki)."""
    return (
        events.groupBy(F.window("event_time", WINDOW).alias("w"), "wiki")
        .agg(*_metrics())
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "wiki",
            *METRIC_NAMES,
        )
    )
