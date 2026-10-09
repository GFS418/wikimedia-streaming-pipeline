"""Silver: flatten bronze rows, then check them (decision D18).

One source of truth: RULES feeds both the pipeline expectations (pass/fail metrics in the
event log and Data quality tab) and the `failed_rules` column that routes a row to silver
or to quarantine, so the metrics and the routing cannot disagree.

Pure PySpark, no pipeline decorators: unit-tested locally (tests/test_silver_rules.py).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

KNOWN_TYPES = ("edit", "new", "log", "categorize", "external")

# Quarantine rules: name -> Spark SQL predicate over the flattened columns; TRUE = passes.
# A NULL result counts as a failure (see failed_rules below).
RULES = {
    "structure_by_type": """
        CASE type
          WHEN 'edit' THEN revision_old IS NOT NULL AND revision_new IS NOT NULL
                           AND length_old IS NOT NULL AND length_new IS NOT NULL
          WHEN 'new' THEN revision_new IS NOT NULL AND length_new IS NOT NULL
                          AND revision_old IS NULL AND length_old IS NULL
          WHEN 'log' THEN log_type IS NOT NULL AND log_action IS NOT NULL
          WHEN 'categorize' THEN revision_old IS NULL AND revision_new IS NULL
                                 AND length_old IS NULL AND length_new IS NULL
          ELSE TRUE
        END""",
    "non_negative_lengths": "coalesce(length_old, 0) >= 0 AND coalesce(length_new, 0) >= 0",
    # Broken timestamps only (decision D18, amended 2026-10-08): meta.dt unparseable, edit
    # time out of range (event_time NULL), or the edit stamped more than 1 s AFTER its own
    # emission (-1 s allows for the 1-second resolution of `timestamp`). Old edit times are
    # legitimate (Commons file-history imports carry 2007-2023 upload times) and are
    # flagged historical_event_time instead.
    "event_time_valid": "meta_dt IS NOT NULL AND event_time IS NOT NULL AND emit_delay_s >= -1",
    "wiki_name_format": "wiki RLIKE '^[a-z0-9_]+$'",
}

# Flag-but-keep conditions: name -> Spark SQL predicate; TRUE = flag the row. Strings, like
# RULES, so nothing touches Spark at import time (classic PySpark needs a session for that).
_KNOWN = ", ".join(f"'{t}'" for t in KNOWN_TYPES)
FLAGS = {
    "unknown_type": f"NOT coalesce(type IN ({_KNOWN}), FALSE)",
    # Edit time more than an hour before emission: real, but not live activity. Observed live
    # delays reach 115.7 s; the 7 rows past an hour were all historical Commons uploads.
    # Gold excludes these explicitly so they are never counted as pipeline lateness.
    "historical_event_time": "emit_delay_s > 3600",
    "extra_fields": "_extra_json IS NOT NULL",
    "rescued_data": "_rescued_data IS NOT NULL",
}


# Years 1 to 9999. Beyond about 10**15 s, timestamp_seconds overflows a long and, under ANSI
# mode, fails the whole update; out-of-range values become NULL instead, and the raw
# event_time NULL fails event_time_valid, which quarantines the row.
_MIN_EPOCH_S, _MAX_EPOCH_S = -62_135_596_800, 253_402_300_799


def _event_time(seconds):
    return F.when(seconds.between(_MIN_EPOCH_S, _MAX_EPOCH_S), F.timestamp_seconds(seconds))


def flatten(bronze: DataFrame) -> DataFrame:
    """One typed column per field; nested contract structs become prefixed columns."""
    v, m, k = "value", "value.meta", "kafka"
    return bronze.select(
        F.col(f"{m}.id").alias("meta_id"),
        # try_: under ANSI mode (default in Spark 4 / DBR 18) to_timestamp THROWS on a
        # malformed string, which would fail the whole update; NULL lets the
        # event_time_valid rule quarantine just that row.
        F.try_to_timestamp(F.col(f"{m}.dt")).alias("meta_dt"),
        _event_time(F.col(f"{v}.timestamp")).alias("event_time"),
        F.to_date(_event_time(F.col(f"{v}.timestamp"))).alias("event_date"),
        F.col(f"{v}.timestamp").alias("event_ts_s"),
        F.col(f"{v}.type").alias("type"),
        F.col(f"{v}.wiki").alias("wiki"),
        F.col(f"{v}.bot").alias("bot"),
        F.col(f"{v}.id").alias("rc_id"),
        F.col(f"{v}.namespace").alias("namespace"),
        F.col(f"{v}.title").alias("title"),
        F.col(f"{v}.title_url").alias("title_url"),
        F.col(f"{v}.user").alias("user"),
        F.col(f"{v}.comment").alias("comment"),
        F.col(f"{v}.parsedcomment").alias("parsedcomment"),
        F.col(f"{v}.minor").alias("minor"),
        F.col(f"{v}.patrolled").alias("patrolled"),
        F.col(f"{v}.notify_url").alias("notify_url"),
        F.col(f"{v}.length.old").alias("length_old"),
        F.col(f"{v}.length.new").alias("length_new"),
        F.col(f"{v}.revision.old").alias("revision_old"),
        F.col(f"{v}.revision.new").alias("revision_new"),
        F.col(f"{v}.server_url").alias("server_url"),
        F.col(f"{v}.server_name").alias("server_name"),
        F.col(f"{v}.server_script_path").alias("server_script_path"),
        F.col(f"{v}.log_id").alias("log_id"),
        F.col(f"{v}.log_type").alias("log_type"),
        F.col(f"{v}.log_action").alias("log_action"),
        F.col(f"{v}.log_action_comment").alias("log_action_comment"),
        F.col(f"{v}.log_params_json").alias("log_params_json"),
        F.col(f"{v}.upstream_schema").alias("upstream_schema"),
        F.col(f"{v}._extra_json").alias("_extra_json"),
        F.col(f"{m}.domain").alias("meta_domain"),
        F.col(f"{m}.stream").alias("meta_stream"),
        F.col(f"{m}.uri").alias("meta_uri"),
        F.col(f"{m}.request_id").alias("meta_request_id"),
        F.col(f"{m}.topic").alias("upstream_topic"),
        F.col(f"{m}.partition").alias("upstream_partition"),
        F.col(f"{m}.offset").alias("upstream_offset"),
        F.col(f"{k}.topic").alias("kafka_topic"),
        F.col(f"{k}.partition").alias("kafka_partition"),
        F.col(f"{k}.offset").alias("kafka_offset"),
        F.col(f"{k}.timestamp_ms").alias("kafka_timestamp_ms"),
        F.col(f"{k}.schema_id").alias("kafka_schema_id"),
        F.col(f"{k}.headers").alias("kafka_headers"),
        "_rescued_data",
        "_source_file",
        "_file_landed_at",
        "_ingested_at",
    ).withColumn(
        # Seconds between the edit (`timestamp`, 1 s resolution) and its emission upstream.
        "emit_delay_s",
        F.unix_micros(F.col("meta_dt")) / F.lit(1_000_000) - F.col("event_ts_s"),
    )


def with_checks(flat: DataFrame) -> DataFrame:
    """Add failed_rules (non-empty -> quarantine) and dq_flags (kept, marked)."""
    failed = [
        F.when(~F.coalesce(F.expr(pred), F.lit(False)), F.lit(name)) for name, pred in RULES.items()
    ]
    flags = [F.when(F.expr(pred), F.lit(name)) for name, pred in FLAGS.items()]
    return flat.withColumn("failed_rules", F.array_compact(F.array(*failed))).withColumn(
        "dq_flags", F.array_compact(F.array(*flags))
    )


def first_copy_wins():
    """AUTO CDC sequence for silver (decision D17): the earliest copy of an event wins.

    Producer duplicates share meta.id, hence Kafka key and partition, and arrive later
    (higher offset and timestamp). Negating both makes the first copy the highest
    sequence, so later copies are dropped as out of order instead of updating the row,
    and silver's change feed carries only inserts.
    """
    return F.struct(
        (-F.col("kafka_timestamp_ms")).alias("neg_kafka_timestamp_ms"),
        (-F.col("kafka_offset")).alias("neg_kafka_offset"),
    )
