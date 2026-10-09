"""Silver: checked, deduplicated events, plus a quarantine of rows that fail the rules.

bronze_recentchange
  -> silver_checked (private): flattened, expectations recorded, failed_rules + dq_flags
       -> silver_recentchange: AUTO CDC keyed on meta_id, first copy wins (D17)
       -> silver_quarantine: rows with any failed rule, kept with the rule names (D15, D18)

Every bronze row ends up in exactly one place: silver (one row per meta_id), quarantine,
or dropped as a later duplicate of an event silver already holds.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from wikistream_lake.silver_rules import RULES, first_copy_wins, flatten, with_checks


@dp.table(
    name="silver_checked",
    private=True,  # internal to the pipeline; not published to Unity Catalog
    comment="Flattened bronze rows with failed_rules and dq_flags (decision D18).",
)
@dp.expect_all(RULES)  # warn: record per-rule metrics; routing is done by failed_rules
def silver_checked():
    return with_checks(flatten(spark.readStream.table("bronze_recentchange")))


@dp.temporary_view(name="silver_valid")
def silver_valid():
    return spark.readStream.table("silver_checked").where(F.size("failed_rules") == 0)


dp.create_streaming_table(
    name="silver_recentchange",
    comment="One row per Wikimedia event (meta_id), first copy wins; dq_flags marks "
    "rows kept but questionable (decisions D17, D18).",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_recentchange",
    source="silver_valid",
    keys=["meta_id"],
    sequence_by=first_copy_wins(),
    except_column_list=["failed_rules"],
    stored_as_scd_type=1,
)


@dp.table(
    name="silver_quarantine",
    comment="Rows that failed at least one silver rule, with the rule names (decision D18).",
    table_properties={"quality": "quarantine"},
)
def silver_quarantine():
    return spark.readStream.table("silver_checked").where(F.size("failed_rules") > 0)
