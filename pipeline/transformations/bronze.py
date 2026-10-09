"""Bronze: every landed row, as landed, plus where and when it arrived.

Append-only streaming table fed by Auto Loader from the read-only external location
(decisions D4, D12). The schema is generated from the Avro contract (decision P3-2);
anything that does not fit it goes to `_rescued_data` and is counted downstream, never
dropped. Duplicates are kept here on purpose: bronze is where they are measured.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from wikistream_lake.landing_schema import LANDING_SCHEMA

LANDING_PATH = spark.conf.get("wikistream.landing_path")


@dp.table(
    name="bronze_recentchange",
    comment="Landed rows from s3 .../landing/recentchange/ (one row per Kafka offset).",
    table_properties={"quality": "bronze"},
)
def bronze_recentchange():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("pathGlobFilter", "*.jsonl.gz")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(LANDING_SCHEMA)
        .load(LANDING_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            # S3 LastModified: when the file landed. With _ingested_at this measures lateness
            # exactly as the pipeline experiences it (decision P3-5).
            F.col("_metadata.file_modification_time").alias("_file_landed_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
