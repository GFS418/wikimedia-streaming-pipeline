# Databricks notebook source
# Phase 0 spike: can Free Edition (serverless) stream files from our read-only S3
# external location into a Delta table with Auto Loader + Trigger.AvailableNow?
#
# Pass criteria:
#   1. rows in target == lines in the landed file(s), distinct meta.id == rows
#   2. a second run with the same checkpoint adds 0 rows (checkpoint = exactly-once
#      at the file level; nothing is reprocessed)
#
# Checkpoints and inferred schema live in a managed UC volume, never in the
# landing bucket (Databricks has read-only access there by design).

# COMMAND ----------

dbutils.widgets.text("source", "s3://wikistream-landing-060647991548/landing/_spike/recentchange/")
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "wikistream_spike")

source = dbutils.widgets.get("source")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
target = f"{catalog}.{schema}.bronze_spike"
state_root = f"/Volumes/{catalog}/{schema}/state"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.state")

# COMMAND ----------

import json

from pyspark.sql import functions as F

rows_before = spark.table(target).count() if spark.catalog.tableExists(target) else 0

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{state_root}/schema/bronze_spike")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

query = (
    stream.writeStream.option("checkpointLocation", f"{state_root}/checkpoints/bronze_spike")
    .trigger(availableNow=True)
    .toTable(target)
)
query.awaitTermination()

# COMMAND ----------

df = spark.table(target)
result = {
    "rows_before": rows_before,
    "rows_after": df.count(),
    "rows_added": df.count() - rows_before,
    "distinct_meta_id": df.select("meta.id").distinct().count(),
    "source_files": df.select("_source_file").distinct().count(),
    "types": {r["type"]: r["count"] for r in df.groupBy("type").count().collect()},
    "top_level_columns": len(df.columns),
    "rescued_rows": df.filter(F.col("_rescued_data").isNotNull()).count()
    if "_rescued_data" in df.columns
    else None,
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
