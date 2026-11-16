# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze ingestion using metadata-driven Auto Loader
# MAGIC Configure the paths/catalog/schema for your workspace before running.

# COMMAND ----------
from pyspark.sql import functions as F
import json

# Replace with DBFS/Repos path or store metadata in a governed Volume.
metadata_path = "/Workspace/Repos/<user>/enterprise_databricks_project/config/source_metadata.json"
with open(metadata_path, "r") as f:
    source_configs = json.load(f)

catalog = "enterprise_dev"
schema = "lakehouse"
landing_base = "abfss://landing@<storage-account>.dfs.core.windows.net/"
checkpoint_base = "abfss://checkpoints@<storage-account>.dfs.core.windows.net/enterprise/"
schema_base = "abfss://schemas@<storage-account>.dfs.core.windows.net/enterprise/"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------
# Process each enabled source using its declared format and independent state paths.
for cfg in source_configs:
    if not cfg.get("enabled", False):
        continue

    source = cfg["source_system"].lower()
    entity = cfg["entity"]
    fmt = cfg["file_format"]
    input_path = landing_base + cfg["landing_subpath"]
    schema_location = schema_base + f"{source}/{entity}/"
    checkpoint_location = checkpoint_base + f"{source}/{entity}/"

    reader = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    if fmt == "csv":
        reader = reader.option("header", cfg.get("header", "true")) \
                       .option("delimiter", cfg.get("delimiter", ","))

    raw = (reader.load(input_path)
        .withColumn("_source_system", F.lit(cfg["source_system"]))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp()))

    query = (raw.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.{schema}.{cfg['target_table']}"))

# COMMAND ----------
# Verify Bronze landing
# MAGIC %sql
# MAGIC SELECT _source_system, COUNT(*) AS rows_ingested, MAX(_ingested_at) AS latest_ingestion
# MAGIC FROM enterprise_dev.lakehouse.bronze_orders
# MAGIC GROUP BY _source_system;
