# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold business aggregates for analytics

# COMMAND ----------
from pyspark.sql import functions as F

catalog, schema = "enterprise_dev", "lakehouse"
silver = spark.table(f"{catalog}.{schema}.silver_orders")

gold_daily_sales = (silver
    .withColumn("order_date", F.to_date("order_ts"))
    .groupBy("order_date", "source_system")
    .agg(F.countDistinct("order_id").alias("order_count"),
         F.sum("quantity").alias("units_sold"),
         F.sum("total_amount").alias("gross_sales"),
         F.countDistinct("customer_id").alias("unique_customers")))

(gold_daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_daily_sales"))

# Data quality KPI by source for operational monitoring.
bronze = spark.table(f"{catalog}.{schema}.bronze_orders")
quarantine = spark.table(f"{catalog}.{schema}.quarantine_orders")
quality = (bronze.groupBy("_source_system").agg(F.count("*").alias("bronze_rows"))
    .join(quarantine.groupBy("_source_system").agg(F.count("*").alias("quarantined_rows")),
          "_source_system", "left")
    .fillna(0, ["quarantined_rows"])
    .withColumn("quarantine_rate_pct", F.round(F.col("quarantined_rows") / F.col("bronze_rows") * 100, 2)))

(quality.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.gold_data_quality_metrics"))

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT * FROM enterprise_dev.lakehouse.gold_daily_sales ORDER BY order_date, source_system;
