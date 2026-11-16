# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver cleansing, validation, quarantine and deduplication

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog, schema = "enterprise_dev", "lakehouse"
bronze = spark.table(f"{catalog}.{schema}.bronze_orders")

# Normalize both metadata columns and source-provided source_system.
orders = (bronze
    .withColumn("order_id", F.trim(F.col("order_id").cast("string")))
    .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
    .withColumn("product_id", F.trim(F.col("product_id").cast("string")))
    .withColumn("order_ts", F.to_timestamp("order_ts"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
    .withColumn("source_system", F.coalesce(F.col("source_system"), F.col("_source_system")))
    .withColumn("total_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)")))

reason = (F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "MISSING_ORDER_ID")
    .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "MISSING_CUSTOMER_ID")
    .when(F.col("order_ts").isNull(), "INVALID_ORDER_TIMESTAMP")
    .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY")
    .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "INVALID_UNIT_PRICE"))

quarantine = orders.withColumn("rejection_reason", reason).filter(F.col("rejection_reason").isNotNull())
valid = orders.withColumn("rejection_reason", reason).filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

# Keep latest arrival for each business key; deterministic tie-breaker by source file.
w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
silver = valid.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

(quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.quarantine_orders"))

# Idempotent upsert pattern for repeatable batch runs.
silver.createOrReplaceTempView("silver_orders_updates")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.silver_orders
USING DELTA AS SELECT * FROM silver_orders_updates WHERE 1 = 0
""")
spark.sql(f"""
MERGE INTO {catalog}.{schema}.silver_orders AS target
USING silver_orders_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source._ingested_at >= target._ingested_at THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT 'silver' AS layer, COUNT(*) AS records FROM enterprise_dev.lakehouse.silver_orders
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine' AS layer, COUNT(*) AS records FROM enterprise_dev.lakehouse.quarantine_orders;
