from pyspark.sql import functions as F
from pyspark.sql.window import Window


def standardize_orders(df):
    """Cast order fields and normalize source values."""
    return (df
        .withColumn("order_id", F.trim(F.col("order_id").cast("string")))
        .withColumn("customer_id", F.trim(F.col("customer_id").cast("string")))
        .withColumn("product_id", F.trim(F.col("product_id").cast("string")))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(18,2)"))
        .withColumn("source_system", F.upper(F.trim(F.col("source_system"))))
        .withColumn("total_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)")))


def split_valid_and_quarantine(df):
    """Return valid rows and invalid rows with a concise rejection reason."""
    reason = (F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), "MISSING_ORDER_ID")
        .when(F.col("customer_id").isNull() | (F.col("customer_id") == ""), "MISSING_CUSTOMER_ID")
        .when(F.col("order_ts").isNull(), "INVALID_ORDER_TIMESTAMP")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "INVALID_UNIT_PRICE"))
    tagged = df.withColumn("rejection_reason", reason)
    return tagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason"), tagged.filter(F.col("rejection_reason").isNotNull())


def deduplicate_orders(df):
    """Keep the most recently ingested version per order_id."""
    w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
