import pytest
from pyspark.sql import SparkSession
from src.transformations import standardize_orders, split_valid_and_quarantine

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("lakehouse-tests").getOrCreate()


def test_standardize_and_split(spark):
    rows = [
        ("A1", "C1", "2026-11-14T10:00:00", "P1", "2", "10.50", "STEP"),
        ("A2", None, "2026-11-14T11:00:00", "P2", "1", "20.00", "ORBIT"),
    ]
    df = spark.createDataFrame(rows, ["order_id", "customer_id", "order_ts", "product_id", "quantity", "unit_price", "source_system"])
    standardized = standardize_orders(df)
    valid, rejected = split_valid_and_quarantine(standardized)
    assert valid.count() == 1
    assert rejected.count() == 1
    assert rejected.first()["rejection_reason"] == "MISSING_CUSTOMER_ID"
