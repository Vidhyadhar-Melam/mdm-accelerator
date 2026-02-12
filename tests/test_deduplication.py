# Create tests/test_deduplication.py with multiple scenarios

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# -------------------------------------------------------------
# PyTest Fixture: Spark Session
# -------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestDeduplication").getOrCreate()

# -------------------------------------------------------------
# Helper Function: Deduplication Logic
# -------------------------------------------------------------
def run_deduplication(df, keys):
    window = Window.partitionBy([col(k) for k in keys]).orderBy(col("ingestion_ts").desc())
    deduped = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1)
    conflicted = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") > 1)
    return deduped, conflicted

# -------------------------------------------------------------
# Test Case 1: Basic Duplicate
# -------------------------------------------------------------
def test_deduplication_basic(spark):
    data = [
        ("cust1", "2024-01-01"),
        ("cust1", "2024-02-01"),  # newer duplicate
        ("cust2", "2024-03-01")
    ]
    df = spark.createDataFrame(data, ["customer_id", "ingestion_ts"])
    deduped, conflicted = run_deduplication(df, ["customer_id"])
    assert deduped.count() == 2
    assert conflicted.count() == 1

# -------------------------------------------------------------
# Test Case 2: No Duplicates
# -------------------------------------------------------------
def test_deduplication_no_duplicates(spark):
    data = [
        ("cust1", "2024-01-01"),
        ("cust2", "2024-02-01"),
        ("cust3", "2024-03-01")
    ]
    df = spark.createDataFrame(data, ["customer_id", "ingestion_ts"])
    deduped, conflicted = run_deduplication(df, ["customer_id"])
    assert deduped.count() == 3
    assert conflicted.count() == 0

# -------------------------------------------------------------
# Test Case 3: Multiple Duplicates
# -------------------------------------------------------------
def test_deduplication_multiple_duplicates(spark):
    data = [
        ("cust1", "2024-01-01"),
        ("cust1", "2024-02-01"),
        ("cust1", "2024-03-01"),  # newest
        ("cust2", "2024-04-01")
    ]
    df = spark.createDataFrame(data, ["customer_id", "ingestion_ts"])
    deduped, conflicted = run_deduplication(df, ["customer_id"])
    assert deduped.count() == 2   # cust1 latest + cust2
    assert conflicted.count() == 2  # older cust1 records

# -------------------------------------------------------------
# Test Case 4: Empty Dataset
# -------------------------------------------------------------
def test_deduplication_empty(spark):
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("ingestion_ts", StringType(), True)
    ])
    df = spark.createDataFrame([], schema)

    deduped, conflicted = run_deduplication(df, ["customer_id"])
    assert deduped.count() == 0
    assert conflicted.count() == 0


# -------------------------------------------------------------
# Test Case 5: Composite Keys
# -------------------------------------------------------------
def test_deduplication_composite_keys(spark):
    data = [
        ("cust1", "email1", "2024-01-01"),
        ("cust1", "email1", "2024-02-01"),  # duplicate on composite key
        ("cust1", "email2", "2024-03-01")   # different email, not duplicate
    ]
    df = spark.createDataFrame(data, ["customer_id", "email", "ingestion_ts"])
    deduped, conflicted = run_deduplication(df, ["customer_id", "email"])
    assert deduped.count() == 2   # latest for cust1+email1, plus cust1+email2
    assert conflicted.count() == 1  # older cust1+email1
