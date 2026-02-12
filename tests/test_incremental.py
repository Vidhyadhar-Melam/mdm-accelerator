import pytest
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# -------------------------------------------------------------
# Spark Session Fixture
# -------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestIncrementalDeduplication").getOrCreate()

# -------------------------------------------------------------
# Helper Function: Incremental Deduplication with Checkpoint
# -------------------------------------------------------------
def run_incremental_deduplication(df, keys, checkpoint_path):
    # Load checkpoint
    last_ts = None
    if checkpoint_path.exists():
        with open(checkpoint_path) as cp:
            last_ts = json.load(cp).get("last_ingestion_ts")

    # Filter only new records if checkpoint exists
    if last_ts:
        df = df.filter(col("ingestion_ts") > last_ts)

    # Deduplication logic
    window = Window.partitionBy([col(k) for k in keys]).orderBy(col("ingestion_ts").desc())
    deduped = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1).drop("row_num")
    conflicted = df.withColumn("row_num", row_number().over(window)).filter(col("row_num") > 1).drop("row_num")

    # Update checkpoint
    if df.count() > 0:
        max_ts = df.agg({"ingestion_ts": "max"}).collect()[0][0]
        with open(checkpoint_path, "w") as cp:
            json.dump({"last_ingestion_ts": str(max_ts)}, cp)

    return deduped, conflicted

# -------------------------------------------------------------
# Test Case: Incremental Deduplication
# -------------------------------------------------------------
def test_incremental_deduplication(spark, tmp_path):
    checkpoint_path = tmp_path / "crm_checkpoint.json"

    # First batch
    data1 = [
        ("cust1", "2024-01-01"),
        ("cust2", "2024-02-01")
    ]
    df1 = spark.createDataFrame(data1, ["customer_id", "ingestion_ts"])
    deduped1, conflicted1 = run_incremental_deduplication(df1, ["customer_id"], checkpoint_path)
    assert deduped1.count() == 2
    assert conflicted1.count() == 0

    # Second batch (new record only)
    data2 = [
        ("cust3", "2024-03-01"),
        ("cust1", "2024-04-01")  # newer duplicate of cust1
    ]
    df2 = spark.createDataFrame(data2, ["customer_id", "ingestion_ts"])
    deduped2, conflicted2 = run_incremental_deduplication(df2, ["customer_id"], checkpoint_path)
    assert deduped2.count() == 2   # cust3 + latest cust1
    assert conflicted2.count() == 0  # no conflicted record because only latest cust1 is processed
