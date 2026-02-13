"""
Deduplication Script (Hybrid Batch + Streaming, Bronze Layer Creation)
--------------------------------------------------------
Objective:
    1. Read raw tables (Delta).
    2. Apply deduplication rules (ranking logic).
    3. Write results into Bronze tables:
       - main = clean (rank=1)
       - conflicted = duplicates (rank>1) [batch only]
    4. Log audit trail for traceability.
    5. Handle errors and continue processing other sources.
    6. Support both batch and streaming modes with checkpoints.
    7. Integrate Delta Time Travel (version/timestamp queries).
"""

import json, uuid, logging, argparse
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_date
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

# -------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------
LOG_PATH = Path(__file__).resolve().parents[1] / "logs" / "deduplication.log"
logging.basicConfig(filename=LOG_PATH, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Deduplication")

# -------------------------------------------------------------
# Spark Session Setup
# -------------------------------------------------------------
builder = (
    SparkSession.builder
    .appName("MDM-Deduplication")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.adaptive.enabled", "true")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# -------------------------------------------------------------
# Load Configs
# -------------------------------------------------------------
with open(PROJECT_ROOT / "config" / "sources.json") as f:
    sources = json.load(f)["sources"]
with open(PROJECT_ROOT / "config" / "paths.json") as f:
    paths = json.load(f)
with open(PROJECT_ROOT / "config" / "environment.json") as f:
    env = json.load(f)

# -------------------------------------------------------------
# Deduplication Logic (Ranking - Batch Only)
# -------------------------------------------------------------
def dedupe_dataframe(df, src):
    keys = src["dedupe_keys"]
    window = Window.partitionBy([col(k) for k in keys]).orderBy(col("ingestion_ts").desc())

    deduped = df.withColumn("row_num", row_number().over(window)) \
                .filter(col("row_num") == 1).drop("row_num")

    conflicted = df.withColumn("row_num", row_number().over(window)) \
                   .filter(col("row_num") > 1).drop("row_num")

    return deduped, conflicted

# -------------------------------------------------------------
# Batch Deduplication per Source
# -------------------------------------------------------------
def dedupe_source_batch(src, run_id):
    try:
        raw_path = PROJECT_ROOT / "storage" / "raw" / src["name"] / "customer"
        logger.info(f"[BATCH] Reading raw path: {raw_path}")

        df = spark.read.format("delta").load(str(raw_path))
        initial_count = df.count()
        logger.info(f"[BATCH] Initial row count for {src['name']}: {initial_count}")

        if initial_count == 0:
            logger.warning(f"[BATCH] {src['name']} raw data empty, skipping")
            return 0, 0

        if "ingestion_date" not in df.columns and "ingestion_ts" in df.columns:
            df = df.withColumn("ingestion_date", to_date(col("ingestion_ts")))

        deduped, conflicted = dedupe_dataframe(df, src)

        main_path = PROJECT_ROOT / "storage" / "bronze" / "main" / src["name"]
        conflicted_path = PROJECT_ROOT / "storage" / "bronze" / "conflicted" / src["name"]

        deduped.write.format("delta").mode("append").partitionBy("ingestion_date").save(str(main_path))
        conflicted.write.format("delta").mode("append").partitionBy("ingestion_date").save(str(conflicted_path))

        clean_count, conflicted_count = deduped.count(), conflicted.count()
        logger.info(f"[BATCH] {src['name']} deduplication complete. Clean={clean_count}, Conflicted={conflicted_count}")

        # Capture Delta version for time travel
        bronze_version = spark.sql(f"DESCRIBE HISTORY delta.`{str(main_path)}`").first().version

        # Audit Lineage Logging with version
        lineage_data = [(run_id, src["name"], datetime.now(), initial_count,
                         clean_count, conflicted_count, env["environment"], bronze_version)]
        lineage_columns = ["run_id","source","lineage_time","raw_count","clean_count",
                           "conflicted_count","environment","bronze_version"]

        lineage_df = spark.createDataFrame(lineage_data, lineage_columns)
        lineage_path = PROJECT_ROOT / paths["audit_lineage"]
        lineage_df.write.format("delta").mode("append").option("mergeSchema","true").save(str(lineage_path))

        return clean_count, conflicted_count

    except Exception as e:
        logger.error(f"[BATCH] Deduplication failed for {src['name']}", exc_info=True)

        # Audit Errors Logging
        error_data = [(str(uuid.uuid4()), "Deduplication", datetime.now(),
                       src["name"], str(e), env["environment"])]
        error_columns = ["error_id","job_name","error_time","source","error_message","environment"]

        error_df = spark.createDataFrame(error_data, error_columns)
        error_path = PROJECT_ROOT / paths["audit_errors"]
        error_df.write.format("delta").mode("append").option("mergeSchema","true").save(str(error_path))

        return 0, 0

# -------------------------------------------------------------
# Streaming Deduplication per Source (dropDuplicates + watermarking)
# -------------------------------------------------------------
def dedupe_source_streaming(src):
    try:
        raw_path = PROJECT_ROOT / "storage" / "raw" / src["name"] / "customer"
        logger.info(f"[STREAMING] Reading raw path: {raw_path}")

        df_stream = spark.readStream.format("delta").load(str(raw_path))
        keys = src["dedupe_keys"]

        if "ingestion_ts" in df_stream.columns:
            df_stream = df_stream.withColumn("ingestion_date", to_date(col("ingestion_ts")))

        deduped = df_stream \
            .withWatermark("ingestion_ts", "10 minutes") \
            .dropDuplicates(keys)

        main_path = PROJECT_ROOT / "storage" / "bronze" / "main" / src["name"]
        checkpoint_main = PROJECT_ROOT / paths["checkpoints"] / f"{src['name']}_main"

        deduped.writeStream.format("delta").outputMode("append") \
            .option("checkpointLocation", str(checkpoint_main)) \
            .partitionBy("ingestion_date") \
            .start(str(main_path))

        logger.info(f"[STREAMING] {src['name']} streaming deduplication started.")
    except Exception as e:
        logger.error(f"[STREAMING] Deduplication failed for {src['name']}", exc_info=True)

# -------------------------------------------------------------
# Helper Function: Time Travel Reads
# -------------------------------------------------------------
def read_delta_time_travel(path, version=None, timestamp=None):
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    if timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    return reader.load(str(path))

# -------------------------------------------------------------
# Main Job with Audit Logging
# -------------------------------------------------------------
def main(mode="batch", selected_source=None):
    run_id = str(uuid.uuid4())
    job_name = "Deduplication"
    start_time = datetime.now()

    total_clean, total_conflicted = 0, 0
    failed_sources = []

    for src in sources:
        if selected_source and src["name"].lower() != selected_source.lower():
            continue

        try:
            if mode == "batch":
                clean, conflicted = dedupe_source_batch(src, run_id)
                total_clean += clean
                total_conflicted += conflicted
                if clean == 0 and conflicted == 0:
                    failed_sources.append(src["name"])
            elif mode == "streaming":
                dedupe_source_streaming(src)
        except Exception as e:
            logger.error(f"Source {src['name']} failed during deduplication", exc_info=True)
            failed_sources.append(src["name"])
            continue

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    if mode == "batch":
        audit_data = [(run_id, job_name, start_time, end_time, duration,
                       total_clean, total_conflicted, env["environment"], ",".join(failed_sources))]
        audit_columns = ["run_id","job_name","start_time","end_time","duration",
                         "records_clean","records_conflicted","environment","failed_sources"]

        audit_df = spark.createDataFrame(audit_data, audit_columns)
        audit_path = PROJECT_ROOT / paths["audit_runs"]

        audit_df.write.format("delta").mode("append").option("mergeSchema", "true").save(str(audit_path))

        if failed_sources:
            logger.warning(f"Deduplication completed with failures. Failed sources: {failed_sources}")
        else:
            logger.info(f"Deduplication complete. Run ID={run_id}, Clean={total_clean}, "
                        f"Conflicted={total_conflicted}, Duration={duration:.2f}s")

    if mode == "streaming":
        logger.info("Streaming deduplication running... awaiting termination.")
        spark.streams.awaitAnyTermination()

    spark.stop()

# -------------------------------------------------------------
# CLI Entry Point
# -------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run deduplication job")
    parser.add_argument("--source", help="Run deduplication for a specific source system (e.g., CRM, ERP, Salesforce, SAP)")
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch",
                        help="Choose execution mode: batch or streaming (default=batch)")
    args = parser.parse_args()

    main(mode=args.mode, selected_source=args.source)
