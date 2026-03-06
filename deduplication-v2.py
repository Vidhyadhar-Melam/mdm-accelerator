# Block 2: Deduplication (RAW Delta → Bronze Delta)
# Production-grade: Snapshot tables + Audit history + Checkpoint table
# Config-driven, Audit-enabled, Delta Lake
# Supports reset_mode flag in environment.json
# Partitions Bronze by job run date

import json, uuid
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

# -------------------------------------------------------------
# Load configs
# -------------------------------------------------------------
sources_path = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/sources.json"
paths_path   = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/paths.json"
env_path     = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/environment.json"
schema_config_path = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/schemas.json"

sources = json.loads(dbutils.fs.head(sources_path, 100000))["sources"]
paths   = json.loads(dbutils.fs.head(paths_path, 100000))
env     = json.loads(dbutils.fs.head(env_path, 100000))
schema_config = json.loads(dbutils.fs.head(schema_config_path, 100000))

# Reset mode flag
reset_mode = env.get("reset_mode", False)

# -------------------------------------------------------------
# Dynamic schema loader
# -------------------------------------------------------------
def load_schema_from_config(schema_key, config):
    type_map = {
        "string": StringType(),
        "double": DoubleType(),
        "date": TimestampType(),   # normalize dates to timestamp
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Checkpoint schema
# -------------------------------------------------------------
checkpoint_schema = StructType([
    StructField("source_system", StringType(), True),
    StructField("entity", StringType(), True),
    StructField("last_ingestion_ts", TimestampType(), True),
    StructField("last_run_id", StringType(), True),
    StructField("last_run_ts", TimestampType(), True),
    StructField("environment", StringType(), True)
])

checkpoint_path = f"{paths['checkpoints']}/dedup_checkpoint"

# -------------------------------------------------------------
# Helper: Deduplicate a source system
# -------------------------------------------------------------
def dedupe_source(src, run_id, error_records, lineage_records, checkpoint_updates):
    try:
        start_time = datetime.now()

        raw_key = f"raw_{src['name'].lower()}_{src['entity'].lower()}"
        if raw_key not in paths:
            raise Exception(f"Missing path for {raw_key} in paths.json")
        raw_path = paths[raw_key]

        df_raw = spark.read.format("delta").load(raw_path)
        if df_raw.count() == 0:
            raise Exception("Empty RAW dataset")

        # Checkpoint lookup (skip if reset_mode = true)
        last_run_ts = None
        if not reset_mode:
            try:
                df_checkpoint = spark.read.format("delta").load(checkpoint_path)
                last_run_ts_row = df_checkpoint.filter(
                    (col("source_system") == src["name"]) & (col("entity") == src["entity"])
                ).agg({"last_ingestion_ts": "max"}).collect()
                if last_run_ts_row and last_run_ts_row[0][0] is not None:
                    last_run_ts = last_run_ts_row[0][0]
            except:
                last_run_ts = None

        # Filter records
        df_new = df_raw.filter(col("ingestion_ts") > last_run_ts) if last_run_ts else df_raw
        if df_new.count() == 0:
            print(f"No new records for {src['name']}_{src['entity']}")
            return 0, 0, "SUCCESS", start_time, datetime.now(), 0.0, "NO_NEW"

        # Deduplication keys
        dedupe_keys = src.get("dedupe_keys", ["customer_id"])
        for key in dedupe_keys:
            if key not in df_new.columns:
                raise Exception(f"Dedup key {key} missing in RAW data")

        w = Window.partitionBy(*dedupe_keys).orderBy(col("ingestion_ts").desc())
        df_ranked = df_new.withColumn("rank", row_number().over(w))

        df_main = df_ranked.filter(col("rank") == 1).drop("rank")
        df_conflicted = df_ranked.filter(col("rank") > 1).drop("rank")

        # Metadata (partition by job run date)
        df_main = (df_main.withColumn("bronze_type", lit("MAIN"))
                           .withColumn("dedupe_run_id", lit(run_id))
                           .withColumn("dedupe_ts", current_timestamp())
                           .withColumn("ingestion_date", to_date(current_timestamp())))

        df_conflicted = (df_conflicted.withColumn("bronze_type", lit("CONFLICTED"))
                                     .withColumn("dedupe_run_id", lit(run_id))
                                     .withColumn("dedupe_ts", current_timestamp())
                                     .withColumn("ingestion_date", to_date(current_timestamp())))

        bronze_main_path = f"{paths['bronze_main']}/{src['name'].lower()}/{src['entity'].lower()}"
        bronze_conflicted_path = f"{paths['bronze_conflicted']}/{src['name'].lower()}/{src['entity'].lower()}"

        # Write pattern
        mode = "overwrite" if reset_mode else "append"
        if df_main.count() > 0:
            df_main.write.format("delta").mode(mode).option("mergeSchema","true") \
                .partitionBy("ingestion_date").save(bronze_main_path)
        if df_conflicted.count() > 0:
            df_conflicted.write.format("delta").mode(mode).option("mergeSchema","true") \
                .partitionBy("ingestion_date").save(bronze_conflicted_path)

        load_type = "DEDUP"
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Lineage record
        lineage_records.append((
            run_id, f"{src['name']}_{src['entity']}", raw_path, bronze_main_path,
            start_time, end_time, env["environment"], load_type
        ))

        # Update checkpoint
        max_ingestion_ts = df_new.agg({"ingestion_ts": "max"}).collect()[0][0]
        checkpoint_updates.append((
            src["name"], src["entity"], max_ingestion_ts, run_id, end_time, env["environment"]
        ))

        return int(df_main.count()), int(df_conflicted.count()), "SUCCESS", start_time, end_time, float(duration), load_type

    except Exception as e:
        print(f"Error deduping {src['name']}_{src['entity']}: {e}")
        error_records.append((
            run_id, f"{src['name']}_{src['entity']}", str(e), datetime.now(), env["environment"]
        ))
        return 0, 0, "FAILED", datetime.now(), datetime.now(), 0.0, "FAILED"

# -------------------------------------------------------------
# Main Job
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Raw-to-Bronze-Dedup"
    job_start = datetime.now()

    new_main, new_conflicted = 0, 0
    audit_records, error_records, lineage_records, checkpoint_updates = [], [], [], []
    overall_status = "SUCCESS"

    for src in sources:
        main_count, conflicted_count, status, start_time, end_time, duration, load_type = dedupe_source(
            src, run_id, error_records, lineage_records, checkpoint_updates
        )
        new_main += main_count
        new_conflicted += conflicted_count
        if status == "FAILED":
            overall_status = "FAILED"

        audit_records.append((
            run_id, job_name, f"{src['name']}_{src['entity']}", start_time, end_time,
            float(duration), int(main_count), int(conflicted_count), env["environment"], status, load_type
        ))

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    # Job summary
    audit_records.append((
        run_id, job_name, "JOB_SUMMARY", job_start, job_end,
        float(job_duration), int(new_main), int(new_conflicted),
        env["environment"], overall_status, "SUMMARY"
    ))

    # Schemas
    audit_schema   = load_schema_from_config("audit", schema_config)
    error_schema   = load_schema_from_config("error", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)

    # Write Audit tables
    spark.createDataFrame(error_records, schema=error_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_errors"])

    spark.createDataFrame(lineage_records, schema=lineage_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])

    # Write checkpoint updates
    if checkpoint_updates:
        mode = "overwrite" if reset_mode else "append"
        spark.createDataFrame(checkpoint_updates, schema=checkpoint_schema) \
            .write.format("delta").mode(mode).option("mergeSchema","true").save(checkpoint_path)

    # Compute totals from Bronze tables
    total_main_records = 0
    total_conflicted_records = 0
    try:
        for src in sources:
            bronze_main_path = f"{paths['bronze_main']}/{src['name'].lower()}/{src['entity'].lower()}"
            bronze_conflicted_path = f"{paths['bronze_conflicted']}/{src['name'].lower()}/{src['entity'].lower()}"
            total_main_records += spark.read.format("delta").load(bronze_main_path).count()
            total_conflicted_records += spark.read.format("delta").load(bronze_conflicted_path).count()
    except:
        pass  # if Bronze is empty on first run

    # Final output
    print(f"Deduplication complete. Run ID={run_id}")
    print(f"New Main={new_main}, New Conflicted={new_conflicted}")
    print(f"Total Main={total_main_records}, Total Conflicted={total_conflicted_records}")
    print(f"Status={overall_status}")

    # Validation
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_errors"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))
    display(spark.read.format("delta").load(checkpoint_path))

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
main()
