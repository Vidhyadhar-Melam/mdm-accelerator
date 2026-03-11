# Block 2: Deduplication (RAW Delta → Bronze Delta)
# -------------------------------------------------------------------
# Purpose:
#   - Deduplicate records from Raw tables into Bronze tables
#   - Latest record per dedupe key → Bronze MAIN
#   - Older duplicates → Bronze CONFLICTED
#   - Checkpoints ensure incremental loads (only new rows processed)
#   - Reset mode allows full reinitialization
#
# Key Updates:
#   - Filter new records by ingestion_ts (stable across runs)
#   - Order deduplication by updated_ts (latest wins)
#   - Prevents reprocessing and inflated totals
#   - Audit schema aligned with ingestion (12 fields)
#   - Production-grade audit mapping: incoming_count = total processed,
#     inserted_count = MAIN records, conflicted_count tracked separately

import json, uuid
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, lit, current_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType, DateType

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

# Reset mode flag (true = full rebuild, false = incremental)
reset_mode = env.get("reset_mode", False)

# -------------------------------------------------------------
# Dynamic schema loader
# -------------------------------------------------------------
def load_schema_from_config(schema_key, config):
    type_map = {
        "string": StringType(),
        "double": DoubleType(),
        "date": DateType(),          # align with ingestion
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Checkpoint schema (track last_ingestion_ts processed)
# -------------------------------------------------------------
checkpoint_schema = StructType([
    StructField("source_system", StringType(), True),
    StructField("entity", StringType(), True),
    StructField("last_ingestion_ts", TimestampType(), True),  # stable checkpoint
    StructField("last_run_id", StringType(), True),
    StructField("last_run_ts", TimestampType(), True),
    StructField("environment", StringType(), True)
])

checkpoint_path = f"{paths['checkpoints']}/dedup_checkpoint"

# -------------------------------------------------------------
# Helper: Deduplicate a source system/entity
# -------------------------------------------------------------
def dedupe_source(src, run_id, error_records, lineage_records, checkpoint_updates):
    try:
        start_time = datetime.now()

        raw_key = f"raw_{src['name'].lower()}_{src['entity'].lower()}"
        raw_path = paths[raw_key]

        # Load Raw data
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

        # Filter new records by ingestion_ts (stable across runs)
        df_new = df_raw.filter(col("ingestion_ts") > last_run_ts) if last_run_ts else df_raw
        if df_new.count() == 0:
            print(f"No new records for {src['name']}_{src['entity']}")
            return 0, 0, 0, "SUCCESS", start_time, datetime.now(), 0.0, "NO_NEW"

        # Deduplication keys (from sources.json)
        dedupe_keys = src.get("dedupe_keys", ["customer_id"])
        for key in dedupe_keys:
            if key not in df_new.columns:
                raise Exception(f"Dedup key {key} missing in RAW data")

        # Window: partition by dedupe keys, order by updated_ts desc
        w = Window.partitionBy(*dedupe_keys).orderBy(col("updated_ts").desc())
        df_ranked = df_new.withColumn("rank", row_number().over(w))

        # Split into MAIN and CONFLICTED
        df_main = df_ranked.filter(col("rank") == 1).drop("rank")
        df_conflicted = df_ranked.filter(col("rank") > 1).drop("rank")

        # Add metadata columns
        df_main = (df_main.withColumn("bronze_type", lit("MAIN"))
                           .withColumn("dedupe_run_id", lit(run_id))
                           .withColumn("dedupe_ts", current_timestamp())
                           .withColumn("ingestion_date", to_date(current_timestamp())))

        df_conflicted = (df_conflicted.withColumn("bronze_type", lit("CONFLICTED"))
                                     .withColumn("dedupe_run_id", lit(run_id))
                                     .withColumn("dedupe_ts", current_timestamp())
                                     .withColumn("ingestion_date", to_date(current_timestamp())))

        # Bronze paths
        bronze_main_path = f"{paths['bronze_main']}/{src['name'].lower()}/{src['entity'].lower()}"
        bronze_conflicted_path = f"{paths['bronze_conflicted']}/{src['name'].lower()}/{src['entity'].lower()}"

        # Write pattern (overwrite if reset, append if incremental)
        mode = "overwrite" if reset_mode else "append"
        if df_main.count() > 0:
            df_main.write.format("delta").mode(mode).option("mergeSchema","true") \
                .partitionBy("ingestion_date").save(bronze_main_path)
        if df_conflicted.count() > 0:
            df_conflicted.write.format("delta").mode(mode).option("mergeSchema","true") \
                .partitionBy("ingestion_date").save(bronze_conflicted_path)

        # Lineage record
        end_time = datetime.now()
        lineage_records.append((
            run_id, f"{src['name']}_{src['entity']}", raw_path, bronze_main_path,
            start_time, end_time, env["environment"], "DEDUP"
        ))

        # Update checkpoint with max ingestion_ts
        max_ingestion_ts = df_new.agg({"ingestion_ts": "max"}).collect()[0][0]
        checkpoint_updates.append((
            src["name"], src["entity"], max_ingestion_ts, run_id, end_time, env["environment"]
        ))

        return int(df_new.count()), int(df_main.count()), int(df_conflicted.count()), "SUCCESS", start_time, end_time, float((end_time - start_time).total_seconds()), "DEDUP"

    except Exception as e:
        print(f"Error deduping {src['name']}_{src['entity']}: {e}")
        error_records.append((
            run_id, f"{src['name']}_{src['entity']}", str(e), datetime.now(), env["environment"]
        ))
        return 0, 0, 0, "FAILED", datetime.now(), datetime.now(), 0.0, "FAILED"

# -------------------------------------------------------------
# Main Job
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Raw-to-Bronze-Dedup"
    job_start = datetime.now()

    total_incoming, total_main, total_conflicted = 0, 0, 0
    audit_records, error_records, lineage_records, checkpoint_updates = [], [], [], []
    overall_status = "SUCCESS"

    # -------------------------------------------------------------
    # Process each source system/entity
    # -------------------------------------------------------------
    for src in sources:
        incoming_count, main_count, conflicted_count, status, start_time, end_time, duration, load_type = dedupe_source(
            src, run_id, error_records, lineage_records, checkpoint_updates
        )
        total_incoming += incoming_count
        total_main += main_count
        total_conflicted += conflicted_count

        if status == "FAILED":
            overall_status = "FAILED"

        # Audit record (production-grade mapping)
        audit_records.append((
            run_id, 
            job_name, 
            f"{src['name']}_{src['entity']}", 
            start_time, 
            end_time,
            float(duration), 
            int(incoming_count),     # total rows processed for this source
            int(main_count),         # MAIN records written to Bronze
            int(conflicted_count),   # CONFLICTED records written to Bronze
            env["environment"], 
            status, 
            load_type
        ))

    # -------------------------------------------------------------
    # Job summary record
    # -------------------------------------------------------------
    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    audit_records.append((
        run_id, 
        job_name, 
        "JOB_SUMMARY", 
        job_start, 
        job_end,
        float(job_duration), 
        int(total_incoming),     # total rows processed across all sources
        int(total_main),         # total MAIN records across all sources
        int(total_conflicted),   # total CONFLICTED records across all sources
        env["environment"], 
        overall_status, 
        "SUMMARY"
    ))

    # -------------------------------------------------------------
    # Schemas for audit/error/lineage
    # -------------------------------------------------------------
    audit_schema   = load_schema_from_config("audit", schema_config)
    error_schema   = load_schema_from_config("error", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)

    # -------------------------------------------------------------
    # Write Audit tables
    # -------------------------------------------------------------
    spark.createDataFrame(audit_records, schema=audit_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    spark.createDataFrame(error_records, schema=error_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_errors"])

    spark.createDataFrame(lineage_records, schema=lineage_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])

    # -------------------------------------------------------------
    # Write checkpoint updates (based on ingestion_ts)
    # -------------------------------------------------------------
    if checkpoint_updates:
        mode = "overwrite" if reset_mode else "append"
        spark.createDataFrame(checkpoint_updates, schema=checkpoint_schema) \
            .write.format("delta").mode(mode).option("mergeSchema","true").save(checkpoint_path)

    # -------------------------------------------------------------
    # Compute totals from Bronze tables
    # -------------------------------------------------------------
    total_main_records = 0
    total_conflicted_records = 0
    try:
        for src in sources:
            bronze_main_path = f"{paths['bronze_main']}/{src['name'].lower()}/{src['entity'].lower()}"
            bronze_conflicted_path = f"{paths['bronze_conflicted']}/{src['name'].lower()}/{src['entity'].lower()}"
            total_main_records += spark.read.format("delta").load(bronze_main_path).count()
            total_conflicted_records += spark.read.format("delta").load(bronze_conflicted_path).count()
    except:
        # If Bronze is empty on first run, skip totals
        pass

    # -------------------------------------------------------------
    # Final output summary
    # -------------------------------------------------------------
    print(f"Deduplication complete. Run ID={run_id}")
    print(f"Incoming={total_incoming}, New Main={total_main}, New Conflicted={total_conflicted}")
    print(f"Total Main={total_main_records}, Total Conflicted={total_conflicted_records}")
    print(f"Status={overall_status}")

    # -------------------------------------------------------------
    # Validation: Display audit and checkpoint tables
    # -------------------------------------------------------------
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_errors"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))
    display(spark.read.format("delta").load(checkpoint_path))

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
main()

