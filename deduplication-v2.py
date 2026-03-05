# Block 2: Self-Deduplication (RAW Delta → Bronze Delta)
# Production-grade: Snapshot tables + Audit history
# Config-driven, Audit-enabled, Delta Lake

import json, uuid
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, lit, current_timestamp, to_date, regexp_replace, when
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

# -------------------------------------------------------------
# Dynamic schema loader
# -------------------------------------------------------------
def load_schema_from_config(schema_key, config):
    type_map = {
        "string": StringType(),
        "double": DoubleType(),
        "date": TimestampType(),  # normalize dates to timestamp for dedup
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Helper: Deduplicate a source system
# -------------------------------------------------------------
def dedupe_source(src, run_id, error_records, lineage_records):
    try:
        start_time = datetime.now()

        # Build raw path key dynamically: raw_<system>_<entity>
        raw_key = f"raw_{src['name'].lower()}_{src['entity'].lower()}"
        if raw_key not in paths:
            raise Exception(f"Missing path for {raw_key} in paths.json")
        raw_path = paths[raw_key]

        # Read RAW Delta table
        df_raw = spark.read.format("delta").load(raw_path)

        if df_raw.count() == 0:
            raise Exception("Empty RAW dataset")

        # Normalize created_ts if present
        if "created_ts" in df_raw.columns:
            df_raw = df_raw.withColumn(
                "created_ts",
                regexp_replace(col("created_ts"), r"(\d{2})-(\d{2})-(\d{4})", r"\3-\2-\1")
            )
            df_raw = df_raw.withColumn("created_ts", to_date(col("created_ts"), "yyyy-MM-dd"))

        # Deduplication keys from config
        dedupe_keys = src.get("dedupe_keys", ["customer_id"])
        for key in dedupe_keys:
            if key not in df_raw.columns:
                raise Exception(f"Dedup key {key} missing in RAW data")

        # Window spec for ranking
        w = Window.partitionBy(*dedupe_keys).orderBy(col("ingestion_ts").desc())
        df_ranked = df_raw.withColumn("rank", row_number().over(w))

        # Split into main bronze and conflicted bronze
        df_main = df_ranked.filter(col("rank") == 1).drop("rank")
        df_conflicted = df_ranked.filter(col("rank") > 1).drop("rank")

        # Add metadata
        df_main = (df_main.withColumn("bronze_type", lit("MAIN"))
                           .withColumn("dedupe_run_id", lit(run_id))
                           .withColumn("dedupe_ts", current_timestamp())
                           .withColumn("ingestion_date", to_date(col("ingestion_ts"))))

        df_conflicted = (df_conflicted.withColumn("bronze_type", lit("CONFLICTED"))
                                     .withColumn("dedupe_run_id", lit(run_id))
                                     .withColumn("dedupe_ts", current_timestamp())
                                     .withColumn("ingestion_date", to_date(col("ingestion_ts"))))

        # Dynamic subfolder paths
        bronze_main_path = f"{paths['bronze_main']}/{src['name'].lower()}/{src['entity'].lower()}"
        bronze_conflicted_path = f"{paths['bronze_conflicted']}/{src['name'].lower()}/{src['entity'].lower()}"

        # ---------------------------------------------------------
        # Production-grade write pattern (append + partition)
        # ---------------------------------------------------------
        df_main.write.format("delta").mode("append").option("mergeSchema","true") \
            .partitionBy("ingestion_date").save(bronze_main_path)

        df_conflicted.write.format("delta").mode("append").option("mergeSchema","true") \
            .partitionBy("ingestion_date").save(bronze_conflicted_path)

        load_type = "DEDUP"

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Lineage record
        lineage_records.append((
            run_id, f"{src['name']}_{src['entity']}", raw_path, bronze_main_path,
            start_time, end_time, env["environment"], load_type
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

    total_main, total_conflicted = 0, 0
    audit_records = []
    error_records = []
    lineage_records = []
    overall_status = "SUCCESS"

    for src in sources:
        main_count, conflicted_count, status, start_time, end_time, duration, load_type = dedupe_source(src, run_id, error_records, lineage_records)
        total_main += main_count
        total_conflicted += conflicted_count

        if status == "FAILED":
            overall_status = "FAILED"

        # Per-source audit record
        audit_records.append((
            run_id, job_name, f"{src['name']}_{src['entity']}", start_time, end_time,
            float(duration), int(main_count), int(conflicted_count), env["environment"], status, load_type
        ))

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    # Job-level summary
    audit_records.append((
        run_id, job_name, "JOB_SUMMARY", job_start, job_end,
        float(job_duration), int(total_main), int(total_conflicted),
        env["environment"], overall_status, "SUMMARY"
    ))

    # -------------------------------------------------------------
    # Load audit schemas dynamically from config
    # -------------------------------------------------------------
    audit_schema   = load_schema_from_config("audit", schema_config)
    error_schema   = load_schema_from_config("error", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)

    # -------------------------------------------------------------
    # Write Audit tables (append mode for history)
    # -------------------------------------------------------------
    audit_df = spark.createDataFrame(audit_records, schema=audit_schema)
    audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    error_df = spark.createDataFrame(error_records, schema=error_schema) if error_records else spark.createDataFrame([], schema=error_schema)
    error_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_errors"])

    lineage_df = spark.createDataFrame(lineage_records, schema=lineage_schema) if lineage_records else spark.createDataFrame([], schema=lineage_schema)
    lineage_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])

    print(f"Deduplication complete. Run ID={run_id}, Main={total_main}, Conflicted={total_conflicted}, Status={overall_status}")

    # Validation: Show audit tables
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_errors"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
main()
