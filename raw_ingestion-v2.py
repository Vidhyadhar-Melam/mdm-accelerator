# Raw Ingestion Script (Databricks, Config-Driven, Delta Lake, Append for History)
# ------------------------------------------------------------------------------
# Features:
# - Reads schemas dynamically from schemas.json
# - Append-only ingestion (no drop/recreate)
# - Safe casting with try_to_date
# - Metadata columns added automatically
# - Audit tables capture run details, errors, and lineage
# ------------------------------------------------------------------------------

import json, uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType, LongType

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
        "date": DateType(),
        "timestamp": TimestampType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]

    # Add metadata fields automatically
    struct_fields += [
        StructField("source_system", StringType(), True),
        StructField("entity", StringType(), True),
        StructField("ingestion_ts", TimestampType(), True),
        StructField("run_id", StringType(), True)
    ]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Ingest function (append-only)
# -------------------------------------------------------------
def ingest_source(src, run_id, error_records, lineage_records):
    try:
        start_time = datetime.now()
        schema_key = f"{src['name'].lower()}_{src['entity'].lower()}"
        expected_schema = load_schema_from_config(schema_key, schema_config)

        # Read CSV
        df_new = spark.read.option("header", "true").option("delimiter", ",").csv(src["path"])

        # Cast columns consistently
        for field in expected_schema.fields:
            if field.name in df_new.columns:
                if isinstance(field.dataType, StringType):
                    df_new = df_new.withColumn(field.name, col(field.name).cast(StringType()))
                if isinstance(field.dataType, DoubleType):
                    df_new = df_new.withColumn(field.name, col(field.name).cast(DoubleType()))
                if isinstance(field.dataType, DateType):
                    df_new = df_new.withColumn(field.name, expr(f"try_to_date({field.name}, 'yyyy-MM-dd')"))

        # Add metadata
        df_new = (df_new.withColumn("source_system", lit(src["name"]))
                        .withColumn("entity", lit(src["entity"]))
                        .withColumn("ingestion_ts", current_timestamp())
                        .withColumn("run_id", lit(run_id)))

        # Build raw path
        system = src['name'].lower()
        entity = src['entity'].lower()
        raw_path = f"s3://databricks-amz-s3-bucket/mdm-accelerator/storage-v2/raw/{system}/{system}_{entity}"

        # Append only
        df_new.write.format("delta").mode("append").save(raw_path)
        load_type = "APPEND"

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        lineage_records.append((run_id, f"{src['name']}_{src['entity']}", src["path"], raw_path,
                                start_time, end_time, env["environment"], load_type))
        return int(df_new.count()), 0, "SUCCESS", start_time, end_time, float(duration), load_type

    except Exception as e:
        print(f"Error ingesting {src['name']} {src['entity']}: {e}")
        error_records.append((run_id, f"{src['name']}_{src['entity']}", str(e), datetime.now(), env["environment"]))
        return 0, 0, "FAILED", datetime.now(), datetime.now(), 0.0, "FAILED"

# -------------------------------------------------------------
# Main Job
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Raw-Ingestion"
    job_start = datetime.now()

    total_loaded, total_rejected = 0, 0
    audit_records, error_records, lineage_records = [], [], []
    overall_status = "SUCCESS"

    for src in sources:
        loaded, rejected, status, start_time, end_time, duration, load_type = ingest_source(src, run_id, error_records, lineage_records)
        total_loaded += loaded
        total_rejected += rejected
        if status == "FAILED":
            overall_status = "FAILED"
        audit_records.append((run_id, job_name, f"{src['name']}_{src['entity']}", start_time, end_time,
                              float(duration), int(loaded), int(rejected), env["environment"], status, load_type))

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    # Add job-level summary record
    audit_records.append((run_id, job_name, "JOB_SUMMARY", job_start, job_end,
                          float(job_duration), int(total_loaded), int(total_rejected),
                          env["environment"], overall_status, "SUMMARY"))

    # -------------------------------------------------------------
    # Define schemas for audit tables
    # -------------------------------------------------------------
    audit_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("job_name", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("duration", DoubleType(), True),
        StructField("records_loaded", LongType(), True),
        StructField("records_rejected", LongType(), True),
        StructField("environment", StringType(), True),
        StructField("status", StringType(), True),
        StructField("load_type", StringType(), True)
    ])

    error_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_time", TimestampType(), True),
        StructField("environment", StringType(), True)
    ])

    lineage_schema = StructType([
        StructField("run_id", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("target_path", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("environment", StringType(), True),
        StructField("load_type", StringType(), True)
    ])

    # -------------------------------------------------------------
    # Write Audit tables (append mode for history)
    # -------------------------------------------------------------
    audit_df = spark.createDataFrame(audit_records, schema=audit_schema)
    audit_df.write.format("delta").mode("append").save(paths["audit_runs"])

    error_df = spark.createDataFrame(error_records, schema=error_schema) if error_records else spark.createDataFrame([], schema=error_schema)
    error_df.write.format("delta").mode("append").save(paths["audit_errors"])

    lineage_df = spark.createDataFrame(lineage_records, schema=lineage_schema) if lineage_records else spark.createDataFrame([], schema=lineage_schema)
    lineage_df.write.format("delta").mode("append").save(paths["audit_lineage"])

    print(f"Ingestion complete. Run ID={run_id}, Loaded={total_loaded}, Rejected={total_rejected}, Status={overall_status}")

    # -------------------------------------------------------------
    # Validation: Show all audit tables fully
    # -------------------------------------------------------------
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_errors"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
main()
