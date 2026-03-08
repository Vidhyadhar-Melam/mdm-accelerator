# Raw Ingestion Script (Databricks, Config-Driven, Delta Lake, Incremental + Idempotent per Entity + Data Quality + Partitioning by Ingestion Date + Config-driven Audit Schemas)
# ------------------------------------------------------------------------------

import json, uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col, to_date
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
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Ingest function (MERGE for incremental + idempotent ingestion)
# -------------------------------------------------------------
def ingest_source(src, run_id, error_records, lineage_records, dq_records):
    try:
        start_time = datetime.now()
        schema_key = f"{src['name'].lower()}_{src['entity'].lower()}"
        expected_schema = load_schema_from_config(schema_key, schema_config)

        # Read CSV for this entity
        df_new = spark.read.option("header", "true").option("delimiter", ",").csv(src["path"])

        # Cast columns consistently
        for field in expected_schema.fields:
            if field.name in df_new.columns:
                df_new = df_new.withColumn(field.name, col(field.name).cast(field.dataType))

        # Add metadata
        df_new = (df_new.withColumn("source_system", lit(src["name"]))
                        .withColumn("entity", lit(src["entity"]))
                        .withColumn("ingestion_ts", current_timestamp())
                        .withColumn("ingestion_date", to_date(current_timestamp()))
                        .withColumn("run_id", lit(run_id)))

        # -----------------------------
        # Data Quality Checks
        # -----------------------------
        dq_issues = []
        if "customer_id" in df_new.columns:
            null_ids = df_new.filter(col("customer_id").isNull()).count()
            if null_ids > 0:
                dq_issues.append(("customer_id_null", null_ids))
        if "amount" in df_new.columns:
            bad_amounts = df_new.filter(col("amount") <= 0).count()
            if bad_amounts > 0:
                dq_issues.append(("amount_nonpositive", bad_amounts))
        if "created_ts" in df_new.columns:
            future_dates = df_new.filter(col("created_ts") > current_timestamp()).count()
            if future_dates > 0:
                dq_issues.append(("created_ts_future", future_dates))

        for issue, count in dq_issues:
            dq_records.append((run_id, f"{src['name']}_{src['entity']}", issue, count, datetime.now(), env["environment"]))

        # -----------------------------
        # Write to Raw Delta (MERGE for idempotency + incremental load)
        # -----------------------------
        raw_key = f"raw_{src['name'].lower()}_{src['entity'].lower()}"
        raw_path = paths[raw_key]

        # Extract natural keys from schema config
        natural_keys = [f["name"] for f in schema_config["schemas"][schema_key]["fields"] if f.get("natural_key", False)]

        if natural_keys:
            # Build merge condition dynamically
            merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in natural_keys])
            df_new.createOrReplaceTempView("s")
            spark.sql(f"""
                MERGE INTO delta.`{raw_path}` t
                USING s
                ON {merge_condition}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
            load_type = "MERGE"
        else:
            # Fallback to append if no natural key defined
            df_new.write.format("delta").mode("append").partitionBy("ingestion_date").save(raw_path)
            load_type = "APPEND"

        # Log lineage
        end_time = datetime.now()
        lineage_records.append((run_id, f"{src['name']}_{src['entity']}", src["path"], raw_path,
                                start_time, end_time, env["environment"], load_type))

        return df_new.count(), 0, "SUCCESS", start_time, end_time, (end_time - start_time).total_seconds(), load_type

    except Exception as e:
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
    audit_records, error_records, lineage_records, dq_records = [], [], [], []
    overall_status = "SUCCESS"

    for src in sources:
        loaded, rejected, status, start_time, end_time, duration, load_type = ingest_source(src, run_id, error_records, lineage_records, dq_records)
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
    # Load audit schemas dynamically from config
    # -------------------------------------------------------------
    audit_schema   = load_schema_from_config("audit", schema_config)
    error_schema   = load_schema_from_config("error", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)
    dq_schema      = load_schema_from_config("dq", schema_config)

    # -------------------------------------------------------------
    # Write Audit tables (append mode for history)
    # -------------------------------------------------------------
    audit_df = spark.createDataFrame(audit_records, schema=audit_schema)
    audit_df.write.format("delta").mode("append").save(paths["audit_runs"])

    error_df = spark.createDataFrame(error_records, schema=error_schema) if error_records else spark.createDataFrame([], schema=error_schema)
    error_df.write.format("delta").mode("append").save(paths["audit_errors"])

    lineage_df = spark.createDataFrame(lineage_records, schema=lineage_schema) if lineage_records else spark.createDataFrame([], schema=lineage_schema)
    lineage_df.write.format("delta").mode("append").save(paths["audit_lineage"])

    dq_df = spark.createDataFrame(dq_records, schema=dq_schema) if dq_records else spark.createDataFrame([], schema=dq_schema)
    dq_df.write.format("delta").mode("append").save(paths["audit_dq"])

    # Print job summary
    print(f"Ingestion complete. Run ID={run_id}, Loaded={total_loaded}, Rejected={total_rejected}, Status={overall_status}")

    # Validation: Show audit tables
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_errors"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))
    display(spark.read.format("delta").load(paths["audit_dq"]))

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
main()
