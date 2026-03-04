# Raw Ingestion Script (Databricks, Config-Driven, Delta Lake, Append for History)

import json, uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col, regexp_replace, to_date, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType

# -------------------------------------------------------------
# Load configs directly from S3 using dbutils.fs.head
# -------------------------------------------------------------
sources_path = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/sources.json"
paths_path   = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/paths.json"
env_path     = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/environment.json"

sources = json.loads(dbutils.fs.head(sources_path, 100000))["sources"]
paths   = json.loads(dbutils.fs.head(paths_path, 100000))
env     = json.loads(dbutils.fs.head(env_path, 100000))

# -------------------------------------------------------------
# Helper: Ingest a source system + entity (Append for History)
# -------------------------------------------------------------
def ingest_source(src, run_id, error_records, lineage_records):
    try:
        start_time = datetime.now()

        # Read CSV directly from S3
        df_new = spark.read.option("header", "true") \
                           .option("inferSchema", "true") \
                           .option("delimiter", ",") \
                           .csv(src["path"])

        # Standardize column names
        for col_name in df_new.columns:
            lower = col_name.lower()
            if lower in ["cust_email","sf_email","erp_email","sap_email","email"]:
                df_new = df_new.withColumnRenamed(col_name,"email")
            if lower in ["cust_phone","phone_number","contact_phone","phone"]:
                df_new = df_new.withColumnRenamed(col_name,"phone")
            if lower in ["id","cust_id","customerid","customer_id"]:
                df_new = df_new.withColumnRenamed(col_name,"customer_id")
            if lower in ["accountid","acct_id","account_id"]:
                df_new = df_new.withColumnRenamed(col_name,"account_id")
            if lower in ["transactionid","txn_id","transaction_id"]:
                df_new = df_new.withColumnRenamed(col_name,"transaction_id")

        # Normalize created_ts column to DateType
        if "created_ts" in df_new.columns:
            df_new = df_new.withColumn(
                "created_ts",
                regexp_replace(col("created_ts"), r"(\d{2})-(\d{2})-(\d{4})", r"\3-\2-\1")
            )
            df_new = df_new.withColumn("created_ts", to_date(col("created_ts"), "yyyy-MM-dd"))
            df_new = df_new.withColumn(
                "created_ts",
                when(col("created_ts").isNull(), to_date(col("created_ts"), "dd-MM-yyyy"))
                .otherwise(col("created_ts"))
            )

        # Add metadata
        df_new = (df_new.withColumn("source_system", lit(src["name"]))
                        .withColumn("entity", lit(src["entity"]))
                        .withColumn("ingestion_ts", current_timestamp())
                        .withColumn("run_id", lit(run_id)))

        # Build raw path: storage-v2/raw/<system>/<system>_<entity>
        system = src['name'].lower()
        entity = src['entity'].lower()
        raw_path = f"s3://databricks-amz-s3-bucket/mdm-accelerator/storage-v2/raw/{system}/{system}_{entity}"

        # Append new records (keep full history in raw)
        (df_new.write.format("delta")
              .mode("append")
              .option("mergeSchema","true")
              .save(raw_path))
        load_type = "APPEND"

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Add lineage record
        lineage_records.append((
            run_id, f"{src['name']}_{src['entity']}", src["path"], raw_path,
            start_time, end_time, env["environment"], load_type
        ))

        return int(df_new.count()), 0, "SUCCESS", start_time, end_time, float(duration), load_type

    except Exception as e:
        print(f"Error ingesting {src['name']} {src['entity']}: {e}")
        error_records.append((
            run_id, f"{src['name']}_{src['entity']}", str(e), datetime.now(), env["environment"]
        ))
        return 0, 0, "FAILED", datetime.now(), datetime.now(), 0.0, "FAILED"

# -------------------------------------------------------------
# Main Job
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Raw-Ingestion"
    job_start = datetime.now()

    total_loaded, total_rejected = 0, 0
    audit_records = []
    error_records = []
    lineage_records = []
    overall_status = "SUCCESS"

    for src in sources:
        loaded, rejected, status, start_time, end_time, duration, load_type = ingest_source(src, run_id, error_records, lineage_records)
        total_loaded += loaded
        total_rejected += rejected

        if status == "FAILED":
            overall_status = "FAILED"

        # Add per-source audit record
        audit_records.append((
            run_id, job_name, f"{src['name']}_{src['entity']}", start_time, end_time,
            float(duration), int(loaded), int(rejected), env["environment"], status, load_type
        ))

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    # Add job-level summary record
    audit_records.append((
        run_id, job_name, "JOB_SUMMARY", job_start, job_end,
        float(job_duration), int(total_loaded), int(total_rejected),
        env["environment"], overall_status, "SUMMARY"
    ))

    # Schemas
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

    # Write Audit Runs
    audit_df = spark.createDataFrame(audit_records, schema=audit_schema)
    audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    # Write Audit Errors (append for history)
    error_df = spark.createDataFrame(error_records, schema=error_schema) if error_records else spark.createDataFrame([], schema=error_schema)
    error_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_errors"])

    # Write Audit Lineage
    lineage_df = spark.createDataFrame(lineage_records, schema=lineage_schema) if lineage_records else spark.createDataFrame([], schema=lineage_schema)
    lineage_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])

    print(f"Ingestion complete. Run ID={run_id}, Loaded={total_loaded}, Rejected={total_rejected}, Status={overall_status}")

    # -------------------------------------------------------------
    # Validation: Show all audit tables fully
    # -------------------------------------------------------------
    print("Displaying full audit tables:")

    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_errors"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
main()
