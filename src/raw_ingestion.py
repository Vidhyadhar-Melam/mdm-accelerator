import json
import uuid
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta import configure_spark_with_delta_pip

# -------------------------------------------------------------
# Start Spark with Delta enabled
# -------------------------------------------------------------
builder = (
    SparkSession.builder
    .appName("MDM-Raw-Ingestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Enable schema auto-merge globally (optional, but helpful)
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# -------------------------------------------------------------
# Load configs
# -------------------------------------------------------------
SOURCES_CONFIG = PROJECT_ROOT / "config" / "sources.json"
PATHS_CONFIG = PROJECT_ROOT / "config" / "paths.json"
ENV_CONFIG = PROJECT_ROOT / "config" / "environment.json"

with open(SOURCES_CONFIG, "r") as f:
    sources = json.load(f)["sources"]

with open(PATHS_CONFIG, "r") as f:
    paths = json.load(f)

with open(ENV_CONFIG, "r") as f:
    env = json.load(f)

# -------------------------------------------------------------
# Helper: Ingest a source system
# -------------------------------------------------------------
def ingest_source(src, run_id):
    try:
        # Read CSV
        df = spark.read.option("header", "true").csv(str(PROJECT_ROOT / src["path"]))

        # 🔄 Standardize column names
        for col_name in df.columns:
            lower = col_name.lower()
            if lower in ["cust_email", "sf_email", "erp_email", "sap_email", "email"]:
                df = df.withColumnRenamed(col_name, "email")
            if lower in ["cust_phone", "phone_number", "contact_phone", "phone"]:
                df = df.withColumnRenamed(col_name, "phone")
            if lower in ["id", "cust_id", "customerid", "customer_id"]:
                df = df.withColumnRenamed(col_name, "customer_id")

        # Add metadata
        df = (
            df.withColumn("source_system", lit(src["name"]))
              .withColumn("ingestion_ts", current_timestamp())
              .withColumn("run_id", lit(run_id))
        )

        # Write to raw Delta
        raw_path = PROJECT_ROOT / "storage" / "raw" / src["name"] / "customer"
        df.write.format("delta").mode("append").save(str(raw_path)) # overwrite to append

        records_loaded = df.count()
        records_rejected = 0  # placeholder for validation logic

        return records_loaded, records_rejected

    except Exception as e:
        print(f"Error ingesting {src['name']}: {e}")
        return 0, 0


# -------------------------------------------------------------
# Main Job
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Raw-Ingestion"
    start_time = datetime.now()

    total_loaded, total_rejected = 0, 0

    for src in sources:
        loaded, rejected = ingest_source(src, run_id)
        total_loaded += loaded
        total_rejected += rejected

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    # ---------------------------------------------------------
    # Write Audit Log (with schema merge enabled)
    # ---------------------------------------------------------
    audit_data = [
        (run_id, job_name, start_time, end_time, duration,
         total_loaded, total_rejected, env["environment"])
    ]

    audit_columns = ["run_id", "job_name", "start_time", "end_time", "duration",
                     "records_loaded", "records_rejected", "environment"]

    audit_df = spark.createDataFrame(audit_data, audit_columns)

    audit_df.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .save(str(PROJECT_ROOT / paths["audit_runs"]))

    print(f"Ingestion complete. Run ID: {run_id}, Loaded: {total_loaded}, Rejected: {total_rejected}, Duration: {duration:.2f}s")

    # ---------------------------------------------------------
    # Validation Block (print audit table)
    # ---------------------------------------------------------
    audit_log_df = spark.read.format("delta").load(str(PROJECT_ROOT / paths["audit_runs"]))
    audit_log_df.printSchema()
    audit_log_df.show()

    # Stop Spark AFTER validation
    spark.stop()

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
if __name__ == "__main__":
    main()
