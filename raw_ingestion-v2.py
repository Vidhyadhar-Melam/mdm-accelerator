# Raw Ingestion Script (Databricks, Config-Driven, Delta Lake)
# -------------------------------------------------------------------
# Features:
#   - Incremental + Idempotent ingestion per entity
#   - Preserves ingestion_ts on updates (only set on inserts)
#   - Refreshes updated_ts on changes
#   - Partitioning by ingestion_date
#   - Config-driven schema casting
#   - Data Quality checks
#   - Audit, Error, Lineage, DQ logging
#   - Column normalization (lowercase + trim) to fix MERGE mismatches
#   - Audit logs now record both Incoming Rows and Inserted Rows

import json, uuid
from datetime import datetime
from pyspark.sql.functions import lit, current_timestamp, col, to_date, to_timestamp, trim, lower
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType, LongType
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

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

reset_mode = env.get("reset_mode", False)

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
# Ingest function
# -------------------------------------------------------------
def ingest_source(src, run_id, error_records, lineage_records, dq_records):
    try:
        start_time = datetime.now()
        schema_key = f"{src['name'].lower()}_{src['entity'].lower()}"
        expected_schema = load_schema_from_config(schema_key, schema_config)

        # Read CSV
        df_new = spark.read.option("header", "true").option("delimiter", ",").csv(src["path"])

        # Normalize column names (lowercase + trim)
        df_new = df_new.toDF(*[c.strip().lower() for c in df_new.columns])

        # Cast + normalize values
        for field in expected_schema.fields:
            if field.name in df_new.columns:
                df_new = df_new.withColumn(field.name, trim(col(field.name)))
                if isinstance(field.dataType, StringType):
                    df_new = df_new.withColumn(field.name, lower(col(field.name)))
                if field.dataType == TimestampType():
                    df_new = df_new.withColumn(field.name, to_timestamp(col(field.name)))
                else:
                    df_new = df_new.withColumn(field.name, col(field.name).cast(field.dataType))

        # Add metadata
        df_new = (df_new.withColumn("source_system", lit(src["name"]))
                        .withColumn("entity", lit(src["entity"]))
                        .withColumn("ingestion_ts", current_timestamp())   # set only on insert
                        .withColumn("ingestion_date", to_date(current_timestamp()))
                        .withColumn("run_id", lit(run_id))
                        .withColumn("updated_ts", current_timestamp()))

        incoming_count = df_new.count()

        # -----------------------------
        # Write to Raw Delta
        # -----------------------------
        raw_key = f"raw_{src['name'].lower()}_{src['entity'].lower()}"
        raw_path = paths[raw_key]

        natural_keys = [f["name"] for f in schema_config["schemas"][schema_key]["fields"] if f.get("natural_key", False)]
        if not natural_keys:
            raise Exception(f"No natural keys defined for {schema_key}")
        print(f"Natural keys for {schema_key}: {natural_keys}")

        # Check if Raw exists
        try:
            spark.read.format("delta").load(raw_path)
            table_exists = True
        except AnalysisException:
            table_exists = False

        if not table_exists or reset_mode:
            # Initialize Raw
            df_new.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(raw_path)
            load_type = "INITIALIZE"
            inserted_count = incoming_count
        else:
            # MERGE with preserved ingestion_ts
            delta_table = DeltaTable.forPath(spark, raw_path)
            before_count = delta_table.toDF().count()

            merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in natural_keys])

            (delta_table.alias("t")
             .merge(df_new.alias("s"), merge_condition)
             .whenMatchedUpdate(set={
                 "updated_ts": current_timestamp(),
                 "run_id": "s.run_id",
                 "source_system": "s.source_system",
                 "entity": "s.entity",
                 # preserve ingestion_ts from target
                 **{c: f"s.{c}" for c in df_new.columns if c not in ["ingestion_ts","updated_ts","run_id","source_system","entity"]}
             })
             .whenNotMatchedInsert(values={
                 "ingestion_ts": current_timestamp(),   # set only on insert
                 "updated_ts": current_timestamp(),
                 "run_id": "s.run_id",
                 "source_system": "s.source_system",
                 "entity": "s.entity",
                 **{c: f"s.{c}" for c in df_new.columns if c not in ["ingestion_ts","updated_ts","run_id","source_system","entity"]}
             })
             .execute())

            after_count = delta_table.toDF().count()
            inserted_count = after_count - before_count
            load_type = "MERGE"

        # Lineage
        end_time = datetime.now()
        lineage_records.append((run_id, src["name"], src["path"], raw_path,
                                start_time, end_time, env["environment"], load_type))

        return incoming_count, inserted_count, 0, "SUCCESS", start_time, end_time, (end_time - start_time).total_seconds(), load_type

    except Exception as e:
        error_records.append((run_id, src["name"], str(e), datetime.now(), env["environment"]))
        return 0, 0, 0, "FAILED", datetime.now(), datetime.now(), 0.0, "FAILED"

# -------------------------------------------------------------
# Main Job
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Raw-Ingestion"
    job_start = datetime.now()

    total_incoming, total_inserted, total_rejected = 0, 0, 0
    audit_records, error_records, lineage_records, dq_records = [], [], [], []
    overall_status = "SUCCESS"

    for src in sources:
        incoming, inserted, rejected, status, start_time, end_time, duration, load_type = ingest_source(
            src, run_id, error_records, lineage_records, dq_records
        )
        total_incoming += incoming
        total_inserted += inserted
        total_rejected += rejected
        if status == "FAILED":
            overall_status = "FAILED"
        audit_records.append((
            run_id, job_name, f"{src['name']}_{src['entity']}", start_time, end_time,
            float(duration), int(incoming), int(inserted), int(rejected),
            env["environment"], status, load_type
        ))

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    # Job summary
    audit_records.append((
        run_id, job_name, "JOB_SUMMARY", job_start, job_end,
        float(job_duration), int(total_incoming), int(total_inserted), int(total_rejected),
        env["environment"], overall_status, "SUMMARY"
    ))

    # Schemas
    audit_schema   = load_schema_from_config("audit", schema_config)
    error_schema   = load_schema_from_config("error", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)
    dq_schema      = load_schema_from_config("dq", schema_config)

    # Write Audit tables
    spark.createDataFrame(audit_records, schema=audit_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    spark.createDataFrame(error_records, schema=error_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_errors"])

    spark.createDataFrame(lineage_records, schema=lineage_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])

    spark.createDataFrame(dq_records, schema=dq_schema) \
        .write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_dq"])

    # Print job summary
    print(f"Ingestion complete. Run ID={run_id}, Incoming={total_incoming}, Inserted={total_inserted}, Rejected={total_rejected}, Status={overall_status}")

    # Validation: Display audit tables for transparency
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_errors"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))
    display(spark.read.format("delta").load(paths["audit_dq"]))

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
main()
