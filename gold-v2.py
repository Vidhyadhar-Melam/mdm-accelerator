# -------------------------------------------------------------
# Gold Layer – Single Source of Truth
# -------------------------------------------------------------
# Purpose:
#   - Read Silver Main + Silver Conflicted records
#   - Apply business-driven deduplication rules
#   - Collapse duplicates into one "golden record" per entity
#   - Enforce single source of truth (SSOT)
#   - Write Gold outputs per entity
#   - Log audit + lineage for traceability
# -------------------------------------------------------------

import json, uuid
from datetime import datetime
from pyspark.sql.functions import col, first, max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType

# -------------------------------------------------------------
# Load Configs
# -------------------------------------------------------------
paths_path   = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/paths.json"
env_path     = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/environment.json"
schema_config_path = "s3://databricks-amz-s3-bucket/mdm-accelerator/config-v2/schemas.json"

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
        "date": StringType(),
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Gold Deduplication Logic
# -------------------------------------------------------------
# Business rule: collapse duplicates by golden identifiers
# For customers: phone number is treated as the primary key
# For accounts: account_id
# For address: postal_code + city
# For transactions: transaction_id
# For contacts: contact_value
# -------------------------------------------------------------

def dedup_gold(df, entity):
    if entity == "customer":
        # Collapse by phone number
        return df.groupBy("phone").agg(
            first("customer_id").alias("customer_id"),
            first("first_name").alias("first_name"),
            first("last_name").alias("last_name"),
            first("email").alias("email"),
            max("created_ts").alias("created_ts"),
            first("source_system").alias("source_system"),
            first("date_of_birth").alias("date_of_birth")
        )
    elif entity == "account":
        return df.groupBy("account_id").agg(
            first("account_name").alias("account_name"),
            max("created_ts").alias("created_ts"),
            first("source_system").alias("source_system")
        )
    elif entity == "address":
        return df.groupBy("postal_code","city").agg(
            first("address_id").alias("address_id"),
            first("state").alias("state"),
            max("created_ts").alias("created_ts"),
            first("source_system").alias("source_system")
        )
    elif entity == "transactions":
        return df.groupBy("transaction_id").agg(
            first("amount").alias("amount"),
            first("currency").alias("currency"),
            max("transaction_date").alias("transaction_date"),
            first("source_system").alias("source_system")
        )
    elif entity == "contacts":
        return df.groupBy("contact_value").agg(
            first("contact_id").alias("contact_id"),
            first("contact_type").alias("contact_type"),
            max("created_ts").alias("created_ts"),
            first("source_system").alias("source_system")
        )
    else:
        return df

# -------------------------------------------------------------
# Global Gold Builder
# -------------------------------------------------------------
def global_gold(run_id):
    # Load Silver Main + Conflicted
    silver_main_path = f"{paths['silver_main']}/global"
    silver_conflicted_path = f"{paths['silver_conflicted']}/global"

    silver_main_df = spark.read.format("delta").load(silver_main_path)
    silver_conflicted_df = spark.read.format("delta").load(silver_conflicted_path)

    # Union both to ensure no data loss
    df_all = silver_main_df.unionByName(silver_conflicted_df, allowMissingColumns=True)

    print("Silver Main count:", silver_main_df.count())
    print("Silver Conflicted count:", silver_conflicted_df.count())
    print("Total records entering Gold:", df_all.count())

    entities = ["customer","account","address","transactions","contacts"]
    gold_union = None
    summary_list = []

    for ent in entities:
        df_entity = df_all.filter(col("entity")==ent)
        if df_entity.count() == 0:
            continue
        df_gold = dedup_gold(df_entity, ent)

        summary_list.append({
            "entity": ent,
            "silver_input": df_entity.count(),
            "gold_output": df_gold.count()
        })

        gold_path = f"{paths['gold']}/{ent}"
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema","true") \
            .partitionBy("source_system").save(gold_path)

        if gold_union is None:
            gold_union = df_gold
        else:
            gold_union = gold_union.unionByName(df_gold, allowMissingColumns=True)

    print("Total Gold records:", gold_union.count())

    # Display summary table
    summary_df = spark.createDataFrame(summary_list)
    display(summary_df)

    return gold_union.count()

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Gold-Global"
    job_start = datetime.now()

    gold_count = global_gold(run_id)

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    print(f"Gold Global complete. Run ID={run_id}, Gold={gold_count}, Duration={job_duration}s")

    # ---------------- Audit Runs ----------------
    audit_schema = load_schema_from_config("audit", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)

    audit_data = [(run_id, job_name, "GLOBAL", job_start, job_end, job_duration,
                   gold_count, 0, env["environment"], "SUCCESS", "GLOBAL_GOLD_SSOT")]

    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
    audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    lineage_data = [(run_id, "GLOBAL", paths["silver_main"], paths["gold"],
                     job_start, job_end, env["environment"], "GLOBAL_GOLD_SSOT")]

    lineage_df = spark.createDataFrame(lineage_data, schema=lineage_schema)
    lineage_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])

    # ---------------- Validation ----------------
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))

# -------------------------------------------------------------
# Entry Point Trigger
# -------------------------------------------------------------
if __name__ == "__main__":
    main()
