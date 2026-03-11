# -------------------------------------------------------------
# Silver Layer – Global Dedup + Survivorship + Similarity
# -------------------------------------------------------------
# Purpose:
#   - Load Bronze data from multiple sources/entities
#   - Apply entity-specific quality checks
#   - Normalize key attributes (email, phone, names, etc.)
#   - Generate blocking keys per entity type
#   - Apply survivorship rules (source priority + recency)
#   - Run similarity scoring to collapse near-duplicates
#   - Write Silver Main (clean survivors) and Silver Conflicted (duplicates)
#   - Log audit and lineage records for traceability
# -------------------------------------------------------------

import json, uuid
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, to_date, row_number, when, regexp_replace, soundex
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, DoubleType, DateType
)
from difflib import SequenceMatcher
from pyspark.sql.functions import udf

# -------------------------------------------------------------
# Load Configs
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
        "date": DateType(),          # align with ingestion/bronze
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Similarity UDF
# -------------------------------------------------------------
def multi_similarity(*args):
    score = 0
    n = len(args) // 2
    weight = 1.0 / n if n > 0 else 0
    for i in range(n):
        a, b = args[2*i], args[2*i+1]
        if a and b:
            score += weight * SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio() * 100
    return score
similarity_udf = udf(multi_similarity, DoubleType())

# -------------------------------------------------------------
# Quality checks
# -------------------------------------------------------------
def apply_quality_checks(df, entity):
    if entity.lower() == "customer":
        return df.filter(col("customer_id").isNotNull()) \
                 .filter(col("email").isNotNull()) \
                 .filter(col("phone").isNotNull())
    elif entity.lower() == "account":
        return df.filter(col("account_id").isNotNull()) \
                 .filter(col("account_name").isNotNull())
    elif entity.lower() == "address":
        # Updated for inline addresses (no address_id)
        return df.filter(col("postal_code").isNotNull()) \
                 .filter(col("city").isNotNull()) \
                 .filter(col("state").isNotNull())
    elif entity.lower() == "transactions":
        return df.filter(col("transaction_id").isNotNull()) \
                 .filter(col("amount").isNotNull()) \
                 .filter(col("currency").isNotNull())
    elif entity.lower() == "contacts":
        return df.filter(col("contact_id").isNotNull()) \
                 .filter(col("contact_value").isNotNull())
    else:
        return df

# -------------------------------------------------------------
# Similarity threshold helper
# -------------------------------------------------------------
def get_similarity_threshold(entity, source_system):
    for src in sources:
        if src["entity"].lower() == entity.lower() and src["name"].lower() == source_system.lower():
            return src.get("similarity_threshold", 80)
    return 80

# -------------------------------------------------------------
# Deduplication per entity
# -------------------------------------------------------------
def dedup_entity(df, entity):
    if entity.lower() == "customer":
        df = df.withColumn("norm_phone", regexp_replace(col("phone"), "[^0-9]", "")) \
               .withColumn("blocking_dob", col("date_of_birth")) \
               .withColumn("blocking_phone_prefix", col("norm_phone").substr(1,5)) \
               .withColumn("blocking_fname", soundex(col("first_name"))) \
               .withColumn("blocking_lname", soundex(col("last_name")))
        partition_cols = ["blocking_dob","blocking_phone_prefix","blocking_fname","blocking_lname"]
        sim_expr = similarity_udf(
            col("conf.first_name"), col("main.first_name"),
            col("conf.last_name"), col("main.last_name"),
            col("conf.email"), col("main.email"),
            col("conf.phone"), col("main.phone")
        )
    elif entity.lower() == "account":
        df = df.withColumn("blocking_account", soundex(col("account_name"))) \
               .withColumn("blocking_group", col("account_group"))
        partition_cols = ["blocking_account","blocking_group"]
        sim_expr = similarity_udf(
            col("conf.account_name"), col("main.account_name"),
            col("conf.account_group"), col("main.account_group"),
            col("conf.account_owner"), col("main.account_owner"),
            col("conf.account_code"), col("main.account_code")
        )
    elif entity.lower() == "address":
        df = df.withColumn("blocking_postal", col("postal_code")) \
               .withColumn("blocking_city", soundex(col("city"))) \
               .withColumn("blocking_state", col("state"))
        partition_cols = ["blocking_postal","blocking_city","blocking_state"]
        sim_expr = similarity_udf(
            col("conf.city"), col("main.city"),
            col("conf.state"), col("main.state"),
            col("conf.postal_code"), col("main.postal_code"),
            col("conf.address_line1"), col("main.address_line1"),
            col("conf.address_line2"), col("main.address_line2")
        )
    elif entity.lower() == "transactions":
        df = df.withColumn("blocking_amount", col("amount")) \
               .withColumn("blocking_currency", col("currency")) \
               .withColumn("blocking_date", to_date(col("transaction_date")))
        partition_cols = ["blocking_amount","blocking_currency","blocking_date"]
        sim_expr = similarity_udf(
            col("conf.account_id"), col("main.account_id"),
            col("conf.transaction_type"), col("main.transaction_type"),
            col("conf.customer_id"), col("main.customer_id"),
            col("conf.source_system"), col("main.source_system"),
            col("conf.invoice_code"), col("main.invoice_code"),
            col("conf.document_number"), col("main.document_number"),
            col("conf.opportunity_id"), col("main.opportunity_id"),
            col("conf.opportunity_stage"), col("main.opportunity_stage"),
            col("conf.campaign_id"), col("main.campaign_id")
        )
    elif entity.lower() == "contacts":
        df = df.withColumn("blocking_contact", soundex(col("contact_value")))
        partition_cols = ["blocking_contact"]
        sim_expr = similarity_udf(
            col("conf.contact_value"), col("main.contact_value"),
            col("conf.contact_type"), col("main.contact_type"),
            col("conf.source_system"), col("main.source_system"),
            col("conf.created_ts"), col("main.created_ts")
        )
    else:
        return df, spark.createDataFrame([], df.schema)

        # Survivorship rules: prioritize Salesforce > ERP > CRM > SAP > others
    priority_expr = when(col("source_system")=="Salesforce",1) \
                    .when(col("source_system")=="ERP",2) \
                    .when(col("source_system")=="CRM",3) \
                    .when(col("source_system")=="SAP",4) \
                    .otherwise(5)

    # Window for survivorship (partition by blocking keys, order by priority + recency)
    w = Window.partitionBy(*partition_cols).orderBy(priority_expr.asc(), col("created_ts").desc())
    df_ranked = df.withColumn("rank", row_number().over(w))

    # Survivors vs duplicates
    df_main = df_ranked.filter(col("rank")==1).drop("rank")
    df_conflicted = df_ranked.filter(col("rank")>1).drop("rank")

    # Similarity join: compare conflicted vs main within same blocking partition
    joined = df_conflicted.alias("conf").join(
        df_main.alias("main"),
        on=partition_cols, how="inner"
    ).withColumn("similarity", sim_expr)

    # Threshold from sources.json
    source_system_val = df.select("source_system").first()[0]
    threshold = get_similarity_threshold(entity, source_system_val)

    # Apply similarity threshold
    df_conflicted_after = joined.where(col("similarity") >= threshold).select("conf.*")
    df_new_main = joined.where(col("similarity") < threshold).select("conf.*")

    # Final Silver sets
    df_main_final = df_main.unionByName(df_new_main, allowMissingColumns=True)
    df_conflicted_final = df_conflicted_after

    # Logging
    print(f"[{entity}] Survivorship -> Main count: {df_main.count()}, Conflicted count: {df_conflicted.count()}")
    print(f"[{entity}] Similarity -> Conflicted after: {df_conflicted_after.count()}, Promoted to Main: {df_new_main.count()}")
    print(f"[{entity}] Final Silver Main: {df_main_final.count()}, Final Silver Conflicted: {df_conflicted_final.count()}")

    return df_main_final, df_conflicted_final

# -------------------------------------------------------------
# Global Silver Dedup + Survivorship + Similarity
# -------------------------------------------------------------
def global_silver(run_id):
    df_all = load_all_bronze_main()
    print("Initial Bronze union count:", df_all.count())

    if "created_ts" in df_all.columns:
        df_all = df_all.withColumn("created_ts", to_date(col("created_ts")))
    else:
        df_all = df_all.withColumn("created_ts", col("ingestion_ts"))

    print("After quality checks:", df_all.count())

    entities = ["customer","account","address","transactions","contacts"]
    silver_main_union = None
    silver_conflicted_union = None

    for ent in entities:
        df_entity = df_all.filter(col("entity")==ent)
        if df_entity.count() == 0:
            continue
        df_main_final, df_conflicted_final = dedup_entity(df_entity, ent)

        if silver_main_union is None:
            silver_main_union = df_main_final
            silver_conflicted_union = df_conflicted_final
        else:
            silver_main_union = silver_main_union.unionByName(df_main_final, allowMissingColumns=True)
            silver_conflicted_union = silver_conflicted_union.unionByName(df_conflicted_final, allowMissingColumns=True)

    print("Silver Main count (after similarity):", silver_main_union.count())
    print("Silver Conflicted count (after similarity):", silver_conflicted_union.count())

    # Write Silver outputs
    silver_main_path = f"{paths['silver_main']}/global"
    silver_conflicted_path = f"{paths['silver_conflicted']}/global"

    silver_main_union.write.format("delta").mode("overwrite").option("mergeSchema","true").save(silver_main_path)
    silver_conflicted_union.write.format("delta").mode("overwrite").option("mergeSchema","true").save(silver_conflicted_path)

    return df_all.count(), silver_main_union.count(), silver_conflicted_union.count(), silver_main_path

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Silver-Global"
    job_start = datetime.now()

    total_count, clean_count, conflicted_count, silver_main_path = global_silver(run_id)

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    print(f"Silver Global complete. Run ID={run_id}, Clean={clean_count}, Conflicted={conflicted_count}, Duration={job_duration}s")

    # ---------------- Audit Runs ----------------
    audit_schema = load_schema_from_config("audit", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)

    # Production-grade audit mapping
    audit_data = [(run_id, job_name, "GLOBAL", job_start, job_end, float(job_duration),
                   int(total_count),      # incoming_count = total processed
                   int(clean_count),      # inserted_count = Silver Main survivors
                   int(conflicted_count), # conflicted_count = Silver Conflicted duplicates
                   env["environment"], "SUCCESS", "GLOBAL_SURVIVORSHIP+SIMILARITY")]

    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
    audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    lineage_data = [(run_id, "GLOBAL", paths["bronze_main"], silver_main_path,
                     job_start, job_end, env["environment"], "GLOBAL_SURVIVORSHIP+SIMILARITY")]

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
