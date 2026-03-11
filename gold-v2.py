# -------------------------------------------------------------
# Gold Layer – Single Source of Truth (SSOT) with Ingestion Date Partitioning
# -------------------------------------------------------------
import json, uuid
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, lit, row_number, when
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType, DateType

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
        "date": DateType(),
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Survivorship priority (general for all entities)
# -------------------------------------------------------------
def survivorship_priority():
    return (
        when(col("source_system")=="Salesforce",1)
        .when(col("source_system")=="ERP",2)
        .when(col("source_system")=="CRM",3)
        .when(col("source_system")=="SAP",4)
        .otherwise(5)
    )

# -------------------------------------------------------------
# Gold Deduplication Logic
# -------------------------------------------------------------
def dedup_gold(df, entity):
    priority_expr = survivorship_priority()

    if entity == "customer":
        # Group by business identity (name + dob)
        w = Window.partitionBy("first_name","last_name") \
                  .orderBy(priority_expr.asc(), col("created_ts").desc())
        return df.withColumn("rank", row_number().over(w)) \
                 .filter(col("rank")==1).drop("rank") \
                 .select(
                     "customer_id","first_name","last_name","date_of_birth",
                     "phone","email",
                     "address_line1","address_line2","city","state","postal_code",
                     "source_system","created_ts"
                 )

    elif entity == "account":
        # Group by account_name
        w = Window.partitionBy("account_name") \
                  .orderBy(priority_expr.asc(), col("created_ts").desc())
        return df.withColumn("rank", row_number().over(w)).filter(col("rank")==1).drop("rank")

    elif entity == "address":
        # Group by full address fields
        w = Window.partitionBy("postal_code","city","state","address_line1","address_line2") \
                  .orderBy(priority_expr.asc(), col("created_ts").desc())
        return df.withColumn("rank", row_number().over(w)).filter(col("rank")==1).drop("rank")

    elif entity == "transactions":
        # Group by account_id + amount + currency + transaction_date
        w = Window.partitionBy("account_id","amount","currency","transaction_date") \
                  .orderBy(priority_expr.asc(), col("created_ts").desc())
        return df.withColumn("rank", row_number().over(w)).filter(col("rank")==1).drop("rank")

    elif entity == "contacts":
        # Group by contact_value + contact_type
        w = Window.partitionBy("contact_value","contact_type") \
                  .orderBy(priority_expr.asc(), col("created_ts").desc())
        return df.withColumn("rank", row_number().over(w)).filter(col("rank")==1).drop("rank")

    else:
        return df

# -------------------------------------------------------------
# Global Gold Builder
# -------------------------------------------------------------
def global_gold(run_id):
    silver_main_path = f"{paths['silver_main']}/global"
    silver_conflicted_path = f"{paths['silver_conflicted']}/global"

    silver_main_df = spark.read.format("delta").load(silver_main_path)
    silver_conflicted_df = spark.read.format("delta").load(silver_conflicted_path)

    df_all = silver_main_df.unionByName(silver_conflicted_df, allowMissingColumns=True)

    print("Silver Main count:", silver_main_df.count())
    print("Silver Conflicted count:", silver_conflicted_df.count())
    print("Total records entering Gold:", df_all.count())

    entities = ["customer","account","address","transactions","contacts"]
    gold_union = None
    summary_list = []
    ingestion_date = datetime.now().strftime("%Y-%m-%d")

    for ent in entities:
        df_entity = df_all.filter(col("entity")==ent)
        if df_entity.count() == 0:
            continue
        df_gold = dedup_gold(df_entity, ent).withColumn("ingestion_date", lit(ingestion_date))

        summary_list.append({
            "entity": ent,
            "silver_input": df_entity.count(),
            "gold_output": df_gold.count()
        })

        gold_path = f"{paths['gold']}/{ent}"
        df_gold.write.format("delta").mode("overwrite").option("mergeSchema","true") \
            .partitionBy("ingestion_date").save(gold_path)

        gold_union = df_gold if gold_union is None else gold_union.unionByName(df_gold, allowMissingColumns=True)

    print("Total Gold records:", gold_union.count())
    summary_df = spark.createDataFrame(summary_list)
    display(summary_df)

    return df_all.count(), gold_union.count()

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Gold-Global"
    job_start = datetime.now()

    incoming_count, gold_count = global_gold(run_id)

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    print(f"Gold Global complete. Run ID={run_id}, Gold={gold_count}, Duration={job_duration}s")

    audit_schema = load_schema_from_config("audit", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)

    audit_data = [(run_id, job_name, "GLOBAL", job_start, job_end, float(job_duration),
                   int(incoming_count), int(gold_count), 0,
                   env["environment"], "SUCCESS", "GLOBAL_GOLD_SSOT")]

    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
    audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    lineage_data = [(run_id, "GLOBAL", paths["silver_main"], paths["gold"],
                     job_start, job_end, env["environment"], "GLOBAL_GOLD_SSOT")]

    lineage_df = spark.createDataFrame(lineage_data, schema=lineage_schema)
    lineage_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])

    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))

# -------------------------------------------------------------
# Entry Point Trigger
# -------------------------------------------------------------
if __name__ == "__main__":
    main()
