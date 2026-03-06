# Silver Layer – Global Dedup + Survivorship + Similarity
import json, uuid
from datetime import datetime
from pyspark.sql import Window
from pyspark.sql.functions import (
    col, lit, to_date, row_number, when, substring_index, soundex
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, DoubleType
)
from difflib import SequenceMatcher

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
        "date": StringType(),   # keep as string, cast later
        "timestamp": TimestampType(),
        "long": LongType()
    }
    fields = config["schemas"][schema_key]["fields"]
    struct_fields = [StructField(f["name"], type_map[f["type"]], True) for f in fields]
    return StructType(struct_fields)

# -------------------------------------------------------------
# Similarity UDF
# -------------------------------------------------------------
def multi_similarity(name1, name2, email1, email2, phone1, phone2, id1, id2):
    score = 0
    if name1 and name2:
        score += 0.3 * SequenceMatcher(None, str(name1), str(name2)).ratio() * 100
    if email1 and email2:
        score += 0.3 * SequenceMatcher(None, str(email1), str(email2)).ratio() * 100
    if phone1 and phone2:
        score += 0.3 * SequenceMatcher(None, str(phone1), str(phone2)).ratio() * 100
    if id1 and id2 and str(id1) == str(id2):
        score += 0.1 * 100
    return score

from pyspark.sql.functions import udf
similarity_udf = udf(multi_similarity, DoubleType())

# -------------------------------------------------------------
# Load all Bronze Main tables
# -------------------------------------------------------------
def load_all_bronze_main():
    dfs = []
    for src in sources:
        bronze_main_path = f"{paths['bronze_main']}/{src['name'].lower()}/{src['entity'].lower()}"
        df = spark.read.format("delta").load(bronze_main_path)
        dfs.append(df)
    df_all = dfs[0]
    for df in dfs[1:]:
        df_all = df_all.unionByName(df, allowMissingColumns=True)
    return df_all

# -------------------------------------------------------------
# Global Silver Dedup + Survivorship + Similarity
# -------------------------------------------------------------
def global_silver(run_id, lineage_records):
    start_time = datetime.now()
    df_all = load_all_bronze_main()
    print("Initial Bronze union count:", df_all.count())

    # Quality checks
    df_all = df_all.filter(col("customer_id").isNotNull()) \
                   .filter(col("email").isNotNull()) \
                   .filter(col("phone").isNotNull()) \
                   .filter(col("email").rlike("^[^@]+@[^@]+\\.[^@]+$")) \
                   .filter(col("phone").rlike("^[0-9+\\- ]{7,20}$"))

    if "created_ts" in df_all.columns:
        df_all = df_all.withColumn("created_ts", to_date(col("created_ts")))
    else:
        df_all = df_all.withColumn("created_ts", col("ingestion_ts"))

    print("After quality checks:", df_all.count())

    # Blocking keys
    df_all = df_all.withColumn("blocking_email", substring_index(col("email"), "@", -1)) \
                   .withColumn("blocking_phone", col("phone").substr(1,3)) \
                   .withColumn("blocking_name", soundex(col("first_name")))

    # Survivorship rules
    priority_expr = when(col("source_system")=="Salesforce",1) \
                    .when(col("source_system")=="ERP",2) \
                    .when(col("source_system")=="CRM",3) \
                    .when(col("source_system")=="SAP",4) \
                    .otherwise(5)

    w = Window.partitionBy("blocking_email","blocking_phone","blocking_name") \
              .orderBy(priority_expr.asc(), col("created_ts").desc())

    df_ranked = df_all.withColumn("rank", row_number().over(w))
    df_main = df_ranked.filter(col("rank")==1).drop("rank")
    df_conflicted = df_ranked.filter(col("rank")>1).drop("rank")

    print("Silver Main count (before similarity):", df_main.count())
    print("Silver Conflicted count (before similarity):", df_conflicted.count())

    # Similarity check
    joined = df_conflicted.alias("conf").join(
        df_main.alias("main"),
        on=["blocking_email","blocking_phone","blocking_name"], how="inner"
    )
    joined = joined.withColumn("similarity", similarity_udf(
        col("conf.first_name"), col("main.first_name"),
        col("conf.email"), col("main.email"),
        col("conf.phone"), col("main.phone"),
        col("conf.customer_id"), col("main.customer_id")
    ))

    threshold = 80
    df_conflicted_after = joined.where(col("similarity") >= threshold).select("conf.*")
    df_new_main = joined.where(col("similarity") < threshold).select("conf.*")

    df_main_final = df_main.unionByName(df_new_main, allowMissingColumns=True)
    df_conflicted_final = df_conflicted_after

    print("Silver Main count (after similarity):", df_main_final.count())
    print("Silver Conflicted count (after similarity):", df_conflicted_final.count())

    # Write outputs
    silver_main_path = f"{paths['silver_main']}/global"
    silver_conflicted_path = f"{paths['silver_conflicted']}/global"

    df_main_final.write.format("delta").mode("overwrite").option("mergeSchema","true") \
        .partitionBy("source_system").save(silver_main_path)
    df_conflicted_final.write.format("delta").mode("overwrite").option("mergeSchema","true") \
        .partitionBy("source_system").save(silver_conflicted_path)

    lineage_records.append((run_id,"GLOBAL",paths["bronze_main"],silver_main_path,datetime.now(),
                            df_all.count(),df_main_final.count(),df_conflicted_final.count(),
                            env["environment"],"GLOBAL_SURVIVORSHIP+SIMILARITY"))

    return df_main_final.count(), df_conflicted_final.count()

# -------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------
def main():
    run_id = str(uuid.uuid4())
    job_name = "Silver-Global"
    job_start = datetime.now()

    lineage_records = []
    clean_count, conflicted_count = global_silver(run_id, lineage_records)

    job_end = datetime.now()
    job_duration = (job_end - job_start).total_seconds()

    print(f"Silver Global complete. Run ID={run_id}, Clean={clean_count}, Conflicted={conflicted_count}, Duration={job_duration}s")

        # ---------------- Audit Runs ----------------
    audit_schema = load_schema_from_config("audit", schema_config)
    lineage_schema = load_schema_from_config("lineage", schema_config)

    # Audit record (11 fields, matching schemas.json)
    audit_data = [(run_id, job_name, "GLOBAL", job_start, job_end, job_duration,
               clean_count, 0, env["environment"], "SUCCESS", "GLOBAL_SURVIVORSHIP+SIMILARITY")]


    audit_df = spark.createDataFrame(audit_data, schema=audit_schema)
    audit_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_runs"])

    # Lineage records
    lineage_df = spark.createDataFrame(lineage_records, schema=lineage_schema)
    lineage_df.write.format("delta").mode("append").option("mergeSchema","true").save(paths["audit_lineage"])


    # ---------------- Validation ----------------
    display(spark.read.format("delta").load(paths["audit_runs"]))
    display(spark.read.format("delta").load(paths["audit_lineage"]))

# -------------------------------------------------------------
# Entry Point Trigger
# -------------------------------------------------------------
if __name__ == "__main__":
    main()
