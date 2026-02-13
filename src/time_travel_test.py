from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Spark setup
builder = (
    SparkSession.builder
    .appName("MDM-TimeTravel")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Path to Bronze table
bronze_main_path = Path("C:/Users/viddi/Desktop/CoE/mdm-accelerator/storage/bronze/main/crm")

# Time travel query by version
df_old = spark.read.format("delta").option("versionAsOf", 2).load(str(bronze_main_path))
df_old.show(truncate=False)

# Time travel query by timestamp
df_time = spark.read.format("delta").option("timestampAsOf", "2026-02-13 10:30:00").load(str(bronze_main_path))
df_time.show(truncate=False)

spark.stop()
