from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path

builder = (
    SparkSession.builder
    .appName("Test-Read")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

PROJECT_ROOT = Path(__file__).resolve().parents[0]
raw_path = PROJECT_ROOT / "storage" / "raw" / "CRM" / "customer"

df = spark.read.format("delta").load(str(raw_path))
print("Row count:", df.count())
df.printSchema()
df.show(5)

spark.stop()
