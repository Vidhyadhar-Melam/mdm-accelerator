from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MDM-Local-Test")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.jars.packages",
        "io.delta:delta-core_2.12:2.4.0"
    )
    .getOrCreate()
)

print("Spark is running")
print(spark.range(10).count())

spark.stop()
