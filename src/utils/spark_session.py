from pyspark.sql import SparkSession

def get_spark(app_name: str = "MDM-Accelerator"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    return spark
