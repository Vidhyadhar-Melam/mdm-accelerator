from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pathlib import Path

# -------------------------------------------------------------
# Start Spark with Delta enabled
# -------------------------------------------------------------
builder = (
    SparkSession.builder
    .appName("Check-Raw")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

PROJECT_ROOT = Path(__file__).resolve().parents[0]

# -------------------------------------------------------------
# Check CRM raw table
# -------------------------------------------------------------
df = spark.read.format("delta").load(str(PROJECT_ROOT / "storage" / "raw" / "CRM" / "customer"))
print("CRM row count:", df.count())
df.printSchema()
df.show(5)

# -------------------------------------------------------------
# Check ERP raw table
# -------------------------------------------------------------
df_erp = spark.read.format("delta").load(str(PROJECT_ROOT / "storage" / "raw" / "ERP" / "customer"))
print("ERP row count:", df_erp.count())
df_erp.printSchema()
df_erp.show(5)

# -------------------------------------------------------------
# Check Salesforce raw table
# -------------------------------------------------------------
df_sf = spark.read.format("delta").load(str(PROJECT_ROOT / "storage" / "raw" / "Salesforce" / "customer"))
print("Salesforce row count:", df_sf.count())
df_sf.printSchema()
df_sf.show(5)

# -------------------------------------------------------------
# Check SAP raw table
# -------------------------------------------------------------
df_sap = spark.read.format("delta").load(str(PROJECT_ROOT / "storage" / "raw" / "SAP" / "customer"))
print("SAP row count:", df_sap.count())
df_sap.printSchema()
df_sap.show(5)

spark.stop()
