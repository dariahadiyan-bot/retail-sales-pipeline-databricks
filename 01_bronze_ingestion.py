# =============================================================================
# NOTEBOOK 1: BRONZE LAYER — Raw Vendor Data Ingestion
# Retail Sales Pipeline | Databricks
# =============================================================================
# PURPOSE: Ingest raw CSVs as-is into the Bronze layer (Delta tables).
# We preserve ALL raw data — even dirty rows — for full auditability.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

spark = SparkSession.builder.appName("RetailPipeline_Bronze").getOrCreate()

# ----------------------------------------------------------------------------
# CONFIG — swap these paths for your actual DBFS or Unity Catalog paths
# ----------------------------------------------------------------------------
RAW_PATH = "/FileStore/retail_pipeline/raw"         # where CSVs are uploaded
BRONZE_DB = "retail_bronze"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# ----------------------------------------------------------------------------
# HELPER: Load CSV with ingestion metadata columns added
# ----------------------------------------------------------------------------
def ingest_csv(filename: str, table_name: str):
    """Read a raw CSV and write to Bronze Delta table with audit columns."""
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "PERMISSIVE")       # don't fail on bad rows
        .csv(f"{RAW_PATH}/{filename}")
    )

    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(filename))
    )

    row_count = df.count()
    print(f"[{filename}] Loaded {row_count} rows → {BRONZE_DB}.{table_name}")
    df.printSchema()

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{BRONZE_DB}.{table_name}")
    )
    return df

# ----------------------------------------------------------------------------
# INGEST ALL THREE SOURCE FILES
# ----------------------------------------------------------------------------
bronze_sales     = ingest_csv("sales.csv",     "raw_sales")
bronze_inventory = ingest_csv("inventory.csv", "raw_inventory")
bronze_customers = ingest_csv("customers.csv", "raw_customers")

# ----------------------------------------------------------------------------
# QUICK PROFILING — surfaces the exact problems we'll fix in Silver
# ----------------------------------------------------------------------------
print("\n====== SALES: Data Quality Issues ======")
bronze_sales.select("sale_id", "Product_ID", "Sale_Date", "unit_price", "customer_id").show(15)
print(f"Null customer_ids : {bronze_sales.filter('customer_id IS NULL').count()}")
print(f"Null unit_price   : {bronze_sales.filter('unit_price IS NULL').count()}")
print(f"Total rows        : {bronze_sales.count()}")

print("\n====== INVENTORY: Data Quality Issues ======")
bronze_inventory.select("`Product ID`", "product_name", "Category", "supplier").show()
print(f"Null supplier     : {bronze_inventory.filter('supplier IS NULL OR supplier = \"\"').count()}")

print("\n====== CUSTOMERS: Data Quality Issues ======")
bronze_customers.select("customer_id", "Full_Name", "email", "phone").show()
print(f"Null emails       : {bronze_customers.filter('email IS NULL').count()}")
print(f"Null phones       : {bronze_customers.filter('phone IS NULL').count()}")
