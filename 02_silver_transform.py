# =============================================================================
# NOTEBOOK 2: SILVER LAYER — Cleaning, Standardization, Deduplication
# Retail Sales Pipeline | Databricks
# =============================================================================
# PURPOSE: Fix every data quality issue identified in Bronze.
# Output: clean, typed, deduplicated Delta tables ready for analytics.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, upper, lower, trim, initcap,
    when, coalesce, lit, to_date,
    regexp_replace, current_timestamp,
    row_number
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RetailPipeline_Silver").getOrCreate()

BRONZE_DB = "retail_bronze"
SILVER_DB = "retail_silver"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")


# =============================================================================
# 1. SILVER SALES
# Issues to fix:
#   - Inconsistent column names (Product_ID, Sale_Date vs lowercase)
#   - Mixed date formats (YYYY-MM-DD, MM/DD/YYYY, "Jan 15 2024", YYYY/MM/DD)
#   - Product IDs are mixed case (prd-002 vs PRD-002)
#   - Null customer_id rows
#   - Null unit_price rows
#   - Duplicate rows (S005=S002, S010=S001, S015=S008)
# =============================================================================

raw_sales = spark.table(f"{BRONZE_DB}.raw_sales")

# Step 1: Rename columns to snake_case standard
sales = raw_sales.select(
    col("sale_id"),
    upper(trim(col("Product_ID"))).alias("product_id"),   # normalize to PRD-XXX
    col("customer_id"),
    col("Sale_Date").alias("sale_date_raw"),
    col("quantity").cast("int"),
    col("unit_price").cast("double"),
    col("store_id"),
    col("_ingested_at"),
    col("_source_file")
)

# Step 2: Parse multiple date formats into a single standard date column
sales = sales.withColumn(
    "sale_date",
    coalesce(
        to_date(col("sale_date_raw"), "yyyy-MM-dd"),
        to_date(col("sale_date_raw"), "MM/dd/yyyy"),
        to_date(col("sale_date_raw"), "MMM dd yyyy"),
        to_date(col("sale_date_raw"), "yyyy/MM/dd")
    )
)

# Step 3: Flag and handle nulls
sales = (
    sales
    .withColumn("has_null_customer", col("customer_id").isNull())
    .withColumn("has_null_price",    col("unit_price").isNull())
    .withColumn("is_valid_record",
        col("customer_id").isNotNull() &
        col("unit_price").isNotNull() &
        col("sale_date").isNotNull()
    )
)

# Step 4: Deduplicate — keep first occurrence of each logical duplicate
# Duplicate key: product_id + customer_id + sale_date + quantity + unit_price
dedup_window = Window.partitionBy(
    "product_id", "customer_id", "sale_date", "quantity", "unit_price"
).orderBy("sale_id")

sales = (
    sales
    .withColumn("row_num", row_number().over(dedup_window))
    .filter(col("row_num") == 1)
    .drop("row_num", "sale_date_raw")
)

# Step 5: Add derived column (revenue per row)
sales = sales.withColumn(
    "line_revenue",
    when(col("is_valid_record"), col("quantity") * col("unit_price"))
    .otherwise(None)
)

print(f"Silver Sales: {sales.count()} rows (after dedup & null flagging)")
sales.filter("is_valid_record = true").show()

(
    sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER_DB}.sales")
)


# =============================================================================
# 2. SILVER INVENTORY
# Issues to fix:
#   - Column name has a space: "Product ID"
#   - Category is mixed case (Electronics, electronics, HOME OFFICE)
#   - Mixed date formats in last_updated
#   - Null supplier
#   - Duplicate product rows
# =============================================================================

raw_inventory = spark.table(f"{BRONZE_DB}.raw_inventory")

inventory = raw_inventory.select(
    col("`Product ID`").alias("product_id"),
    trim(col("product_name")).alias("product_name"),
    initcap(lower(trim(col("Category")))).alias("category"),   # normalize case
    col("stock_qty").cast("int"),
    col("reorder_level").cast("int"),
    col("unit_cost").cast("double"),
    col("last_updated").alias("last_updated_raw"),
    when(
        col("supplier").isNull() | (trim(col("supplier")) == ""),
        lit("Unknown")
    ).otherwise(trim(col("supplier"))).alias("supplier"),
    col("_ingested_at")
)

# Parse mixed date formats
inventory = inventory.withColumn(
    "last_updated",
    coalesce(
        to_date(col("last_updated_raw"), "yyyy-MM-dd"),
        to_date(col("last_updated_raw"), "MM/dd/yyyy"),
        to_date(col("last_updated_raw"), "MMM d yyyy")
    )
).drop("last_updated_raw")

# Deduplicate on product_id (keep first row)
dedup_window_inv = Window.partitionBy("product_id").orderBy("product_id")
inventory = (
    inventory
    .withColumn("row_num", row_number().over(dedup_window_inv))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# Flag low-stock items for business use
inventory = inventory.withColumn(
    "is_low_stock",
    col("stock_qty") <= col("reorder_level")
)

print(f"Silver Inventory: {inventory.count()} rows (after dedup)")
inventory.show()

(
    inventory.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER_DB}.inventory")
)


# =============================================================================
# 3. SILVER CUSTOMERS
# Issues to fix:
#   - Full_Name is mixed case (all caps, all lower, mixed)
#   - Invalid emails (missing domain, format issues)
#   - Phone numbers inconsistent format (555-1234, 5559999)
#   - Null emails and phones
# =============================================================================

raw_customers = spark.table(f"{BRONZE_DB}.raw_customers")

customers = raw_customers.select(
    col("customer_id"),
    initcap(lower(trim(col("Full_Name")))).alias("customer_name"),  # Title Case
    # Validate email: must contain @ and a dot after @
    when(
        col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"),
        lower(trim(col("email")))
    ).otherwise(lit(None)).alias("email"),
    # Standardize phone: strip to digits, then format as XXX-XXXX if 7 digits
    regexp_replace(col("phone"), r"[^\d]", "").alias("phone_digits"),
    col("city"),
    to_date(col("signup_date"), "yyyy-MM-dd").alias("signup_date"),
    initcap(lower(col("loyalty_tier"))).alias("loyalty_tier"),
    col("_ingested_at")
)

# Build clean phone field
customers = customers.withColumn(
    "phone",
    when(
        F.length(col("phone_digits")) == 7,
        F.concat(
            F.substring(col("phone_digits"), 1, 3),
            lit("-"),
            F.substring(col("phone_digits"), 4, 4)
        )
    ).otherwise(None)
).drop("phone_digits")

# Flag contact completeness
customers = customers.withColumn(
    "has_full_contact",
    col("email").isNotNull() & col("phone").isNotNull()
)

print(f"Silver Customers: {customers.count()} rows")
customers.show()

(
    customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{SILVER_DB}.customers")
)

print("\n✅ Silver layer complete.")
