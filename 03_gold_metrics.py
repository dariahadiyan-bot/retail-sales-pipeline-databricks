# =============================================================================
# NOTEBOOK 3: GOLD LAYER — Business Metrics & Reporting Tables
# Retail Sales Pipeline | Databricks
# =============================================================================
# PURPOSE: Join Silver tables and compute business-ready aggregations.
# These tables feed directly into dashboards or BI tools.
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum, avg, count, round, desc, rank
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("RetailPipeline_Gold").getOrCreate()

SILVER_DB = "retail_silver"
GOLD_DB   = "retail_gold"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

# Load Silver tables
sales     = spark.table(f"{SILVER_DB}.sales").filter("is_valid_record = true")
inventory = spark.table(f"{SILVER_DB}.inventory")
customers = spark.table(f"{SILVER_DB}.customers")


# =============================================================================
# GOLD TABLE 1: daily_revenue
# Business question: How is revenue trending day over day?
# =============================================================================

daily_revenue = (
    sales
    .groupBy("sale_date")
    .agg(
        F.sum("line_revenue").alias("total_revenue"),
        F.count("sale_id").alias("total_transactions"),
        F.sum("quantity").alias("total_units_sold"),
        F.avg("line_revenue").alias("avg_order_value")
    )
    .withColumn("total_revenue",   round(col("total_revenue"), 2))
    .withColumn("avg_order_value", round(col("avg_order_value"), 2))
    .orderBy("sale_date")
)

print("=== GOLD: Daily Revenue ===")
daily_revenue.show()

(
    daily_revenue.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.daily_revenue")
)


# =============================================================================
# GOLD TABLE 2: product_performance
# Business question: Which products drive the most revenue and volume?
# =============================================================================

product_performance = (
    sales
    .join(inventory.select("product_id", "product_name", "category", "unit_cost"),
          on="product_id", how="left")
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("quantity").alias("total_units_sold"),
        F.sum("line_revenue").alias("total_revenue"),
        F.avg("unit_price").alias("avg_selling_price"),
        F.first("unit_cost").alias("unit_cost")
    )
    .withColumn("total_revenue",     round(col("total_revenue"), 2))
    .withColumn("avg_selling_price", round(col("avg_selling_price"), 2))
    # Gross margin %
    .withColumn(
        "gross_margin_pct",
        round(
            (col("avg_selling_price") - col("unit_cost")) / col("avg_selling_price") * 100,
            1
        )
    )
    .orderBy(desc("total_revenue"))
)

print("=== GOLD: Product Performance ===")
product_performance.show()

(
    product_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.product_performance")
)


# =============================================================================
# GOLD TABLE 3: inventory_health
# Business question: What needs to be reordered? What's turning fast?
# =============================================================================

# Units sold per product (for turnover calc)
units_sold = (
    sales
    .groupBy("product_id")
    .agg(F.sum("quantity").alias("units_sold_period"))
)

inventory_health = (
    inventory
    .join(units_sold, on="product_id", how="left")
    .withColumn("units_sold_period", F.coalesce(col("units_sold_period"), F.lit(0)))
    # Turnover ratio = units sold / current stock (higher = faster moving)
    .withColumn(
        "inventory_turnover_ratio",
        round(col("units_sold_period") / F.greatest(col("stock_qty"), F.lit(1)), 2)
    )
    .withColumn(
        "reorder_urgency",
        F.when(col("stock_qty") == 0, "OUT OF STOCK")
         .when(col("is_low_stock") == True, "REORDER NOW")
         .when(col("stock_qty") <= col("reorder_level") * 1.5, "WATCH")
         .otherwise("OK")
    )
    .select(
        "product_id", "product_name", "category", "supplier",
        "stock_qty", "reorder_level", "units_sold_period",
        "inventory_turnover_ratio", "reorder_urgency", "is_low_stock"
    )
    .orderBy(desc("inventory_turnover_ratio"))
)

print("=== GOLD: Inventory Health ===")
inventory_health.show()

(
    inventory_health.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.inventory_health")
)


# =============================================================================
# GOLD TABLE 4: customer_summary
# Business question: Who are our best customers? Loyalty segment breakdown?
# =============================================================================

customer_sales = (
    sales
    .groupBy("customer_id")
    .agg(
        F.count("sale_id").alias("total_orders"),
        F.sum("line_revenue").alias("total_spend"),
        F.avg("line_revenue").alias("avg_order_value"),
        F.min("sale_date").alias("first_purchase"),
        F.max("sale_date").alias("last_purchase")
    )
)

customer_summary = (
    customers
    .join(customer_sales, on="customer_id", how="left")
    .withColumn("total_spend",     round(col("total_spend"), 2))
    .withColumn("avg_order_value", round(col("avg_order_value"), 2))
    .orderBy(desc("total_spend"))
)

print("=== GOLD: Customer Summary ===")
customer_summary.show()

(
    customer_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.customer_summary")
)


# =============================================================================
# GOLD TABLE 5: executive_kpis
# Business question: One-row summary for the exec dashboard
# =============================================================================

kpis = (
    sales
    .agg(
        round(F.sum("line_revenue"), 2).alias("total_revenue"),
        F.count("sale_id").alias("total_transactions"),
        round(F.avg("line_revenue"), 2).alias("avg_order_value"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("unique_products_sold"),
        F.min("sale_date").alias("period_start"),
        F.max("sale_date").alias("period_end")
    )
)

print("=== GOLD: Executive KPIs ===")
kpis.show(vertical=True)

(
    kpis.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{GOLD_DB}.executive_kpis")
)

print("\n✅ Gold layer complete. All business tables ready for BI.")
