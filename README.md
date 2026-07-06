# 🛒 Retail Sales Pipeline — Databricks Medallion Architecture

**A portfolio data engineering project demonstrating vendor data ingestion,
multi-format cleaning, medallion layering, and business metric generation.**

---

## Project Overview

This pipeline simulates a common real-world scenario: **multiple vendors sending
messy, inconsistent CSVs** that must be ingested, cleaned, and transformed into
reliable business intelligence.

Built on the **medallion architecture** (Bronze → Silver → Gold) using
**Apache Spark on Databricks** with **Delta Lake** as the storage format.

---

## Architecture

```
Vendor CSVs (messy)
       │
       ▼
┌─────────────┐
│   BRONZE    │  Raw ingestion, no transformations, full auditability
│  Delta Lake │  + _ingested_at, _source_file metadata columns
└─────────────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Clean, typed, deduplicated, standardized
│  Delta Lake │  Fixes: dates, casing, nulls, duplicates, invalid emails
└─────────────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Business-ready aggregations & KPI tables
│  Delta Lake │  Feeds dashboards, BI tools, exec reporting
└─────────────┘
```

---

## Data Quality Issues (Intentionally Injected)

| Table | Issues |
|-------|--------|
| `sales.csv` | Mixed-case product IDs, 4 date formats, null customer & price, 3 duplicate rows |
| `inventory.csv` | Column name with space, mixed category casing, null supplier, 1 duplicate product |
| `customers.csv` | ALL CAPS / all lowercase names, invalid emails, inconsistent phone formats, nulls |

---

## Notebooks

| # | Notebook | Layer | What It Does |
|---|----------|-------|--------------|
| 1 | `01_bronze_ingestion.py` | Bronze | Loads CSVs with `PERMISSIVE` mode, adds audit columns, profiles issues |
| 2 | `02_silver_transform.py` | Silver | Cleans columns, parses 4 date formats, deduplicates, validates emails, standardizes names |
| 3 | `03_gold_metrics.py` | Gold | Produces 5 business tables: daily revenue, product performance, inventory health, customer summary, exec KPIs |

---

## Gold Layer Tables (Business Outputs)

| Table | Business Question |
|-------|------------------|
| `daily_revenue` | How is revenue trending day over day? |
| `product_performance` | Which products drive the most revenue? What's the gross margin? |
| `inventory_health` | What needs reordering? What's turning fast? |
| `customer_summary` | Who are our best customers? |
| `executive_kpis` | One-row summary: total revenue, AOV, unique customers |

---

## Tech Stack

- **Apache Spark** (PySpark) — distributed data processing
- **Delta Lake** — ACID-compliant storage with time-travel
- **Databricks** — execution environment
- **Python** — transformation logic

---

## Key Engineering Decisions

**Why keep dirty rows in Bronze?**  
Auditability. If a business user asks "why was this record excluded?", you need
the original raw data to answer. Bronze is your source of truth.

**Why flag nulls instead of dropping them in Silver?**  
Dropping is destructive. Flagging with `is_valid_record` lets downstream consumers
decide their own tolerance. Gold only uses `is_valid_record = true`.

**Why `PERMISSIVE` mode on CSV read?**  
Realistic vendor files have malformed rows. `PERMISSIVE` places bad rows into a
`_corrupt_record` column instead of crashing the job — the pipeline keeps running.

**Why Delta Lake instead of Parquet?**  
Delta gives you ACID transactions, schema enforcement, and time travel with `VERSION AS OF` — critical for production pipelines where you need to audit or roll back.

---

## How to Run in Databricks

1. Upload the 3 CSVs to DBFS: `dbfs:/FileStore/retail_pipeline/raw/`
2. Run notebooks in order: `01` → `02` → `03`
3. Query Gold tables in Databricks SQL or connect a BI tool

---

*Built by Daria — portfolio project demonstrating medallion architecture,
data quality handling, and business metric generation.*
