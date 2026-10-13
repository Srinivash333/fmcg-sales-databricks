# FMCG Sales Analytics — Databricks Lakehouse Pipeline

End-to-end Medallion architecture pipeline for Indian FMCG (Fast-Moving Consumer Goods) sales data, built on Databricks with Unity Catalog, Delta Lake, and AI/BI Lakeview dashboards.

## Architecture

```
S3 (CSV) → Bronze (raw) → Silver (cleansed) → Gold (analytics-ready) → Dashboard
```

**Unity Catalog**: `fmcg` with three schemas — `fmcg.bronze`, `fmcg.silver`, `fmcg.gold`

All Delta tables have **Change Data Feed (CDF)** enabled for incremental processing.

## Repository Structure

```
fmcg-sales-databricks/
├── setup1/
│   ├── setup_catalog.sql             # Creates fmcg catalog + bronze/silver/gold schemas
│   ├── dim_date_table_creation.sql   # Builds fmcg.gold.dim_date (monthly grain, Jan 2024–Dec 2025)
│   └── utilities.py                  # Shared schema-name variables (bronze, silver, gold)
├── 2_dimension_data_processing/
│   ├── 1_customer_data_processing.py  # Customers: dedup, city standardization, null fixes
│   ├── 2_products_data_processing.py  # Products: dedup, category title-casing, product_code hashing
│   └── 3_pricing_data_processing.py   # Pricing: cleansed pricing dimension
├── 3_fact_data_processing/
│   ├── 1_full_load_fact.py            # Full load: S3 → bronze → silver → gold → fact_orders (monthly)
│   └── 2_incremental_load_fact.py     # Incremental load via Delta CDF
├── dashboards/
│   └── AtliQon_BI.lvdash.json         # AtliQon BI dashboard (AI/BI Lakeview)
└── README.md
```

## Pipeline Stages

### 1. Setup (`setup1/`)

| Notebook | Description |
| --- | --- |
| `setup_catalog.sql` | Creates the `fmcg` catalog and `bronze` / `silver` / `gold` schemas |
| `dim_date_table_creation.sql` | Generates a monthly-grain date dimension (`fmcg.gold.dim_date`) covering Jan 2024 – Dec 2025 with `date_key`, `year`, `quarter`, `year_quarter` columns |
| `utilities.py` | Defines shared `bronze_schema`, `silver_schema`, `gold_schema` variables; imported via `%run` by all downstream notebooks |

### 2. Dimension Processing (`2_dimension_data_processing/`)

Each notebook follows the same pattern: **read CSV from S3 → write to bronze → read bronze → transform → write to silver**.

| Notebook | Source | Key Transformations |
| --- | --- | --- |
| `1_customer_data_processing` | `s3://.../customers/*.csv` | Dedup on `customer_id`, trim whitespace, standardize city names (e.g. `Bengalore` → `Bengaluru`, `Hyderbad` → `Hyderabad`), title-case `customer_name`, business-confirmed city fixes for nulls |
| `2_products_data_processing` | `s3://.../products/*.csv` | Dedup on `product_id`, title-case categories, SHA-256 `product_code` generation |
| `3_pricing_data_processing` | `s3://.../pricing/*.csv` | Cleansed pricing dimension |

Notebooks are parameterized via **dbutils widgets** (`catalog`, `data_source`) for reuse across environments.

### 3. Fact Processing (`3_fact_data_processing/`)

| Notebook | Description |
| --- | --- |
| `1_full_load_fact` | Reads order CSVs from S3 `landing/` folder → appends to `fmcg.bronze.orders` (CDF enabled) → moves files to `processed/` → joins with `fmcg.silver.products` for `product_code` → upserts into `fmcg.silver.orders` via Delta `MERGE` → aggregates to monthly grain → upserts into `fmcg.gold.fact_orders` |
| `2_incremental_load_fact` | Incremental processing using Delta Change Data Feed to capture only new/changed rows since last run |

**Upsert pattern** (used throughout): checks `tableExists` → creates with `overwrite` on first run, then `DeltaTable.merge().whenMatchedUpdateAll().whenNotMatchedInsertAll()` on subsequent runs.

**Monthly aggregation**: daily-grain child data is grouped by `month_start + product_code + customer_code` and summed, then merged into the parent `fmcg.gold.fact_orders` table.

### 4. Dashboard (`dashboards/`)

The **AtliQon BI** dashboard (AI/BI Lakeview) provides:

* **4 KPI counters**: Total Revenue (₹), Units Sold, Order Count, Customers
* **Monthly revenue trend** (line chart)
* **Revenue by channel** (pie), **revenue by market** (bar)
* **Units by division** (bar)
* **Product detail table**

Backing view: `fmcg.gold.vw_fact_orders_enriched` — enriched with price (`price_inr`) and total amount (`total_amount_inr`) for revenue calculations.

## Data Overview

* **Domain**: Indian FMCG sales (sporting goods & nutrition)
* **Time range**: January 2024 – December 2025
* **Markets**: Bengaluru, Hyderabad, New Delhi
* **Channels**: Retailer (dominant), Direct, Acquisition
* **Record count**: ~93,000 orders in `fmcg.gold.fact_orders`
* **Source**: S3 bucket (`s3://fmcg-sales-data-2026-srinivas/`)

## Tech Stack

* **Databricks Lakehouse** — compute & storage
* **Unity Catalog** — governance (`fmcg` catalog)
* **Delta Lake** — ACID transactions, Change Data Feed, MERGE upserts
* **PySpark & SQL** — transformations
* **AI/BI Lakeview** — dashboards
* **Amazon S3** — raw data landing zone

## Reproduction Guide

1. Run `setup1/setup_catalog` to create the catalog and schemas
2. Run `setup1/dim_date_table_creation` to build the date dimension
3. Run dimension notebooks in order: `1_customer_data_processing` → `2_products_data_processing` → `3_pricing_data_processing`
4. Run `1_full_load_fact` for the initial load
5. Schedule `2_incremental_load_fact` for ongoing updates
6. Import the dashboard JSON via the Databricks dashboard editor
