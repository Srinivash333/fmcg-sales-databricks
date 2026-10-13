# FMCG Sales Data Pipeline (Databricks)

End-to-end FMCG (Fast-Moving Consumer Goods) sales analytics pipeline built on Databricks.

## Project Structure

```
fmcg-sales-databricks/
├── setup1/                           # Catalog & infrastructure setup
│   ├── setup_catalog.sql             # Unity Catalog & schema creation
│   ├── dim_date_table_creation.sql   # Date dimension table
│   └── utilities.py                  # Shared utility functions
├── 2_dimension_data_processing/      # Dimension tables
│   ├── 1_customer_data_processing.py
│   ├── 2_products_data_processing.py
│   └── 3_pricing_data_processing.py
├── 3_fact_data_processing/          # Fact tables
│   ├── 1_full_load_fact.py          # Initial full load
│   └── 2_incremental_load_fact.py   # Incremental loading
└── dashboards/
    └── AtliQon_BI.lvdash.json       # AtliQon BI dashboard (Lakeview)
```

## Tech Stack
- Databricks Lakehouse (Unity Catalog, Delta Lake)
- PySpark & SQL
- AI/BI Lakeview Dashboards

## Data Coverage
- Indian FMCG sales data spanning Jan 2024 – Dec 2025
- Dimensions: customer, product, pricing, date
- Facts: orders with quantity, price (INR), and total amount
