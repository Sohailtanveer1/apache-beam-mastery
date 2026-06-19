# Project 1 — Batch ETL: GCS CSV → BigQuery

**Level:** Beginner  
**Runner:** DirectRunner (dev) / DataflowRunner (prod)  
**GCP Services:** Cloud Storage, BigQuery, Dataflow

---

## Overview

A production-style batch ETL pipeline that:
1. Reads raw CSV sales records from a GCS bucket
2. Validates and cleans each row
3. Applies a currency conversion enrichment
4. Writes results to a BigQuery table (partitioned by date)

```
┌─────────────┐     ReadFromText     ┌──────────────────┐
│  GCS Bucket │ ──────────────────▶  │  Parse CSV Rows  │
│  sales/*.csv│                      └────────┬─────────┘
└─────────────┘                               │
                                    ┌─────────▼─────────┐
                                    │  Validate & Clean  │ (DLQ for bad rows)
                                    └─────────┬─────────┘
                                              │
                                    ┌─────────▼─────────┐
                                    │ Enrich (FX rates)  │ (Side input)
                                    └─────────┬─────────┘
                                              │
                                    ┌─────────▼──────────────────┐
                                    │  WriteToBigQuery            │
                                    │  project:dataset.sales_etl  │
                                    └─────────────────────────────┘
```

---

## Input Format

CSV files in `gs://YOUR_BUCKET/input/sales/`:

```csv
order_id,customer_id,order_date,product_id,quantity,unit_price,currency
ORD-001,CUST-A,2024-01-15,PROD-X,2,49.99,USD
ORD-002,CUST-B,2024-01-15,PROD-Y,1,199.00,EUR
```

---

## Output BigQuery Schema

| Column | Type | Mode | Description |
|--------|------|------|-------------|
| order_id | STRING | REQUIRED | Unique order identifier |
| customer_id | STRING | REQUIRED | Customer identifier |
| order_date | DATE | REQUIRED | Date of order |
| product_id | STRING | REQUIRED | Product identifier |
| quantity | INTEGER | REQUIRED | Units ordered |
| unit_price | FLOAT64 | REQUIRED | Price per unit (original currency) |
| currency | STRING | REQUIRED | ISO currency code |
| amount_usd | FLOAT64 | REQUIRED | Total amount in USD |
| ingested_at | TIMESTAMP | REQUIRED | Pipeline run timestamp |

---

## Setup

```bash
# 1. Create BigQuery dataset
bq mk --dataset YOUR_PROJECT:sales_etl_demo

# 2. Create GCS bucket and upload sample data
gsutil mb gs://YOUR_BUCKET
gsutil cp sample_data/sales.csv gs://YOUR_BUCKET/input/sales/

# 3. Run locally
python Projects/01_Beginner_GCS_to_BigQuery/pipeline.py \
  --project=YOUR_PROJECT_ID \
  --input=gs://YOUR_BUCKET/input/sales/*.csv \
  --output_table=YOUR_PROJECT_ID:sales_etl_demo.processed_sales \
  --dlq_path=gs://YOUR_BUCKET/dlq/sales/

# 4. Run on Dataflow
python Projects/01_Beginner_GCS_to_BigQuery/pipeline.py \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT_ID \
  --region=us-central1 \
  --temp_location=gs://YOUR_BUCKET/temp \
  --input=gs://YOUR_BUCKET/input/sales/*.csv \
  --output_table=YOUR_PROJECT_ID:sales_etl_demo.processed_sales \
  --dlq_path=gs://YOUR_BUCKET/dlq/sales/
```

---

## Key Learning Points

- `ReadFromText(skip_header_lines=1)` to skip CSV headers
- Side inputs for reference data (FX rates from a GCS JSON file)
- `WriteToBigQuery` with `CREATE_IF_NEEDED` and `WRITE_APPEND`
- Dead letter queue for malformed rows
- BigQuery table partitioning (`$YYYYMMDD` suffix)
