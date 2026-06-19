"""
Intermediate 05 — Reading from and Writing to BigQuery

Concepts:
  - beam.io.ReadFromBigQuery → Read a BQ table or query result
  - beam.io.WriteToBigQuery  → Write rows to a BQ table
  - Schema definition (dict-based and JSON string)
  - Write dispositions: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
  - Create dispositions: CREATE_IF_NEEDED, CREATE_NEVER
  - Dynamic destinations (route rows to different tables at runtime)

Prerequisites:
  - A GCP project with BigQuery API enabled
  - Appropriate IAM roles: roles/bigquery.dataEditor

Run (DirectRunner reads/writes BQ directly from local machine):
  python Intermediate/05_bigquery_read_write.py \
    --project=YOUR_PROJECT_ID \
    --dataset=beam_demo \
    --runner=DirectRunner

Run on Dataflow:
  python Intermediate/05_bigquery_read_write.py \
    --runner=DataflowRunner \
    --project=YOUR_PROJECT_ID \
    --region=us-central1 \
    --temp_location=gs://YOUR_BUCKET/temp \
    --dataset=beam_demo
"""

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# ── Schema definition ─────────────────────────────────────────────────────
# BigQuery schema can be defined as a list of dicts or a JSON-string.
OUTPUT_SCHEMA = {
    "fields": [
        {"name": "user_id",       "type": "STRING",  "mode": "REQUIRED"},
        {"name": "event_type",    "type": "STRING",  "mode": "REQUIRED"},
        {"name": "amount",        "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "processed_at",  "type": "TIMESTAMP","mode": "REQUIRED"},
        {"name": "is_large",      "type": "BOOL",    "mode": "REQUIRED"},
    ]
}

SAMPLE_ROWS = [
    {"user_id": "u1", "event_type": "purchase", "amount": 250.0},
    {"user_id": "u2", "event_type": "click",    "amount": None},
    {"user_id": "u1", "event_type": "purchase", "amount": 1500.0},
    {"user_id": "u3", "event_type": "purchase", "amount": 75.0},
]


def enrich_row(row):
    """Add derived fields before writing to BigQuery."""
    from datetime import datetime, timezone
    return {
        **row,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "is_large": (row.get("amount") or 0) > 500,
    }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",  required=True)
    parser.add_argument("--dataset",  default="beam_demo")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    project = known_args.project
    dataset = known_args.dataset
    output_table = f"{project}:{dataset}.processed_events"
    purchases_table = f"{project}:{dataset}.purchases_only"

    logging.basicConfig(level=logging.INFO)

    with beam.Pipeline(options=options) as p:

        # ── READ FROM BIGQUERY ─────────────────────────────────────────
        # Option 1: Read an entire table
        # rows = p | "ReadTable" >> beam.io.ReadFromBigQuery(
        #     table=f"{project}:{dataset}.raw_events"
        # )

        # Option 2: Read using a SQL query (preferred for large tables)
        # rows = p | "ReadQuery" >> beam.io.ReadFromBigQuery(
        #     query=f"SELECT * FROM `{project}.{dataset}.raw_events` WHERE date = '2024-01-01'",
        #     use_standard_sql=True,
        # )

        # For demo purposes, we use in-memory data
        rows = p | "CreateSampleRows" >> beam.Create(SAMPLE_ROWS)

        enriched = rows | "EnrichRows" >> beam.Map(enrich_row)

        # ── WRITE TO BIGQUERY — single table ──────────────────────────
        enriched | "WriteAllEvents" >> WriteToBigQuery(
            table=output_table,
            schema=OUTPUT_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            # batch_size controls how many rows are sent per API call
            batch_size=500,
        )

        # ── WRITE TO BIGQUERY — dynamic destinations ──────────────────
        # Route rows to different tables based on a field value.
        def get_destination(row, project, dataset):
            event = row["event_type"]
            return f"{project}:{dataset}.events_{event}"

        def get_schema(destination):
            # Schema can be fixed or derived from the destination name.
            return OUTPUT_SCHEMA

        enriched | "WriteDynamic" >> WriteToBigQuery(
            table=lambda row: get_destination(row, project, dataset),
            schema=get_schema,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )

    logging.info("Pipeline complete.")


if __name__ == "__main__":
    run()
