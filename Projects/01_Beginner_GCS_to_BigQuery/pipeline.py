"""
Project 1 — Batch ETL: GCS CSV to BigQuery
See README.md for full architecture details.

Run (local):
  python Projects/01_Beginner_GCS_to_BigQuery/pipeline.py \
    --project=YOUR_PROJECT_ID \
    --input=gs://YOUR_BUCKET/input/sales/*.csv \
    --output_table=YOUR_PROJECT_ID:sales_etl_demo.processed_sales \
    --dlq_path=gs://YOUR_BUCKET/dlq/sales/
"""

import argparse
import json
import logging
import traceback
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


OUTPUT_SCHEMA = {
    "fields": [
        {"name": "order_id",     "type": "STRING",    "mode": "REQUIRED"},
        {"name": "customer_id",  "type": "STRING",    "mode": "REQUIRED"},
        {"name": "order_date",   "type": "DATE",      "mode": "REQUIRED"},
        {"name": "product_id",   "type": "STRING",    "mode": "REQUIRED"},
        {"name": "quantity",     "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "unit_price",   "type": "FLOAT64",   "mode": "REQUIRED"},
        {"name": "currency",     "type": "STRING",    "mode": "REQUIRED"},
        {"name": "amount_usd",   "type": "FLOAT64",   "mode": "REQUIRED"},
        {"name": "ingested_at",  "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

FX_RATES = {"USD": 1.0, "EUR": 1.09, "GBP": 1.27, "JPY": 0.0067}

SUCCESS_TAG = "success"
DLQ_TAG     = "dlq"


class ParseCSVDoFn(beam.DoFn):
    HEADERS = ["order_id", "customer_id", "order_date",
               "product_id", "quantity", "unit_price", "currency"]

    def process(self, line):
        try:
            parts = [p.strip() for p in line.split(",")]
            if len(parts) != len(self.HEADERS):
                raise ValueError(f"Expected {len(self.HEADERS)} columns, got {len(parts)}")
            row = dict(zip(self.HEADERS, parts))
            row["quantity"]   = int(row["quantity"])
            row["unit_price"] = float(row["unit_price"])
            yield row
        except Exception as e:
            yield pvalue.TaggedOutput(DLQ_TAG, {
                "raw_line":    line,
                "error":       str(e),
                "failed_at":   datetime.now(timezone.utc).isoformat(),
            })


class EnrichRowDoFn(beam.DoFn):
    def process(self, row, fx_rates):
        currency = row.get("currency", "USD").upper()
        rate = fx_rates.get(currency, 1.0)
        yield {
            **row,
            "amount_usd":  round(row["quantity"] * row["unit_price"] * rate, 2),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",        required=True)
    parser.add_argument("--output_table", required=True)
    parser.add_argument("--dlq_path",     required=True)
    parser.add_argument("--project",      required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:

        fx_side = p | "CreateFX" >> beam.Create(list(FX_RATES.items()))

        parsed = (
            p
            | "ReadCSV"    >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
            | "ParseCSV"   >> beam.ParDo(ParseCSVDoFn()).with_outputs(DLQ_TAG, main=SUCCESS_TAG)
        )

        enriched = (
            parsed[SUCCESS_TAG]
            | "EnrichRows" >> beam.ParDo(EnrichRowDoFn(), fx_rates=beam.pvalue.AsDict(fx_side))
        )

        enriched | "WriteBQ" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=OUTPUT_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )

        parsed[DLQ_TAG] | "FormatDLQ" >> beam.Map(json.dumps) | "WriteDLQ" >> beam.io.WriteToText(
            known_args.dlq_path + "errors",
            file_name_suffix=".json",
        )

    logging.info("Pipeline complete.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
