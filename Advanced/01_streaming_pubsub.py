"""
Advanced 01 — Streaming Pipeline: Reading from Pub/Sub

Concepts:
  - beam.io.ReadFromPubSub → Unbounded PCollection from a Pub/Sub topic/subscription
  - Fixed windowing on an unbounded stream
  - Writing windowed aggregations to BigQuery
  - --streaming flag requirement for Dataflow

Run on Dataflow (streaming jobs never end; Ctrl+C or drain from Console):
  python Advanced/01_streaming_pubsub.py \
    --runner=DataflowRunner \
    --project=YOUR_PROJECT_ID \
    --region=us-central1 \
    --temp_location=gs://YOUR_BUCKET/temp \
    --subscription=projects/YOUR_PROJECT_ID/subscriptions/YOUR_SUB \
    --output_table=YOUR_PROJECT_ID:dataset.table \
    --streaming \
    --enable_streaming_engine
"""

import argparse
import json
import logging
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.transforms.window import TimestampedValue


OUTPUT_SCHEMA = {
    "fields": [
        {"name": "window_start",  "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "event_type",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "event_count",   "type": "INTEGER",   "mode": "REQUIRED"},
    ]
}


class ParseMessageDoFn(beam.DoFn):
    """Parses a Pub/Sub message JSON payload. Yields None on parse failure."""

    def process(self, element):
        try:
            data = json.loads(element.decode("utf-8"))
            yield data
        except (json.JSONDecodeError, AttributeError) as e:
            logging.warning(f"Failed to parse message: {element!r} — {e}")


class AddWindowInfoDoFn(beam.DoFn):
    """Attaches window boundary info to each aggregated element."""

    def process(self, element, window=beam.DoFn.WindowParam):
        event_type, count = element
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "event_type":   event_type,
            "event_count":  count,
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription", required=True,
                        help="Full Pub/Sub subscription path")
    parser.add_argument("--output_table", required=True,
                        help="BigQuery table (project:dataset.table)")
    parser.add_argument("--window_seconds", type=int, default=60,
                        help="Fixed window size in seconds")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=known_args.subscription,
                with_attributes=False,  # Set True to access message metadata
                timestamp_attribute=None,  # Use Pub/Sub publish time
            )
        )

        parsed = (
            messages
            | "ParseJSON" >> beam.ParDo(ParseMessageDoFn())
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        windowed_counts = (
            parsed
            | "ExtractType"  >> beam.Map(lambda msg: (msg.get("event_type", "unknown"), 1))
            | "FixedWindow"  >> beam.WindowInto(
                window.FixedWindows(known_args.window_seconds)
            )
            | "CountPerType" >> beam.CombinePerKey(sum)
            | "AddWindowInfo">> beam.ParDo(AddWindowInfoDoFn())
        )

        windowed_counts | "WriteToBQ" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=OUTPUT_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
