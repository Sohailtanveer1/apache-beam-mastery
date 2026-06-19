"""
Project 3 — Real-Time Streaming Analytics Pipeline
Pub/Sub → Dataflow → BigQuery (with DLQ to GCS)

Architecture: See README.md

Deploy command:
  python Projects/03_Advanced_Streaming_Analytics/pipeline.py \
    --runner=DataflowRunner \
    --project=YOUR_PROJECT_ID \
    --region=us-central1 \
    --temp_location=gs://YOUR_BUCKET/temp \
    --subscription=projects/YOUR_PROJECT_ID/subscriptions/ecommerce-events-sub \
    --bq_dataset=streaming_analytics \
    --dlq_path=gs://YOUR_BUCKET/dlq/streaming/ \
    --streaming \
    --enable_streaming_engine \
    --max_num_workers=20
"""

import argparse
import json
import logging
import traceback
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue, window
from apache_beam.coders import coders
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
)
from apache_beam.transforms.userstate import (
    BagStateSpec,
    CombiningValueStateSpec,
)
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# ─────────────────────────────────────────────────────────────────────────────
# Constants & Tag Names
# ─────────────────────────────────────────────────────────────────────────────

VALID_TAG = "valid"
DLQ_TAG   = "dlq"

FX_RATES = {"USD": 1.0, "EUR": 1.09, "GBP": 1.27, "JPY": 0.0067, "CAD": 0.74}

REQUIRED_FIELDS = {"event_id", "user_id", "event_type", "product_id",
                   "amount", "currency", "region"}


# ─────────────────────────────────────────────────────────────────────────────
# BigQuery Schemas
# ─────────────────────────────────────────────────────────────────────────────

REVENUE_SCHEMA = {
    "fields": [
        {"name": "window_start",       "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "product_id",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "total_revenue_usd",  "type": "FLOAT64",   "mode": "REQUIRED"},
        {"name": "transaction_count",  "type": "INTEGER",   "mode": "REQUIRED"},
    ]
}

EVENT_COUNT_SCHEMA = {
    "fields": [
        {"name": "window_start",  "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "event_type",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "region",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "event_count",   "type": "INTEGER",   "mode": "REQUIRED"},
    ]
}

SESSION_SCHEMA = {
    "fields": [
        {"name": "window_start",         "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "user_id",              "type": "STRING",    "mode": "REQUIRED"},
        {"name": "session_event_count",  "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "session_revenue",      "type": "FLOAT64",   "mode": "REQUIRED"},
    ]
}

DLQ_SCHEMA = {
    "fields": [
        {"name": "original_payload", "type": "STRING",    "mode": "REQUIRED"},
        {"name": "error_type",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "error_message",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "failed_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: Parse and Validate Pub/Sub Messages
# ─────────────────────────────────────────────────────────────────────────────

class ParseAndValidateDoFn(beam.DoFn):
    """
    Parses a Pub/Sub message bytes payload as JSON, validates required fields,
    and coerces types. Malformed records go to the DLQ output tag.
    """

    def process(self, element):
        raw = None
        try:
            raw = element.decode("utf-8") if isinstance(element, bytes) else element
            event = json.loads(raw)

            # ── Field presence check ──────────────────────────────────
            missing = REQUIRED_FIELDS - set(event.keys())
            if missing:
                raise ValueError(f"Missing required fields: {sorted(missing)}")

            # ── Type coercion ─────────────────────────────────────────
            event["amount"] = float(event["amount"])
            if event["amount"] < 0:
                raise ValueError(f"Negative amount: {event['amount']}")

            event["currency"] = str(event["currency"]).upper()
            event["event_type"] = str(event["event_type"]).lower()

            yield event  # → main (VALID) output

        except Exception as exc:
            yield pvalue.TaggedOutput(DLQ_TAG, {
                "original_payload": raw or str(element),
                "error_type":       type(exc).__name__,
                "error_message":    str(exc),
                "failed_at":        datetime.now(timezone.utc).isoformat(),
            })


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: Deduplicate by event_id within a 10-minute window
# ─────────────────────────────────────────────────────────────────────────────

class DeduplicateEventDoFn(beam.DoFn):
    """
    Uses BagState to track seen event IDs within a window.
    Drops duplicate event_id values (common in at-least-once Pub/Sub delivery).
    """

    SEEN_IDS = BagStateSpec("seen_event_ids", coders.StrUtf8Coder())

    def process(self, element, seen_state=beam.DoFn.StateParam(SEEN_IDS)):
        event_id, event = element
        seen = set(seen_state.read())

        if event_id in seen:
            logging.debug(f"Dropped duplicate event: {event_id}")
            return  # Silently drop — already processed

        seen_state.add(event_id)
        yield event


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3: Currency normalization (Map — pure function, no state)
# ─────────────────────────────────────────────────────────────────────────────

def normalize_currency(event):
    """Converts event amount to USD using a static FX rate table."""
    rate = FX_RATES.get(event["currency"], 1.0)
    return {
        **event,
        "amount_usd": round(event["amount"] * rate, 4),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4a: Revenue aggregation per product per 1-minute window
# ─────────────────────────────────────────────────────────────────────────────

class RevenueAggregateDoFn(beam.DoFn):
    """Attaches window info to (product_id, {revenue, count}) aggregations."""

    def process(self, element, window=beam.DoFn.WindowParam):
        product_id, agg = element
        total_revenue, count = agg
        yield {
            "window_start":      window.start.to_utc_datetime().isoformat(),
            "product_id":        product_id,
            "total_revenue_usd": round(total_revenue, 2),
            "transaction_count": count,
        }


class RevenueAccumulatorFn(beam.CombineFn):
    """Combines (revenue, count) tuples with partial aggregation."""

    def create_accumulator(self):
        return (0.0, 0)  # (sum_revenue, count)

    def add_input(self, accumulator, element):
        total, count = accumulator
        return (total + element, count + 1)

    def merge_accumulators(self, accumulators):
        total = sum(a[0] for a in accumulators)
        count = sum(a[1] for a in accumulators)
        return (total, count)

    def extract_output(self, accumulator):
        return accumulator  # (total_revenue, count)


def build_revenue_pipeline(enriched, window_size_seconds: int, project: str, dataset: str):
    """
    Aggregates purchase revenue per product in fixed 1-minute windows.
    Only considers 'purchase' events.
    """
    return (
        enriched
        | "FilterPurchases"     >> beam.Filter(lambda e: e["event_type"] == "purchase")
        | "KeyByProduct"        >> beam.Map(lambda e: (e["product_id"], e["amount_usd"]))
        | "RevenueWindow"       >> beam.WindowInto(window.FixedWindows(window_size_seconds))
        | "AggregateRevenue"    >> beam.CombinePerKey(RevenueAccumulatorFn())
        | "FormatRevenue"       >> beam.ParDo(RevenueAggregateDoFn())
        | "WriteRevenue"        >> WriteToBigQuery(
            table=f"{project}:{dataset}.revenue_per_minute",
            schema=REVENUE_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4b: Event type counts per region per 1-minute window
# ─────────────────────────────────────────────────────────────────────────────

class EventCountWindowDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        key, count = element
        event_type, region = key
        yield {
            "window_start": window.start.to_utc_datetime().isoformat(),
            "event_type":   event_type,
            "region":       region,
            "event_count":  count,
        }


def build_event_count_pipeline(enriched, window_size_seconds: int, project: str, dataset: str):
    """Counts events by (event_type, region) in fixed windows."""
    return (
        enriched
        | "KeyByTypeRegion"   >> beam.Map(
            lambda e: ((e["event_type"], e["region"]), 1)
        )
        | "EventCountWindow"  >> beam.WindowInto(window.FixedWindows(window_size_seconds))
        | "CountPerTypeRegion">> beam.CombinePerKey(sum)
        | "FormatEventCount"  >> beam.ParDo(EventCountWindowDoFn())
        | "WriteEventCounts"  >> WriteToBigQuery(
            table=f"{project}:{dataset}.event_type_counts",
            schema=EVENT_COUNT_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4c: User session metrics using Session Windows
# ─────────────────────────────────────────────────────────────────────────────

class SessionMetricsFn(beam.CombineFn):
    """Per-user accumulator: tracks event count and total revenue in a session."""

    def create_accumulator(self):
        return {"count": 0, "revenue": 0.0}

    def add_input(self, acc, element):
        return {
            "count":   acc["count"] + 1,
            "revenue": acc["revenue"] + element.get("amount_usd", 0.0),
        }

    def merge_accumulators(self, accs):
        return {
            "count":   sum(a["count"] for a in accs),
            "revenue": sum(a["revenue"] for a in accs),
        }

    def extract_output(self, acc):
        return acc


class SessionWindowOutputDoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        user_id, metrics = element
        yield {
            "window_start":        window.start.to_utc_datetime().isoformat(),
            "user_id":             user_id,
            "session_event_count": metrics["count"],
            "session_revenue":     round(metrics["revenue"], 2),
        }


def build_session_pipeline(enriched, gap_seconds: int, project: str, dataset: str):
    """
    Groups events by user into sessions separated by `gap_seconds` of inactivity.
    Emits session-level metrics when the session closes.
    """
    return (
        enriched
        | "KeyByUser"      >> beam.Map(lambda e: (e["user_id"], e))
        | "SessionWindow"  >> beam.WindowInto(
            window.Sessions(gap_seconds),
            # Allowed lateness: accept events up to 30s late
            allowed_lateness=beam.transforms.window.Duration(seconds=30),
        )
        | "AggSession"     >> beam.CombinePerKey(SessionMetricsFn())
        | "FormatSession"  >> beam.ParDo(SessionWindowOutputDoFn())
        | "WriteSession"   >> WriteToBigQuery(
            table=f"{project}:{dataset}.user_session_metrics",
            schema=SESSION_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline Entry Point
# ─────────────────────────────────────────────────────────────────────────────

def run(argv=None):
    parser = argparse.ArgumentParser(
        description="Ecommerce real-time streaming analytics pipeline"
    )
    parser.add_argument(
        "--subscription", required=True,
        help="Full Pub/Sub subscription resource path: "
             "projects/PROJECT/subscriptions/SUB_NAME",
    )
    parser.add_argument(
        "--project", required=True,
        help="GCP Project ID",
    )
    parser.add_argument(
        "--bq_dataset", default="streaming_analytics",
        help="BigQuery dataset name (must exist)",
    )
    parser.add_argument(
        "--dlq_path", required=True,
        help="GCS prefix for dead-letter records (e.g. gs://bucket/dlq/)",
    )
    parser.add_argument(
        "--revenue_window_seconds", type=int, default=60,
        help="Fixed window size for revenue aggregation (seconds)",
    )
    parser.add_argument(
        "--event_window_seconds", type=int, default=60,
        help="Fixed window size for event count aggregation (seconds)",
    )
    parser.add_argument(
        "--session_gap_seconds", type=int, default=300,
        help="Inactivity gap to close a user session (seconds)",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    logging.info(
        "Starting streaming pipeline | subscription=%s | dataset=%s | dlq=%s",
        known_args.subscription, known_args.bq_dataset, known_args.dlq_path,
    )

    with beam.Pipeline(options=options) as p:

        # ── Stage 1: Ingest from Pub/Sub ──────────────────────────────
        raw_messages = (
            p
            | "ReadPubSub" >> beam.io.ReadFromPubSub(
                subscription=known_args.subscription,
                with_attributes=False,
            )
        )

        # ── Stage 2: Parse, validate, and route to DLQ ────────────────
        tagged = (
            raw_messages
            | "ParseValidate" >> beam.ParDo(
                ParseAndValidateDoFn()
            ).with_outputs(DLQ_TAG, main=VALID_TAG)
        )

        valid_events = tagged[VALID_TAG]
        dlq_events   = tagged[DLQ_TAG]

        # ── Stage 3: Deduplicate by event_id (10-min dedup window) ────
        deduped = (
            valid_events
            | "KeyByEventId"  >> beam.Map(lambda e: (e["event_id"], e))
            | "DedupWindow"   >> beam.WindowInto(window.FixedWindows(600))
            | "DeduplicateId" >> beam.ParDo(DeduplicateEventDoFn())
        )

        # ── Stage 4: Normalize currency → USD ─────────────────────────
        enriched = (
            deduped
            | "NormalizeCurrency" >> beam.Map(normalize_currency)
        )

        # ── Stage 5a: Revenue per product (fixed windows) ─────────────
        build_revenue_pipeline(
            enriched,
            known_args.revenue_window_seconds,
            known_args.project,
            known_args.bq_dataset,
        )

        # ── Stage 5b: Event type counts per region ────────────────────
        build_event_count_pipeline(
            enriched,
            known_args.event_window_seconds,
            known_args.project,
            known_args.bq_dataset,
        )

        # ── Stage 5c: User session metrics ────────────────────────────
        build_session_pipeline(
            enriched,
            known_args.session_gap_seconds,
            known_args.project,
            known_args.bq_dataset,
        )

        # ── Stage 6: Write DLQ records to GCS ─────────────────────────
        # In a high-volume pipeline, route DLQ to BigQuery as well so
        # SREs can query and replay failed records via SQL.
        (
            dlq_events
            | "FormatDLQJson" >> beam.Map(json.dumps)
            | "WriteDLQtoGCS" >> beam.io.WriteToText(
                known_args.dlq_path + "errors",
                file_name_suffix=".json",
                # Streaming-safe: use a window-based filename
                shard_name_template="-SSSSS-of-NNNNN",
            )
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    run()
