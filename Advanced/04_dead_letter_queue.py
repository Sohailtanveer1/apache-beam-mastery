"""
Advanced 04 — Error Handling: Dead Letter Queue (DLQ) Pattern

Concepts:
  - Catching exceptions inside DoFn.process() without crashing the pipeline
  - Routing failed records to a "dead letter" sink (GCS/BigQuery/Pub/Sub)
  - The try/except + TaggedOutput pattern
  - Preserving the original payload + error metadata for later reprocessing

The DLQ pattern is ESSENTIAL for production pipelines.
Without it, a single malformed record can kill an entire streaming job.

Run:
  python Advanced/04_dead_letter_queue.py
"""

import apache_beam as beam
from apache_beam import pvalue
import json
import traceback
from datetime import datetime, timezone


SUCCESS_TAG = "success"
DLQ_TAG     = "dlq"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SafeParseAndTransformDoFn(beam.DoFn):
    """
    Parses a JSON message, validates it, and applies a business transform.
    Errors are emitted to the DLQ output tag instead of raising.

    DLQ records contain:
      - original_payload: the raw bytes/string that failed
      - error_type:       the exception class name
      - error_message:    human-readable error description
      - stack_trace:      full traceback for debugging
      - failed_at:        ISO-8601 timestamp
    """

    def process(self, element):
        try:
            data = json.loads(element)

            # ── Business validation ────────────────────────────────────
            if "user_id" not in data:
                raise ValueError("Missing required field: user_id")
            if not isinstance(data.get("amount"), (int, float)):
                raise TypeError(f"amount must be numeric, got: {type(data.get('amount'))}")
            if data["amount"] < 0:
                raise ValueError(f"Negative amount not allowed: {data['amount']}")

            # ── Business transform ─────────────────────────────────────
            transformed = {
                **data,
                "amount_cents": int(data["amount"] * 100),
                "is_high_value": data["amount"] > 500,
                "processed_at": _now_iso(),
            }
            yield transformed  # → main (success) output

        except Exception as e:
            yield pvalue.TaggedOutput(DLQ_TAG, {
                "original_payload": element,
                "error_type":       type(e).__name__,
                "error_message":    str(e),
                "stack_trace":      traceback.format_exc(),
                "failed_at":        _now_iso(),
            })


def run():
    # Mix of good and bad records
    raw_messages = [
        '{"user_id": "u1", "amount": 250.0}',          # ✓ valid
        '{"user_id": "u2", "amount": 1500.0}',          # ✓ valid (high value)
        '{"user_id": "u3", "amount": -50.0}',           # ✗ negative amount
        '{"amount": 100.0}',                            # ✗ missing user_id
        '{"user_id": "u5", "amount": "not_a_number"}',  # ✗ wrong type
        'INVALID_JSON{{{',                              # ✗ malformed JSON
        '{"user_id": "u6", "amount": 0.0}',             # ✓ valid (zero)
    ]

    with beam.Pipeline() as p:

        results = (
            p
            | "CreateMessages" >> beam.Create(raw_messages)
            | "ParseAndTransform" >> beam.ParDo(
                SafeParseAndTransformDoFn()
            ).with_outputs(DLQ_TAG, main=SUCCESS_TAG)
        )

        successes = results[SUCCESS_TAG]
        failures  = results[DLQ_TAG]

        # ── Success path → BigQuery / downstream processing ──────────
        successes | "PrintSuccess" >> beam.Map(
            lambda r: print(f"[SUCCESS] user={r['user_id']} "
                            f"amount=${r['amount']:.2f} "
                            f"high_value={r['is_high_value']}")
        )

        # ── DLQ path → GCS / separate BigQuery table ─────────────────
        # In production: | beam.io.WriteToText("gs://bucket/dlq/") or WriteToBigQuery(...)
        failures | "PrintDLQ" >> beam.Map(
            lambda r: print(f"[DLQ] error={r['error_type']}: {r['error_message']}\n"
                            f"      payload={r['original_payload'][:60]}")
        )

        # ── Metric: count DLQ records ─────────────────────────────────
        dlq_count = (
            failures
            | "CountDLQ" >> beam.combiners.Count.Globally()
        )
        dlq_count | "PrintDLQCount" >> beam.Map(
            lambda n: print(f"\n[METRIC] Total DLQ records: {n}")
        )


if __name__ == "__main__":
    run()
