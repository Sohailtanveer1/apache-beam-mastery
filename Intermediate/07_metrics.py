"""
Intermediate 07 — Metrics: Counter, Distribution, Gauge

Source: https://beam.apache.org/documentation/programming-guide/#metrics

Beam Metrics give you observability into a running pipeline WITHOUT
writing to an external system. They appear in:
  - Dataflow Monitoring UI (Dataflow runner)
  - Pipeline result object (DirectRunner)
  - Cloud Monitoring / Stackdriver (Dataflow runner, automatically)

Three metric types:
  Counter     → monotonically increasing integer count
  Distribution → min / max / sum / count of a numeric distribution
  Gauge        → snapshot of a value at a point in time (latest wins)

Metrics are scoped by (namespace, name). Use a consistent namespace
per pipeline or team (e.g., "my_pipeline" or "data_eng").

Run:
  python Intermediate/07_metrics.py
"""

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsResults


NAMESPACE = "ecommerce_pipeline"


class ValidateAndEnrichDoFn(beam.DoFn):
    """
    DoFn that instruments every meaningful code path with metrics.
    Counters track volume; distributions track value skew; gauge tracks state.
    """

    def __init__(self):
        # ── Counters ─────────────────────────────────────────────────
        # Use counters for anything you'd COUNT in a SQL WHERE clause.
        self.records_processed = Metrics.counter(NAMESPACE, "records_processed")
        self.records_valid      = Metrics.counter(NAMESPACE, "records_valid")
        self.records_invalid    = Metrics.counter(NAMESPACE, "records_invalid")
        self.null_amount        = Metrics.counter(NAMESPACE, "null_amount")
        self.high_value_orders  = Metrics.counter(NAMESPACE, "high_value_orders")

        # ── Distributions ─────────────────────────────────────────────
        # Distribution tracks: count, sum, min, max of a numeric series.
        # Perfect for latency, dollar amounts, record sizes.
        self.amount_distribution = Metrics.distribution(NAMESPACE, "order_amount_usd")

        # ── Gauge ──────────────────────────────────────────────────────
        # Gauge stores the LATEST value set. Useful for "current batch size"
        # or "last seen timestamp". Not suitable for aggregates.
        self.last_order_amount = Metrics.gauge(NAMESPACE, "last_order_amount_cents")

    def process(self, element):
        self.records_processed.inc()

        # Validate
        if element.get("amount") is None:
            self.null_amount.inc()
            self.records_invalid.inc()
            return  # Drop invalid record

        amount = float(element["amount"])
        self.records_valid.inc()

        # Track distribution of order amounts
        self.amount_distribution.update(int(amount))  # Distribution takes int

        # Track gauge (last seen value)
        self.last_order_amount.set(int(amount * 100))  # in cents

        # Count high-value orders separately
        if amount > 500:
            self.high_value_orders.inc()

        yield {**element, "amount": amount, "processed": True}


def print_metrics(result):
    """Reads metrics from the pipeline result and prints them."""
    query = beam.metrics.MetricsFilter().with_namespace(NAMESPACE)
    metrics = result.metrics().query(query)

    print("\n=== Pipeline Metrics ===")

    print("\n-- Counters --")
    for counter in metrics["counters"]:
        print(f"  {counter.key.metric.name}: {counter.committed}")

    print("\n-- Distributions --")
    for dist in metrics["distributions"]:
        d = dist.committed
        print(f"  {dist.key.metric.name}: "
              f"count={d.count} sum={d.sum} min={d.min} max={d.max}")

    print("\n-- Gauges --")
    for gauge in metrics["gauges"]:
        print(f"  {gauge.key.metric.name}: {gauge.committed.value} "
              f"(at {gauge.committed.timestamp})")


def run():
    orders = [
        {"order_id": "o1", "amount": 120.0},
        {"order_id": "o2", "amount": None},       # invalid
        {"order_id": "o3", "amount": 750.0},      # high value
        {"order_id": "o4", "amount": 45.0},
        {"order_id": "o5", "amount": 1200.0},     # high value
        {"order_id": "o6", "amount": 320.0},
        {"order_id": "o7", "amount": None},       # invalid
    ]

    # Use run() not the context manager so we can access the result object
    pipeline = beam.Pipeline()

    processed = (
        pipeline
        | "CreateOrders"  >> beam.Create(orders)
        | "ValidateEnrich">> beam.ParDo(ValidateAndEnrichDoFn())
    )
    processed | "Print" >> beam.Map(print)

    result = pipeline.run()
    result.wait_until_finish()

    print_metrics(result)


if __name__ == "__main__":
    run()
