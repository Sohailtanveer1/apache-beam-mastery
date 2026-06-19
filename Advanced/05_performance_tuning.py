"""
Advanced 05 — Performance Tuning on Google Cloud Dataflow

Concepts & Techniques:
  1. Reshuffle          → Break fusion, improve parallelism
  2. combinePerKey      → Partial aggregation (reducer-side combine)
  3. Batching API calls → Reduce external service overhead
  4. Avoid hot keys     → Salting technique for skewed data
  5. Efficient coders   → AvroRecord, FastAvro, custom coders
  6. Resource hints     → Annotate stages with CPU/memory requirements

Run locally (DirectRunner):
  python Advanced/05_performance_tuning.py

Run on Dataflow:
  python Advanced/05_performance_tuning.py \
    --runner=DataflowRunner \
    --project=YOUR_PROJECT \
    --region=us-central1 \
    --temp_location=gs://YOUR_BUCKET/temp \
    --max_num_workers=50 \
    --machine_type=n1-standard-8 \
    --enable_streaming_engine
"""

import random
import apache_beam as beam
from apache_beam import window
from apache_beam.transforms import trigger


# ── 1. Reshuffle: Break Pipeline Fusion ──────────────────────────────────
# Dataflow fuses (chains) consecutive transforms into a single stage for
# efficiency. Sometimes fusion prevents optimal parallelism. Reshuffle
# breaks fusion and redistributes elements across workers.

def demo_reshuffle():
    """Use Reshuffle after a large Read to allow more workers to pick up work."""
    print("=== Reshuffle Demo ===")
    with beam.Pipeline() as p:
        (
            p
            | "CreateLargeData" >> beam.Create(range(1000))
            # Without Reshuffle, Beam may fuse Read+Transform into one stage.
            | "BreakFusion" >> beam.Reshuffle()
            | "HeavyTransform" >> beam.Map(lambda x: x * x)
            | "Count" >> beam.combiners.Count.Globally()
            | "Print" >> beam.Map(print)
        )


# ── 2. Hot Key Salting ────────────────────────────────────────────────────
# When one key has far more records than others (e.g., "USA" vs "Andorra"),
# a single worker handles all of them → bottleneck. Salting adds a random
# suffix to spread work, then removes it in a second aggregation pass.

NUM_SALT_BUCKETS = 10


def add_salt(element):
    key, value = element
    salt = random.randint(0, NUM_SALT_BUCKETS - 1)
    return (f"{key}_{salt}", value)


def remove_salt(element):
    salted_key, count = element
    original_key = salted_key.rsplit("_", 1)[0]
    return (original_key, count)


def demo_hot_key_salting():
    """Count items by country, using salting to handle the hot 'US' key."""
    print("\n=== Hot Key Salting Demo ===")
    # Simulated: 90% of records are US
    data = [("US", 1)] * 90 + [("UK", 1)] * 5 + [("DE", 1)] * 5

    with beam.Pipeline() as p:
        (
            p
            | "CreateData"     >> beam.Create(data)
            | "AddSalt"        >> beam.Map(add_salt)
            | "PartialCombine" >> beam.CombinePerKey(sum)
            | "RemoveSalt"     >> beam.Map(remove_salt)
            | "FinalCombine"   >> beam.CombinePerKey(sum)
            | "Print"          >> beam.Map(print)
        )


# ── 3. Batching External API Calls ────────────────────────────────────────
# Never call an external API element-by-element in process().
# Accumulate elements in a bundle and batch-call the API in finish_bundle().

class BatchAPICallDoFn(beam.DoFn):
    """
    Batches elements within a bundle and makes a single API call per batch.
    Reduces API round-trips from O(n) to O(n / batch_size).
    """

    BATCH_SIZE = 100

    def start_bundle(self):
        self._buffer = []

    def process(self, element):
        self._buffer.append(element)
        if len(self._buffer) >= self.BATCH_SIZE:
            yield from self._flush()

    def finish_bundle(self):
        if self._buffer:
            yield from self._flush()

    def _flush(self):
        # Replace with actual batch API call (e.g., BigQuery insertAll, enrichment API)
        results = [{"input": item, "enriched": True} for item in self._buffer]
        self._buffer = []
        for r in results:
            yield beam.utils.windowed_value.WindowedValue(
                r,
                timestamp=0,
                windows=[window.GlobalWindow()],
            )


# ── 4. Triggering Strategy for Streaming ─────────────────────────────────
# Default: wait for watermark. For faster results use early/speculative firings.

def demo_triggering():
    """
    AfterWatermark with early firings:
    - Fire a speculative result every 30 seconds of processing time (early)
    - Fire the final result when watermark passes the window end (on-time)
    - Fire on any late data (late)
    """
    print("\n=== Trigger Strategy (conceptual — requires streaming runner) ===")
    print(
        "beam.WindowInto(\n"
        "    window.FixedWindows(60),\n"
        "    trigger=trigger.AfterWatermark(\n"
        "        early=trigger.AfterProcessingTime(30),  # speculative every 30s\n"
        "        late=trigger.AfterCount(1),             # fire on each late element\n"
        "    ),\n"
        "    accumulation_mode=trigger.AccumulationMode.ACCUMULATING,\n"
        ")"
    )


if __name__ == "__main__":
    demo_reshuffle()
    demo_hot_key_salting()
    demo_triggering()
