"""
Advanced 03 — Custom DoFn Patterns

Concepts:
  - DoFn lifecycle: setup(), start_bundle(), process(), finish_bundle(), teardown()
  - Multiple outputs with TaggedOutputs
  - yield vs return in DoFn.process()
  - Initializing expensive resources (DB connections, ML models) once per worker

Run:
  python Advanced/03_custom_dofn_patterns.py
"""

import apache_beam as beam
from apache_beam import pvalue


# ── Output tags for multi-output DoFns ───────────────────────────────────
VALID_TAG   = "valid"
INVALID_TAG = "invalid"
VIP_TAG     = "vip"


class ValidateAndRouteDoFn(beam.DoFn):
    """
    Validates transaction records and routes them to three output PCollections:
      - valid   → well-formed transactions
      - invalid → malformed / missing required fields
      - vip     → valid AND amount > 1000 (duplicate of valid, additional tag)

    Multiple outputs are the preferred Beam pattern for routing / forking.
    They avoid re-scanning the same data in multiple Filter steps.
    """

    REQUIRED_FIELDS = {"user_id", "amount", "currency"}

    def process(self, element):
        missing = self.REQUIRED_FIELDS - set(element.keys())
        if missing:
            yield pvalue.TaggedOutput(INVALID_TAG, {
                **element,
                "error": f"Missing fields: {missing}",
            })
            return

        try:
            amount = float(element["amount"])
        except (ValueError, TypeError):
            yield pvalue.TaggedOutput(INVALID_TAG, {
                **element,
                "error": f"Non-numeric amount: {element['amount']}",
            })
            return

        valid_row = {**element, "amount": amount}
        yield valid_row  # Default (main) output

        if amount > 1000:
            yield pvalue.TaggedOutput(VIP_TAG, valid_row)


class ExpensiveInitDoFn(beam.DoFn):
    """
    Demonstrates proper resource lifecycle management.

    setup()        → called once per worker process (initialize DB pool, load model)
    start_bundle() → called at the start of each bundle (reset counters)
    process()      → called for each element
    finish_bundle()→ called at the end of each bundle (flush buffers)
    teardown()     → called once per worker (clean up resources)
    """

    def setup(self):
        # Called once when the worker starts up.
        # Load large models, open connection pools HERE — not in __init__.
        self._model = None  # Placeholder: self._model = load_model("gs://...")
        self._batch_buffer = []
        print("[setup] Worker initialized")

    def start_bundle(self):
        self._batch_buffer = []

    def process(self, element):
        self._batch_buffer.append(element)
        # Yield immediately; real pattern would batch API calls.
        yield {**element, "processed": True}

    def finish_bundle(self):
        # Flush any buffered writes/API calls here.
        print(f"[finish_bundle] Processed {len(self._batch_buffer)} elements")
        self._batch_buffer = []

    def teardown(self):
        print("[teardown] Worker shutting down")


def run():
    transactions = [
        {"user_id": "u1", "amount": "250.0",   "currency": "USD"},
        {"user_id": "u2", "amount": "1500.0",  "currency": "EUR"},
        {"user_id": "u3", "amount": "not_num", "currency": "GBP"},
        {"user_id": "u4",                       "currency": "USD"},  # missing amount
        {"user_id": "u5", "amount": "75.0",    "currency": "USD"},
    ]

    with beam.Pipeline() as p:
        all_txns = p | "CreateTxns" >> beam.Create(transactions)

        results = all_txns | "ValidateRoute" >> beam.ParDo(
            ValidateAndRouteDoFn()
        ).with_outputs(VALID_TAG, INVALID_TAG, VIP_TAG, main=VALID_TAG)

        valid   = results[VALID_TAG]
        invalid = results[INVALID_TAG]
        vip     = results[VIP_TAG]

        valid   | "PrintValid"   >> beam.Map(lambda r: print(f"[VALID]   {r}"))
        invalid | "PrintInvalid" >> beam.Map(lambda r: print(f"[INVALID] {r}"))
        vip     | "PrintVIP"     >> beam.Map(lambda r: print(f"[VIP]     {r}"))


if __name__ == "__main__":
    run()
