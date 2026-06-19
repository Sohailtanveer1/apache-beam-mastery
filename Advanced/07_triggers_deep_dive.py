"""
Advanced 07 — Triggers Deep Dive

Source: https://beam.apache.org/documentation/programming-guide/#triggers

Triggers control WHEN Beam emits the aggregated results of a window.
Without a trigger, Beam fires once when the watermark passes the window end.
Triggers let you fire earlier (speculatively) or later (for late data).

Trigger types:
  AfterWatermark        → fire when watermark passes window end (default)
  AfterProcessingTime   → fire after N seconds of processing time
  AfterCount            → fire after N elements arrive
  AfterAny / AfterAll   → composite: OR / AND of sub-triggers
  Repeatedly            → keep firing every time the sub-trigger fires
  OrFinally             → fire repeatedly until a termination condition

Accumulation modes (what happens to past pane data on re-fire):
  ACCUMULATING → each pane contains ALL data seen so far (default for streaming)
  DISCARDING   → each pane contains only NEW data since the last pane

Note: Triggers only matter on unbounded (streaming) PCollections or
      bounded PCollections with explicit timestamps. The DirectRunner
      does NOT fully simulate streaming trigger behavior — use
      DataflowRunner or TestStream for accurate testing.

Run (conceptual demo with TestStream):
  python Advanced/07_triggers_deep_dive.py
"""

import apache_beam as beam
from apache_beam import window
from apache_beam.transforms import trigger
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.window import TimestampedValue


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build a TestStream to simulate an event-time stream
# TestStream lets you inject elements AND advance the watermark in tests.
# ─────────────────────────────────────────────────────────────────────────────

def make_test_stream():
    """
    Simulates an event stream with a 60-second window.
    Events arrive at t=5, t=15, t=25 (early), then watermark advances past
    window end at t=65, then a late element arrives at t=30.
    """
    return (
        TestStream()
        .advance_watermark_to(0)
        .add_elements([TimestampedValue(("click", 1), 5)])
        .add_elements([TimestampedValue(("click", 1), 15)])
        .advance_processing_time(15)   # advance processing clock 15s
        .add_elements([TimestampedValue(("click", 1), 25)])
        .advance_watermark_to(65)      # watermark passes window [0, 60)
        .add_elements([TimestampedValue(("click", 1), 30)])  # LATE element
        .advance_watermark_to_infinity()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Trigger Recipes
# ─────────────────────────────────────────────────────────────────────────────

def trigger_after_watermark_with_early_and_late():
    """
    THE most common production trigger pattern.

    - Early:    fire a speculative result every 10s of processing time
    - On-time:  fire when watermark passes the window end
    - Late:     fire on each late element (accumulating all data so far)
    """
    return beam.WindowInto(
        window.FixedWindows(60),
        trigger=trigger.AfterWatermark(
            early=trigger.AfterProcessingTime(10),
            late=trigger.AfterCount(1),
        ),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
        allowed_lateness=beam.transforms.window.Duration(seconds=30),
    )


def trigger_after_count():
    """
    Fire every time 5 elements arrive in the window.
    Useful for micro-batching: trade latency for throughput.
    """
    return beam.WindowInto(
        window.GlobalWindows(),
        trigger=trigger.Repeatedly(trigger.AfterCount(5)),
        accumulation_mode=trigger.AccumulationMode.DISCARDING,
    )


def trigger_after_processing_time():
    """
    Fire every 30 seconds of wall-clock time, regardless of watermark.
    Use when you need regular heartbeat outputs (dashboards, monitoring).
    """
    return beam.WindowInto(
        window.GlobalWindows(),
        trigger=trigger.Repeatedly(trigger.AfterProcessingTime(30)),
        accumulation_mode=trigger.AccumulationMode.DISCARDING,
    )


def trigger_or_finally():
    """
    Fire repeatedly every 10 seconds OR stop when 100 elements arrive
    (whichever comes first). OrFinally is a "circuit breaker" pattern.
    """
    return beam.WindowInto(
        window.GlobalWindows(),
        trigger=trigger.OrFinally(
            trigger.Repeatedly(trigger.AfterProcessingTime(10)),
            trigger.AfterCount(100),
        ),
        accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Accumulation mode comparison
# ─────────────────────────────────────────────────────────────────────────────

ACCUM_COMMENT = """
ACCUMULATING vs DISCARDING — which to choose:

  ACCUMULATING:
    Pane 1 (early): count=2   ← 2 elements arrived
    Pane 2 (on-time): count=3 ← ALL 3 elements (includes the 2 from pane 1)
    ✓ Use when downstream replaces the old result (e.g. UPDATE in BigQuery)
    ✗ Causes double-counting if downstream appends (e.g. INSERT in BigQuery)

  DISCARDING:
    Pane 1 (early): count=2   ← 2 new elements
    Pane 2 (on-time): count=1 ← only the 1 NEW element since last fire
    ✓ Use when downstream accumulates (e.g. append-only BigQuery inserts)
    ✗ Requires downstream to sum across panes for a complete aggregate
"""


# ─────────────────────────────────────────────────────────────────────────────
# Runnable demo using TestStream
# ─────────────────────────────────────────────────────────────────────────────

class AddWindowAndPaneInfoDoFn(beam.DoFn):
    def process(self, element,
                window=beam.DoFn.WindowParam,
                pane_info=beam.DoFn.PaneInfoParam):
        key, count = element
        yield {
            "key":        key,
            "count":      count,
            "window":     f"[{window.start}–{window.end})",
            "is_early":   pane_info.is_early,
            "is_on_time": pane_info.is_on_time,
            "is_late":    pane_info.is_late,
        }


def run():
    print(ACCUM_COMMENT)
    print("=== AfterWatermark with early+late trigger (TestStream) ===\n")

    options = beam.options.pipeline_options.PipelineOptions(
        streaming=True,
    )

    with beam.Pipeline(options=options) as p:
        stream = p | "TestStream" >> make_test_stream()

        (
            stream
            | "Window" >> trigger_after_watermark_with_early_and_late()
            | "Count"  >> beam.CombinePerKey(sum)
            | "Info"   >> beam.ParDo(AddWindowAndPaneInfoDoFn())
            | "Print"  >> beam.Map(print)
        )

    print("\n=== Trigger reference table printed above; see comments for recipes ===")


if __name__ == "__main__":
    run()
