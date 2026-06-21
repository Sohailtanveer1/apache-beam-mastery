"""
Beginner 07 — Element Timestamps, Watermarks, and Late Data

Source: Beam Programming Guide §3.2.6, §8.4, §8.5

Key concepts:
  - Every PCollection element has an intrinsic timestamp.
  - Unbounded sources (Pub/Sub, Kafka) assign timestamps automatically.
  - Bounded sources (TextIO, Create) assign the same timestamp to every element.
    You must assign timestamps manually when you need time-based windowing on
    batch data.
  - Watermark = the system's estimate of "all data up to time T has arrived".
  - Late data = elements whose timestamp falls behind the watermark when they arrive.
  - allowed_lateness = how long after the watermark to still accept late elements.

Run:
  python Beginner/07_timestamps_and_watermarks.py
"""

import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.window import TimestampedValue


# ── Pattern 1: Assign timestamps from a field in the data ─────────────────
# Use this when your batch records have an embedded time field (e.g., log lines).

class AddTimestampFromFieldDoFn(beam.DoFn):
    """
    Reads the 'event_time' (Unix seconds) from each record and attaches it
    as the element's Beam timestamp using TimestampedValue.

    Without this, all elements in a bounded PCollection share the same
    default timestamp, so they all land in one global window.
    """

    def process(self, element):
        ts = element["event_time"]          # Unix epoch seconds
        yield TimestampedValue(element, ts)


# ── Pattern 2: Inspect the timestamp inside a DoFn ────────────────────────

class InspectTimestampDoFn(beam.DoFn):
    """
    beam.DoFn.TimestampParam injects the element's assigned timestamp.
    beam.DoFn.WindowParam injects the window the element falls into.
    """

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        window=beam.DoFn.WindowParam,
    ):
        yield {
            **element,
            "beam_ts_seconds": float(timestamp),
            "window_start":    float(window.start),
            "window_end":      float(window.end),
        }


def run():
    # Simulated log events with embedded Unix timestamps.
    # t=0  → 1970-01-01T00:00:00Z (epoch start, for simplicity)
    events = [
        {"user": "alice", "action": "buy",    "event_time": 5},
        {"user": "bob",   "action": "view",   "event_time": 10},
        {"user": "carol", "action": "buy",    "event_time": 65},   # different window
        {"user": "alice", "action": "view",   "event_time": 70},   # different window
        {"user": "bob",   "action": "buy",    "event_time": 30},   # same window as t=5,10
    ]

    with beam.Pipeline() as p:

        # ── Step 1: Assign timestamps from the 'event_time' field ─────
        timestamped = (
            p
            | "Create"        >> beam.Create(events)
            | "AddTimestamps" >> beam.ParDo(AddTimestampFromFieldDoFn())
        )

        # ── Step 2: Apply a 60-second Fixed Window ────────────────────
        # Now that elements have meaningful timestamps, windowing works correctly.
        # Without AddTimestampFromFieldDoFn, all elements would land in window [0, 60).
        windowed = (
            timestamped
            | "FixedWindow" >> beam.WindowInto(
                window.FixedWindows(60),
                # Allow elements up to 30 seconds late.
                # Late elements are processed rather than dropped.
                allowed_lateness=beam.transforms.window.Duration(seconds=30),
            )
        )

        # ── Step 3: Inspect timestamps and window info ────────────────
        enriched = windowed | "InspectTS" >> beam.ParDo(InspectTimestampDoFn())

        # ── Step 4: Count events per window ──────────────────────────
        counts = (
            enriched
            | "PairWithOne"   >> beam.Map(lambda e: (e["window_start"], 1))
            | "SumPerWindow"  >> beam.CombinePerKey(sum)
            | "FormatOutput"  >> beam.Map(
                lambda kv: f"Window starting at t={kv[0]:.0f}s → {kv[1]} events"
            )
        )

        enriched | "PrintEnriched" >> beam.Map(print)
        print("\n=== Window event counts ===")
        counts  | "PrintCounts"   >> beam.Map(print)


if __name__ == "__main__":
    run()
