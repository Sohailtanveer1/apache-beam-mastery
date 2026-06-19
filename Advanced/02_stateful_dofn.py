"""
Advanced 02 — Stateful Processing: State API and Timers

Concepts:
  - ReadModifyWriteStateSpec  → Read/write a per-key state cell
  - BagStateSpec              → Accumulate elements in a per-key bag
  - CombiningValueStateSpec   → Combine elements into a single per-key value
  - TimerSpec (event-time)    → Fire a callback after watermark advances past threshold
  - Per-key state is isolated: state for key "alice" is independent of "bob"

Use cases:
  - Session detection without windowing
  - Running aggregations with early/late fire control
  - Deduplication
  - Rate limiting per key

Run:
  python Advanced/02_stateful_dofn.py
"""

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    BagStateSpec,
    CombiningValueStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam import window


# ── Example 1: Running counter per key with event-time timer ─────────────

class RunningCounterDoFn(beam.DoFn):
    """
    Maintains a per-key running count and fires a summary timer
    after the watermark passes the first-event time + 60 seconds.

    State is scoped per key AND per window.
    """

    COUNT_STATE = CombiningValueStateSpec("count", combine_fn=sum)
    TIMER = TimerSpec("summary_timer", TimeDomain.WATERMARK)

    def process(
        self,
        element,
        count_state=beam.DoFn.StateParam(COUNT_STATE),
        timer=beam.DoFn.TimerParam(TIMER),
        timestamp=beam.DoFn.TimestampParam,
    ):
        key, value = element
        count_state.add(1)

        # Set the timer to fire 60s after this element's timestamp.
        # Calling set() multiple times is idempotent — only the latest wins.
        timer.set(timestamp + 60)

        yield (key, f"running_count={count_state.read()}")

    @on_timer(TIMER)
    def on_timer_fn(self, count_state=beam.DoFn.StateParam(COUNT_STATE)):
        yield f"[TIMER] Final count: {count_state.read()}"


# ── Example 2: Deduplication using BagState ───────────────────────────────

class DeduplicateDoFn(beam.DoFn):
    """
    Deduplicates events within a window using a BagState to track seen IDs.

    Note: BagState is an unbounded accumulator — use it only inside
    bounded windows or clear it explicitly.
    """

    SEEN_IDS = BagStateSpec("seen_ids", coders.StrUtf8Coder())

    def process(self, element, seen_state=beam.DoFn.StateParam(SEEN_IDS)):
        event_id, payload = element
        seen = set(seen_state.read())

        if event_id in seen:
            return  # Drop duplicate

        seen_state.add(event_id)
        yield (event_id, payload)


# ── Example 3: Threshold alerting per sensor ─────────────────────────────

class ThresholdAlertDoFn(beam.DoFn):
    """
    Emits an alert when a sensor's running average exceeds a threshold.
    Tracks sum and count separately to compute a true running average.
    """

    SUM_STATE   = CombiningValueStateSpec("sum",   combine_fn=sum)
    COUNT_STATE = CombiningValueStateSpec("count", combine_fn=sum)

    THRESHOLD = 75.0

    def process(
        self,
        element,
        sum_state=beam.DoFn.StateParam(SUM_STATE),
        count_state=beam.DoFn.StateParam(COUNT_STATE),
    ):
        sensor_id, reading = element
        sum_state.add(reading)
        count_state.add(1)

        total   = sum_state.read()
        n       = count_state.read()
        average = total / n

        yield {
            "sensor_id": sensor_id,
            "reading":   reading,
            "avg":       round(average, 2),
            "n":         n,
            "alert":     average > self.THRESHOLD,
        }


def run():
    events = [
        ("s1", 60.0), ("s1", 80.0), ("s1", 90.0), ("s1", 70.0),
        ("s2", 40.0), ("s2", 45.0), ("s2", 50.0),
    ]

    dupes = [
        ("e1", "buy"), ("e2", "click"), ("e1", "buy"),   # e1 is a duplicate
        ("e3", "view"), ("e2", "click"),                 # e2 is a duplicate
    ]

    print("=== Threshold Alerting ===")
    with beam.Pipeline() as p:
        results = (
            p
            | "CreateSensorData" >> beam.Create(events)
            | "WindowSensor" >> beam.WindowInto(window.FixedWindows(3600))
            | "ThresholdAlert" >> beam.ParDo(ThresholdAlertDoFn())
        )
        results | "PrintAlerts" >> beam.Map(print)

    print("\n=== Deduplication ===")
    with beam.Pipeline() as p:
        deduped = (
            p
            | "CreateDupes"  >> beam.Create(dupes)
            | "WindowDedup"  >> beam.WindowInto(window.FixedWindows(3600))
            | "Deduplicate"  >> beam.ParDo(DeduplicateDoFn())
        )
        deduped | "PrintDeduped" >> beam.Map(print)


if __name__ == "__main__":
    run()
