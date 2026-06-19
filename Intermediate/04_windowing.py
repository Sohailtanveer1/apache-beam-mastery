"""
Intermediate 04 — Windowing: Fixed, Sliding, and Session Windows

Concepts:
  - beam.window.FixedWindows    → Tumbling windows of fixed duration
  - beam.window.SlidingWindows  → Overlapping windows (size + period)
  - beam.window.Sessions        → Activity-based windows with a gap threshold
  - Timestamps and watermarks in bounded pipelines
  - beam.WindowInto transform

Note: In a bounded (batch) pipeline we must assign timestamps manually
using beam.window.TimestampedValue. In a real streaming pipeline
(Pub/Sub), timestamps come from the message metadata.

Run:
  python Intermediate/04_windowing.py
"""

import apache_beam as beam
from apache_beam import window
from apache_beam.transforms.window import TimestampedValue
import time

# Simulated event stream: (user_id, event_type, unix_timestamp)
EVENTS = [
    # t=0s  — 00:00
    ("alice", "click",  0),
    ("alice", "view",   2),
    ("bob",   "click",  5),
    # t=60s  — 00:01
    ("alice", "click",  62),
    ("bob",   "view",   65),
    ("carol", "click",  70),
    # t=120s — 00:02
    ("alice", "purchase", 122),
    ("bob",   "click",    125),
    # t=300s — 00:05 (session gap > 60s from alice's last event at 122s)
    ("alice", "click",  300),
    ("carol", "view",   310),
]


def add_timestamps(element):
    """Assign the event timestamp so Beam can apply time-based windowing."""
    user, event_type, ts = element
    return TimestampedValue((user, event_type), ts)


def count_events(kv):
    key, events = kv
    return f"{key}: {len(list(events))} events"


def run():
    with beam.Pipeline() as p:

        timestamped = (
            p
            | "CreateEvents" >> beam.Create(EVENTS)
            | "AddTimestamps" >> beam.Map(add_timestamps)
        )

        # ── Fixed Windows (1-minute tumbling) ───────────────────────────
        print("\n=== Fixed Windows (60s) ===")
        (
            timestamped
            | "FixedWindow" >> beam.WindowInto(window.FixedWindows(60))
            | "KeyByUser_F" >> beam.Map(lambda e: (e[0], 1))
            | "GroupFixed"  >> beam.GroupByKey()
            | "CountFixed"  >> beam.Map(count_events)
            | "PrintFixed"  >> beam.Map(lambda x: print(f"[Fixed] {x}"))
        )

        # ── Sliding Windows (2-minute window, 1-minute slide) ─────────
        # Each event falls into MULTIPLE overlapping windows.
        print("\n=== Sliding Windows (120s window, 60s period) ===")
        (
            timestamped
            | "SlidingWindow" >> beam.WindowInto(window.SlidingWindows(120, 60))
            | "KeyByUser_S"   >> beam.Map(lambda e: (e[0], 1))
            | "GroupSliding"  >> beam.GroupByKey()
            | "CountSliding"  >> beam.Map(count_events)
            | "PrintSliding"  >> beam.Map(lambda x: print(f"[Sliding] {x}"))
        )

        # ── Session Windows (gap duration = 60s) ──────────────────────
        # Windows close after 60 seconds of inactivity per key.
        # alice has two sessions: [0s–122s] and [300s+]
        print("\n=== Session Windows (60s gap) ===")
        (
            timestamped
            | "SessionWindow" >> beam.WindowInto(window.Sessions(60))
            | "KeyByUser_Se"  >> beam.Map(lambda e: (e[0], 1))
            | "GroupSession"  >> beam.GroupByKey()
            | "CountSession"  >> beam.Map(count_events)
            | "PrintSession"  >> beam.Map(lambda x: print(f"[Session] {x}"))
        )


if __name__ == "__main__":
    run()
