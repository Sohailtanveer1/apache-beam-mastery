"""
Advanced 09 — State & Timers: Real-World Examples

Source: Beam Programming Guide §11.5.1, §11.5.2, §11.5.3

Three production-grade patterns directly from the Beam guide:

  1. Joining Clicks and Views (§11.5.1)
     - Two input streams keyed by link ID
     - Wait up to 1 hour (event-time) for both events to arrive
     - Emit joined result; GC after 1 hour of inactivity

  2. Batching RPCs (§11.5.2)
     - Buffer events per user key for 10 seconds (processing-time timer)
     - Send one batched RPC per timer fire instead of one per element
     - Guards against double-setting the timer with an IS_TIMER_SET flag

  3. Looping Timers (§11.5.3)
     - Timer sets another timer on every fire → periodic heartbeat per key
     - Pattern for "emit something every N seconds even when no data arrives"
     - Note: Python SDK does not support drain detection in OnTimer

Run:
  python Advanced/09_state_timers_real_examples.py
"""

import apache_beam as beam
from apache_beam import window, coders
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    CombiningValueStateSpec,
    BagStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.utils.timestamp import Timestamp, Duration
from apache_beam.transforms.window import TimestampedValue


# ─────────────────────────────────────────────────────────────────────────────
# Example 1: Click-View Stream Join (§11.5.1)
# ─────────────────────────────────────────────────────────────────────────────

class ClickViewJoinDoFn(beam.DoFn):
    """
    Joins a 'view' event with a 'click' event on the same link_id.
    - Waits up to 1 hour of event-time for both sides to arrive.
    - Input elements: (link_id, {"type": "view"|"click", ...})
    - Both view and click may arrive in any order.
    - GC timer clears incomplete joins after 1 hour of inactivity.
    """

    VIEW_STATE  = ReadModifyWriteStateSpec("view",  coders.BytesCoder())
    CLICK_STATE = ReadModifyWriteStateSpec("click", coders.BytesCoder())
    MAX_TS      = CombiningValueStateSpec("max_ts", combine_fn=max)
    GC_TIMER    = TimerSpec("gc", TimeDomain.WATERMARK)

    TIMEOUT_SECS = 3600  # 1 hour

    def process(
        self,
        element,
        ts=beam.DoFn.TimestampParam,
        view=beam.DoFn.StateParam(VIEW_STATE),
        click=beam.DoFn.StateParam(CLICK_STATE),
        max_ts=beam.DoFn.StateParam(MAX_TS),
        gc=beam.DoFn.TimerParam(GC_TIMER),
    ):
        link_id, event = element
        import json

        # Store each side of the join in its own state cell.
        if event["type"] == "view":
            view.write(json.dumps(event).encode())
        else:
            click.write(json.dumps(event).encode())

        # Advance the GC timer: 1 hour after the latest event seen.
        max_ts.add(float(ts))
        expiry = Timestamp(micros=int(max_ts.read() * 1e6)) + Duration(
            seconds=self.TIMEOUT_SECS
        )
        gc.set(expiry)

        # If both sides are now present, emit the join and clear state.
        v_raw = view.read()
        c_raw = click.read()
        if v_raw and c_raw:
            v = json.loads(v_raw.decode())
            c = json.loads(c_raw.decode())
            yield (link_id, {"view": v, "click": c, "joined": True})
            view.clear()
            click.clear()
            max_ts.clear()

    @on_timer(GC_TIMER)
    def gc_callback(
        self,
        view=beam.DoFn.StateParam(VIEW_STATE),
        click=beam.DoFn.StateParam(CLICK_STATE),
        max_ts=beam.DoFn.StateParam(MAX_TS),
        key=beam.DoFn.KeyParam,
    ):
        # Emit an incomplete-join record so the event isn't silently dropped.
        v_raw = view.read()
        c_raw = click.read()
        yield (key, {
            "joined": False,
            "reason": "timeout",
            "had_view":  v_raw is not None,
            "had_click": c_raw is not None,
        })
        view.clear()
        click.clear()
        max_ts.clear()


# ─────────────────────────────────────────────────────────────────────────────
# Example 2: Batching RPCs (§11.5.2)
# ─────────────────────────────────────────────────────────────────────────────

def mock_rpc(batch):
    """Simulates an external RPC call that accepts a batch of events."""
    return f"RPC({len(batch)} events: {batch})"


class BatchRPCDoFn(beam.DoFn):
    """
    Buffers events per user key and sends a batched RPC every 10 seconds
    of processing time (wall-clock).

    Key design points from §11.5.2:
    - IS_TIMER_SET prevents setting the timer multiple times unnecessarily.
    - Buffer is cleared after each RPC send.
    - The timer fires the RPC; new events can accumulate immediately after.
    """

    BUFFER       = BagStateSpec("buffer",       coders.StrUtf8Coder())
    IS_TIMER_SET = ReadModifyWriteStateSpec("is_timer_set", coders.BooleanCoder())
    OUTPUT_TIMER = TimerSpec("output", TimeDomain.REAL_TIME)

    BATCH_WINDOW_SECS = 10

    def process(
        self,
        element,
        buffer=beam.DoFn.StateParam(BUFFER),
        is_timer_set=beam.DoFn.StateParam(IS_TIMER_SET),
        timer=beam.DoFn.TimerParam(OUTPUT_TIMER),
    ):
        _, event = element
        buffer.add(event)

        # Only set the timer once; let it batch everything in the window.
        if not is_timer_set.read():
            timer.set(Timestamp.now() + Duration(seconds=self.BATCH_WINDOW_SECS))
            is_timer_set.write(True)

    @on_timer(OUTPUT_TIMER)
    def output_callback(
        self,
        buffer=beam.DoFn.StateParam(BUFFER),
        is_timer_set=beam.DoFn.StateParam(IS_TIMER_SET),
    ):
        batch = list(buffer.read())
        if batch:
            yield mock_rpc(batch)
        buffer.clear()
        is_timer_set.write(False)


# ─────────────────────────────────────────────────────────────────────────────
# Example 3: Looping Timer (§11.5.3)
# ─────────────────────────────────────────────────────────────────────────────

class LoopingHeartbeatDoFn(beam.DoFn):
    """
    Emits a periodic heartbeat (every 60 seconds of event-time) for each key,
    even when no new data arrives.

    Pattern: the timer callback re-sets the timer → infinite loop per key.

    Warning from §11.5.3:
    - Python SDK does not support detecting drain in OnTimer.
    - This loop runs forever and cannot terminate safely during a drain.
    - In production, add a sentinel or max-iteration check if needed.
    """

    TIMER = TimerSpec("heartbeat", TimeDomain.WATERMARK)
    COUNT = CombiningValueStateSpec("heartbeat_count", combine_fn=sum)

    INTERVAL_SECS = 60

    def process(
        self,
        element,
        ts=beam.DoFn.TimestampParam,
        timer=beam.DoFn.TimerParam(TIMER),
    ):
        key, value = element
        timer.set(ts + Duration(seconds=self.INTERVAL_SECS))
        yield (key, f"data_event: {value}")

    @on_timer(TIMER)
    def heartbeat(
        self,
        timestamp=beam.DoFn.TimestampParam,
        key=beam.DoFn.KeyParam,
        count=beam.DoFn.StateParam(COUNT),
        timer=beam.DoFn.TimerParam(TIMER),
    ):
        count.add(1)
        yield (key, f"heartbeat #{count.read()} at t={float(timestamp):.0f}s")
        # Re-arm the timer to create the loop.
        timer.set(timestamp + Duration(seconds=self.INTERVAL_SECS))


def run():
    # ── Example 1: Click-View Join ───────────────────────────────────
    print("=== Click-View Stream Join ===")
    events = [
        ("link-1", {"type": "view",  "page": "home",    "ts": 10}),
        ("link-2", {"type": "click", "product": "shoe", "ts": 12}),
        ("link-1", {"type": "click", "product": "book", "ts": 15}),  # joins with link-1 view
        ("link-3", {"type": "view",  "page": "product", "ts": 20}),
        # link-2 view and link-3 click never arrive → GC timer handles them
    ]
    with beam.Pipeline() as p:
        (
            p
            | "CreateEvents"  >> beam.Create(events)
            | "WindowJoin"    >> beam.WindowInto(window.FixedWindows(3600))
            | "JoinClickView" >> beam.ParDo(ClickViewJoinDoFn())
            | "PrintJoins"    >> beam.Map(print)
        )

    # ── Example 2: Batching RPCs ─────────────────────────────────────
    print("\n=== Batching RPCs ===")
    user_events = [
        ("user1", "page_view"), ("user1", "add_to_cart"),
        ("user2", "search"),    ("user1", "purchase"),
        ("user2", "click"),
    ]
    with beam.Pipeline() as p:
        (
            p
            | "CreateUserEvents" >> beam.Create(user_events)
            | "WindowBatch"      >> beam.WindowInto(window.FixedWindows(3600))
            | "BatchRPC"         >> beam.ParDo(BatchRPCDoFn())
            | "PrintRPC"         >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
