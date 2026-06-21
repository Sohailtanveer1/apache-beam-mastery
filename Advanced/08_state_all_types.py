"""
Advanced 08 — All State Types in Apache Beam Python SDK

Source: Beam Programming Guide §11.1, §11.4

State is always:
  - Per-key: each unique key has its own isolated state cells.
  - Per-window: state resets when a new window starts for that key.
  - Scoped to a ParDo transform: different ParDos cannot share state.

State types available in Python SDK:
  ReadModifyWriteStateSpec  → read a value, modify it, write it back (like a variable)
  CombiningValueStateSpec   → efficiently combine inputs (like a CombineFn)
  BagStateSpec              → append-only bag of values (unordered)
  SetStateSpec              → bag of UNIQUE values (deduplication)
  OrderedListStateSpec      → ordered list of timestamped values

Garbage collection strategies (§11.4):
  1. Window-based: state is cleared when the window expires.
  2. Timer-based: set a timer to clear state after a period of inactivity.

Run:
  python Advanced/08_state_all_types.py
"""

import apache_beam as beam
from apache_beam import window
from apache_beam import coders
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    CombiningValueStateSpec,
    BagStateSpec,
    SetStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.utils.timestamp import Timestamp, Duration


# ── 1. ReadModifyWriteState — read a scalar, mutate it, write back ────────
# Guide name: ValueState (Python SDK calls it ReadModifyWriteState)

class RunningCountDoFn(beam.DoFn):
    """
    Counts total elements seen per key using ReadModifyWriteState.
    Use this when you need a simple scalar that you read and overwrite.
    """
    COUNT = ReadModifyWriteStateSpec("count", coders.VarIntCoder())

    def process(self, element, state=beam.DoFn.StateParam(COUNT)):
        key, value = element
        current = state.read() or 0
        state.write(current + 1)
        yield (key, f"count_so_far={current + 1}")


# ── 2. CombiningValueState — efficiently accumulate without reading first ─

class SumWithCombiningStateDoFn(beam.DoFn):
    """
    Accumulates a sum per key using CombiningValueState.
    add() is more efficient than ReadModifyWriteState for commutative ops
    because the runner can partially combine before merging.
    """
    TOTAL = CombiningValueStateSpec("total", combine_fn=sum)

    def process(self, element, state=beam.DoFn.StateParam(TOTAL)):
        key, amount = element
        state.add(amount)
        yield (key, f"running_total={state.read()}")


# ── 3. BagState — append values, read all at once ─────────────────────────

class BufferAndFlushDoFn(beam.DoFn):
    """
    Buffers all values for a key and flushes once a threshold is met.
    BagState is append-only — no read-before-write needed for adds.
    Clear the bag explicitly once you're done with it.
    """
    BUFFER      = BagStateSpec("buffer", coders.VarIntCoder())
    FLUSH_SIZE  = 3

    def process(self, element, state=beam.DoFn.StateParam(BUFFER)):
        key, value = element
        state.add(value)
        buffered = list(state.read())
        if len(buffered) >= self.FLUSH_SIZE:
            yield (key, f"FLUSH: {sorted(buffered)}")
            state.clear()           # Release memory after flush


# ── 4. SetState — accumulate UNIQUE values (deduplication) ───────────────

class UniqueAccumulatorDoFn(beam.DoFn):
    """
    Accumulates unique values per key using SetState.
    Duplicate values are silently ignored (set semantics).
    """
    SEEN = SetStateSpec("seen_values", coders.VarIntCoder())

    def process(self, element, state=beam.DoFn.StateParam(SEEN)):
        key, value = element
        state.add(value)
        unique_so_far = list(state.read())
        yield (key, f"unique_values={sorted(unique_so_far)}")


# ── 5. Timer-based Garbage Collection ─────────────────────────────────────
# State that is never cleared grows unboundedly, degrading performance.
# This pattern sets a timer that fires after N seconds of inactivity
# and clears all state for that key.

class StateWithGCDoFn(beam.DoFn):
    """
    Accumulates events per key, but clears state if the key goes
    inactive for 60 seconds (event-time).

    From §11.4: "Updating a timer that garbage collects state.
    As long as there is activity, the timer is pushed out. Once the key
    goes inactive, the timer fires and state is cleaned up."
    """
    ALL_ELEMENTS   = BagStateSpec("elements", coders.StrUtf8Coder())
    MAX_TIMESTAMP  = CombiningValueStateSpec("max_ts", combine_fn=max)
    GC_TIMER       = TimerSpec("gc_timer", TimeDomain.WATERMARK)

    def process(
        self,
        element,
        ts=beam.DoFn.TimestampParam,
        elements=beam.DoFn.StateParam(ALL_ELEMENTS),
        max_ts=beam.DoFn.StateParam(MAX_TIMESTAMP),
        gc=beam.DoFn.TimerParam(GC_TIMER),
    ):
        key, event = element
        elements.add(event)
        max_ts.add(float(ts))

        # Push the GC timer out by 60s from the latest event timestamp.
        # As long as events keep arriving, state is preserved.
        expiry = Timestamp(micros=int(max_ts.read() * 1e6)) + Duration(seconds=60)
        gc.set(expiry)

        yield (key, f"buffered: {list(elements.read())}")

    @on_timer(GC_TIMER)
    def gc_callback(
        self,
        elements=beam.DoFn.StateParam(ALL_ELEMENTS),
        max_ts=beam.DoFn.StateParam(MAX_TIMESTAMP),
        key=beam.DoFn.KeyParam,
        timestamp=beam.DoFn.TimestampParam,
    ):
        count = len(list(elements.read()))
        elements.clear()
        max_ts.clear()
        yield (key, f"GC fired at t={float(timestamp):.0f}s — cleared {count} elements")


def run():
    keyed_ints = [
        ("alice", 10), ("bob", 20), ("alice", 30),
        ("alice", 40), ("bob",  5), ("alice", 10),
    ]

    print("=== ReadModifyWriteState: Running count ===")
    with beam.Pipeline() as p:
        (
            p
            | "Create1"  >> beam.Create(keyed_ints)
            | "Window1"  >> beam.WindowInto(window.FixedWindows(3600))
            | "Count"    >> beam.ParDo(RunningCountDoFn())
            | "Print1"   >> beam.Map(print)
        )

    print("\n=== CombiningValueState: Running sum ===")
    with beam.Pipeline() as p:
        (
            p
            | "Create2"  >> beam.Create(keyed_ints)
            | "Window2"  >> beam.WindowInto(window.FixedWindows(3600))
            | "Sum"      >> beam.ParDo(SumWithCombiningStateDoFn())
            | "Print2"   >> beam.Map(print)
        )

    print("\n=== BagState: Buffer and flush at size 3 ===")
    with beam.Pipeline() as p:
        (
            p
            | "Create3"   >> beam.Create(keyed_ints)
            | "Window3"   >> beam.WindowInto(window.FixedWindows(3600))
            | "BufFlush"  >> beam.ParDo(BufferAndFlushDoFn())
            | "Print3"    >> beam.Map(print)
        )

    print("\n=== SetState: Unique value accumulation (dedup) ===")
    # Note: alice sends 10 twice — SetState silently deduplicates.
    with beam.Pipeline() as p:
        (
            p
            | "Create4"  >> beam.Create(keyed_ints)
            | "Window4"  >> beam.WindowInto(window.FixedWindows(3600))
            | "Unique"   >> beam.ParDo(UniqueAccumulatorDoFn())
            | "Print4"   >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
