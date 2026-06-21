"""
Intermediate 09 — Advanced CombineFn: Full Lifecycle and Patterns

Source: Beam Programming Guide §4.2.4

A CombineFn is the right tool when:
  - Your aggregation requires a custom accumulator (not a simple sum/max).
  - You need pre/post processing around the aggregation.
  - Your output type differs from the input type (e.g., inputs are numbers,
    output is a dict with mean + stddev).
  - You want Beam to apply combiner lifting (partial aggregation before shuffle),
    which significantly reduces data shuffled across the network.

The 5 lifecycle methods:
  create_accumulator()       → returns a fresh empty accumulator
  add_input(acc, element)    → adds one element into the accumulator
  merge_accumulators([accs]) → merges multiple partial accumulators into one
  extract_output(acc)        → computes the final result from the accumulator
  compact(acc)               → (optional) compresses the accumulator before transfer

Run:
  python Intermediate/09_combine_fn_advanced.py
"""

import math
import apache_beam as beam


# ── CombineFn 1: Mean average (the canonical example) ─────────────────────

class MeanCombineFn(beam.CombineFn):
    """
    Computes the arithmetic mean of numeric values.
    Accumulator = (running_sum, count)
    """

    def create_accumulator(self):
        return (0.0, 0)          # (sum, count)

    def add_input(self, acc, element):
        total, count = acc
        return (total + element, count + 1)

    def merge_accumulators(self, accumulators):
        totals, counts = zip(*accumulators)
        return (sum(totals), sum(counts))

    def extract_output(self, acc):
        total, count = acc
        return total / count if count else float("nan")

    def compact(self, acc):
        # compact() is called before transferring an accumulator across the wire.
        # In this case there's nothing to compress, so we return it unchanged.
        return acc


# ── CombineFn 2: Stats (mean + std dev + count) ───────────────────────────

class StatsCombineFn(beam.CombineFn):
    """
    Single-pass computation of mean, variance, and standard deviation.
    Uses Welford's online algorithm — numerically stable and efficient.

    Accumulator = {"n": int, "mean": float, "M2": float}
    """

    def create_accumulator(self):
        return {"n": 0, "mean": 0.0, "M2": 0.0}

    def add_input(self, acc, element):
        n    = acc["n"] + 1
        delta = element - acc["mean"]
        mean  = acc["mean"] + delta / n
        M2    = acc["M2"] + delta * (element - mean)
        return {"n": n, "mean": mean, "M2": M2}

    def merge_accumulators(self, accumulators):
        # Parallel variant of Welford's algorithm for merging partial stats.
        combined = {"n": 0, "mean": 0.0, "M2": 0.0}
        for acc in accumulators:
            n1, n2 = combined["n"], acc["n"]
            if n2 == 0:
                continue
            n      = n1 + n2
            delta  = acc["mean"] - combined["mean"]
            mean   = (combined["mean"] * n1 + acc["mean"] * n2) / n
            M2     = combined["M2"] + acc["M2"] + delta ** 2 * n1 * n2 / n
            combined = {"n": n, "mean": mean, "M2": M2}
        return combined

    def extract_output(self, acc):
        n = acc["n"]
        if n < 2:
            return {"count": n, "mean": acc["mean"], "stddev": 0.0}
        variance = acc["M2"] / (n - 1)
        return {
            "count":  n,
            "mean":   round(acc["mean"], 4),
            "stddev": round(math.sqrt(variance), 4),
        }


# ── CombineFn 3: BoundedSum (the simple function example from the guide) ──

def bounded_sum(values, bound=500):
    """
    Simple combine function (not a CombineFn subclass).
    Use with CombineGlobally for simple one-liners.
    """
    return min(sum(values), bound)


def run():
    scores_by_player = [
        ("alice", 95.0), ("alice", 87.0), ("alice", 91.0),
        ("bob",   70.0), ("bob",   82.0),
        ("carol", 60.0), ("carol", 55.0), ("carol", 75.0), ("carol", 88.0),
    ]
    all_scores = [s for _, s in scores_by_player]

    print("=== Mean per player (CombinePerKey) ===")
    with beam.Pipeline() as p:
        (
            p
            | "CreateScores" >> beam.Create(scores_by_player)
            | "MeanPerPlayer" >> beam.CombinePerKey(MeanCombineFn())
            | "Print" >> beam.Map(lambda kv: print(f"  {kv[0]}: mean={kv[1]:.2f}"))
        )

    print("\n=== Global stats (CombineGlobally) ===")
    with beam.Pipeline() as p:
        (
            p
            | "CreateAll"    >> beam.Create(all_scores)
            | "GlobalStats"  >> beam.CombineGlobally(StatsCombineFn())
            | "Print"        >> beam.Map(print)
        )

    print("\n=== Stats per player (CombinePerKey with StatsCombineFn) ===")
    with beam.Pipeline() as p:
        (
            p
            | "CreateScores2"  >> beam.Create(scores_by_player)
            | "StatsPerPlayer" >> beam.CombinePerKey(StatsCombineFn())
            | "Print"          >> beam.Map(lambda kv: print(f"  {kv[0]}: {kv[1]}"))
        )

    print("\n=== BoundedSum with and without bound ===")
    with beam.Pipeline() as p:
        pc = p | "CreateNumbers" >> beam.Create([1, 10, 100, 1000])
        (
            pc
            | "DefaultBound" >> beam.CombineGlobally(bounded_sum)
            | "PrintDefault" >> beam.Map(lambda x: print(f"  bounded(500): {x}"))
        )
        (
            pc
            | "LargeBound" >> beam.CombineGlobally(bounded_sum, bound=5000)
            | "PrintLarge" >> beam.Map(lambda x: print(f"  bounded(5000): {x}"))
        )

    print("\n=== CombineGlobally with .without_defaults() ===")
    # without_defaults() returns empty PCollection when input is empty,
    # instead of the accumulator's zero value.
    with beam.Pipeline() as p:
        empty_pc = p | "CreateEmpty" >> beam.Create([])
        (
            empty_pc
            | "SumNoDefault"   >> beam.CombineGlobally(sum).without_defaults()
            | "PrintNoDefault" >> beam.Map(lambda x: print(f"  sum of empty: {x}"))
        )
        # ^ Prints nothing, because the PCollection is empty.


if __name__ == "__main__":
    run()
