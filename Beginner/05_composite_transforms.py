"""
Beginner 05 — Composite Transforms

Source: https://beam.apache.org/documentation/programming-guide/#composite-transforms

A Composite Transform is a reusable PTransform subclass that wraps
a sequence of simpler transforms into one named, testable unit.
This is the primary way to build libraries of Beam building blocks.

Pattern:
  class MyTransform(beam.PTransform):
      def expand(self, pcoll):
          return pcoll | Step1 | Step2 | Step3

Run:
  python Beginner/05_composite_transforms.py
"""

import re
import apache_beam as beam


# ── Composite 1: Reusable word-count transform ────────────────────────────

class CountWords(beam.PTransform):
    """
    Encapsulates tokenization + counting into one reusable unit.
    Usage: pcoll | "CountWords" >> CountWords()
    """

    def expand(self, pcoll):
        return (
            pcoll
            | "Tokenize"    >> beam.FlatMap(lambda line: re.findall(r"[a-zA-Z']+", line.lower()))
            | "PairWithOne" >> beam.Map(lambda w: (w, 1))
            | "SumPerWord"  >> beam.CombinePerKey(sum)
        )


# ── Composite 2: Parameterized composite ─────────────────────────────────

class NormalizeAndFilter(beam.PTransform):
    """
    Strips punctuation, lowercases, and filters by minimum word length.
    Parameterization happens in __init__; expansion in expand().
    """

    def __init__(self, min_length: int = 4):
        super().__init__()
        self.min_length = min_length

    def expand(self, pcoll):
        min_len = self.min_length
        return (
            pcoll
            | "Strip"  >> beam.Map(lambda w: re.sub(r"[^a-z]", "", w.lower()))
            | "Filter" >> beam.Filter(lambda w: len(w) >= min_len)
        )


# ── Composite 3: Nested composites ───────────────────────────────────────

class TopNWords(beam.PTransform):
    """
    Returns the top-N most frequent words from a PCollection of lines.
    Internally uses CountWords, so composites can nest.
    """

    def __init__(self, n: int = 5):
        super().__init__()
        self.n = n

    def expand(self, pcoll):
        n = self.n
        return (
            pcoll
            | "Count"  >> CountWords()
            | "TopN"   >> beam.transforms.combiners.Top.Of(
                n, key=lambda kv: kv[1]
            )
            # Top.Of returns a list inside a single-element PCollection; flatten it.
            | "Flatten" >> beam.FlatMap(lambda top: top)
        )


def run():
    lines = [
        "To be or not to be that is the question",
        "Whether tis nobler in the mind to suffer",
        "The slings and arrows of outrageous fortune",
        "Or to take arms against a sea of troubles",
    ]

    with beam.Pipeline() as p:
        text = p | "Create" >> beam.Create(lines)

        print("=== Word Counts ===")
        (
            text
            | "WordCount" >> CountWords()
            | "Print1"    >> beam.Map(print)
        )

        print("\n=== Top 5 Words ===")
        (
            text
            | "Top5"   >> TopNWords(n=5)
            | "Print2" >> beam.Map(print)
        )

        print("\n=== Normalized (min length 5) ===")
        words = text | "Tokenize2" >> beam.FlatMap(str.split)
        (
            words
            | "Normalize" >> NormalizeAndFilter(min_length=5)
            | "Print3"    >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
