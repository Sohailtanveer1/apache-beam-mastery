"""
Beginner 02 — Core PTransforms: Map, FlatMap, Filter, Combine

Concepts:
  - beam.Map      → 1-to-1 transformation
  - beam.FlatMap  → 1-to-many transformation (tokenization, unnesting)
  - beam.Filter   → Keep elements matching a predicate
  - beam.CombineGlobally / beam.CombinePerKey → Aggregation

Run:
  python Beginner/02_basic_transforms.py
"""

import apache_beam as beam
from apache_beam.transforms.combinefn_lifecycle import CombineFnMixin


# ---------------------------------------------------------------------------
# Custom DoFn — the building block of all Beam transforms.
# A DoFn subclass gives you full lifecycle control (setup, teardown, etc.).
# ---------------------------------------------------------------------------
class SplitLineDoFn(beam.DoFn):
    """Splits a sentence into individual (word, 1) pairs."""

    def process(self, element):
        for word in element.strip().split():
            if word:
                yield (word.lower(), 1)


def run():
    sentences = [
        "Apache Beam provides a unified model",
        "Google Cloud Dataflow runs Beam pipelines",
        "Beam supports both batch and streaming",
        "Python is a great language for data engineering",
    ]

    with beam.Pipeline() as p:

        lines = p | "CreateSentences" >> beam.Create(sentences)

        # ── FlatMap ──────────────────────────────────────────────────────
        # FlatMap expects the function to return an iterable.
        # Each item in that iterable becomes a separate element in the output.
        words = (
            lines
            | "Tokenize" >> beam.FlatMap(lambda line: line.lower().split())
        )

        # ── Filter ───────────────────────────────────────────────────────
        # Filter keeps elements where the function returns True.
        long_words = (
            words
            | "FilterShortWords" >> beam.Filter(lambda w: len(w) >= 5)
        )
        long_words | "PrintLongWords" >> beam.Map(lambda w: print(f"Long word: {w}"))

        # ── Map with side output via DoFn ─────────────────────────────────
        word_pairs = (
            lines
            | "MakeWordPairs" >> beam.ParDo(SplitLineDoFn())
        )

        # ── CombinePerKey — sum the counts per word ───────────────────────
        word_counts = (
            word_pairs
            | "SumPerWord" >> beam.CombinePerKey(sum)
        )
        word_counts | "PrintWordCounts" >> beam.Map(lambda kv: print(f"{kv[0]}: {kv[1]}"))

        # ── CombineGlobally — total word count across all sentences ───────
        all_pairs = (
            lines
            | "AllWordPairs" >> beam.FlatMap(
                lambda line: [(w.lower(), 1) for w in line.split()]
            )
        )
        total_words = (
            all_pairs
            | "ExtractCounts" >> beam.Values()
            | "TotalCount" >> beam.CombineGlobally(sum)
        )
        total_words | "PrintTotal" >> beam.Map(lambda n: print(f"\nTotal words: {n}"))


if __name__ == "__main__":
    run()
