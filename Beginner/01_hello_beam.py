"""
Beginner 01 — Hello, Beam!

Concepts:
  - Creating a Pipeline object
  - Creating an in-memory PCollection with beam.Create
  - Applying a basic ParDo (Map)
  - Running with DirectRunner

Run:
  python Beginner/01_hello_beam.py
"""

import apache_beam as beam


def run():
    # A Pipeline is the top-level object that manages all transforms.
    # With no options specified, DirectRunner is used automatically.
    with beam.Pipeline() as pipeline:

        # beam.Create builds a PCollection from a Python iterable (in-memory).
        # This is the typical starting point for local tests.
        words = (
            pipeline
            | "CreateWords" >> beam.Create(
                ["apache", "beam", "python", "gcp", "dataflow", "bigquery"]
            )
        )

        # beam.Map applies a one-to-one function to every element.
        upper_words = (
            words
            | "Uppercase" >> beam.Map(str.upper)
        )

        # beam.Map with a lambda works identically.
        word_lengths = (
            words
            | "GetLengths" >> beam.Map(lambda w: (w, len(w)))
        )

        # Print each element to stdout (only use in development!).
        upper_words | "PrintUppercase" >> beam.Map(print)
        word_lengths | "PrintLengths" >> beam.Map(print)


if __name__ == "__main__":
    run()
