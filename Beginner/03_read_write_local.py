"""
Beginner 03 — Reading and Writing Local Files

Concepts:
  - beam.io.ReadFromText  → Read a local text file into a PCollection
  - beam.io.WriteToText   → Write a PCollection to a local text file
  - Basic word-count pipeline (the "Hello World" of Beam)

Run:
  python Beginner/03_read_write_local.py

This script auto-creates a sample input file so it is self-contained.
"""

import os
import apache_beam as beam

INPUT_FILE = "/tmp/beam_input.txt"
OUTPUT_PREFIX = "/tmp/beam_output"


def create_sample_input():
    content = (
        "To be or not to be that is the question\n"
        "Whether tis nobler in the mind to suffer\n"
        "The slings and arrows of outrageous fortune\n"
        "Or to take arms against a sea of troubles\n"
    )
    with open(INPUT_FILE, "w") as f:
        f.write(content)
    print(f"Sample input written to: {INPUT_FILE}")


def run():
    create_sample_input()

    with beam.Pipeline() as p:

        # ReadFromText emits one string per line.
        # strip_trailing_newlines=True (default) cleans up line endings.
        lines = p | "ReadLines" >> beam.io.ReadFromText(INPUT_FILE)

        # Word-count transformation.
        word_counts = (
            lines
            | "Tokenize" >> beam.FlatMap(lambda line: line.lower().split())
            | "PairWithOne" >> beam.Map(lambda w: (w, 1))
            | "SumPerWord" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(lambda kv: f"{kv[0]}: {kv[1]}")
        )

        # WriteToText writes each element as one line.
        # Beam automatically shards the output; the shard suffix is appended.
        word_counts | "WriteResults" >> beam.io.WriteToText(
            OUTPUT_PREFIX,
            file_name_suffix=".txt",
            num_shards=1,       # Force single output file (dev only)
        )

    print(f"\nResults written to: {OUTPUT_PREFIX}-00000-of-00001.txt")
    output_file = f"{OUTPUT_PREFIX}-00000-of-00001.txt"
    if os.path.exists(output_file):
        with open(output_file) as f:
            print(f.read())


if __name__ == "__main__":
    run()
