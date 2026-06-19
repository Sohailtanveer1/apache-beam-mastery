"""
Beginner 04 — Reading and Writing Google Cloud Storage (GCS)

Concepts:
  - Reading from a GCS path (gs://...)
  - Writing to a GCS path
  - Pipeline options for GCP configuration
  - The --runner flag switches between DirectRunner (local) and DataflowRunner (GCP)

Prerequisites:
  - gcloud auth application-default login
  - A GCS bucket with read/write permissions

Run locally (reads/writes GCS but runs on your machine):
  python Beginner/04_read_write_gcs.py \
    --project=YOUR_PROJECT_ID \
    --input=gs://YOUR_BUCKET/input/hamlet.txt \
    --output=gs://YOUR_BUCKET/output/word_counts

Run on Dataflow:
  python Beginner/04_read_write_gcs.py \
    --runner=DataflowRunner \
    --project=YOUR_PROJECT_ID \
    --region=us-central1 \
    --temp_location=gs://YOUR_BUCKET/temp \
    --input=gs://YOUR_BUCKET/input/hamlet.txt \
    --output=gs://YOUR_BUCKET/output/word_counts
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions


def run(argv=None):
    parser = argparse.ArgumentParser(description="GCS word count pipeline")
    parser.add_argument(
        "--input",
        default="gs://dataflow-samples/shakespeare/hamlet.txt",
        help="GCS path to input file (supports wildcards: gs://bucket/prefix/*.txt)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="GCS prefix for output files (e.g. gs://bucket/output/wc)",
    )

    # parse_known_args lets Beam handle its own flags (--runner, --project, etc.)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    # SaveMainSession makes top-level imports available to workers.
    options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True

    logging.basicConfig(level=logging.INFO)

    with beam.Pipeline(options=options) as p:
        word_counts = (
            p
            | "ReadFromGCS" >> beam.io.ReadFromText(known_args.input)
            | "Tokenize" >> beam.FlatMap(
                lambda line: [w.lower().strip(".,!?;:\"'") for w in line.split() if w]
            )
            | "FilterEmpty" >> beam.Filter(bool)
            | "PairWithOne" >> beam.Map(lambda w: (w, 1))
            | "SumPerWord" >> beam.CombinePerKey(sum)
            | "FormatOutput" >> beam.Map(lambda kv: f"{kv[0]}\t{kv[1]}")
        )

        word_counts | "WriteToGCS" >> beam.io.WriteToText(
            known_args.output,
            file_name_suffix=".txt",
        )


if __name__ == "__main__":
    run()
