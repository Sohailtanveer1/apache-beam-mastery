"""
Intermediate 01 — Submitting a Batch Pipeline to Google Cloud Dataflow

Concepts:
  - PipelineOptions subclasses (StandardOptions, GoogleCloudOptions, WorkerOptions)
  - Programmatic Dataflow job configuration
  - Autoscaling configuration
  - Custom labels for cost attribution

Run:
  python Intermediate/01_dataflow_runner.py \
    --project=YOUR_PROJECT_ID \
    --region=us-central1 \
    --bucket=YOUR_BUCKET \
    --env=prod
"""

import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    WorkerOptions,
    SetupOptions,
)


def build_pipeline_options(project: str, region: str, bucket: str, env: str) -> PipelineOptions:
    """
    Constructs a fully configured PipelineOptions object for Dataflow.
    Separating option construction from pipeline logic keeps code testable.
    """
    options = PipelineOptions()

    # ── Execution engine ─────────────────────────────────────────────────
    std = options.view_as(StandardOptions)
    std.runner = "DataflowRunner"

    # ── GCP project & location ───────────────────────────────────────────
    gcp = options.view_as(GoogleCloudOptions)
    gcp.project = project
    gcp.region = region
    gcp.job_name = f"intermediate-demo-{env}"
    gcp.temp_location = f"gs://{bucket}/temp"
    gcp.staging_location = f"gs://{bucket}/staging"
    # Tag the job for cost attribution in billing reports
    gcp.labels = {"env": env, "team": "data-engineering", "pipeline": "intermediate-demo"}

    # ── Worker configuration ─────────────────────────────────────────────
    workers = options.view_as(WorkerOptions)
    workers.machine_type = "n1-standard-4"
    workers.num_workers = 2
    workers.max_num_workers = 20
    workers.disk_size_gb = 50
    # Use a service account with least-privilege permissions
    workers.service_account_email = f"beam-runner@{project}.iam.gserviceaccount.com"

    # ── Packaging ────────────────────────────────────────────────────────
    setup = options.view_as(SetupOptions)
    setup.save_main_session = True  # Ship top-level symbols to workers
    # Uncomment to bundle local packages:
    # setup.setup_file = "./setup.py"

    return options


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--env", default="dev")
    known_args, _ = parser.parse_known_args(argv)

    logging.basicConfig(level=logging.INFO)

    options = build_pipeline_options(
        known_args.project, known_args.region, known_args.bucket, known_args.env
    )

    input_path = f"gs://{known_args.bucket}/input/*.csv"
    output_path = f"gs://{known_args.bucket}/output/dataflow_demo"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadCSV" >> beam.io.ReadFromText(input_path, skip_header_lines=1)
            | "ParseRow" >> beam.Map(lambda line: dict(zip(
                ["id", "name", "amount"],
                line.split(",")
            )))
            | "FilterPositive" >> beam.Filter(lambda r: float(r["amount"]) > 0)
            | "FormatJSON" >> beam.Map(lambda r: str(r))
            | "WriteOutput" >> beam.io.WriteToText(output_path, file_name_suffix=".json")
        )

    logging.info("Dataflow job submitted. Monitor at: "
                 "https://console.cloud.google.com/dataflow/jobs")


if __name__ == "__main__":
    run()
