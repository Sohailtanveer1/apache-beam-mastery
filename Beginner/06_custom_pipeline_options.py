"""
Beginner 06 — Custom PipelineOptions

Source: https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options

PipelineOptions is the standard way to pass configuration to a pipeline.
You extend it to add your own flags — they merge cleanly with Beam's
built-in flags (--runner, --project, etc.) on the command line.

Run (show defaults):
  python Beginner/06_custom_pipeline_options.py

Run with custom flags:
  python Beginner/06_custom_pipeline_options.py \
    --input=gs://my-bucket/data/*.csv \
    --output=gs://my-bucket/out/ \
    --batch_size=500 \
    --env=prod \
    --runner=DirectRunner
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# ── Define custom options by subclassing PipelineOptions ─────────────────
# All Beam option groups follow this same pattern.

class MyJobOptions(PipelineOptions):
    """
    Custom flags specific to this pipeline.
    Accessible via options.view_as(MyJobOptions).input, etc.
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        # Required argument — pipeline will fail fast if not provided
        parser.add_argument(
            "--input",
            required=True,
            help="Input file path or GCS glob (e.g. gs://bucket/data/*.csv)",
        )
        parser.add_argument(
            "--output",
            required=True,
            help="Output path prefix (local or GCS)",
        )
        # Optional with default
        parser.add_argument(
            "--batch_size",
            type=int,
            default=100,
            help="Number of records per batch for API calls (default: 100)",
        )
        parser.add_argument(
            "--env",
            default="dev",
            choices=["dev", "staging", "prod"],
            help="Deployment environment (default: dev)",
        )
        # Boolean flag: --skip_validation / --no-skip_validation
        parser.add_argument(
            "--skip_validation",
            action="store_true",
            default=False,
            help="Skip input validation (use in dev only)",
        )


def run(argv=None):
    # parse_known_args: your flags + Beam's built-in flags co-exist
    options = PipelineOptions(argv)

    # Access your custom flags via view_as()
    job_opts = options.view_as(MyJobOptions)

    # Make imports available on Dataflow workers
    options.view_as(
        beam.options.pipeline_options.SetupOptions
    ).save_main_session = True

    print(f"Input:            {job_opts.input}")
    print(f"Output:           {job_opts.output}")
    print(f"Batch size:       {job_opts.batch_size}")
    print(f"Environment:      {job_opts.env}")
    print(f"Skip validation:  {job_opts.skip_validation}")

    with beam.Pipeline(options=options) as p:
        result = (
            p
            | "Create"  >> beam.Create([1, 2, 3])
            | "Double"  >> beam.Map(lambda x: x * 2)
        )
        result | "Print" >> beam.Map(print)


if __name__ == "__main__":
    # Demo: inject default values programmatically when running without CLI
    import sys
    if "--input" not in " ".join(sys.argv):
        sys.argv += ["--input=gs://demo/input", "--output=gs://demo/output"]
    run()
