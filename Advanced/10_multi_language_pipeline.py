"""
Advanced 10 — Multi-Language Pipelines

Source: Beam Programming Guide §13

Apache Beam lets you use transforms from one SDK language inside a pipeline
written in another language. This is called a "multi-language pipeline."

Common use case: use the Java SDK's richer I/O ecosystem (Kafka, JDBC, etc.)
from within a Python pipeline, without writing any Java code.

How it works:
  1. Python pipeline starts a local Java expansion service.
  2. The expansion service serialises the Java transform and injects it into
     the Python pipeline graph.
  3. Beam's runner (e.g., Dataflow) executes both Python and Java transforms.

Python SDK provides two main mechanisms:
  - beam.io.kafka.ReadFromKafka / WriteToKafka  → dedicated wrappers
  - beam.transforms.external.JavaExternalTransform → generic Java class access

Prerequisites:
  - Java 11+ installed
  - apache-beam[gcp] 2.32.0+
  - The Java Beam SDK jar on the classpath (auto-downloaded by the Python SDK)

NOTE: This file is structured as a reference + runnable stub.
      The Kafka sections require a running Kafka cluster to execute.
      Replace bootstrap_servers and topic with your actual values.

Run (stub mode — no Kafka needed):
  python Advanced/10_multi_language_pipeline.py --stub_mode

Run (with real Kafka):
  python Advanced/10_multi_language_pipeline.py \
    --runner=DataflowRunner \
    --project=YOUR_PROJECT \
    --region=us-central1 \
    --temp_location=gs://YOUR_BUCKET/temp \
    --bootstrap_servers=broker:9092 \
    --input_topic=my-input-topic \
    --streaming
"""

import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions


# ─────────────────────────────────────────────────────────────────────────────
# Pattern 1: Using the built-in Python wrapper for Java Kafka I/O
# ─────────────────────────────────────────────────────────────────────────────

def build_kafka_pipeline(options, bootstrap_servers: str, input_topic: str):
    """
    Reads from a Kafka topic using the Java KafkaIO cross-language transform.
    The Python SDK wraps the Java transform — no Java code required from you.

    beam.io.kafka.ReadFromKafka internally:
      1. Starts a Java expansion service.
      2. Creates a KafkaIO.Read Java transform.
      3. Injects it into the Python pipeline graph.
    """
    # Only import if apache-beam[gcp] is installed with Java support
    from apache_beam.io.kafka import ReadFromKafka

    with beam.Pipeline(options=options) as p:
        messages = (
            p
            | "ReadKafka" >> ReadFromKafka(
                consumer_config={
                    "bootstrap.servers": bootstrap_servers,
                    "auto.offset.reset":  "earliest",
                },
                topics=[input_topic],
                # Returns (key_bytes, value_bytes) tuples
            )
        )

        parsed = (
            messages
            | "ParseJSON" >> beam.Map(
                lambda kv: json.loads(kv[1].decode("utf-8"))
            )
            | "FilterNone" >> beam.Filter(lambda x: x is not None)
        )

        parsed | "Print" >> beam.Map(print)


# ─────────────────────────────────────────────────────────────────────────────
# Pattern 2: JavaExternalTransform — use any Java transform without Java code
# ─────────────────────────────────────────────────────────────────────────────

def build_java_external_transform_example(options):
    """
    Uses JavaExternalTransform to call a Java PTransform directly from Python.

    This lets you use any Java transform in the Beam SDK or your own Java
    library, as long as it meets the requirements:
      1. Public constructor or static factory method.
      2. Public builder methods that return the same transform type.

    The expansion service auto-starts if no address is provided (Beam 2.36+).
    """
    from apache_beam.transforms.external import JavaExternalTransform

    # Replace with your actual Java transform class and jar path.
    # Example: using the built-in GenerateSequence Java transform.
    with beam.Pipeline(options=options) as p:

        # JavaExternalTransform.from_callable() wraps a Java class.
        # The class name must be fully qualified.
        result = (
            p
            | "JavaSequence" >> JavaExternalTransform(
                "org.apache.beam.sdk.io.GenerateSequence",
                classpath=[],  # Add your jar path here if needed
            )
            .from_(0)           # Calls the Java from() builder method
            .to(10)             # Calls the Java to() builder method
        )
        result | "Print" >> beam.Map(print)


# ─────────────────────────────────────────────────────────────────────────────
# Pattern 3: SqlTransform — run SQL on a PCollection
# ─────────────────────────────────────────────────────────────────────────────

def build_sql_transform_example():
    """
    SqlTransform executes a SQL query on a schema PCollection.
    Internally uses the Java Calcite SQL engine via cross-language.
    Requires: apache-beam[gcp] and Java 11+.
    """
    import typing
    from apache_beam.transforms.sql import SqlTransform

    class Order(typing.NamedTuple):
        order_id: str
        user_id:  str
        amount:   float
        region:   str

    orders = [
        Order("o1", "u1", 250.0, "us-east"),
        Order("o2", "u2",  50.0, "eu-west"),
        Order("o3", "u1", 900.0, "us-east"),
        Order("o4", "u3", 120.0, "eu-west"),
        Order("o5", "u2", 300.0, "us-east"),
    ]

    with beam.Pipeline() as p:
        result = (
            p
            | "CreateOrders" >> beam.Create(orders)
            | "SQL" >> SqlTransform("""
                SELECT
                  region,
                  COUNT(*)       AS order_count,
                  SUM(amount)    AS total_revenue
                FROM PCOLLECTION
                WHERE amount > 100
                GROUP BY region
            """)
        )
        result | "Print" >> beam.Map(print)


# ─────────────────────────────────────────────────────────────────────────────
# Stub mode — runs without Kafka or Java
# ─────────────────────────────────────────────────────────────────────────────

def run_stub():
    """
    Runs a simple pipeline to verify the file is importable and
    the concepts are understandable without a Kafka cluster.
    """
    print("Running in stub mode (no Kafka or Java required).\n")
    print("Patterns demonstrated in this file:")
    print("  1. ReadFromKafka     — Java KafkaIO via cross-language transform")
    print("  2. JavaExternalTransform — arbitrary Java transforms from Python")
    print("  3. SqlTransform      — SQL queries on schema PCollections")
    print("\nTo run with real Kafka, see the module docstring for CLI flags.")

    # SqlTransform demo (works locally if Java 11+ is installed)
    try:
        print("\n=== SqlTransform Demo ===")
        build_sql_transform_example()
    except Exception as e:
        print(f"SqlTransform requires Java 11+: {e}")
        print("Install Java 11+ to run this demo.")


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--stub_mode",       action="store_true", default=False)
    parser.add_argument("--bootstrap_servers", default="localhost:9092")
    parser.add_argument("--input_topic",       default="my-topic")
    known_args, pipeline_args = parser.parse_known_args(argv)

    if known_args.stub_mode:
        run_stub()
        return

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True

    build_kafka_pipeline(
        options,
        known_args.bootstrap_servers,
        known_args.input_topic,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
