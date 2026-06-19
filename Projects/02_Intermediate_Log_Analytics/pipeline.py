"""
Project 2 — Log Analytics with Windowing & Side Inputs
See README.md for full architecture details.
"""

import argparse
import logging
import re
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam import pvalue, window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

# Apache Combined Log Format regex
CLF_PATTERN = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d{3}) (?P<bytes>\S+)'
)

SUCCESS_TAG = "success"
DLQ_TAG     = "dlq"

OUTPUT_SCHEMA = {
    "fields": [
        {"name": "window_start",    "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end",      "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "endpoint",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "method",          "type": "STRING",    "mode": "REQUIRED"},
        {"name": "status_class",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "request_count",   "type": "INTEGER",   "mode": "REQUIRED"},
        {"name": "is_spike",        "type": "BOOL",      "mode": "REQUIRED"},
    ]
}


class ParseCLFDoFn(beam.DoFn):
    def process(self, line):
        match = CLF_PATTERN.match(line)
        if not match:
            yield pvalue.TaggedOutput(DLQ_TAG, {"raw": line, "error": "no_match"})
            return
        d = match.groupdict()
        try:
            ts = datetime.strptime(d["time"], "%d/%b/%Y:%H:%M:%S %z").timestamp()
        except ValueError:
            ts = datetime.now(timezone.utc).timestamp()
        yield beam.window.TimestampedValue({
            "ip":     d["ip"],
            "method": d["method"],
            "path":   d["path"].split("?")[0],  # strip query string
            "status": int(d["status"]),
            "status_class": str(d["status"])[0] + "xx",
            "ts": ts,
        }, ts)


class AddWindowInfoDoFn(beam.DoFn):
    def process(self, element, spike_threshold, window=beam.DoFn.WindowParam):
        key, count = element
        path, method, status_class = key
        yield {
            "window_start":  window.start.to_utc_datetime().isoformat(),
            "window_end":    window.end.to_utc_datetime().isoformat(),
            "endpoint":      path,
            "method":        method,
            "status_class":  status_class,
            "request_count": count,
            "is_spike":      count > spike_threshold,
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",           required=True)
    parser.add_argument("--output_table",    required=True)
    parser.add_argument("--spike_threshold", type=int, default=100)
    parser.add_argument("--window_size",     type=int, default=300)
    parser.add_argument("--window_period",   type=int, default=60)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadLogs"  >> beam.io.ReadFromText(known_args.input)
            | "ParseCLF"  >> beam.ParDo(ParseCLFDoFn()).with_outputs(DLQ_TAG, main=SUCCESS_TAG)
        )

        metrics = (
            parsed[SUCCESS_TAG]
            | "KeyByEndpoint" >> beam.Map(
                lambda r: ((r["path"], r["method"], r["status_class"]), 1)
            )
            | "SlidingWindow" >> beam.WindowInto(
                window.SlidingWindows(known_args.window_size, known_args.window_period)
            )
            | "CountPerEndpoint" >> beam.CombinePerKey(sum)
            | "AddWindowInfo" >> beam.ParDo(
                AddWindowInfoDoFn(),
                spike_threshold=known_args.spike_threshold,
            )
        )

        metrics | "WriteBQ" >> WriteToBigQuery(
            table=known_args.output_table,
            schema=OUTPUT_SCHEMA,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
