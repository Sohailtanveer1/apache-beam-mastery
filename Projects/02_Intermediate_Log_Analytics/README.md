# Project 2 — Log Analytics with Windowing & Side Inputs

**Level:** Intermediate  
**Runner:** DirectRunner (dev) / DataflowRunner (prod)  
**GCP Services:** Cloud Storage, BigQuery, Dataflow

---

## Overview

Analyzes Apache/Nginx HTTP access logs to detect:
- Per-endpoint request rates in 5-minute windows
- Spike detection: endpoints exceeding a configurable RPS threshold
- Error rate (4xx/5xx) per IP address using sliding windows
- Enrichment with a GeoIP side input (country lookup by IP prefix)

```
gs://bucket/logs/*.log
        │
        ▼ ReadFromText
┌───────────────────┐
│  Parse CLF Format │  (Common Log Format)
└────────┬──────────┘
         │
         ├─── [VALID] ──────────────────────────────────────────────┐
         │                                                          │
         ▼ beam.WindowInto(SlidingWindows(300, 60))                 │
┌──────────────────────────────┐                                   │
│  Count requests per endpoint │◀── side input: endpoint allowlist │
│  Detect spikes vs baseline   │                                   │
└──────────────┬───────────────┘                                   │
               │                                                   │
               ▼                                                   │
┌──────────────────────────────┐                                   │
│  WriteToBigQuery             │       WriteToBigQuery DLQ ◀───────┘
│  project:ds.request_metrics  │       project:ds.parse_errors
└──────────────────────────────┘
```

---

## Log Format (Apache Combined Log Format)

```
127.0.0.1 - frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
```

---

## BigQuery Output Schema — `request_metrics`

| Column | Type | Description |
|--------|------|-------------|
| window_start | TIMESTAMP | Window start time |
| window_end | TIMESTAMP | Window end time |
| endpoint | STRING | HTTP path |
| method | STRING | HTTP verb (GET, POST, ...) |
| status_class | STRING | "2xx", "4xx", "5xx" |
| request_count | INTEGER | Requests in this window |
| is_spike | BOOL | Count > spike_threshold |
| country | STRING | GeoIP country (from side input) |

---

## Setup & Run

```bash
# Run locally
python Projects/02_Intermediate_Log_Analytics/pipeline.py \
  --project=YOUR_PROJECT_ID \
  --input=gs://YOUR_BUCKET/logs/*.log \
  --output_table=YOUR_PROJECT_ID:log_analytics.request_metrics \
  --spike_threshold=100 \
  --window_size=300 \
  --window_period=60

# Run on Dataflow
python Projects/02_Intermediate_Log_Analytics/pipeline.py \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT_ID \
  --region=us-central1 \
  --temp_location=gs://YOUR_BUCKET/temp \
  --input=gs://YOUR_BUCKET/logs/*.log \
  --output_table=YOUR_PROJECT_ID:log_analytics.request_metrics
```

---

## Key Learning Points

- Regex-based log parsing in a DoFn
- `SlidingWindows(size, period)` for overlapping rate calculations
- Side input (`AsDict`) for the GeoIP lookup table
- `beam.combiners.Count.PerKey()` inside a window
- `AddWindowInfoDoFn` to attach window boundaries to output rows
- Multi-output: valid records vs parse errors (DLQ)
