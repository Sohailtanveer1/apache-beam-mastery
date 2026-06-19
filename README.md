# Apache Beam Mastery (Python + GCP)

> **Go from absolute beginner to production-grade advanced** using the Apache Beam Python SDK on Google Cloud Platform.

---

## Table of Contents

1. [What is Apache Beam?](#what-is-apache-beam)
2. [Repository Structure](#repository-structure)
3. [Prerequisites & Installation](#prerequisites--installation)
4. [GCP Authentication & Setup](#gcp-authentication--setup)
5. [Running Pipelines Locally (DirectRunner)](#running-pipelines-locally-directrunner)
6. [Submitting Jobs to Google Cloud Dataflow](#submitting-jobs-to-google-cloud-dataflow)
7. [Learning Path](#learning-path)
8. [Projects](#projects)
9. [50 Practice Questions](#50-practice-questions)
10. [Contributing](#contributing)

---

## What is Apache Beam?

Apache Beam is a **unified programming model** for defining both batch and streaming data-parallel processing pipelines. You write one pipeline; Beam runners (DirectRunner, DataflowRunner, SparkRunner, FlinkRunner) execute it.

```
Your Python Pipeline Code
        |
    Apache Beam SDK
        |
   ┌────┴────────────────────────┐
   │                             │
DirectRunner             DataflowRunner
(local dev/test)       (GCP production)
```

**Core Concepts:**

| Concept | Description |
|---------|-------------|
| `Pipeline` | The container for the entire data processing job |
| `PCollection` | An immutable, distributed dataset (bounded or unbounded) |
| `PTransform` | An operation that transforms one or more PCollections |
| `DoFn` | The user-defined function inside a `ParDo` transform |
| `Runner` | The execution engine (DirectRunner, DataflowRunner, etc.) |

---

## Repository Structure

```
apache-beam-mastery/
│
├── README.md                         # This file
├── requirements.txt                  # Python dependencies
│
├── Beginner/
│   ├── 01_hello_beam.py              # First pipeline, DirectRunner
│   ├── 02_basic_transforms.py        # Map, FlatMap, Filter, Combine
│   ├── 03_read_write_local.py        # Read/write local text files
│   └── 04_read_write_gcs.py          # Read/write Google Cloud Storage
│
├── Intermediate/
│   ├── 01_dataflow_runner.py         # Submitting to Dataflow
│   ├── 02_groupbykey_cogroupbykey.py # Joins in Beam
│   ├── 03_side_inputs.py             # Side inputs pattern
│   ├── 04_windowing.py               # Fixed, Sliding, Session windows
│   └── 05_bigquery_read_write.py     # BigQuery I/O
│
├── Advanced/
│   ├── 01_streaming_pubsub.py        # Streaming from Pub/Sub
│   ├── 02_stateful_dofn.py           # State & Timers API
│   ├── 03_custom_dofn_patterns.py    # Advanced DoFn patterns
│   ├── 04_dead_letter_queue.py       # Error handling / DLQ
│   └── 05_performance_tuning.py      # Dataflow performance best practices
│
├── Projects/
│   ├── 01_Beginner_GCS_to_BigQuery/
│   │   ├── README.md
│   │   └── pipeline.py
│   ├── 02_Intermediate_Log_Analytics/
│   │   ├── README.md
│   │   └── pipeline.py
│   └── 03_Advanced_Streaming_Analytics/
│       ├── README.md
│       └── pipeline.py               # Full production streaming pipeline
│
└── Practice_50_Questions/
    ├── QUESTIONS.md                  # All 50 questions listed
    └── solutions/                    # Community solutions (PRs welcome!)
```

---

## Prerequisites & Installation

**Python version:** 3.9, 3.10, or 3.11 (Beam 2.57 requirement)

```bash
# 1. Clone the repository
git clone https://github.com/Sohailtanveer1/apache-beam-mastery.git
cd apache-beam-mastery

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate          # Linux/macOS
.venv\Scripts\activate             # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

---

## GCP Authentication & Setup

### Option A: Application Default Credentials (ADC) — Recommended for Local Dev

ADC is the simplest way to authenticate. The Beam SDK and all GCP client libraries automatically pick up these credentials.

```bash
# Install the Google Cloud CLI
# https://cloud.google.com/sdk/docs/install

# Log in and set ADC
gcloud auth application-default login

# Set your project
gcloud config set project YOUR_PROJECT_ID
```

### Option B: Service Account Key — Recommended for CI/CD & Production

```bash
# 1. Create a service account
gcloud iam service-accounts create beam-runner \
  --display-name="Apache Beam Runner SA"

# 2. Grant required roles
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:beam-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:beam-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:beam-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:beam-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

# 3. Download the key JSON
gcloud iam service-accounts keys create ~/key.json \
  --iam-account=beam-runner@YOUR_PROJECT_ID.iam.gserviceaccount.com

# 4. Point the SDK to the key
export GOOGLE_APPLICATION_CREDENTIALS=~/key.json
```

**Required IAM Roles Summary:**

| Role | Purpose |
|------|---------|
| `roles/dataflow.worker` | Run Dataflow jobs |
| `roles/dataflow.admin` | Submit and monitor jobs |
| `roles/bigquery.dataEditor` | Read/write BigQuery tables |
| `roles/storage.objectAdmin` | Read/write GCS buckets |
| `roles/pubsub.subscriber` | Read from Pub/Sub topics |
| `roles/pubsub.publisher` | Write to Pub/Sub topics |

---

## Running Pipelines Locally (DirectRunner)

The **DirectRunner** runs your pipeline on your local machine — no GCP account needed. Perfect for development and testing.

```bash
python Beginner/01_hello_beam.py
```

All Beginner examples default to `DirectRunner`. No additional flags required.

---

## Submitting Jobs to Google Cloud Dataflow

Replace the placeholders with your actual GCP values.

### Batch Job

```bash
python your_pipeline.py \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT_ID \
  --region=us-central1 \
  --temp_location=gs://YOUR_BUCKET/temp \
  --staging_location=gs://YOUR_BUCKET/staging \
  --job_name=my-batch-job-$(date +%Y%m%d-%H%M%S) \
  --setup_file=./setup.py
```

### Streaming Job

```bash
python your_streaming_pipeline.py \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT_ID \
  --region=us-central1 \
  --temp_location=gs://YOUR_BUCKET/temp \
  --staging_location=gs://YOUR_BUCKET/staging \
  --job_name=my-streaming-job \
  --streaming \
  --enable_streaming_engine \
  --autoscaling_algorithm=THROUGHPUT_BASED \
  --max_num_workers=10
```

### Key Dataflow Pipeline Options

| Option | Description |
|--------|-------------|
| `--runner` | `DataflowRunner` for GCP, `DirectRunner` for local |
| `--project` | Your GCP Project ID |
| `--region` | Dataflow region (e.g., `us-central1`) |
| `--temp_location` | GCS path for temporary files (required) |
| `--staging_location` | GCS path for staging pipeline artifacts |
| `--job_name` | Unique job name |
| `--num_workers` | Initial worker count |
| `--max_num_workers` | Max workers for autoscaling |
| `--machine_type` | Worker machine type (e.g., `n1-standard-4`) |
| `--disk_size_gb` | Worker disk size |
| `--streaming` | Enable streaming mode |
| `--enable_streaming_engine` | Use Dataflow Streaming Engine (recommended) |
| `--service_account_email` | Custom service account for workers |
| `--network` / `--subnetwork` | VPC network configuration |

---

## Learning Path

```
┌─────────────────────────────────────────────────────┐
│  BEGINNER (Start Here)                              │
│  DirectRunner • PTransforms • Local/GCS I/O         │
│  Files: Beginner/01 → 04                            │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  INTERMEDIATE                                        │
│  DataflowRunner • Joins • Side Inputs • Windowing   │
│  BigQuery I/O                                        │
│  Files: Intermediate/01 → 05                        │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  ADVANCED                                            │
│  Streaming (Pub/Sub) • Stateful Processing          │
│  Dead Letter Queues • Performance Tuning            │
│  Files: Advanced/01 → 05                            │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  PROJECTS (Apply Everything)                        │
│  01 Batch ETL • 02 Log Analytics • 03 Streaming     │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  50 PRACTICE QUESTIONS (Build Muscle Memory)        │
│  20 Beginner • 20 Intermediate • 10 Advanced        │
└─────────────────────────────────────────────────────┘
```

---

## Projects

| # | Level | Title | Description |
|---|-------|-------|-------------|
| 1 | Beginner | [Batch ETL: GCS → BigQuery](Projects/01_Beginner_GCS_to_BigQuery/README.md) | Read CSV from GCS, transform, write to BigQuery |
| 2 | Intermediate | [Log Analytics with Windowing](Projects/02_Intermediate_Log_Analytics/README.md) | Parse HTTP logs, apply sliding windows, detect anomalies |
| 3 | Advanced | [Real-time Streaming Analytics](Projects/03_Advanced_Streaming_Analytics/README.md) | Pub/Sub → windowed aggregation → BigQuery (with DLQ) |

---

## 50 Practice Questions

See [`Practice_50_Questions/QUESTIONS.md`](Practice_50_Questions/QUESTIONS.md) for the full list.

---

## Contributing

Pull requests for solutions to practice questions are very welcome!

1. Fork the repo
2. Create a branch: `git checkout -b solution/q-15-sliding-window`
3. Add your solution to `Practice_50_Questions/solutions/`
4. Submit a PR

---

*Built with the Apache Beam Python SDK 2.57 | Targeting GCP Dataflow, BigQuery, GCS, Pub/Sub*
