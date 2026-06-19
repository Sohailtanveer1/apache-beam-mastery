# Project 3 — Real-Time Streaming Analytics

**Level:** Advanced  
**Runner:** DataflowRunner (streaming mode)  
**GCP Services:** Pub/Sub, Cloud Storage, BigQuery, Dataflow, Secret Manager (optional)

---

## Architecture

```
 ┌──────────────────────────────────────────────────────────────────┐
 │                   EVENT PRODUCERS                                │
 │  Mobile App / Web / IoT devices                                  │
 └──────────────────────────┬───────────────────────────────────────┘
                            │ JSON events published
                            ▼
              ┌─────────────────────────┐
              │  Cloud Pub/Sub Topic    │
              │  projects/P/topics/T    │
              └────────────┬────────────┘
                           │ ReadFromPubSub
                           ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │                  DATAFLOW STREAMING PIPELINE                    │
 │                                                                  │
 │  ┌──────────────┐    ┌───────────────┐    ┌──────────────────┐  │
 │  │ Parse & Val  │───▶│ Enrich w/ BQ  │───▶│ FixedWindow(60s) │  │
 │  │ (DLQ on err) │    │ side input    │    │                  │  │
 │  └──────────────┘    └───────────────┘    └────────┬─────────┘  │
 │                                                    │            │
 │  ┌─────────────────────────────────────────────────┼──────────┐ │
 │  │               Fan-out to 3 aggregations         │          │ │
 │  │                                                 │          │ │
 │  │  ┌──────────────┐  ┌────────────────┐  ┌───────▼───────┐  │ │
 │  │  │ Revenue/min  │  │ Event type cnt │  │ User activity │  │ │
 │  │  │ per product  │  │ per region     │  │ session track │  │ │
 │  │  └──────┬───────┘  └───────┬────────┘  └──────┬────────┘  │ │
 │  └─────────┼─────────────────┼──────────────────┼────────────┘ │
 │            │                 │                  │              │
 └────────────┼─────────────────┼──────────────────┼──────────────┘
              │                 │                  │
              ▼                 ▼                  ▼
    ┌─────────────────────────────────────────────────────┐
    │              BigQuery Streaming Inserts              │
    │  dataset.revenue_per_minute                         │
    │  dataset.event_type_counts                          │
    │  dataset.user_session_metrics                       │
    └─────────────────────────────────────────────────────┘
              │
              ▼ (failures only)
    ┌──────────────────────┐
    │  GCS Dead Letter     │
    │  gs://bucket/dlq/    │
    └──────────────────────┘
```

---

## Pub/Sub Message Schema

Each message is a JSON-encoded byte string:

```json
{
  "event_id":    "evt-abc123",
  "user_id":     "usr-456",
  "event_type":  "purchase",
  "product_id":  "prod-789",
  "amount":      49.99,
  "currency":    "USD",
  "region":      "us-east",
  "timestamp":   "2024-06-15T10:30:00Z"
}
```

---

## BigQuery Output Schemas

### `revenue_per_minute`
| Column | Type |
|--------|------|
| window_start | TIMESTAMP |
| product_id | STRING |
| total_revenue_usd | FLOAT64 |
| transaction_count | INTEGER |

### `event_type_counts`
| Column | Type |
|--------|------|
| window_start | TIMESTAMP |
| event_type | STRING |
| region | STRING |
| event_count | INTEGER |

### `user_session_metrics`
| Column | Type |
|--------|------|
| window_start | TIMESTAMP |
| user_id | STRING |
| session_event_count | INTEGER |
| session_revenue | FLOAT64 |

---

## Setup & Deploy

```bash
# 1. Create Pub/Sub topic and subscription
gcloud pubsub topics create ecommerce-events
gcloud pubsub subscriptions create ecommerce-events-sub \
  --topic=ecommerce-events \
  --ack-deadline=60

# 2. Create BigQuery dataset
bq mk --dataset YOUR_PROJECT:streaming_analytics

# 3. Deploy streaming pipeline to Dataflow
python Projects/03_Advanced_Streaming_Analytics/pipeline.py \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT_ID \
  --region=us-central1 \
  --temp_location=gs://YOUR_BUCKET/temp \
  --staging_location=gs://YOUR_BUCKET/staging \
  --subscription=projects/YOUR_PROJECT_ID/subscriptions/ecommerce-events-sub \
  --bq_dataset=streaming_analytics \
  --dlq_path=gs://YOUR_BUCKET/dlq/streaming/ \
  --streaming \
  --enable_streaming_engine \
  --autoscaling_algorithm=THROUGHPUT_BASED \
  --max_num_workers=20 \
  --machine_type=n1-standard-4 \
  --job_name=ecommerce-streaming-$(date +%Y%m%d-%H%M%S)

# 4. Publish a test message
gcloud pubsub topics publish ecommerce-events \
  --message='{"event_id":"e1","user_id":"u1","event_type":"purchase","product_id":"p1","amount":99.99,"currency":"USD","region":"us-east","timestamp":"2024-06-15T10:30:00Z"}'
```

---

## Draining vs Cancelling the Job

```bash
# Drain: finish processing in-flight messages, then stop (preferred)
gcloud dataflow jobs drain JOB_ID --region=us-central1

# Cancel: stop immediately (may lose in-flight data)
gcloud dataflow jobs cancel JOB_ID --region=us-central1
```

---

## Key Learning Points

- `ReadFromPubSub` with `with_attributes=True` for message metadata
- Multiple fanout aggregations from a single enriched PCollection
- `FixedWindows` and `Sessions` in a true streaming context
- `AddWindowInfoDoFn` using `beam.DoFn.WindowParam`
- Stateful deduplication using `BagStateSpec`
- Production DLQ routing with `TaggedOutput`
- `enable_streaming_engine` for reduced cost on Dataflow
- Watermark vs processing-time triggers
