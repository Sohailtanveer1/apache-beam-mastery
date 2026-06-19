# Apache Beam Concepts Cheat Sheet

Quick reference for the core concepts from the [official programming guide](https://beam.apache.org/documentation/programming-guide/).

---

## Pipeline Execution Model

```
Pipeline
  └─ PCollection (immutable, distributed dataset)
       └─ PTransform (operation)
            └─ PCollection (output)
```

| Component | Role |
|-----------|------|
| `Pipeline` | Job container; holds all transforms |
| `PCollection` | Distributed dataset (bounded = batch, unbounded = stream) |
| `PTransform` | Data operation (Map, GroupByKey, Window, etc.) |
| `DoFn` | User logic inside a ParDo |
| `Runner` | Execution engine (DirectRunner, DataflowRunner) |

---

## Core Transforms — Quick Reference

| Transform | Input → Output | Use When |
|-----------|---------------|----------|
| `beam.Map(fn)` | 1 → 1 | Simple element transformation |
| `beam.FlatMap(fn)` | 1 → N | Tokenize, unnest, explode |
| `beam.Filter(fn)` | 1 → 0 or 1 | Keep/drop elements |
| `beam.ParDo(DoFn)` | 1 → any | Full control; side outputs; lifecycle |
| `beam.GroupByKey` | (k,v)... → (k,[v...]) | Aggregate by key |
| `beam.CombinePerKey(fn)` | (k,v)... → (k,v) | Efficient per-key aggregation |
| `beam.CombineGlobally(fn)` | v... → v | Global scalar (sum, max, count) |
| `beam.CoGroupByKey` | {tag: (k,v)}... → (k,{tag:[v]}) | Multi-PCollection join |
| `beam.Flatten` | [pcoll...] → pcoll | Union of PCollections |
| `beam.Partition(fn, N)` | pcoll → (p0,p1,...pN) | Fan-out by key/value |
| `beam.Reshuffle` | pcoll → pcoll | Break fusion; re-parallelize |

---

## DoFn Lifecycle

```
setup()            ← once per worker process (load models, open connection pools)
  start_bundle()   ← once per bundle (reset buffers)
    process()      ← once per element (your main logic)
  finish_bundle()  ← once per bundle (flush buffers)
teardown()         ← once per worker process (close connections)
```

---

## Schemas

Define element types as named fields for introspection and SQL-style access.

```python
import typing, apache_beam as beam

class Order(typing.NamedTuple):
    order_id: str
    amount:   float
    region:   str

# Schema is auto-inferred from NamedTuple
orders | beam.Select("order_id", "amount")          # project fields
orders | beam.Filter(lambda o: o.amount > 100)      # field access
orders | beam.Map(lambda o: beam.Row(               # ad-hoc Row
    order_id=o.order_id, amount_cents=int(o.amount * 100)
))
```

---

## Side Inputs

Pass a small, reference PCollection into a transform without a shuffle.

```python
from apache_beam.pvalue import AsSingleton, AsDict, AsList

# AsSingleton → scalar value
threshold_pc = p | beam.Create([500.0])
pcoll | beam.Map(fn, threshold=AsSingleton(threshold_pc))

# AsDict → {key: value} lookup table
lookup_pc = p | beam.Create([("k1", "v1"), ("k2", "v2")])
pcoll | beam.Map(fn, lookup=AsDict(lookup_pc))

# AsList → [value, ...] list
blocklist_pc = p | beam.Create(["bad_user_1", "bad_user_2"])
pcoll | beam.Map(fn, blocked=AsList(blocklist_pc))
```

---

## Windowing

| Window Type | API | Use Case |
|-------------|-----|----------|
| Fixed (tumbling) | `FixedWindows(60)` | Per-minute/hour aggregations |
| Sliding | `SlidingWindows(300, 60)` | Rolling 5-min avg, updated every 1 min |
| Session | `Sessions(300)` | User activity sessions (gap-based) |
| Global | `GlobalWindows()` | Entire stream as one window |

```python
import apache_beam as beam
from apache_beam import window

pcoll | beam.WindowInto(window.FixedWindows(60))         # 1-min tumbling
pcoll | beam.WindowInto(window.SlidingWindows(300, 60))  # 5-min sliding
pcoll | beam.WindowInto(window.Sessions(300))            # 5-min gap sessions
```

---

## Triggers

```python
from apache_beam.transforms import trigger

# The standard production pattern
beam.WindowInto(
    window.FixedWindows(60),
    trigger=trigger.AfterWatermark(
        early=trigger.AfterProcessingTime(10),  # speculative every 10s
        late=trigger.AfterCount(1),             # fire on each late element
    ),
    accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
    allowed_lateness=beam.transforms.window.Duration(seconds=30),
)
```

| Mode | Behavior | Use With |
|------|----------|----------|
| `ACCUMULATING` | Each pane has ALL data so far | BigQuery MERGE / upsert |
| `DISCARDING` | Each pane has only NEW data | BigQuery INSERT (append) |

---

## State & Timers

```python
from apache_beam.transforms.userstate import (
    CombiningValueStateSpec, BagStateSpec, TimerSpec, on_timer
)
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam import coders

class MyStatefulDoFn(beam.DoFn):
    # State is per-key AND per-window
    COUNT = CombiningValueStateSpec("count", combine_fn=sum)
    SEEN  = BagStateSpec("seen_ids", coders.StrUtf8Coder())
    TIMER = TimerSpec("flush_timer", TimeDomain.WATERMARK)

    def process(self, element,
                count=beam.DoFn.StateParam(COUNT),
                seen=beam.DoFn.StateParam(SEEN),
                timer=beam.DoFn.TimerParam(TIMER),
                ts=beam.DoFn.TimestampParam):
        count.add(1)
        seen.add(element["id"])
        timer.set(ts + 60)   # fire 60s after this element
        yield element

    @on_timer(TIMER)
    def on_flush(self, count=beam.DoFn.StateParam(COUNT)):
        yield {"flushed_count": count.read()}
```

---

## Metrics

```python
from apache_beam.metrics import Metrics

class MyDoFn(beam.DoFn):
    def __init__(self):
        self.processed   = Metrics.counter("ns", "processed")
        self.amount_dist = Metrics.distribution("ns", "amount")
        self.last_val    = Metrics.gauge("ns", "last_value")

    def process(self, element):
        self.processed.inc()
        self.amount_dist.update(int(element["amount"]))
        self.last_val.set(int(element["amount"]))
        yield element
```

Query after pipeline finishes (DirectRunner):
```python
result = pipeline.run()
result.wait_until_finish()
metrics = result.metrics().query(
    beam.metrics.MetricsFilter().with_namespace("ns")
)
```

---

## Composite Transforms

```python
class WordCount(beam.PTransform):
    """Reusable transform: lines → (word, count) pairs."""
    def expand(self, pcoll):
        return (
            pcoll
            | beam.FlatMap(str.split)
            | beam.Map(lambda w: (w.lower(), 1))
            | beam.CombinePerKey(sum)
        )

# Usage — exactly like a built-in transform:
lines | "MyWordCount" >> WordCount()
```

---

## Custom PipelineOptions

```python
from apache_beam.options.pipeline_options import PipelineOptions

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument("--input",      required=True)
        parser.add_argument("--output",     required=True)
        parser.add_argument("--batch_size", type=int, default=100)

# CLI: python pipeline.py --input=gs://... --output=gs://... --runner=DataflowRunner
options  = PipelineOptions()
my_opts  = options.view_as(MyOptions)
print(my_opts.batch_size)   # 100
```

---

## Coders

```python
from apache_beam import coders

class MyObjectCoder(coders.Coder):
    def encode(self, value) -> bytes: ...
    def decode(self, encoded: bytes): ...
    def is_deterministic(self) -> bool: return True

# Register globally (Beam picks it up automatically for MyObject elements)
coders.registry.register_coder(MyObject, MyObjectCoder)
```

---

## Dead Letter Queue Pattern

```python
SUCCESS, DLQ = "success", "dlq"

class SafeDoFn(beam.DoFn):
    def process(self, element):
        try:
            yield transform(element)          # → main output
        except Exception as e:
            yield beam.pvalue.TaggedOutput(DLQ, {
                "payload": str(element),
                "error":   str(e),
            })

results  = pcoll | beam.ParDo(SafeDoFn()).with_outputs(DLQ, main=SUCCESS)
good     = results[SUCCESS]   # → BigQuery
failures = results[DLQ]       # → GCS / DLQ table
```

---

## Dataflow Runner Flags (Most Used)

```bash
python pipeline.py \
  --runner=DataflowRunner \
  --project=YOUR_PROJECT \
  --region=us-central1 \
  --temp_location=gs://BUCKET/temp \
  --max_num_workers=20 \
  --machine_type=n1-standard-4 \
  --streaming \                        # streaming jobs only
  --enable_streaming_engine \          # streaming jobs only (recommended)
  --service_account_email=SA@P.iam.gserviceaccount.com
```
