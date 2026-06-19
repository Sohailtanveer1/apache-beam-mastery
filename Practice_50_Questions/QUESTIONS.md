# 50 Apache Beam Practice Questions

Build real muscle memory by solving these exercises from scratch using the **Apache Beam Python SDK**.  
All questions assume you have `apache-beam[gcp]` installed. Use `DirectRunner` unless stated otherwise.

> **Tip:** For each question, write the complete, runnable pipeline — not just the transform.  
> Add solutions to `solutions/q_XX_slug.py` and submit a PR!

---

## Beginner (Questions 1–20)

Focus: DirectRunner, Create, Map, FlatMap, Filter, Combine, ReadFromText, WriteToText, basic DoFns.

---

**Q1.** Write a pipeline that reads a list of integers from `beam.Create`, filters out all even numbers, and prints the remaining odd numbers.

---

**Q2.** Write a `ParDo` (using a `DoFn`) that takes a list of words and emits only words that are strictly longer than 3 characters. Test with the list: `["go", "beam", "is", "great", "at", "scale"]`.

---

**Q3.** Write a pipeline that reads a local text file line by line and writes only non-empty lines to a new output file.

---

**Q4.** Given a `PCollection` of `(name, score)` tuples, use `CombinePerKey` to find the **maximum** score per name.

---

**Q5.** Write a pipeline that takes a list of sentences and outputs each unique word exactly once (i.e., a distinct/deduplicate pipeline). *(Hint: GroupByKey can help.)*

---

**Q6.** Write a `DoFn` that accepts a CSV line (e.g., `"alice,29,engineer"`) and yields a Python dict: `{"name": "alice", "age": 29, "role": "engineer"}`. Handle lines with the wrong number of columns by silently skipping them.

---

**Q7.** Write a pipeline that reads numbers from `beam.Create([5, 3, 8, 1, 9, 2, 7])` and computes the **sum**, **min**, **max**, and **mean** using `CombineGlobally`. Print all four results.

---

**Q8.** Write a `FlatMap` that takes a sentence and emits every pair of consecutive words as a tuple. For example, `"the quick brown fox"` → `("the","quick"), ("quick","brown"), ("brown","fox")`.

---

**Q9.** Write a pipeline that reads a local CSV file with columns `product,price,quantity`, computes `revenue = price * quantity` per row, and writes the results as `product,revenue` to an output file.

---

**Q10.** Write a pipeline that counts the total number of elements in a `PCollection` (using `beam.combiners.Count.Globally()`) and prints it.

---

**Q11.** Write a pipeline that reads lines from a text file and outputs a `PCollection` of `(line_length, count)` pairs — i.e., a frequency distribution of line lengths.

---

**Q12.** Write a `DoFn` with a `setup()` method that loads a dictionary from a JSON file (mock it as a hard-coded dict) once per worker, and uses it inside `process()` to look up a category for each element.

---

**Q13.** Write a pipeline that takes a list of email addresses and separates them into two `PCollection`s: `valid_emails` (contain `@`) and `invalid_emails`, using `beam.partition` (the `beam.Partition` transform).

---

**Q14.** Write a pipeline that reads a GCS file (`gs://...`) using `ReadFromText` and counts the frequency of each word. Run it with `DirectRunner`. *(Replace the GCS path with a public dataset such as `gs://dataflow-samples/shakespeare/hamlet.txt`.)*

---

**Q15.** Write a pipeline that takes a `PCollection` of integers and computes a running **word count histogram**: group numbers into buckets `[0–9]`, `[10–19]`, `[20–29]`, etc., and count how many numbers fall in each bucket.

---

**Q16.** Write a pipeline that reads a list of `(country, city)` tuples and outputs a `(country, [list_of_cities])` mapping using `GroupByKey`. Sort the city list alphabetically inside a `Map`.

---

**Q17.** Write a pipeline that reads a text file and outputs the **top-10 most frequent words** using `beam.combiners.TopCombineFn` or `beam.transforms.combiners.Top.Of`.

---

**Q18.** Write a pipeline that writes a `PCollection` of dicts to a **newline-delimited JSON** file (one JSON object per line) using `WriteToText` and `json.dumps` in a `Map`.

---

**Q19.** Write a complete **word count** pipeline that:
- Reads from a text file
- Lowercases all words
- Strips punctuation
- Filters words shorter than 2 characters
- Counts occurrences
- Writes `word\tcount` pairs to an output file

---

**Q20.** Write a pipeline that takes a `PCollection` of `(user_id, page_view_count)` tuples and computes the **average** page views per user using a custom `CombineFn` (implement `create_accumulator`, `add_input`, `merge_accumulators`, `extract_output`).

---

## Intermediate (Questions 21–40)

Focus: DataflowRunner, GroupByKey, CoGroupByKey, Side Inputs, Windowing (Fixed/Sliding/Session), BigQuery I/O, Pipeline Options.

---

**Q21.** Write a pipeline using `CoGroupByKey` to perform a **left join** between a `PCollection` of `(order_id, customer_id)` and a `PCollection` of `(customer_id, customer_name)`. Output `(order_id, customer_name_or_UNKNOWN)`.

---

**Q22.** Implement a **sliding window** of 5 minutes updating every 1 minute (`SlidingWindows(300, 60)`) on a simulated stream of `(user_id, click)` events, and count clicks per user per window.

---

**Q23.** Write a pipeline that uses a **side input** (`AsDict`) to enrich a `PCollection` of product records with their category, where the category lookup table is a separate `PCollection` of `(product_id, category)` tuples.

---

**Q24.** Write a pipeline with a `FixedWindows(60)` transform on a timestamped `PCollection`. Add a `DoFn` that reads `beam.DoFn.WindowParam` and outputs the window's start and end times alongside the aggregated value.

---

**Q25.** Write a pipeline that reads from a BigQuery table using `ReadFromBigQuery` (use a `query=` parameter with standard SQL) and writes the result to a GCS text file.

---

**Q26.** Write a pipeline that writes a `PCollection` of dicts to BigQuery using `WriteToBigQuery` with `WRITE_TRUNCATE` disposition. Define the schema programmatically as a Python dict.

---

**Q27.** Implement **Session Windows** (`Sessions(gap_duration=120)`) on a `PCollection` of `(user_id, event)` timestamped events. Count events per user per session and output sessions with more than 3 events.

---

**Q28.** Write a pipeline that takes two `PCollection`s — `orders` and `cancellations` — both keyed by `order_id`, and uses `CoGroupByKey` to identify orders that appear in `orders` but NOT in `cancellations`.

---

**Q29.** Write a `DoFn` that uses a **singleton side input** (`AsSingleton`) to receive a global discount rate (a `float`) and applies it to every `amount` field in the main `PCollection`.

---

**Q30.** Write a pipeline that reads a CSV from GCS, applies a `Map` transform to parse rows into dicts, and writes each row to **dynamically determined BigQuery tables** based on the value of a `region` field (e.g., rows with `region=us` go to `dataset.events_us`).

---

**Q31.** Configure a pipeline with **Dataflow-specific pipeline options** programmatically (not via CLI): set `machine_type`, `max_num_workers`, `region`, `job_name`, and `temp_location`. Print the resolved options object.

---

**Q32.** Write a pipeline that reads a `PCollection` of `(key, value)` pairs, applies `GroupByKey`, and then flattens the result back into individual `(key, value)` pairs — essentially a no-op roundtrip to understand GBK mechanics.

---

**Q33.** Write a pipeline that reads from two different GCS files, creates two `PCollection`s, and **flattens** them into a single `PCollection` using `beam.Flatten`. Then count the total elements.

---

**Q34.** Implement a **trigger** strategy on a `FixedWindows(60)` window that fires an early (speculative) result every 10 seconds of processing time AND a final result at the watermark, using `AfterWatermark(early=AfterProcessingTime(10))`.

---

**Q35.** Write a pipeline that reads a BigQuery table, applies a `Filter` to keep only rows where `status == 'ACTIVE'`, enriches each row with `processed_at = datetime.now()`, and writes back to a different BigQuery table.

---

**Q36.** Write a pipeline that implements **k-way merge**: given 3 `PCollection`s of integers, merge them and output only integers that appear in **at least 2** of the 3 collections. *(Hint: tag each collection, `CoGroupByKey`, count non-empty tag lists.)*

---

**Q37.** Write a pipeline that reads a `PCollection` of log lines, uses `FlatMap` with a regex to extract `(ip_address, status_code)` pairs, and uses `CombinePerKey` with a custom `CombineFn` to compute the **error rate** (4xx+5xx / total) per IP.

---

**Q38.** Write a `DoFn` that uses a **list side input** (`AsList`) to receive a list of blocked user IDs and filters them out of the main `PCollection` of events.

---

**Q39.** Write a pipeline that applies a `Map` transform to convert Unix timestamps (integers) into ISO-8601 date strings (`YYYY-MM-DD`), then groups records by date using `GroupByKey` and counts events per day.

---

**Q40.** Write a pipeline that reads from a BigQuery table using `ReadFromBigQuery`, computes the sum of a `revenue` column per `category` using `CombinePerKey`, and writes the result to a new BigQuery table. Submit it to **Dataflow** using the CLI flags.

---

## Advanced (Questions 41–50)

Focus: Streaming (Pub/Sub), State & Timers API, Custom CombineFns, Dead Letter Queues, Performance, Dataflow Flex Templates.

---

**Q41.** Create a **stateful DoFn** that tracks the **running average** of a sensor metric (keyed by sensor ID). The DoFn should emit an alert tuple `(sensor_id, "ALERT", avg)` whenever the running average exceeds a configurable threshold (pass the threshold as a constructor argument).

---

**Q42.** Write a streaming pipeline that reads from Pub/Sub, applies a `FixedWindows(30)` window, and writes the **count of messages per window** to a BigQuery table. Include a DLQ for unparseable messages.

---

**Q43.** Implement a stateful DoFn using `BagStateSpec` that **deduplicates** events within a 5-minute window by `event_id`. Explain (in a comment) the memory implications of using BagState in a long-running streaming job.

---

**Q44.** Write a stateful DoFn with an **event-time timer** (`TimerSpec` with `TimeDomain.WATERMARK`) that collects all events for a key within a window and emits a single aggregated record when the timer fires. This is the "manual windowing" pattern.

---

**Q45.** Implement a **Dead Letter Queue** pattern for a streaming pipeline: a `DoFn` that tries to call an external enrichment API (mock it with a random failure rate), routes successes to the main output and failures (with full error metadata) to a DLQ `PCollection` written to GCS.

---

**Q46.** Write a pipeline that uses the **hot key salting** technique to compute a `CombinePerKey(sum)` on a `PCollection` where 80% of records share the same key (`"hot_key"`). Measure and comment on why naive `CombinePerKey` would be slow without salting.

---

**Q47.** Implement a **two-step aggregation** pipeline:
1. Step 1: Sum `revenue` by `(product_id, region)` in 1-minute windows.
2. Step 2: Using the output of Step 1 as a side input, compute the **percentage contribution** of each product to the regional total. Output `(product_id, region, pct_of_regional_revenue)`.

---

**Q48.** Write a streaming pipeline that reads from **two Pub/Sub topics** (orders and inventory), uses `CoGroupByKey` inside a `FixedWindows(60)` window to join them by `product_id`, and emits an alert when `orders.quantity > inventory.stock`.

---

**Q49.** Write a pipeline that reads a large CSV from GCS, applies a `Reshuffle()` after the read to break fusion, then uses a **batching DoFn** (`BatchAPICallDoFn` pattern) to call a mock enrichment service in batches of 100, and writes results to BigQuery.

---

**Q50.** Design and write a **complete end-to-end streaming pipeline** that:
- Reads from a Pub/Sub subscription
- Parses and validates JSON messages (DLQ for failures)
- Deduplicates by `event_id` using `BagStateSpec` in a 10-minute window
- Applies `FixedWindows(60)` and aggregates `(event_type, count)` per window
- Writes aggregated results to BigQuery
- Writes DLQ records to GCS

This is effectively Project 3 written from scratch, without looking at the solution.

---

## Solutions

Community solutions are welcome! Add your solution to `solutions/q_XX_description.py` and open a PR.

```
solutions/
├── q_01_filter_odd_numbers.py
├── q_02_filter_words_by_length.py
├── ...
```

*Solutions for all 50 questions will be added progressively.*
