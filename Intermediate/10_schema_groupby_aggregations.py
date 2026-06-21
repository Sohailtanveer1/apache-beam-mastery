"""
Intermediate 10 — Schema-based GroupBy and Aggregations

Source: Beam Programming Guide §6.6.2

Schema-aware GroupBy lets you group and aggregate records by field name
instead of writing manual key-extraction lambdas.

Key transforms covered:
  beam.GroupBy(field)             → group by one or more named fields
  .aggregate_field(field, fn, name) → apply a CombineFn or combine fn per group
  beam.Select(field, ...)           → project a subset of schema fields
  beam.Filter(predicate)            → filter by field expression
  beam.FlattenWith                  → union PCollections in a chain-friendly way

Prerequisites: elements must have a schema (NamedTuple or beam.Row).

Run:
  python Intermediate/10_schema_groupby_aggregations.py
"""

import typing
import apache_beam as beam
from apache_beam.transforms.combiners import CountCombineFn


# ── Define schemas with NamedTuple ────────────────────────────────────────
# Beam auto-infers schemas from NamedTuples.

class Purchase(typing.NamedTuple):
    user_id:   str
    item_id:   str
    category:  str
    region:    str
    amount:    float
    quantity:  int


PURCHASES = [
    Purchase("u1", "laptop",   "electronics", "us-east", 1200.0, 1),
    Purchase("u1", "mouse",    "electronics", "us-east",   25.0, 2),
    Purchase("u2", "shirt",    "clothing",    "eu-west",   45.0, 3),
    Purchase("u2", "headset",  "electronics", "eu-west",  150.0, 1),
    Purchase("u3", "laptop",   "electronics", "us-west", 1100.0, 1),
    Purchase("u3", "trousers", "clothing",    "us-west",   80.0, 2),
    Purchase("u1", "monitor",  "electronics", "us-east",  350.0, 1),
    Purchase("u4", "jacket",   "clothing",    "eu-west",  200.0, 1),
]


def run():
    with beam.Pipeline() as p:
        purchases = p | "CreatePurchases" >> beam.Create(PURCHASES)

        # ── 1. beam.Select — project only the fields you need ─────────
        # Output schema: Row(user_id=..., amount=...)
        print("=== Select: user_id + amount ===")
        (
            purchases
            | "SelectFields" >> beam.Select("user_id", "amount")
            | "PrintSelect"  >> beam.Map(print)
        )

        # ── 2. beam.GroupBy — simple group by one field ───────────────
        # Output: (key_row, iterable_of_purchase_rows)
        print("\n=== GroupBy category (no aggregation) ===")
        (
            purchases
            | "GroupByCategory" >> beam.GroupBy("category")
            | "FormatGrouped"   >> beam.Map(
                lambda kv: f"  {kv[0].category}: {[p.item_id for p in kv[1]]}"
            )
            | "PrintGrouped" >> beam.Map(print)
        )

        # ── 3. GroupBy with aggregate_field ───────────────────────────
        # aggregate_field(source_field, combine_fn, output_field_name)
        # The output schema is inferred from the aggregation.
        print("\n=== GroupBy user_id → count + total spend ===")
        (
            purchases
            | "AggPerUser" >> beam.GroupBy("user_id")
                .aggregate_field("item_id",  CountCombineFn(),  "num_items")
                .aggregate_field("amount",   sum,               "total_spend")
            | "PrintAgg" >> beam.Map(
                lambda r: print(
                    f"  user={r.user_id}: "
                    f"items={r.num_items}, "
                    f"total=${r.total_spend:.2f}"
                )
            )
        )

        # ── 4. GroupBy multiple fields ────────────────────────────────
        print("\n=== GroupBy category + region → count + avg amount ===")
        (
            purchases
            | "AggByCatRegion" >> beam.GroupBy("category", "region")
                .aggregate_field("item_id", CountCombineFn(), "order_count")
                .aggregate_field("amount",  sum,              "revenue")
            | "PrintCatRegion" >> beam.Map(
                lambda r: print(
                    f"  {r.category}/{r.region}: "
                    f"orders={r.order_count}, "
                    f"revenue=${r.revenue:.2f}"
                )
            )
        )

        # ── 5. beam.Filter on a schema field ─────────────────────────
        print("\n=== Filter: only electronics purchases > $100 ===")
        (
            purchases
            | "FilterElec"  >> beam.Filter(
                lambda p: p.category == "electronics" and p.amount > 100
            )
            | "PrintFilter" >> beam.Map(
                lambda p: print(f"  {p.user_id}: {p.item_id} ${p.amount}")
            )
        )

        # ── 6. beam.FlattenWith — chain-friendly union ────────────────
        # FlattenWith merges a PCollection with additional sources inline.
        extra = p | "CreateExtra" >> beam.Create([
            Purchase("u5", "book", "books", "us-east", 15.0, 1),
        ])
        print("\n=== FlattenWith extra purchases ===")
        (
            purchases
            | "AddExtra"     >> beam.FlattenWith(extra)
            | "CountAll"     >> beam.combiners.Count.Globally()
            | "PrintCount"   >> beam.Map(lambda n: print(f"  Total records: {n}"))
        )


if __name__ == "__main__":
    run()
