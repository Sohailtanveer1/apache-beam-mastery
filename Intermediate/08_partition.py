"""
Intermediate 08 — Partition Transform

Source: https://beam.apache.org/documentation/programming-guide/#partition

beam.Partition splits one PCollection into N fixed PCollections based on
a partitioning function you provide. It is the idiomatic Beam way to
fan-out a single stream into multiple downstream paths without
re-scanning the data.

Key rule: the partitioning function must return an integer in [0, N).
          Elements returning the same integer go into the same output PCollection.

Compare:
  - beam.Filter  → 1 input → 1 output (drops elements)
  - beam.Partition → 1 input → N outputs (routes elements, no drops)
  - TaggedOutput → 1 input → N outputs (dynamic at DoFn level)

Run:
  python Intermediate/08_partition.py
"""

import apache_beam as beam


# ── Example 1: Partition by tier ─────────────────────────────────────────

TIERS = {"bronze": 0, "silver": 1, "gold": 2}

def partition_by_tier(element, num_partitions):
    """Routes customers into 3 buckets by tier."""
    return TIERS.get(element["tier"], 0)  # unknown tiers → bronze


# ── Example 2: Partition by amount range ─────────────────────────────────

def partition_by_amount(element, num_partitions):
    """
    Routes transactions into 4 buckets:
      0 → micro  (< $10)
      1 → small  ($10–$99)
      2 → medium ($100–$999)
      3 → large  ($1000+)
    """
    amount = element["amount"]
    if amount < 10:
        return 0
    elif amount < 100:
        return 1
    elif amount < 1000:
        return 2
    else:
        return 3


def run():
    customers = [
        {"id": "c1", "name": "Alice", "tier": "gold"},
        {"id": "c2", "name": "Bob",   "tier": "silver"},
        {"id": "c3", "name": "Carol", "tier": "bronze"},
        {"id": "c4", "name": "Dave",  "tier": "gold"},
        {"id": "c5", "name": "Eve",   "tier": "silver"},
        {"id": "c6", "name": "Frank", "tier": "unknown"},  # → bronze bucket
    ]

    transactions = [
        {"id": "t1", "amount": 5.0},
        {"id": "t2", "amount": 49.99},
        {"id": "t3", "amount": 250.0},
        {"id": "t4", "amount": 1500.0},
        {"id": "t5", "amount": 0.99},
        {"id": "t6", "amount": 999.0},
    ]

    with beam.Pipeline() as p:

        # ── Partition customers by tier ────────────────────────────────
        cust_pcoll = p | "CreateCustomers" >> beam.Create(customers)

        # Partition returns a tuple of N PCollections
        bronze, silver, gold = (
            cust_pcoll
            | "PartitionByTier" >> beam.Partition(partition_by_tier, 3)
        )

        bronze | "PrintBronze" >> beam.Map(lambda c: print(f"[Bronze] {c['name']}"))
        silver | "PrintSilver" >> beam.Map(lambda c: print(f"[Silver] {c['name']}"))
        gold   | "PrintGold"   >> beam.Map(lambda c: print(f"[Gold]   {c['name']}"))

        # ── Partition transactions by amount range ─────────────────────
        txn_pcoll = p | "CreateTxns" >> beam.Create(transactions)

        micro, small, medium, large = (
            txn_pcoll
            | "PartitionByAmount" >> beam.Partition(partition_by_amount, 4)
        )

        print()
        micro  | "PrintMicro"  >> beam.Map(lambda t: print(f"[Micro  <$10]    {t}"))
        small  | "PrintSmall"  >> beam.Map(lambda t: print(f"[Small  $10-$99] {t}"))
        medium | "PrintMedium" >> beam.Map(lambda t: print(f"[Medium $100-$999]{t}"))
        large  | "PrintLarge"  >> beam.Map(lambda t: print(f"[Large  $1000+]  {t}"))

        # ── Practical use: apply different logic per partition ─────────
        # Each partition is a normal PCollection — apply any transform to it.
        gold_discounted = (
            gold
            | "ApplyGoldDiscount" >> beam.Map(
                lambda c: {**c, "discount_pct": 20}
            )
        )
        gold_discounted | "PrintDiscounts" >> beam.Map(
            lambda c: print(f"[Discount] {c['name']} gets {c['discount_pct']}% off")
        )


if __name__ == "__main__":
    run()
