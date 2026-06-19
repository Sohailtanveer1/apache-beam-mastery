"""
Intermediate 06 — Schemas and Coders

Source: https://beam.apache.org/documentation/programming-guide/#schemas
        https://beam.apache.org/documentation/programming-guide/#data-encoding-and-type-safety

SCHEMAS
-------
A schema is a type description for PCollection elements expressed as
named fields. Beam infers schemas from:
  - typing.NamedTuple  (recommended, most Pythonic)
  - @dataclasses.dataclass with @beam.dataclasses_schema annotation
  - beam.Row           (ad-hoc schema-aware rows)

Schemas unlock SQL-style field access, beam.Select, beam.Filter by
field expression, and cross-language compatibility.

CODERS
------
A Coder tells Beam how to serialize/deserialize PCollection elements
when shuffling data between workers. Beam picks coders automatically
for most types; you only need custom coders for non-standard classes.

Run:
  python Intermediate/06_schemas_and_coders.py
"""

import typing
import apache_beam as beam
from apache_beam import coders
from apache_beam.typehints import schemas


# ──────────────────────────────���──────────────────────────────��───────────────
# SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

# ── 1. NamedTuple schema (most common pattern) ────────────────────────────
# Beam auto-registers a schema for any NamedTuple with type annotations.

class Transaction(typing.NamedTuple):
    order_id:   str
    user_id:    str
    product_id: str
    amount:     float
    currency:   str
    is_refund:  bool


class Product(typing.NamedTuple):
    product_id: str
    category:   str
    price_usd:  float


# ── 2. beam.Row — lightweight anonymous schema row ─────────────────────────
# Use when you don't want to define a class upfront.

def make_row(data: dict):
    return beam.Row(
        order_id=data["order_id"],
        amount=float(data["amount"]),
    )


# ── 3. Schema-aware transforms: beam.Select ───────────────────────────────
# beam.Select projects named fields from a schema PCollection.
# No lambda needed — operates on field names directly.

def run_schemas():
    sample_transactions = [
        Transaction("o1", "u1", "p1", 120.0,  "USD", False),
        Transaction("o2", "u2", "p2", 450.0,  "EUR", False),
        Transaction("o3", "u1", "p1", -50.0,  "USD", True),   # refund
        Transaction("o4", "u3", "p3", 1200.0, "GBP", False),
    ]

    with beam.Pipeline() as p:
        txns = p | "CreateTxns" >> beam.Create(sample_transactions)

        # ── beam.Select: project specific fields ──────────────────────
        projected = (
            txns
            | "SelectFields" >> beam.Select("order_id", "amount", "currency")
        )
        projected | "PrintProjected" >> beam.Map(
            lambda r: print(f"[Select] {r}")
        )

        # ── Filter using schema field expression ──────────────────────
        purchases = (
            txns
            | "FilterRefunds" >> beam.Filter(lambda t: not t.is_refund)
            | "FilterLarge"   >> beam.Filter(lambda t: t.amount > 100)
        )
        purchases | "PrintPurchases" >> beam.Map(
            lambda t: print(f"[Purchase] {t.order_id}: ${t.amount} {t.currency}")
        )

        # ── Convert to beam.Row for ad-hoc processing ──────────────────
        as_rows = (
            txns
            | "ToRows" >> beam.Map(lambda t: beam.Row(
                order_id=t.order_id,
                amount_cents=int(t.amount * 100),
            ))
        )
        as_rows | "PrintRows" >> beam.Map(lambda r: print(f"[Row] {r}"))


# ─────────────────────────────────────────────────────────────────────────────
# CODERS
# ─────────────────────────────────────────────────────────────────────────────

# ── Custom Coder for a non-standard class ────────────────────────────────

class Point:
    """Simple 2D point — not a NamedTuple, so needs a custom coder."""
    def __init__(self, x: float, y: float):
        self.x = x
        self.y = y

    def __repr__(self):
        return f"Point({self.x}, {self.y})"


class PointCoder(coders.Coder):
    """Encodes Point as 'x,y' UTF-8 bytes."""

    def encode(self, value: Point) -> bytes:
        return f"{value.x},{value.y}".encode("utf-8")

    def decode(self, encoded: bytes) -> Point:
        x, y = encoded.decode("utf-8").split(",")
        return Point(float(x), float(y))

    def is_deterministic(self) -> bool:
        # Must return True for use as GroupByKey key coder
        return True


# Register globally so Beam picks it up automatically for Point elements
coders.registry.register_coder(Point, PointCoder)


def run_coders():
    points = [Point(1.0, 2.5), Point(3.0, 4.0), Point(-1.5, 0.0)]

    with beam.Pipeline() as p:
        result = (
            p
            | "CreatePoints" >> beam.Create(points).with_output_types(Point)
            | "Scale"        >> beam.Map(lambda pt: Point(pt.x * 2, pt.y * 2))
        )
        result | "Print" >> beam.Map(lambda pt: print(f"[Coder] {pt}"))


if __name__ == "__main__":
    print("=== Schemas Demo ===")
    run_schemas()
    print("\n=== Coders Demo ===")
    run_coders()
