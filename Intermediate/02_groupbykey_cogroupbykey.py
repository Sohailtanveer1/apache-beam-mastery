"""
Intermediate 02 — GroupByKey and CoGroupByKey (Joins in Beam)

Concepts:
  - GroupByKey   → Groups all values for a given key (like SQL GROUP BY)
  - CoGroupByKey → Joins two or more PCollections by key (like SQL JOIN)

Run:
  python Intermediate/02_groupbykey_cogroupbykey.py
"""

import apache_beam as beam


def run_group_by_key():
    """Demonstrates GroupByKey: aggregate orders by customer."""
    print("\n=== GroupByKey Demo ===")

    orders = [
        ("alice", {"item": "laptop",   "amount": 1200.0}),
        ("bob",   {"item": "mouse",    "amount": 25.0}),
        ("alice", {"item": "keyboard", "amount": 75.0}),
        ("carol", {"item": "monitor",  "amount": 350.0}),
        ("bob",   {"item": "webcam",   "amount": 80.0}),
        ("alice", {"item": "headset",  "amount": 120.0}),
    ]

    with beam.Pipeline() as p:
        results = (
            p
            | "CreateOrders" >> beam.Create(orders)
            # GroupByKey collects all values for each key into an iterable.
            # Input:  (key, value), (key, value), ...
            # Output: (key, [value, value, ...])
            | "GroupByCustomer" >> beam.GroupByKey()
            | "SumPerCustomer" >> beam.Map(
                lambda kv: {
                    "customer": kv[0],
                    "total": sum(o["amount"] for o in kv[1]),
                    "order_count": len(list(kv[1])),
                }
            )
        )
        results | "Print" >> beam.Map(print)


def run_cogroup_by_key():
    """Demonstrates CoGroupByKey: join customers with their orders (left join)."""
    print("\n=== CoGroupByKey Demo (Left Join) ===")

    customers = [
        ("c001", {"name": "Alice",   "tier": "gold"}),
        ("c002", {"name": "Bob",     "tier": "silver"}),
        ("c003", {"name": "Carol",   "tier": "gold"}),
        ("c004", {"name": "Dave",    "tier": "bronze"}),  # no orders
    ]

    orders = [
        ("c001", {"order_id": "o1", "amount": 1200.0}),
        ("c001", {"order_id": "o2", "amount": 75.0}),
        ("c002", {"order_id": "o3", "amount": 25.0}),
        ("c003", {"order_id": "o4", "amount": 350.0}),
        # c004 intentionally has no orders
    ]

    with beam.Pipeline() as p:
        pcust   = p | "CreateCustomers" >> beam.Create(customers)
        porders = p | "CreateOrders"    >> beam.Create(orders)

        # CoGroupByKey joins on the key field.
        # The result is: (key, {"customers": [...], "orders": [...]})
        joined = (
            {"customers": pcust, "orders": porders}
            | "CoGroup" >> beam.CoGroupByKey()
        )

        def format_join(element):
            customer_id, grouped = element
            cust_info = list(grouped["customers"])
            cust_orders = list(grouped["orders"])

            if not cust_info:
                return  # Skip keys with no customer record

            name = cust_info[0]["name"]
            tier = cust_info[0]["tier"]
            total = sum(o["amount"] for o in cust_orders)
            return f"{name} ({tier}) | orders: {len(cust_orders)} | total: ${total:.2f}"

        formatted = joined | "Format" >> beam.Map(format_join)
        formatted | "Print" >> beam.Filter(bool) | "PrintF" >> beam.Map(print)


if __name__ == "__main__":
    run_group_by_key()
    run_cogroup_by_key()
