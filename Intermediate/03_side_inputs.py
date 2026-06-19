"""
Intermediate 03 — Side Inputs

Concepts:
  - AsSingleton  → Pass a single scalar value as a side input
  - AsDict       → Pass a key-value PCollection as a dictionary
  - AsList       → Pass a PCollection as a list
  - The pattern: use side inputs for lookup tables / reference data

Side inputs are a core Beam pattern. They allow a small, slowly changing
PCollection (e.g., a config table, an exchange rate table) to be broadcast
to all workers so that the main PCollection can join against it without
a full GroupByKey shuffle.

Run:
  python Intermediate/03_side_inputs.py
"""

import apache_beam as beam
from apache_beam.pvalue import AsSingleton, AsDict, AsList


def run():
    transactions = [
        {"id": "t1", "user_id": "u1", "amount_usd": 100.0},
        {"id": "t2", "user_id": "u2", "amount_usd": 250.0},
        {"id": "t3", "user_id": "u3", "amount_usd": 50.0},
        {"id": "t4", "user_id": "u1", "amount_usd": 75.0},
        {"id": "t5", "user_id": "u4", "amount_usd": 300.0},  # unknown user
    ]

    # Reference data — small, fits in memory on each worker.
    user_profiles = [
        ("u1", {"name": "Alice",   "country": "US",  "discount": 0.10}),
        ("u2", {"name": "Bob",     "country": "UK",  "discount": 0.05}),
        ("u3", {"name": "Carol",   "country": "DE",  "discount": 0.15}),
    ]

    # Exchange rates vs USD
    fx_rates = [
        ("US", 1.00),
        ("UK", 0.79),
        ("DE", 0.92),
    ]

    vip_threshold = [500.0]  # Singleton: total spend to qualify as VIP

    with beam.Pipeline() as p:

        # ── Build the side input PCollections ───────────────────────────
        si_users    = p | "CreateUsers"     >> beam.Create(user_profiles)
        si_fx       = p | "CreateFX"        >> beam.Create(fx_rates)
        si_threshold= p | "CreateThreshold" >> beam.Create(vip_threshold)
        main        = p | "CreateTxns"      >> beam.Create(transactions)

        # ── Enrich transactions with side inputs ─────────────────────────
        def enrich_transaction(txn, users_dict, fx_dict, vip_limit):
            user_id = txn["user_id"]
            profile = users_dict.get(user_id)

            if profile is None:
                yield {**txn, "enriched": False, "name": "UNKNOWN",
                       "amount_local": txn["amount_usd"], "currency": "USD",
                       "is_vip": False}
                return

            country = profile["country"]
            fx = fx_dict.get(country, 1.0)
            discounted = txn["amount_usd"] * (1 - profile["discount"])
            local_amount = discounted * fx

            yield {
                **txn,
                "enriched": True,
                "name": profile["name"],
                "country": country,
                "discount": profile["discount"],
                "amount_local": round(local_amount, 2),
                "currency": country,
                "is_vip": txn["amount_usd"] >= vip_limit,
            }

        enriched = (
            main
            | "EnrichTxns" >> beam.FlatMap(
                enrich_transaction,
                users_dict=AsDict(si_users),    # PCollection → dict
                fx_dict=AsDict(si_fx),          # PCollection → dict
                vip_limit=AsSingleton(si_threshold),  # PCollection → scalar
            )
        )

        enriched | "Print" >> beam.Map(print)


if __name__ == "__main__":
    run()
