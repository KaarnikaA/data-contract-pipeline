"""
Creates a few corrupted versions of the reference data, to prove our
data contract actually catches real problems.

Each one gets saved separately so you can swap them into data/incoming/
one at a time and watch the DAG fail for a different reason each time.
"""

import pandas as pd

df = pd.read_csv("data/reference/ecommerce_orders_10k_updated.csv")

# 1. schema drift - rename a column, like an upstream system silently changed its field name
schema_drift = df.copy()
schema_drift = schema_drift.rename(columns={"customer_segment": "customer_tier"})
schema_drift.to_csv("data/incoming/corrupted_schema_drift.csv", index=False)

# 2. null spike - wipe out a chunk of a required column
null_spike = df.copy()
null_spike.loc[null_spike.sample(frac=0.15, random_state=1).index, "price"] = None
null_spike.to_csv("data/incoming/corrupted_null_spike.csv", index=False)

# 3. distribution shift - prices suddenly 50x higher, like a currency or unit bug upstream
distribution_shift = df.copy()
distribution_shift["price"] = distribution_shift["price"] * 50
distribution_shift.to_csv("data/incoming/corrupted_distribution_shift.csv", index=False)

print("Created 3 corrupted batches in data/incoming/")