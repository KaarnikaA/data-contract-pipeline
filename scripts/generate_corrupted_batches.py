import pandas as pd

df = pd.read_csv("data/reference/ecommerce_orders_10k_updated.csv")


schema_drift = df.copy()
schema_drift = schema_drift.rename(columns={"customer_segment": "customer_tier"})
schema_drift.to_csv("data/incoming/corrupted_schema_drift.csv", index=False)


null_spike = df.copy()
null_spike.loc[null_spike.sample(frac=0.15, random_state=1).index, "price"] = None
null_spike.to_csv("data/incoming/corrupted_null_spike.csv", index=False)

distribution_shift = df.copy()
distribution_shift["price"] = distribution_shift["price"] * 50
distribution_shift.to_csv("data/incoming/corrupted_distribution_shift.csv", index=False)

print("Created 3 corrupted batches in data/incoming/")
