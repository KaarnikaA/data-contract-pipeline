"""
Compares the incoming batch against our reference data to check for
statistical drift - catches things like a price distribution shifting
even when every individual value still looks "valid" (which is exactly
the gap our GX contract can't catch, as we proved in Step 7).
"""

import sys
import pandas as pd

from evidently import Report, Dataset, DataDefinition
from evidently.presets import DataDriftPreset

incoming_path = sys.argv[1] if len(sys.argv) > 1 else "data/incoming/orders_batch.csv"

reference_df = pd.read_csv("data/reference/ecommerce_orders_10k_updated.csv")
current_df = pd.read_csv(incoming_path)

numeric_columns = ["price", "qty", "total_price"]
data_definition = DataDefinition(numerical_columns=numeric_columns)

reference_dataset = Dataset.from_pandas(reference_df, data_definition=data_definition)
current_dataset = Dataset.from_pandas(current_df, data_definition=data_definition)

report = Report(metrics=[DataDriftPreset()])
result = report.run(reference_data=reference_dataset, current_data=current_dataset)

result_dict = result.dict()

# the first metric in a DataDriftPreset report is always DriftedColumnsCount
# it looks like {'count': X, 'share': Y} - X is how many columns drifted
drift_summary = result_dict["metrics"][0]["value"]
drifted_count = drift_summary["count"]
drifted_share = drift_summary["share"]

print(f"Checking drift for: {incoming_path}")
print(f"Drifted columns: {drifted_count} ({drifted_share:.0%} of checked columns)")

result.save_html("data/reports/drift_report.html")
print("Full report saved to data/reports/drift_report.html")

if drifted_count > 0:
    raise ValueError(f"Data drift detected: {drifted_count} column(s) drifted out of 3 checked.")