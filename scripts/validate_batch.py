import sys
import great_expectations as gx
import pandas as pd

csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/reference/ecommerce_orders_10k_updated.csv"

context = gx.get_context(project_root_dir=".")


data_source = context.data_sources.get("orders_datasource")
data_asset = data_source.get_asset("orders_asset")
batch_definition = data_asset.get_batch_definition("orders_batch")

df = pd.read_csv(csv_path)
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

suite = context.suites.get("orders_contract")

result = batch.validate(suite)

print(f"\nValidating: {csv_path}")
print(f"Overall success: {result.success}")

for r in result.results:
    status = "PASS" if r.success else "FAIL"
    column = r.expectation_config.kwargs.get("column", "")
    exp_type = r.expectation_config.type
    print(f"  [{status}] {exp_type} — {column}")
