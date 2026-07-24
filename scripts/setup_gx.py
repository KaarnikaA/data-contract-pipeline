"""
Sets up our Great Expectations project and defines the data contract
for the e-commerce orders dataset.

Safe to re-run anytime - it'll reuse the datasource/asset if they already
exist, and always rebuilds the contract (suite) fresh so edits take effect.
"""

import great_expectations as gx
import pandas as pd

# this creates (or loads, if it already exists) the GX project folder
context = gx.get_context(project_root_dir=".")

# point GX at our reference csv as a pandas datasource
# (wrapped in try/except because GX 1.x errors out if these already exist,
# and re-running this script should just reuse them instead of failing)
try:
    data_source = context.data_sources.add_pandas(name="orders_datasource")
except Exception:
    data_source = context.data_sources.get("orders_datasource")

try:
    data_asset = data_source.add_dataframe_asset(name="orders_asset")
except Exception:
    data_asset = data_source.get_asset("orders_asset")

try:
    batch_definition = data_asset.add_batch_definition_whole_dataframe("orders_batch")
except Exception:
    batch_definition = data_asset.get_batch_definition("orders_batch")

# load the reference data so we can build the suite against real values
df = pd.read_csv("data/reference/ecommerce_orders_10k_updated.csv")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# always start fresh - delete the old contract if it exists, then rebuild it
try:
    context.suites.delete("orders_contract")
except Exception:
    pass

suite = context.suites.add(gx.ExpectationSuite(name="orders_contract"))

suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="user_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeOfType(column="user_id", type_="int64")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="product_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="category")
)
suite.add_expectation(
    gx.expectations.ExpectColumnDistinctValuesToBeInSet(
        column="category",
        value_set=["Sports", "Home", "Electronics", "Clothing", "Toys", "Grocery", "Books", "Beauty"]
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="price")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="price", min_value=0.01, max_value=100000)
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="qty")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="qty", min_value=1, max_value=1000)
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="total_price")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="total_price", min_value=0.01, max_value=1000000)
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_date")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="country")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_segment")
)
suite.add_expectation(
    gx.expectations.ExpectColumnDistinctValuesToBeInSet(
        column="customer_segment",
        value_set=["Low-Value", "Mid-Value", "High-Value"]
    )
)

print(f"Contract '{suite.name}' created with {len(suite.expectations)} rules.")