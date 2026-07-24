"""
Checks whatever CSV is sitting in data/incoming/ against our data contract
(orders_contract, defined in scripts/setup_gx.py), then checks it for
statistical drift against the reference data (Evidently), then alerts
on Slack if either check failed.

Flow: validate_orders_contract -> check_data_drift -> send_slack_alert
                                                        (only runs if
                                                         either check failed)

To test: drop a CSV into data/incoming/orders_batch.csv and trigger this DAG.
"""

import os
from datetime import datetime

import great_expectations as gx
import pandas as pd
import requests
from dotenv import load_dotenv

from evidently import Report, Dataset, DataDefinition
from evidently.presets import DataDriftPreset

from airflow import DAG
from airflow.operators.python import PythonOperator

INCOMING_FILE = "/home/kaarvin/projects/data-contract-pipeline/data/incoming/orders_batch.csv"
REFERENCE_FILE = "/home/kaarvin/projects/data-contract-pipeline/data/reference/ecommerce_orders_10k_updated.csv"
GX_PROJECT_DIR = "/home/kaarvin/projects/data-contract-pipeline"
DRIFT_REPORT_OUT = "/home/kaarvin/projects/data-contract-pipeline/data/incoming/drift_report.html"
ENV_FILE = "/home/kaarvin/projects/data-contract-pipeline/.env"


def validate_incoming_batch(**context):
    """Runs the orders_contract suite against the incoming file and fails the task if the contract is broken."""

    df = pd.read_csv(INCOMING_FILE)

    gx_context = gx.get_context(project_root_dir=GX_PROJECT_DIR)

    data_source = gx_context.data_sources.get("orders_datasource")
    data_asset = data_source.get_asset("orders_asset")
    batch_definition = data_asset.get_batch_definition("orders_batch")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    suite = gx_context.suites.get("orders_contract")
    result = batch.validate(suite)

    print(f"Validating: {INCOMING_FILE}")
    print(f"Overall success: {result.success}")

    failed_rules = []
    for r in result.results:
        status = "PASS" if r.success else "FAIL"
        column = r.expectation_config.kwargs.get("column", "")
        exp_type = r.expectation_config.type
        print(f"  [{status}] {exp_type} — {column}")
        if not r.success:
            failed_rules.append(f"{exp_type} on column '{column}'")

    if not result.success:
        # push the failure reason to XCom so the Slack task can read it later
        context["ti"].xcom_push(key="failure_reason", value=f"Data contract violated: {failed_rules}")
        raise ValueError(f"Data contract violated. Failed rules: {failed_rules}")


def check_data_drift(**context):
    """Compares the incoming batch against reference data for statistical drift and fails the task if anything drifted."""

    reference_df = pd.read_csv(REFERENCE_FILE)
    current_df = pd.read_csv(INCOMING_FILE)

    numeric_columns = ["price", "qty", "total_price"]
    data_definition = DataDefinition(numerical_columns=numeric_columns)

    reference_dataset = Dataset.from_pandas(reference_df, data_definition=data_definition)
    current_dataset = Dataset.from_pandas(current_df, data_definition=data_definition)

    report = Report(metrics=[DataDriftPreset()])
    result = report.run(reference_data=reference_dataset, current_data=current_dataset)

    result_dict = result.dict()
    drift_summary = result_dict["metrics"][0]["value"]
    drifted_count = drift_summary["count"]
    drifted_share = drift_summary["share"]

    print(f"Checking drift for: {INCOMING_FILE}")
    print(f"Drifted columns: {drifted_count} ({drifted_share:.0%} of checked columns)")

    result.save_html(DRIFT_REPORT_OUT)
    print(f"Full report saved to {DRIFT_REPORT_OUT}")

    if drifted_count > 0:
        context["ti"].xcom_push(
            key="failure_reason",
            value=f"Data drift detected in {int(drifted_count)} column(s) ({drifted_share:.0%} of checked columns)"
        )
        raise ValueError(f"Data drift detected: {drifted_count} column(s) drifted.")


def send_slack_alert(**context):
    """Sends a Slack message summarizing what failed. Only runs if validate_orders_contract or check_data_drift failed."""

    load_dotenv(ENV_FILE)
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        raise ValueError("SLACK_WEBHOOK_URL not found - check your .env file")

    ti = context["ti"]

    # pull whatever failure reason either upstream task pushed - only one
    # of these will actually have a value, since only one (or both) failed
    gx_reason = ti.xcom_pull(task_ids="validate_orders_contract", key="failure_reason")
    drift_reason = ti.xcom_pull(task_ids="check_data_drift", key="failure_reason")

    reasons = [r for r in [gx_reason, drift_reason] if r]
    reason_text = "\n".join(reasons) if reasons else "A pipeline task failed, but no specific reason was recorded."

    message = f":rotating_light: *Data pipeline alert - orders_contract_check*\n{reason_text}"

    response = requests.post(webhook_url, json={"text": message})

    if response.status_code == 200:
        print("Slack alert sent successfully")
    else:
        print(f"Failed to send Slack alert: {response.status_code} - {response.text}")


with DAG(
    dag_id="orders_contract_check",
    description="Validates incoming orders data against our data contract, checks for drift, alerts on Slack if either fails",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-contract", "portfolio-project"],
) as dag:

    validate_task = PythonOperator(
        task_id="validate_orders_contract",
        python_callable=validate_incoming_batch,
    )

    drift_task = PythonOperator(
        task_id="check_data_drift",
        python_callable=check_data_drift,
    )

    slack_alert_task = PythonOperator(
        task_id="send_slack_alert",
        python_callable=send_slack_alert,
        trigger_rule="one_failed",  # runs if EITHER upstream task failed, unlike the default rule
    )

    validate_task >> drift_task >> slack_alert_task



# """
# Checks whatever CSV is sitting in data/incoming/ against our data contract
# (orders_contract, defined in scripts/setup_gx.py), then checks it for
# statistical drift against the reference data (Evidently).

# Flow: validate_orders_contract -> check_data_drift

# If either fails, the task goes red in Airflow (this is what will
# trigger the Slack alert in Step 9).

# To test: drop a CSV into data/incoming/orders_batch.csv and trigger this DAG.
# """

# from datetime import datetime
# import great_expectations as gx
# import pandas as pd

# from evidently import Report, Dataset, DataDefinition
# from evidently.presets import DataDriftPreset

# from airflow import DAG
# from airflow.operators.python import PythonOperator

# INCOMING_FILE = "/home/kaarvin/projects/data-contract-pipeline/data/incoming/orders_batch.csv"
# REFERENCE_FILE = "/home/kaarvin/projects/data-contract-pipeline/data/reference/ecommerce_orders_10k_updated.csv"
# GX_PROJECT_DIR = "/home/kaarvin/projects/data-contract-pipeline"
# DRIFT_REPORT_OUT = "/home/kaarvin/projects/data-contract-pipeline/data/incoming/drift_report.html"


# def validate_incoming_batch():
#     """Runs the orders_contract suite against the incoming file and fails the task if the contract is broken."""

#     df = pd.read_csv(INCOMING_FILE)

#     context = gx.get_context(project_root_dir=GX_PROJECT_DIR)

#     data_source = context.data_sources.get("orders_datasource")
#     data_asset = data_source.get_asset("orders_asset")
#     batch_definition = data_asset.get_batch_definition("orders_batch")
#     batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

#     suite = context.suites.get("orders_contract")
#     result = batch.validate(suite)

#     print(f"Validating: {INCOMING_FILE}")
#     print(f"Overall success: {result.success}")

#     failed_rules = []
#     for r in result.results:
#         status = "PASS" if r.success else "FAIL"
#         column = r.expectation_config.kwargs.get("column", "")
#         exp_type = r.expectation_config.type
#         print(f"  [{status}] {exp_type} — {column}")
#         if not r.success:
#             failed_rules.append(f"{exp_type} on column '{column}'")

#     if not result.success:
#         raise ValueError(f"Data contract violated. Failed rules: {failed_rules}")


# def check_data_drift():
#     """Compares the incoming batch against reference data for statistical drift and fails the task if anything drifted."""

#     reference_df = pd.read_csv(REFERENCE_FILE)
#     current_df = pd.read_csv(INCOMING_FILE)

#     numeric_columns = ["price", "qty", "total_price"]
#     data_definition = DataDefinition(numerical_columns=numeric_columns)

#     reference_dataset = Dataset.from_pandas(reference_df, data_definition=data_definition)
#     current_dataset = Dataset.from_pandas(current_df, data_definition=data_definition)

#     report = Report(metrics=[DataDriftPreset()])
#     result = report.run(reference_data=reference_dataset, current_data=current_dataset)

#     result_dict = result.dict()
#     drift_summary = result_dict["metrics"][0]["value"]
#     drifted_count = drift_summary["count"]
#     drifted_share = drift_summary["share"]

#     print(f"Checking drift for: {INCOMING_FILE}")
#     print(f"Drifted columns: {drifted_count} ({drifted_share:.0%} of checked columns)")

#     result.save_html(DRIFT_REPORT_OUT)
#     print(f"Full report saved to {DRIFT_REPORT_OUT}")

#     if drifted_count > 0:
#         raise ValueError(f"Data drift detected: {drifted_count} column(s) drifted out of {len(numeric_columns)} checked.")


# with DAG(
#     dag_id="orders_contract_check",
#     description="Validates incoming orders data against our data contract and checks for statistical drift",
#     start_date=datetime(2026, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["data-contract", "portfolio-project"],
# ) as dag:

#     validate_task = PythonOperator(
#         task_id="validate_orders_contract",
#         python_callable=validate_incoming_batch,
#     )

#     drift_task = PythonOperator(
#         task_id="check_data_drift",
#         python_callable=check_data_drift,
#     )

#     validate_task >> drift_task


