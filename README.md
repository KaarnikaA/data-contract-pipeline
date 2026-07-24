# Data Contract & Silent Pipeline Failure Detector

Data pipelines usually don't fail loudly. A schema change upstream, a spike in nulls, a
shifted value distribution — none of that throws an exception, it just quietly
corrupts whatever's downstream. This project catches that stuff before it spreads.

It's an Airflow pipeline that checks every incoming batch two ways: against an explicit
data contract (Great Expectations — nulls, types, ranges, allowed categories), and for
statistical drift against a reference baseline (Evidently AI). If either check fails,
it fires a Slack alert with the exact reason.



## Why two checks, not one

I built the contract first, assumed it'd catch most things, then tested it against a
simulated 50x price spike — and it passed. Every value was still technically "valid"
(not null, still under the max price ceiling), so a rule-based check had nothing to
flag. Evidently caught it immediately, because the *distribution* had clearly shifted
even though no individual value broke a rule.



That's the actual finding this project is built around: rule-based contracts and
statistical drift detection catch different classes of failure, and you need both.

## Pipeline

```
data/incoming/orders_batch.csv
        │
        ▼
validate_orders_contract   
        │ 
        ▼
check_data_drift           
        │  
        ▼
send_slack_alert            
```

## Tools

Airflow · Great Expectations · Evidently AI · Slack Incoming Webhooks · pandas


## Setup

```bash
git clone <your-repo-url>
cd data-contract-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
pip install great_expectations evidently python-dotenv requests
```

Set up Airflow:
```bash
export AIRFLOW_HOME=~/projects/data-contract-pipeline/airflow_home
airflow db migrate
airflow users create --username airflow --firstname Kar --lastname Admin \
  --role Admin --email admin@example.com --password airflow
```

Point Airflow at this repo's DAGs — edit `airflow_home/airflow.cfg`:
```
dags_folder = <absolute path to this repo>/dags
```

Add a Slack webhook — create `.env` at the project root:
```
SLACK_WEBHOOK_URL=<your incoming webhook url>
```

## Run it

```bash
python3 scripts/setup_gx.py     # builds the data contract
airflow standalone               # starts Airflow, visit localhost:8080
```

Drop a batch into `data/incoming/orders_batch.csv`, then trigger the
`orders_contract_check` DAG from the UI (or `airflow dags trigger orders_contract_check`).

To see the failure detection in action:
```bash
python3 scripts/generate_corrupted_batches.py
```
Copy any of these into `data/incoming/orders_batch.csv` and re-trigger:
- `corrupted_schema_drift.csv` — caught by Great Expectations (renamed column)
- `corrupted_null_spike.csv` — caught by Great Expectations (nulls in a required column)
- `corrupted_distribution_shift.csv` — caught by Evidently, **not** Great Expectations

## What this demonstrates

- Treating data contracts as a first-class part of the pipeline, not an afterthought
- Knowing the limits of rule-based validation and layering in drift detection to cover the gap
- Building pipelines that fail loudly and specifically instead of silently
- Using Airflow trigger rules correctly to alert on partial failure, not just total failure
