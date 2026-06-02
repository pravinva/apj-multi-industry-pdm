"""
Reference Airflow DAG used as the migration source for hackathon demo.

This DAG is intentionally small but real Airflow code: three Salesforce bulk
steps (insert, upsert, update) connected in sequence.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def sf_insert_callable(**context):
    # In many teams this is custom requests/simple-salesforce code.
    # Migration target in Databricks: salesforce_api.salesforce_insert
    print("Submitting Salesforce INSERT bulk job")
    return {"operation": "insert", "status": "submitted"}


def sf_upsert_callable(**context):
    # Migration target in Databricks: salesforce_api.SalesforceUpsertOperator
    print("Submitting Salesforce UPSERT bulk job")
    return {"operation": "upsert", "status": "submitted"}


def sf_update_callable(**context):
    # Migration target in Databricks: salesforce_api.salesforce_update
    print("Submitting Salesforce UPDATE bulk job")
    return {"operation": "update", "status": "submitted"}


with DAG(
    dag_id="salesforce_bulk_migration_source",
    description="Source Airflow DAG for Salesforce migration demo",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hackathon", "salesforce", "migration"],
) as dag:
    insert_task = PythonOperator(
        task_id="insert_accounts",
        python_callable=sf_insert_callable,
    )

    upsert_task = PythonOperator(
        task_id="upsert_accounts",
        python_callable=sf_upsert_callable,
    )

    update_task = PythonOperator(
        task_id="update_accounts",
        python_callable=sf_update_callable,
    )

    insert_task >> upsert_task >> update_task
