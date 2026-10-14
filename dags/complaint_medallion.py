"""Manual-trigger Composer DAG: extract -> raw load -> SQL normalize -> SQL publish."""

import hashlib
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from complaint_pipeline.config import Config
from complaint_pipeline.extract import capture
from complaint_pipeline.ingest import load_raw
from complaint_pipeline.warehouse import policy_hash, run_sql


def extract_batch(**context):
    config = Config.from_dict(Variable.get("complaint_pipeline_config", deserialize_json=True))
    config.require_cloud()
    suffix = hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
    run_id = "capture-" + suffix
    # Stable across retries; a changed normalization policy gets a different attempt.
    processing_id = "sql-" + suffix + "-" + policy_hash(config)[:12]
    return {
        "config": asdict(config),
        "manifest_uri": capture(config, run_id),
        "processing_id": processing_id,
    }


def execute_stage(stage, **context):
    prepared = context["ti"].xcom_pull(task_ids="extract")
    config = Config.from_dict(prepared["config"])
    args = (config, prepared["manifest_uri"], prepared["processing_id"])
    if stage == "load_raw":
        return load_raw(*args)
    return run_sql(*args, stage)


with DAG(
    dag_id="complaint_medallion",
    description="API samples transformed with BigQuery SQL into Iceberg Silver and Gold",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["bigquery", "iceberg", "tutorial"],
) as dag:
    extraction = PythonOperator(task_id="extract", python_callable=extract_batch)
    loading = PythonOperator(
        task_id="load_raw", python_callable=execute_stage, op_kwargs={"stage": "load_raw"}
    )
    normalization = PythonOperator(
        task_id="normalize_sql", python_callable=execute_stage, op_kwargs={"stage": "normalize"}
    )
    publication = PythonOperator(
        task_id="publish_sql", python_callable=execute_stage, op_kwargs={"stage": "publish"}
    )
    extraction >> loading >> normalization >> publication
