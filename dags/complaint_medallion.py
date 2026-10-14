"""Manual-trigger Composer DAG. Install the package beside this file (see docs)."""

import hashlib
import json
import uuid
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

from complaint_pipeline.config import Config
from complaint_pipeline.extract import capture
from complaint_pipeline.warehouse import publish


def prepare(**context):
    # Read Variables only at execution time, so parsing the DAG needs no cloud access.
    config = Config(**Variable.get("complaint_pipeline_config", deserialize_json=True))
    config.require_cloud()
    # Retry the same Airflow run against exactly the same immutable Bronze capture.
    # A new trigger creates a fresh sample. Logical date is not a historical API filter.
    run_id = "capture-" + hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
    manifest_uri = capture(config, run_id)
    processing_id = uuid.uuid4().hex
    environment = {
        "serviceAccountEmail": config.worker_service_account,
        "tempLocation": config.temp_root,
        "stagingLocation": config.temp_root.rstrip("/") + "/staging",
        "numWorkers": 1,
        "maxWorkers": config.max_workers,
        "machineType": "e2-standard-2",
    }
    if config.subnetwork:
        environment["subnetwork"] = config.subnetwork
    return {
        "config": asdict(config),
        "manifest_uri": manifest_uri,
        "processing_id": processing_id,
        "launch_body": {
            "launchParameter": {
                "jobName": "complaints-" + processing_id[:20],
                "containerSpecGcsPath": config.flex_template_uri,
                "parameters": {
                    "config_json": json.dumps(asdict(config), ensure_ascii=False),
                    "manifest_uri": manifest_uri,
                    "processing_id": processing_id,
                },
                "environment": environment,
            }
        },
    }


def publish_batch(**context):
    prepared = context["ti"].xcom_pull(task_ids="prepare")
    return publish(
        Config(**prepared["config"]), prepared["manifest_uri"], prepared["processing_id"]
    )


with DAG(
    dag_id="complaint_medallion",
    description="Bounded complaint samples through Bronze, staging, Silver and Gold",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["iceberg", "medallion", "tutorial"],
) as dag:
    preparation = PythonOperator(task_id="prepare", python_callable=prepare)
    dataflow = DataflowStartFlexTemplateOperator(
        task_id="normalize",
        project_id="{{ ti.xcom_pull(task_ids='prepare')['config']['project_name'] }}",
        location="{{ ti.xcom_pull(task_ids='prepare')['config']['region'] }}",
        body="{{ ti.xcom_pull(task_ids='prepare')['launch_body'] }}",
        wait_until_finished=True,
        deferrable=False,
        append_job_name=True,
    )
    publication = PythonOperator(task_id="publish", python_callable=publish_batch)
    preparation >> dataflow >> publication
