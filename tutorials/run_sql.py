"""Lesson: submit a SQL file with typed parameters and wait for its BigQuery job."""

import argparse
from pathlib import Path

from google.cloud import bigquery

from complaint_pipeline.config import Config
from complaint_pipeline.warehouse import query_parameters

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--processing-id", required=True)
    parser.add_argument("--stage", choices=["normalize", "publish"], required=True)
    parser.add_argument("--sql", required=True)
    args = parser.parse_args()
    config = Config.load(args.config)
    # Values come from the committed manifest/config; identifiers are in the SQL file.
    # query_parameters returns typed ScalarQueryParameter / ArrayQueryParameter objects.
    parameters = query_parameters(config, args.manifest, args.processing_id, args.stage)
    sql = Path(args.sql).read_text(encoding="utf-8")
    client = bigquery.Client(project=config.project_name, location=config.region)
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=parameters,
            maximum_bytes_billed=config.max_bytes_billed,
            labels={"pipeline": "complaints", "stage": args.stage},
        ),
    )
    print(f"BigQuery job: {job.job_id}", flush=True)
    job.result()
    print("Completed successfully")
