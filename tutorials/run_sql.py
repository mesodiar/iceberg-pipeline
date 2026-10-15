"""Lesson: submit a SQL file with typed parameters and wait for its BigQuery job."""

import argparse
from pathlib import Path

from google.cloud import bigquery

from complaint_pipeline.config import Config
from complaint_pipeline.warehouse import configure_query_sql, query_parameters


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--processing-id", required=True)
    parser.add_argument("--stage", choices=["normalize", "publish"], required=True)
    parser.add_argument("--sql", required=True)
    args = parser.parse_args(argv)
    config = Config.load(args.config)
    config.require_cloud()
    # Resolve placeholder table identifiers before submitting; keep the source file unchanged.
    sql = configure_query_sql(Path(args.sql).read_text(encoding="utf-8"), config)
    # Values come from the committed manifest/config, separately from table identifiers.
    # query_parameters returns typed ScalarQueryParameter / ArrayQueryParameter objects.
    print(f"sql: {sql}")

    parameters = query_parameters(config, args.manifest, args.processing_id, args.stage)
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


if __name__ == "__main__":
    main()
