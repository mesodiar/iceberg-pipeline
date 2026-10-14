"""Render reviewable SQL and publish completed batches with bound query parameters."""

from dataclasses import asdict
from importlib.resources import files
from string import Template

from . import CONTRACT_VERSION
from .normalize import DATA_FIELDS, STAGING_FIELDS, load_manifest, policy_hash
from .storage import Store


def ddl_columns(fields):
    types = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL"}
    result = []
    for field in fields:
        kind = types.get(field[1], field[1])
        if len(field) == 3:
            kind = f"ARRAY<{kind}>"
        result.append(f"{field[0]} {kind}")
    return ",\n  ".join(result)


def render_sql(name, config):
    if name not in {"bootstrap", "publish"}:
        raise ValueError("Unknown SQL script")
    template = files("complaint_pipeline").joinpath("sql", name + ".sql").read_text()
    names = [field[0] for field in DATA_FIELDS]
    substitutions = asdict(config) | {
        "warehouse_root": config.warehouse_root.rstrip("/"),
        "staging_columns": ddl_columns(STAGING_FIELDS),
        "data_columns": ddl_columns(DATA_FIELDS),
        "data_names": ", ".join(names),
        "source_names": ", ".join(f"source.{name}" for name in names),
        "update_columns": ",\n    ".join(f"{name} = source.{name}" for name in names),
    }
    return Template(template).substitute(substitutions)


def publish(config, manifest_uri, processing_id):
    from google.cloud import bigquery

    config.require_cloud()
    manifest, manifest_hash = load_manifest(Store(config.project_name), manifest_uri)
    values = [
        ("run_id", "STRING", manifest["run_id"]),
        ("processing_id", "STRING", processing_id),
        ("manifest_sha256", "STRING", manifest_hash),
        ("policy_hash", "STRING", policy_hash(config)),
        ("contract_version", "STRING", CONTRACT_VERSION),
        ("expected_count", "INT64", manifest["record_count"]),
        ("observed_at", "TIMESTAMP", manifest["observed_at"]),
        ("coverage", "STRING", manifest["coverage"]),
        ("max_rejected_fraction", "FLOAT64", config.max_rejected_fraction),
    ]
    client = bigquery.Client(project=config.project_name, location=config.region)
    job = client.query(
        render_sql("publish", config),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(*value) for value in values],
            maximum_bytes_billed=config.max_bytes_billed,
            labels={"pipeline": "complaints", "stage": "publish"},
        ),
    )
    job.result()
    return job.job_id
