"""Render reviewable SQL and publish completed batches with bound query parameters."""

import re
from dataclasses import asdict
from importlib.resources import files

from . import CONTRACT_VERSION
from .config import safe_run_id
from .extract import digest
from .manifest import load_manifest
from .schema import DATA_FIELDS, RAW_FIELDS, STAGING_FIELDS
from .storage import Store, json_bytes


def policy_hash(config):
    return digest(
        json_bytes(
            {
                "contract_version": CONTRACT_VERSION,
                "normalization_sql_sha256": digest(
                    files("complaint_pipeline").joinpath("sql", "normalize.sql").read_bytes()
                ),
                "coordinate_order": config.coordinate_order,
                "closed_states": sorted(config.closed_states),
                "open_states": sorted(config.open_states),
            }
        )
    )


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
    if name not in {"bootstrap", "normalize", "publish"}:
        raise ValueError("Unknown SQL script")
    template = files("complaint_pipeline").joinpath("sql", name + ".sql").read_text()
    names = [field[0] for field in DATA_FIELDS]
    substitutions = asdict(config) | {
        "warehouse_root": config.warehouse_root.rstrip("/"),
        "staging_columns": ddl_columns(STAGING_FIELDS),
        "staging_names": ", ".join(field[0] for field in STAGING_FIELDS),
        "raw_columns": ddl_columns(RAW_FIELDS),
        "data_columns": ddl_columns(DATA_FIELDS),
        "data_names": ", ".join(names),
        "source_names": ", ".join(f"source.{name}" for name in names),
        "update_columns": ",\n    ".join(f"{name} = source.{name}" for name in names),
    }
    # Replace identifiers only; JSON paths and regex dollar signs remain literal SQL.
    return re.sub(r"\$\{([a-z_]+)\}", lambda match: substitutions[match[1]], template)


def query_parameters(config, manifest_uri, processing_id, stage):
    from google.cloud import bigquery

    config.require_cloud()
    safe_run_id(processing_id)
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
    parameters = [bigquery.ScalarQueryParameter(*value) for value in values]
    if stage == "normalize":
        parameters.extend(
            [
                bigquery.ScalarQueryParameter("expected_pages", "INT64", len(manifest["pages"])),
                bigquery.ScalarQueryParameter(
                    "coordinate_order", "STRING", config.coordinate_order
                ),
                bigquery.ArrayQueryParameter(
                    "closed_states", "STRING", sorted(config.closed_states)
                ),
                bigquery.ArrayQueryParameter("open_states", "STRING", sorted(config.open_states)),
            ]
        )
    return parameters


def configure_query_sql(sql, config):
    """Resolve table identifiers in the expanded lesson SQL using local configuration.

    Already-rendered identifiers are retained. Query parameters, JSON paths and ordinary
    string values are not substituted; this handles the lesson's backtick table references.
    """
    datasets = {
        "staging": config.staging_dataset,
        "silver": config.silver_dataset,
        "gold": config.gold_dataset,
        "ops": config.ops_dataset,
    }

    def replace_table(match):
        dataset = datasets.get(match[1], match[1])
        return f"`{config.project_name}.{dataset}.{match[2]}`"

    return re.sub(
        r"`project_name\.([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)`",
        replace_table,
        sql,
    )


def run_sql(config, manifest_uri, processing_id, stage):
    from google.cloud import bigquery

    if stage not in {"normalize", "publish"}:
        raise ValueError("Expected normalize or publish")
    parameters = query_parameters(config, manifest_uri, processing_id, stage)
    client = bigquery.Client(project=config.project_name, location=config.region)
    job = client.query(
        render_sql(stage, config),
        job_config=bigquery.QueryJobConfig(
            query_parameters=parameters,
            maximum_bytes_billed=config.max_bytes_billed,
            labels={"pipeline": "complaints", "stage": stage},
        ),
    )
    job.result()
    return job.job_id


def publish(config, manifest_uri, processing_id):
    return run_sql(config, manifest_uri, processing_id, "publish")
