"""Transport verified page envelopes into BigQuery; business logic lives in SQL."""

from .config import safe_run_id
from .extract import digest
from .manifest import load_manifest, verified_page
from .schema import RAW_FIELDS
from .storage import Store, join, json_bytes


def prepare_load(store, manifest_uri, processing_id):
    """One NDJSON row per page, preserving raw_json exactly (no record conversion)."""
    safe_run_id(processing_id)
    manifest, manifest_hash = load_manifest(store, manifest_uri)
    lines = []
    for page in manifest["pages"]:
        envelope = verified_page(store, page)
        lines.append(
            json_bytes(
                {
                    "run_id": manifest["run_id"],
                    "processing_id": processing_id,
                    "manifest_sha256": manifest_hash,
                    "source_uri": page["uri"],
                    "envelope_sha256": page["sha256"],
                    "body_sha256": page["body_sha256"],
                    "fetched_at": page["fetched_at"],
                    "record_count": page["record_count"],
                    "raw_json": envelope["raw_json"],
                }
            )
        )
    return b"\n".join(lines) + b"\n"


def load_raw(config, manifest_uri, processing_id):
    from google.api_core.exceptions import Conflict
    from google.cloud import bigquery

    config.require_cloud()
    store = Store(config.project_name)
    payload = prepare_load(store, manifest_uri, processing_id)
    # Content-addressed, immutable transport file; a lost response can safely reuse it.
    payload_hash = digest(payload)
    uri = join(config.bronze_root, "_loads", payload_hash + ".jsonl")
    try:
        store.put_once(uri, payload)
    except FileExistsError:
        if store.read(uri) != payload:
            raise ValueError("Existing load file differs from verified Bronze bytes") from None
    destination = f"{config.project_name}.{config.staging_dataset}.raw_pages"
    job_id = "complaints_load_" + digest(json_bytes([destination, payload_hash]))
    client = bigquery.Client(project=config.project_name, location=config.region)
    try:
        job = client.load_table_from_uri(
            uri,
            destination,
            job_id=job_id,
            job_config=bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField(*field) for field in RAW_FIELDS],
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                create_disposition="CREATE_NEVER",
                write_disposition="WRITE_APPEND",
                max_bad_records=0,
                ignore_unknown_values=False,
                labels={"pipeline": "complaints", "stage": "load"},
            ),
        )
    except Conflict:
        job = client.get_job(job_id, location=config.region)
    job.result()  # Failed jobs require a new processing ID; never proceed to normalization.
    return job.job_id
