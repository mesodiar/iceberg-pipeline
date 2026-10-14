"""Schemas shared by table DDL and the raw-page load job."""

DATA_FIELDS = [
    ("row_id", "STRING"),
    ("run_id", "STRING"),
    ("ticket_id", "STRING"),
    ("created_at", "TIMESTAMP"),
    ("source_updated_at", "TIMESTAMP"),
    ("observed_at", "TIMESTAMP"),
    ("state", "STRING"),
    ("state_group", "STRING"),
    ("organization", "STRING"),
    ("categories", "STRING", "REPEATED"),
    ("problem_categories", "STRING", "REPEATED"),
    ("reopen_count", "INTEGER"),
    ("star", "FLOAT"),
    ("longitude", "FLOAT"),
    ("latitude", "FLOAT"),
    ("content_hash", "STRING"),
    ("source_uri", "STRING"),
    ("contract_version", "STRING"),
    ("policy_hash", "STRING"),
]
STAGING_FIELDS = DATA_FIELDS + [
    ("processing_id", "STRING"),
    ("is_valid", "BOOLEAN"),
    ("rejection_reasons", "STRING", "REPEATED"),
]


RAW_FIELDS = [
    ("run_id", "STRING"),
    ("processing_id", "STRING"),
    ("manifest_sha256", "STRING"),
    ("source_uri", "STRING"),
    ("envelope_sha256", "STRING"),
    ("body_sha256", "STRING"),
    ("fetched_at", "TIMESTAMP"),
    ("record_count", "INTEGER"),
    ("raw_json", "STRING"),
]
