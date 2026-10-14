# Inspect and test the SQL pipeline

Use [the build-along](build-along.md) for the main tutorial. This companion separates offline
transport checks from actual SQL execution: Python does not emulate BigQuery normalization.

## Inspect a synthetic capture offline

```sh
pip install -e '.[gcp,dev]'
complaint-pipeline capture-fixture --input examples/complaints.json
complaint-pipeline prepare-load --manifest work/bronze/run_id=demo/manifest.json \
  --processing-id offline-1 --output work/raw-pages-offline-1.jsonl
```

Open the manifest and prepared file. The fixture contains four records: three valid and one
with invalid timestamp, reopen count and coordinates. The prepared file has one raw page row
and keeps all four records intact. It does not label accepted/rejected records yet.
`prepare-load` refuses to overwrite an output file; choose a new output path to repeat it.

## Read the SQL

1. `docs/sql/01_tables.sql`: raw/outcome/receipt tables and managed Iceberg tables.
2. `docs/sql/02_normalize.sql`: page assertions, JSON expansion, typed values and rejection reasons.
3. `docs/sql/03_publish.sql`: quality gates, ticket deduplication, Silver/Gold DML and receipt.

The canonical scripts are under `src/complaint_pipeline/sql/`. Generate runnable copies with
`render-sql --config config/local.json`; the rendered documentation copies use `project_name`.
The test suite verifies these copies match the templates.

## Run local tests

```sh
python -m unittest discover -s tests -v
ruff check .
```

Tests cover bounded capture, extraction resume, byte integrity, raw transport, load-job
reconnection/failure, config migration and SQL rendering. The live BigQuery test is skipped
unless explicitly enabled. These tests do not pretend a Python implementation validates SQL.

## Run the BigQuery SQL integration test

With ADC, billing, and `bigquery.jobs.create` in your project:

```sh
RUN_BIGQUERY_SQL_TESTS=1 TEST_BIGQUERY_PROJECT=project_name \
  python -m unittest discover -s tests -p test_sql_integration.py -v
```

This runs the actual normalization and publication scripts using temporary standard tables.
It tests invalid types, timezone requirements, coordinate handling, numeric overflow/NaN,
unknown statuses, stable business hashes, normalization retry, ticket deduplication, metric
reconciliation, quality-gate rollback and publication replay. It creates no persistent datasets or buckets; queries
may incur charges and have a 1 GB billed-bytes cap.

The temporary-table test validates SQL logic. It does not validate managed Iceberg IAM,
physical files or Composer deployment. Complete the manual build-along to verify those in
your own environment. The synthetic test permits a high rejection fraction solely so it can
test publication alongside intentionally malformed records; keep the default 5% for real runs.
