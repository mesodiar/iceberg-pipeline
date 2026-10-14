# Troubleshooting the BigQuery SQL tutorial

## Raw load fails

Confirm `staging.raw_pages` exists in the same region as the load job and source bucket.
Run the new bootstrap DDL if upgrading. Load the prepared NDJSON file, not the original nested
API response or the manifest. Each NDJSON row holds a whole response in `raw_json`.

Use the declared raw schema; do not autodetect complaint fields, skip bad rows, or ignore
unknown fields. The caller needs BigQuery Job User, staging write permissions and source-object
read access. Being able to upload as one identity does not prove Composer can read it.

The application loader uses deterministic job IDs. On an uncertain submission it reconnects
to the existing job. A terminally failed load needs a new processing ID after fixing its cause.
Manual `bq load` can append duplicates; normalization tolerates identical page copies.

## Manifest, page, or checksum assertion fails

Python checks original envelope and body bytes before loading. SQL checks loaded body hashes,
API response shape, manifest identity, page count, record count and latest fetch time. Never
repair a committed capture by overwriting its page files. Inspect the exact manifest URI,
run ID and processing ID; use a fresh capture if its original files are corrupt.

Raw and outcome tables expire partitions after 30 days from observation time. Increase the
retention deliberately before reprocessing older observations; otherwise their partitions
can expire immediately. A published receipt remains authoritative even after staging expires.

## SQL completes but records are rejected

```sql
SELECT ticket_id, source_uri, rejection_reasons
FROM `project_name.staging.rejected_rows`
WHERE run_id = 'sql-lesson-001' AND processing_id = 'sql-lesson-001-attempt-1';
```

Check timezone-bearing timestamps, timestamp order, string/array categories, nonnegative
INT64 reopen counts, finite ratings in 0..5 and coordinate order/ranges. Unknown status labels
are retained as `unknown`. SQL helper functions and their validation rules are in
`src/complaint_pipeline/sql/normalize.sql`. Update the template and regenerate your local SQL
copy together, using a new attempt for an unpublished capture.

## Publication quality gate fails

Reconcile accepted plus rejected records against the manifest. The default rejected fraction
is 5%. The four-row fixture deliberately has one invalid record, so it fails this gate.
Inspect failures rather than raising the production threshold simply to make a job succeed.
The isolated SQL integration test uses a permissive threshold only for synthetic assertions.
Staging stays available when publication fails; Silver, Gold and the receipt roll back together.

## A published run cannot be replaced

The receipt binds a run ID to its manifest and SQL policy. A changed policy, SQL template or
contract cannot silently replace it. Use a new capture for a new observation or design an
explicit historical migration. See [the migration guide](migration-to-bigquery-sql.md).

## SQL access, region or cost errors

Check the query location, project, dataset permissions and `max_bytes_billed` in local config.
A query cost cap can stop a correct query; review its estimate and data volume before changing
the cap. The connection service account needs its warehouse bucket grants; the SQL caller
needs job and dataset access. The build-along keeps all locations in one region.

## Composer import or task failure

Upload the entire `complaint_pipeline` package including its SQL subdirectory beside the DAG.
Use the documented Airflow 2.x image, compatible Storage/BigQuery dependencies, and a valid
`complaint_pipeline_config` variable. Inspect the failed task's BigQuery job ID and wait for
that job's final state before retrying. If a SQL request timed out while its job is still
running, wait for completion before manually clearing/retrying tasks. Do not overlap manual
publication and DAG publication.

## Previous Dataflow errors

`pipeline.pb` access and `ZONE_RESOURCE_POOL_EXHAUSTED` errors belong to the retired Dataflow
path. The current tutorial submits BigQuery load/query jobs and does not start worker VMs.
Follow [migration](migration-to-bigquery-sql.md) rather than relaunching the former Beam command.
