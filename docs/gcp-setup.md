# GCP setup reference: BigQuery SQL pipeline

For commands with explanations and checkpoints, use [the build-along](build-along.md).
The application does not provision infrastructure. Use `project_name` in shared examples and
put your actual identifiers in ignored `config/local.json`.

## Resources

Keep buckets, datasets, the connection and jobs in one supported region (`asia-southeast1`
in the tutorial). Create these resources yourself:

| Resource | Purpose |
| --- | --- |
| Raw GCS bucket | Immutable API page envelopes, manifests, NDJSON load files |
| Warehouse GCS bucket | Exclusively managed Iceberg data and metadata |
| `staging` dataset | Ordinary `raw_pages`, `complaint_rows`, and `rejected_rows` view |
| `silver` dataset | Managed Iceberg current state and observation history |
| `gold` dataset | Managed Iceberg sample metrics and observed transitions |
| `ops` dataset | Ordinary publication receipts |
| Cloud resource connection | BigQuery's delegated access to warehouse objects |
| Composer environment/account | Optional until manual end-to-end flow works |

Required APIs: Storage, BigQuery, BigQuery Connection, IAM and Composer. Composer may require
additional service-agent setup under your organization's policy. There is no application
Dataflow worker account, pipeline-temp bucket, Artifact Registry repository or Flex Template.

## Access matrix

| Identity | Required application access |
| --- | --- |
| Manual extractor / loader | Raw bucket object read/create; BigQuery Job User on project; staging Data Editor |
| Manual SQL runner | BigQuery Job User; read/write on staging, Silver, Gold and ops |
| Composer environment account | Composer Worker; BigQuery Job User; four datasets Data Editor; raw bucket object read/create |
| Connection service account | Storage Object User and Storage Legacy Bucket Reader on warehouse bucket |
| Table setup operator | Dataset/table creation plus connection delegation permission |
| Environment creator | Composer environment creation and permission to act as environment account |

The runtime pipeline does not directly manipulate warehouse objects. The connection service
account does. Refer to [managed Iceberg requirements](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#required_roles)
and [Composer service-account setup](https://docs.cloud.google.com/composer/docs/composer-3/create-environments).
Use ADC locally and the attached environment identity in Composer; do not store key files.

## Table setup

Install `.[gcp]`, edit your local config, then render and review the table DDL:

```sh
mkdir -p work/sql
complaint-pipeline render-sql --config config/local.json --name bootstrap > work/sql/01_tables.sql
bq --project_id=project_name --location=asia-southeast1 query \
  --use_legacy_sql=false < work/sql/01_tables.sql
```

All Iceberg storage prefixes must be empty and nonoverlapping at first creation. Existing
schemas are not migrated by `CREATE TABLE IF NOT EXISTS`. Staging retention is 30 days from
observation time; adjust it before replaying older captures.

## Application commands after learning the manual steps

These commands reuse the visible SQL from the build-along. They are useful for automation,
not a replacement for the tutorial's individual `bq load` and query examples.

```sh
complaint-pipeline extract --config config/local.json --run-id sql-sample-001

export MANIFEST_URI="gs://project_name-raw/bronze/traffy/run_id=sql-sample-001/manifest.json"
export PROCESSING_ID="sql-sample-001-attempt-1"
complaint-pipeline load-raw --config config/local.json \
  --manifest "$MANIFEST_URI" --processing-id "$PROCESSING_ID"
complaint-pipeline normalize --config config/local.json \
  --manifest "$MANIFEST_URI" --processing-id "$PROCESSING_ID"
complaint-pipeline publish --config config/local.json \
  --manifest "$MANIFEST_URI" --processing-id "$PROCESSING_ID"
```

Wait for each command to succeed before continuing. `normalize` now submits BigQuery SQL;
it no longer writes local accepted/quarantine files. Inspect `staging.rejected_rows` for
invalid records. Serialize publishers, including manual commands and other DAGs.

## Composer deployment

Follow Chapter 8 in the build-along. Deploy `src/complaint_pipeline/` (including `sql/`) next
to the DAG in the environment's DAG bucket. Install compatible Storage and BigQuery Python
clients, set `complaint_pipeline_config` in Airflow Variables, and manually trigger the DAG.
No Beam runtime or image build is involved. Configure network egress to the public API.

The DAG uses Airflow 2.x APIs. Choose a supported Composer 3 image with Airflow 2.x explicitly.
It keeps `schedule=None`, `catchup=False`, and `max_active_runs=1` until you choose a schedule.
