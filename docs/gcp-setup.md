# Manual GCP setup and deployment

This guide gives you the resource contract and runnable application commands. It does not
automatically create projects, buckets, datasets, connections, IAM bindings, or Composer.
`project_name` is deliberately a placeholder and is not a valid project ID as written.
Change it everywhere in your local configuration and command examples.

## 1. Resources you create

Use one supported region for buckets, datasets, the BigQuery connection, Dataflow, and Composer.
The example uses `asia-southeast1`; check availability in your own project.

| Resource | Example / purpose |
| --- | --- |
| GCP project with billing | `project_name` |
| Raw bucket | `gs://project_name-raw`: Bronze and quarantine, private access |
| Warehouse bucket | `gs://project_name-warehouse`: exclusively BigQuery-managed Iceberg data |
| Temporary bucket | `gs://project_name-pipeline-temp`: Dataflow staging, load jobs, templates |
| BigQuery datasets | `staging`, `silver`, `gold`, `ops`, in the chosen region |
| Cloud resource connection | `project_name.asia-southeast1.lakehouse_connection` |
| Dataflow worker account | `dataflow-worker@project_name.iam.gserviceaccount.com` |
| Composer environment account | Your dedicated orchestration service account |
| Artifact Registry Docker repository | `pipeline-images` in your chosen region |
| Composer environment | Create when you are ready to test orchestration |

Enable the APIs needed for BigQuery, BigQuery Connection, BigQuery Storage, Cloud Storage,
Dataflow, Compute Engine, Artifact Registry, and Composer. Cloud Build is optional if you build
containers with your own Docker daemon. Set billing alerts and retain the default small batch
and worker limits during tutorial development.

Create each Iceberg table in its own **empty, nonoverlapping** warehouse prefix. Do not write
or delete warehouse objects directly or apply generic object-age deletion rules there.
BigQuery owns these files. See the official [managed Iceberg setup and limitations](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery).

## 2. Access contract

Assign only the access each identity needs; your organization's IAM policy may use custom roles.
These are application requirements, in addition to standard service-agent/environment setup.

| Identity | Access needed |
| --- | --- |
| BigQuery connection service account | `roles/storage.objectUser` and `roles/storage.legacyBucketReader` on the warehouse bucket |
| Dataflow worker | Dataflow Worker; BigQuery Job User; staging dataset Data Editor; raw bucket read; quarantine/temp object create/read/delete; Artifact Registry Reader |
| Composer environment account | Raw objects read/create; Dataflow job create/read/cancel; Service Account User on the worker account; template/temp and image read access; BigQuery Job User; staging read; Silver/Gold/ops read and DML |
| Manual runner / publisher | Corresponding extraction, Dataflow-launch, worker impersonation, staging, and publication permissions |
| Table-setup operator | Dataset table creation plus permission to delegate/use the Cloud resource connection |

The [official connection IAM requirements](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#required_roles)
describe the connection service account grants. BigQuery operations use that connection to
manage warehouse files. The Dataflow pipeline writes only the ordinary staging table and
quarantine/temp objects; it does not write Iceberg storage directly.

Configure network egress to the public API for the extractor. Flex launchers and workers need
Google API access and Python package access because this template stages a package and cloud
dependencies at launch. If using a private subnet, provide the appropriate Google API routing,
NAT/package mirror, and worker-to-worker firewall access. `subnetwork` accepts the full regional
subnetwork URL. Offline custom worker containers are a future deployment option.

Use application default credentials for manual development (`gcloud auth application-default login`)
and attached service accounts in Composer/Dataflow. No service account key files are needed.

## 3. Configuration and table DDL

```sh
cp config/example.json config/local.json
```

Edit every resource identifier, `project_name`, region, worker email, and `flex_template_uri`.
Bucket names must be globally unique. Review status labels and coordinate order against a small
sample. Keep the file out of Git. Install the cloud/Beam dependencies in Python 3.11:

```sh
pip install -e '.[gcp,beam,dev]'
mkdir -p work
complaint-pipeline render-sql --config config/local.json --name bootstrap > work/bootstrap.sql
```

Review `work/bootstrap.sql` and execute it in the BigQuery console or, after replacing the
command placeholders:

```sh
bq --project_id=project_name --location=asia-southeast1 query \
  --use_legacy_sql=false < work/bootstrap.sql
```

`CREATE TABLE IF NOT EXISTS` is for initial setup, not schema migrations. It does not repair
pre-existing tables with a different schema. Silver and Gold use `WITH CONNECTION`, Parquet
storage, and Iceberg format. Publication uses documented [BigQuery multi-statement transactions](https://docs.cloud.google.com/bigquery/docs/transactions).

## 4. First manual batch

Capture into the configured GCS Bronze location:

```sh
complaint-pipeline extract --config config/local.json --run-id sample-001
```

It prints the committed manifest URI. Pass that exact URI to Dataflow. The following command
uses your installed Python package to submit the batch directly, before introducing Composer:

```sh
python -m complaint_pipeline.beam_pipeline \
  --config_json "$(cat config/local.json)" \
  --manifest_uri gs://project_name-raw/bronze/traffy/run_id=sample-001/manifest.json \
  --processing_id sample-001-attempt-1 \
  --runner DataflowRunner \
  --project project_name \
  --region asia-southeast1 \
  --job_name complaints-sample-001 \
  --temp_location gs://project_name-pipeline-temp/dataflow/temp \
  --staging_location gs://project_name-pipeline-temp/dataflow/staging \
  --service_account_email dataflow-worker@project_name.iam.gserviceaccount.com \
  --num_workers 1 --max_num_workers 2 \
  --setup_file ./setup.py \
  --requirements_file ./docker/requirements-dataflow.txt
```

Set `--subnetwork` here as well when your network requires it. The command submits without
waiting. **Wait until the job is `JOB_STATE_DONE` in Dataflow before publishing.** Do not
publish merely because a staging row count looks complete. The batch also writes quarantine.

For a startup error involving `pipeline.pb`, see the
[staging-bucket permission checks](troubleshooting.md).

```sh
complaint-pipeline publish \
  --config config/local.json \
  --manifest gs://project_name-raw/bronze/traffy/run_id=sample-001/manifest.json \
  --processing-id sample-001-attempt-1
```

The publication binds values as query parameters and sets `maximum_bytes_billed` from config.
It does not create tables or other infrastructure. Replay the identical `publish` command to
verify that Silver, Gold, and the receipt do not gain duplicate records.

## 5. Build the Flex Template

Build from the repository root, targeting the Dataflow Linux architecture. These commands
build/push an application image and template into resources you already created:

```sh
gcloud auth configure-docker asia-southeast1-docker.pkg.dev
docker buildx build --platform linux/amd64 \
  --file docker/Dockerfile \
  --tag asia-southeast1-docker.pkg.dev/project_name/pipeline-images/complaints:0.1.0 \
  --push .

gcloud dataflow flex-template build \
  gs://project_name-pipeline-temp/templates/complaints.json \
  --project=project_name \
  --image=asia-southeast1-docker.pkg.dev/project_name/pipeline-images/complaints:0.1.0 \
  --sdk-language=PYTHON \
  --metadata-file=docker/metadata.json
```

The image has the package and Beam SDK installed. `setup.py` and the requirements file stage
the code and cloud dependencies onto the default SDK workers. The launcher submits and exits;
it never waits for the job. This follows the [Flex Template packaging guidance](https://docs.cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates).
Pin the launcher base image to a tested digest for longer-term reproducibility; the starter
Dockerfile uses the official launcher `latest` tag and pins Beam to 2.77.0.

## 6. Deploy the Composer DAG

Use a Composer image with a compatible Airflow 2.10+ API and Google provider that exposes
`DataflowStartFlexTemplateOperator(wait_until_finished=True)`. Check the provider version
included in your chosen image; do not install standalone Airflow into a managed Composer environment.

Composer needs the cloud storage and BigQuery Python clients, but not Apache Beam. They are
usually present through the Google provider; check and add the `gcp` extra's dependencies if
needed. Copy these paths into the environment's DAGs folder, preserving the package hierarchy:

```text
dags/
  complaint_medallion.py
  complaint_pipeline/
    __init__.py
    config.py
    extract.py
    normalize.py
    storage.py
    warehouse.py
    sql/
      bootstrap.sql
      publish.sql
```

The package content comes from `src/complaint_pipeline/`. Keep Python files and SQL resources
on the same code version as the Flex Template. Prefer immutable image versions for changes.
In the Airflow UI, create a Variable named `complaint_pipeline_config` containing the full
JSON from `config/local.json`. The DAG reads it at task execution time.

Trigger `complaint_medallion` manually. It performs:

```text
prepare: read config → bounded API capture → immutable manifest
normalize: launch Flex Template → wait for successful Dataflow completion
publish: validate staging → commit Silver + Gold + publication receipt
```

The DAG has `schedule=None`, `catchup=False`, and `max_active_runs=1`. Once source access and
sampling behavior are understood, choose your own schedule. Clearing/retrying the same Airflow
run reuses its capture; triggering a new run fetches a new sample. Changing the logical date
does not backfill historical source state. For historical Bronze replay, use the saved manifest
through the manual Dataflow and publish commands.

## 7. Cloud smoke tests after setup

1. Inspect `ops.published_runs`: input = accepted + rejected; distinct tickets may be lower
   than accepted rows because offset pagination can repeat tickets.
2. Repeat publication and confirm all published row counts remain unchanged.
3. Replay the same manifest with a fresh processing ID, wait for Dataflow, then republish;
   the existing receipt still makes publication a no-op.
4. Try publishing an unpublished manifest before all staging rows exist. The assertion must
   fail and Silver/Gold must remain unchanged.
5. Use a separate synthetic cloud capture with invalid data to test quarantine and the quality
   threshold. The offline fixture has 25% invalid rows, so the default 5% gate intentionally
   rejects it. Use a separate tutorial config if you want to publish that fixture.
6. Publish a later observation and then replay an older one. Current state must retain the
   highest source version, while observation history keeps both.

Keep a failed Dataflow attempt's staging/quarantine for diagnosis. Publish only a completed
attempt. An attempt uses an explicit `processing_id`; a different ID isolates a rerun after
code fixes. Avoid concurrent manual publication. Remove Composer when you no longer need the
tutorial environment, and manage raw/temp retention separately from Iceberg table maintenance.
