# Troubleshooting the Dataflow tutorial

## Worker startup fails with `ZONE_RESOURCE_POOL_EXHAUSTED`

This error means Compute Engine cannot supply the requested worker VM in the selected
zone at that time. It is a capacity failure; changing storage IAM or increasing a quota
does not make that capacity available.

Inspect the failed job's worker type and shuffle mode:

```sh
gcloud dataflow jobs describe JOB_ID --full \
  --project=project_name --region=asia-southeast1 \
  --format='yaml(currentState,environment.shuffleMode,environment.workerPools.machineType,environment.workerPools.zone)'
```

Keep `--region=asia-southeast1` and omit `--zone`, `--worker_zone`, and
`--worker_region`. Google recommends regional placement with Dataflow Shuffle for batch
jobs. Shuffle is enabled by default for this batch pipeline; `SERVICE_BASED` in the job
description confirms it. Regional placement improves availability but cannot guarantee
capacity. See [Dataflow regional placement](https://docs.cloud.google.com/dataflow/docs/concepts/regional-endpoints#regional_placement).

If the failed job used `n1-standard-1`, retry with `e2-standard-2`, which is also the worker
type used by this repository's Composer DAG. This requests a different VM family and more
CPU/memory per worker, so its resource usage and cost differ. Availability is not guaranteed.
Google lists changing the worker machine type as a
[worker-pool failure recovery option](https://docs.cloud.google.com/dataflow/docs/guides/common-errors#worker-pool-failure).

From the repository root, with the build-along environment variables still set and the
previous job in `JOB_STATE_FAILED`, submit a new attempt:

```sh
export TUTORIAL_ATTEMPT="$(date -u +%Y%m%d%H%M%S)"
export TUTORIAL_PROCESSING_ID="lesson-001-attempt-${TUTORIAL_ATTEMPT}"

python -m complaint_pipeline.beam_pipeline \
  --config_json "$(cat config/local.json)" \
  --manifest_uri "$TUTORIAL_MANIFEST" \
  --processing_id "$TUTORIAL_PROCESSING_ID" \
  --runner DataflowRunner \
  --project "$TUTORIAL_PROJECT" \
  --region asia-southeast1 \
  --job_name "complaints-lesson-001-${TUTORIAL_ATTEMPT}" \
  --temp_location "gs://${TUTORIAL_TEMP_BUCKET}/dataflow/temp" \
  --staging_location "gs://${TUTORIAL_TEMP_BUCKET}/dataflow/staging" \
  --service_account_email "$TUTORIAL_WORKER" \
  --worker_machine_type e2-standard-2 \
  --num_workers 1 --max_num_workers 2 \
  --setup_file ./setup.py \
  --requirements_file ./docker/requirements-dataflow.txt
```

Reuse the existing Bronze manifest and tables. Use the new `TUTORIAL_PROCESSING_ID` in
subsequent staging checks and publication; replace the tutorial's literal
`lesson-001-attempt-1` where necessary. Wait for `JOB_STATE_DONE` before publishing.
If capacity is still unavailable, wait and resubmit with another fresh attempt ID.
A failed job does not resume automatically. Keep the existing region for this tutorial
so the workers stay near its buckets and BigQuery datasets.

## Unable to open a staging `pipeline.pb` file

`pipeline.pb` is the staged Beam pipeline definition. If Dataflow cannot read it, startup can
fail before the record transformations run. A successful upload by your local identity does
not prove that the Dataflow worker can read the same object.

First check that the exact object from the error exists. Replace the example object path:

```sh
gcloud storage objects describe \
  'gs://project_name-pipeline-temp/dataflow/staging/JOB_STAGING_DIRECTORY/pipeline.pb'
```

This uses your local `gcloud` identity. If it reports an access error, existence remains
unconfirmed. If it succeeds, inspect the failed job with the full view:

```sh
gcloud dataflow jobs describe JOB_ID --full \
  --project=project_name --region=asia-southeast1
```

The full view can itself fail with a useful message identifying the worker account and the
missing permission, for example `storage.objects.get`. Use the account named in that error,
not an assumed default account. Check its bucket and project grants:

```sh
gcloud storage buckets get-iam-policy gs://project_name-pipeline-temp
gcloud projects get-iam-policy project_name
```

For a confirmed missing object permission on this pipeline's staging/temp bucket, give the
configured worker object read/write/delete access at that bucket:

```sh
gcloud storage buckets add-iam-policy-binding gs://project_name-pipeline-temp \
  --member='serviceAccount:dataflow-worker@project_name.iam.gserviceaccount.com' \
  --role=roles/storage.objectUser
```

The worker also requires the Dataflow Worker role on the job's project. If absent, grant it:

```sh
gcloud projects add-iam-policy-binding project_name \
  --member='serviceAccount:dataflow-worker@project_name.iam.gserviceaccount.com' \
  --role=roles/dataflow.worker
```

These grants serve different purposes: bucket object access permits reading the staged graph
and managing temporary data; Dataflow Worker permits execution of Dataflow work units. A role
with a similar display name is not evidence that the required object permission is present.
See Google's [Dataflow permissions guidance](https://docs.cloud.google.com/dataflow/docs/concepts/security-and-permissions)
and [permission troubleshooting](https://docs.cloud.google.com/dataflow/docs/guides/troubleshoot-permissions).

Allow the IAM change to propagate, then resubmit the original Beam command using a fresh
`--job_name` and `--processing_id`. The already-captured Bronze manifest can be reused.
A failed job does not automatically restart when its permissions change. Publish only after
the new job reaches `JOB_STATE_DONE`.

If the error persists, inspect the new job's full error for the named identity and resource.
Check conditional or inherited IAM policies, deny policies, and any security perimeter indicated
by the logs. Do not solve a worker object-access problem by granting project-wide Owner or
Storage Admin. An object that is actually absent requires correcting staging/upload or cleanup
behavior rather than changing permissions.

For this repository, keep `config/local.json`, the explicit manual `--service_account_email`
flag, and the Composer configuration aligned. Manual Beam launches take their execution
identity from the Beam flag; editing the JSON field alone does not change that launch flag.
