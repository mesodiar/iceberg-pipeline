# Migrate the existing Dataflow tutorial to BigQuery SQL

The pipeline now runs `extract → load_raw → normalize_sql → publish_sql`. Python transports
raw API pages; BigQuery runs all record-level processing. The previous Dataflow source,
Beam dependency, Flex container and deployment instructions have been removed from the
active project. Git history retains the earlier implementation.

## Reuse what you already created

Keep the raw/warehouse buckets, four datasets, Cloud resource connection and existing
Silver/Gold/ops tables if their schemas match `docs/sql/01_tables.sql`. Existing immutable
Bronze manifests and envelopes remain readable. The ignored local config and `docs/sql_real/`
are not modified by this migration.

1. Install `pip install -e '.[gcp,dev]'`. An existing virtual environment may still contain
   Beam; a fresh environment avoids those unused packages.
2. Review `config/example.json`. Remove retired fields from your local config when convenient:
   `temp_root`, `quarantine_root`, `worker_service_account`, `flex_template_uri`, `subnetwork`,
   `max_workers`. `Config.load` temporarily accepts these with a warning; unknown fields still fail.
3. Render the new bootstrap SQL and run it yourself. It adds `staging.raw_pages` and
   `staging.rejected_rows`; existing outcome/Silver/Gold/receipt schemas are unchanged.
4. Give the local runner/Composer identity BigQuery job and dataset permissions plus raw-bucket
   read/create access. Keep connection-account warehouse access. See the [IAM matrix](gcp-setup.md).
5. Load a capture into `raw_pages`, then run `02_normalize.sql` and `03_publish.sql` in order,
   using the same new processing ID throughout. The detailed steps are in the build-along.

## Existing captures and publication receipts

Normalization has contract version `2-sql`. Source-row and content hashes now use BigQuery's
serialization; they are not byte-compatible with the former Python hashes. The policy hash
also includes the normalization SQL file's SHA-256, classification lists and coordinate order.

An **unpublished** capture can be reused with a new processing ID. Do not publish old Beam
staging rows under the new contract. A capture **already published** under contract 1 will
correctly fail the policy guard if you try to republish it with the new SQL contract. Keep its
receipt and history; fetch a new observation using a new run ID. Rewriting old published data
requires a separate migration plan, not deleting receipts or bypassing the guard.

Existing current/history rows can coexist with new SQL observations. The current-state merge
compares source update timestamps before observation time. Identical timestamps use row IDs
as a deterministic tie-breaker, whose ordering changes across contracts.

## Update Composer after a successful manual SQL run

Pause the old DAG and wait for its active runs to finish before replacing the DAG and package.
Do not clear old Dataflow task instances into the new task layout. Upload the new package and
DAG, update the config variable, and trigger a new Airflow run. Check the four new task IDs.

The raw loader reconnects to a deterministic BigQuery job ID after a lost submission response.
If a load job itself failed, changing permissions does not revive that job ID: make a new
manual processing attempt, or trigger a fresh DAG run. SQL task retries are safe when
serialized; normalization replaces the current attempt and publication uses its receipt.

## Optional cleanup you perform

After the SQL path works, review your old Dataflow jobs, image repository, pipeline-temp bucket,
worker account and role grants. Remove resources only after confirming they are unused by
other jobs. This code change neither deletes resources nor revokes IAM. Leave managed Iceberg
warehouse files under BigQuery's control.

The old standalone diagram image, if present, describes the old pipeline. The Mermaid diagrams
in README and the build-along are the current architecture.
