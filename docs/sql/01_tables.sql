-- Run manually after creating datasets, the connection, and empty warehouse prefixes.
-- All staging tables are ordinary BigQuery tables. Only Silver and Gold use Iceberg.
CREATE TABLE IF NOT EXISTS `project_name.staging.raw_pages` (
  run_id STRING,
  processing_id STRING,
  manifest_sha256 STRING,
  source_uri STRING,
  envelope_sha256 STRING,
  body_sha256 STRING,
  fetched_at TIMESTAMP,
  record_count INT64,
  raw_json STRING
)
PARTITION BY DATE(fetched_at)
CLUSTER BY run_id, processing_id
OPTIONS (partition_expiration_days = 30);

CREATE TABLE IF NOT EXISTS `project_name.staging.complaint_rows` (
  row_id STRING,
  run_id STRING,
  ticket_id STRING,
  created_at TIMESTAMP,
  source_updated_at TIMESTAMP,
  observed_at TIMESTAMP,
  state STRING,
  state_group STRING,
  organization STRING,
  categories ARRAY<STRING>,
  problem_categories ARRAY<STRING>,
  reopen_count INT64,
  star FLOAT64,
  longitude FLOAT64,
  latitude FLOAT64,
  content_hash STRING,
  source_uri STRING,
  contract_version STRING,
  policy_hash STRING,
  processing_id STRING,
  is_valid BOOL,
  rejection_reasons ARRAY<STRING>
)
PARTITION BY DATE(observed_at)
CLUSTER BY run_id, processing_id
OPTIONS (partition_expiration_days = 30);

CREATE OR REPLACE VIEW `project_name.staging.rejected_rows` AS
SELECT * FROM `project_name.staging.complaint_rows` WHERE NOT is_valid;

CREATE TABLE IF NOT EXISTS `project_name.ops.published_runs` (
  run_id STRING, manifest_sha256 STRING, policy_hash STRING, contract_version STRING,
  observed_at TIMESTAMP, published_at TIMESTAMP, input_rows INT64,
  accepted_rows INT64, rejected_rows INT64, distinct_tickets INT64,
  processing_id STRING, coverage STRING
);

CREATE TABLE IF NOT EXISTS `project_name.silver.complaints_current` (
  row_id STRING,
  run_id STRING,
  ticket_id STRING,
  created_at TIMESTAMP,
  source_updated_at TIMESTAMP,
  observed_at TIMESTAMP,
  state STRING,
  state_group STRING,
  organization STRING,
  categories ARRAY<STRING>,
  problem_categories ARRAY<STRING>,
  reopen_count INT64,
  star FLOAT64,
  longitude FLOAT64,
  latitude FLOAT64,
  content_hash STRING,
  source_uri STRING,
  contract_version STRING,
  policy_hash STRING
)
CLUSTER BY ticket_id, state_group
WITH CONNECTION `project_name.asia-southeast1.lakehouse_connection`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = 'gs://project_name-warehouse/silver/complaints_current/'
);

CREATE TABLE IF NOT EXISTS `project_name.silver.complaint_observation_history` (
  row_id STRING,
  run_id STRING,
  ticket_id STRING,
  created_at TIMESTAMP,
  source_updated_at TIMESTAMP,
  observed_at TIMESTAMP,
  state STRING,
  state_group STRING,
  organization STRING,
  categories ARRAY<STRING>,
  problem_categories ARRAY<STRING>,
  reopen_count INT64,
  star FLOAT64,
  longitude FLOAT64,
  latitude FLOAT64,
  content_hash STRING,
  source_uri STRING,
  contract_version STRING,
  policy_hash STRING
)
PARTITION BY DATE(observed_at)
CLUSTER BY ticket_id
WITH CONNECTION `project_name.asia-southeast1.lakehouse_connection`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = 'gs://project_name-warehouse/silver/complaint_observation_history/'
);

CREATE TABLE IF NOT EXISTS `project_name.gold.daily_backlog` (
  run_id STRING, observation_date DATE, observed_at TIMESTAMP, organization STRING,
  state_group STRING, tickets INT64, coverage STRING
)
PARTITION BY observation_date
WITH CONNECTION `project_name.asia-southeast1.lakehouse_connection`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = 'gs://project_name-warehouse/gold/daily_backlog/'
);

CREATE TABLE IF NOT EXISTS `project_name.gold.complaint_age_distribution` (
  run_id STRING, observation_date DATE, observed_at TIMESTAMP, age_band STRING,
  tickets INT64, coverage STRING
)
PARTITION BY observation_date
WITH CONNECTION `project_name.asia-southeast1.lakehouse_connection`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = 'gs://project_name-warehouse/gold/complaint_age_distribution/'
);

CREATE TABLE IF NOT EXISTS `project_name.gold.observed_state_changes` (
  ticket_id STRING, from_state STRING, to_state STRING,
  previous_observed_at TIMESTAMP, observed_at TIMESTAMP, run_id STRING,
  reopen_count_delta INT64
)
PARTITION BY DATE(observed_at)
CLUSTER BY ticket_id
WITH CONNECTION `project_name.asia-southeast1.lakehouse_connection`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = 'gs://project_name-warehouse/gold/observed_state_changes/'
);
