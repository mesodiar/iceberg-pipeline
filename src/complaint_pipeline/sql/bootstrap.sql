-- Run manually after creating datasets, the connection, and empty warehouse prefixes.
-- All staging tables are ordinary BigQuery tables. Only Silver and Gold use Iceberg.
CREATE TABLE IF NOT EXISTS `${project_name}.${staging_dataset}.complaint_rows` (
  ${staging_columns}
)
PARTITION BY DATE(observed_at)
CLUSTER BY run_id, processing_id
OPTIONS (partition_expiration_days = 30);

CREATE TABLE IF NOT EXISTS `${project_name}.${ops_dataset}.published_runs` (
  run_id STRING, manifest_sha256 STRING, policy_hash STRING, contract_version STRING,
  observed_at TIMESTAMP, published_at TIMESTAMP, input_rows INT64,
  accepted_rows INT64, rejected_rows INT64, distinct_tickets INT64,
  processing_id STRING, coverage STRING
);

CREATE TABLE IF NOT EXISTS `${project_name}.${silver_dataset}.complaints_current` (
  ${data_columns}
)
CLUSTER BY ticket_id, state_group
WITH CONNECTION `${project_name}.${region}.${connection_id}`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = '${warehouse_root}/silver/complaints_current/'
);

CREATE TABLE IF NOT EXISTS `${project_name}.${silver_dataset}.complaint_observation_history` (
  ${data_columns}
)
PARTITION BY DATE(observed_at)
CLUSTER BY ticket_id
WITH CONNECTION `${project_name}.${region}.${connection_id}`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = '${warehouse_root}/silver/complaint_observation_history/'
);

CREATE TABLE IF NOT EXISTS `${project_name}.${gold_dataset}.daily_backlog` (
  run_id STRING, observation_date DATE, observed_at TIMESTAMP, organization STRING,
  state_group STRING, tickets INT64, coverage STRING
)
PARTITION BY observation_date
WITH CONNECTION `${project_name}.${region}.${connection_id}`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = '${warehouse_root}/gold/daily_backlog/'
);

CREATE TABLE IF NOT EXISTS `${project_name}.${gold_dataset}.complaint_age_distribution` (
  run_id STRING, observation_date DATE, observed_at TIMESTAMP, age_band STRING,
  tickets INT64, coverage STRING
)
PARTITION BY observation_date
WITH CONNECTION `${project_name}.${region}.${connection_id}`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = '${warehouse_root}/gold/complaint_age_distribution/'
);

CREATE TABLE IF NOT EXISTS `${project_name}.${gold_dataset}.observed_state_changes` (
  ticket_id STRING, from_state STRING, to_state STRING,
  previous_observed_at TIMESTAMP, observed_at TIMESTAMP, run_id STRING,
  reopen_count_delta INT64
)
PARTITION BY DATE(observed_at)
CLUSTER BY ticket_id
WITH CONNECTION `${project_name}.${region}.${connection_id}`
OPTIONS (
  file_format = 'PARQUET', table_format = 'ICEBERG',
  storage_uri = '${warehouse_root}/gold/observed_state_changes/'
);
