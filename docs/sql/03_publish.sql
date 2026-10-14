-- Parameters are bound by the Python publisher; identifiers are rendered from validated config.
-- Serialize publishers. Composer enforces max_active_runs=1; manual runs must do likewise.
BEGIN TRANSACTION;

ASSERT NOT EXISTS (
  SELECT 1 FROM `project_name.ops.published_runs`
  WHERE run_id = @run_id
    AND (manifest_sha256 != @manifest_sha256 OR policy_hash != @policy_hash)
) AS 'A published run cannot be replaced with different input or normalization policy';

IF NOT EXISTS (
  SELECT 1 FROM `project_name.ops.published_runs` WHERE run_id = @run_id
) THEN
  ASSERT @expected_count > 0 AS 'Empty capture cannot be published';

  -- Delivery retries can append duplicate rows. Divergent copies must fail, not be hidden.
  ASSERT NOT EXISTS (
    SELECT row_id
    FROM `project_name.staging.complaint_rows`
    WHERE run_id = @run_id AND processing_id = @processing_id
    GROUP BY row_id HAVING COUNT(DISTINCT TO_JSON_STRING(complaint_rows)) > 1
  ) AS 'Conflicting staging rows with the same source identity';

  CREATE TEMP TABLE batch AS
  SELECT * FROM `project_name.staging.complaint_rows`
  WHERE run_id = @run_id AND processing_id = @processing_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY row_id) = 1;

  ASSERT (SELECT COUNT(*) FROM batch) = @expected_count
    AS 'Staging count does not reconcile with the committed Bronze manifest';
  ASSERT NOT EXISTS (
    SELECT 1 FROM batch
    WHERE policy_hash IS NULL OR policy_hash != @policy_hash
      OR contract_version IS NULL OR contract_version != @contract_version
      OR is_valid IS NULL OR row_id IS NULL
  ) AS 'Staging normalization contract differs from publication contract';
  ASSERT (SELECT COUNTIF(NOT is_valid) / COUNT(*) FROM batch) <= @max_rejected_fraction
    AS 'Quarantine ratio exceeds the configured quality gate';
  ASSERT (SELECT COUNTIF(is_valid) FROM batch) > 0 AS 'No valid rows';
  ASSERT NOT EXISTS (
    SELECT 1 FROM batch WHERE is_valid AND (
      ticket_id IS NULL OR created_at IS NULL OR source_updated_at IS NULL
      OR observed_at IS NULL OR content_hash IS NULL OR state IS NULL
    )
  ) AS 'Required valid-row fields are missing';

  -- Live offset pagination can repeat a ticket. Keep its newest source version in this run.
  CREATE TEMP TABLE candidates AS
  SELECT row_id, run_id, ticket_id, created_at, source_updated_at, observed_at, state, state_group, organization, categories, problem_categories, reopen_count, star, longitude, latitude, content_hash, source_uri, contract_version, policy_hash FROM batch WHERE is_valid
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ticket_id
    ORDER BY source_updated_at DESC, observed_at DESC, row_id DESC
  ) = 1;

  -- One observation per ticket per capture, including unchanged observations.
  INSERT INTO `project_name.silver.complaint_observation_history` (row_id, run_id, ticket_id, created_at, source_updated_at, observed_at, state, state_group, organization, categories, problem_categories, reopen_count, star, longitude, latitude, content_hash, source_uri, contract_version, policy_hash)
  SELECT row_id, run_id, ticket_id, created_at, source_updated_at, observed_at, state, state_group, organization, categories, problem_categories, reopen_count, star, longitude, latitude, content_hash, source_uri, contract_version, policy_hash FROM candidates;

  MERGE `project_name.silver.complaints_current` AS target
  USING candidates AS source ON target.ticket_id = source.ticket_id
  WHEN MATCHED AND (
    source.source_updated_at > target.source_updated_at OR
    (source.source_updated_at = target.source_updated_at AND
      (source.observed_at > target.observed_at OR
        (source.observed_at = target.observed_at AND source.row_id > target.row_id)))
  ) THEN UPDATE SET row_id = source.row_id,
    run_id = source.run_id,
    ticket_id = source.ticket_id,
    created_at = source.created_at,
    source_updated_at = source.source_updated_at,
    observed_at = source.observed_at,
    state = source.state,
    state_group = source.state_group,
    organization = source.organization,
    categories = source.categories,
    problem_categories = source.problem_categories,
    reopen_count = source.reopen_count,
    star = source.star,
    longitude = source.longitude,
    latitude = source.latitude,
    content_hash = source.content_hash,
    source_uri = source.source_uri,
    contract_version = source.contract_version,
    policy_hash = source.policy_hash
  WHEN NOT MATCHED THEN INSERT (row_id, run_id, ticket_id, created_at, source_updated_at, observed_at, state, state_group, organization, categories, problem_categories, reopen_count, star, longitude, latitude, content_hash, source_uri, contract_version, policy_hash) VALUES (source.row_id, source.run_id, source.ticket_id, source.created_at, source.source_updated_at, source.observed_at, source.state, source.state_group, source.organization, source.categories, source.problem_categories, source.reopen_count, source.star, source.longitude, source.latitude, source.content_hash, source.source_uri, source.contract_version, source.policy_hash);

  -- Each run describes its own sample, never all-city totals or a carried-forward population.
  INSERT INTO `project_name.gold.daily_backlog`
    (run_id, observation_date, observed_at, organization, state_group, tickets, coverage)
  SELECT @run_id, DATE(@observed_at, 'Asia/Bangkok'), @observed_at,
    organization, state_group, COUNT(*), @coverage
  FROM candidates GROUP BY organization, state_group;

  INSERT INTO `project_name.gold.complaint_age_distribution`
    (run_id, observation_date, observed_at, age_band, tickets, coverage)
  SELECT @run_id, DATE(@observed_at, 'Asia/Bangkok'), @observed_at,
    CASE
      WHEN TIMESTAMP_DIFF(@observed_at, created_at, DAY) < 0 THEN 'future_timestamp'
      WHEN TIMESTAMP_DIFF(@observed_at, created_at, DAY) < 7 THEN '0-6 days'
      WHEN TIMESTAMP_DIFF(@observed_at, created_at, DAY) < 30 THEN '7-29 days'
      WHEN TIMESTAMP_DIFF(@observed_at, created_at, DAY) < 90 THEN '30-89 days'
      ELSE '90+ days'
    END, COUNT(*), @coverage
  FROM candidates WHERE state_group = 'open' GROUP BY 4;

  -- Recompute transitions for affected tickets, including when older captures arrive late.
  DELETE FROM `project_name.gold.observed_state_changes`
  WHERE ticket_id IN (SELECT ticket_id FROM candidates);

  INSERT INTO `project_name.gold.observed_state_changes`
    (ticket_id, from_state, to_state, previous_observed_at, observed_at, run_id,
      reopen_count_delta)
  SELECT ticket_id, previous_state, state, previous_observed_at, observed_at, run_id,
    reopen_count - previous_reopen_count
  FROM (
    SELECT ticket_id, state, observed_at, run_id, reopen_count,
      LAG(state) OVER observations AS previous_state,
      LAG(observed_at) OVER observations AS previous_observed_at,
      LAG(reopen_count) OVER observations AS previous_reopen_count
    FROM `project_name.silver.complaint_observation_history`
    WHERE ticket_id IN (SELECT ticket_id FROM candidates)
    WINDOW observations AS (PARTITION BY ticket_id ORDER BY observed_at, row_id)
  )
  WHERE previous_state IS NOT NULL AND (
    previous_state != state OR reopen_count IS DISTINCT FROM previous_reopen_count
  );

  ASSERT (
    SELECT SUM(tickets) FROM `project_name.gold.daily_backlog`
    WHERE run_id = @run_id
  ) = (SELECT COUNT(*) FROM candidates) AS 'Gold sample count mismatch';

  INSERT INTO `project_name.ops.published_runs`
    (run_id, manifest_sha256, policy_hash, contract_version, observed_at, published_at,
      input_rows, accepted_rows, rejected_rows, distinct_tickets, processing_id, coverage)
  SELECT @run_id, @manifest_sha256, @policy_hash, @contract_version,
    @observed_at, CURRENT_TIMESTAMP(), COUNT(*), COUNTIF(is_valid), COUNTIF(NOT is_valid),
    (SELECT COUNT(*) FROM candidates), @processing_id, @coverage
  FROM batch;
END IF;

COMMIT TRANSACTION;
