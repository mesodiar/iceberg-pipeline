-- All business transformations run in BigQuery. Python only verifies/transports pages.
-- Read this script in order: helpers -> page gates -> UNNEST -> types -> validation -> staging.
CREATE TEMP FUNCTION text_value(v JSON) AS (
  IF(JSON_TYPE(v) = 'string', NULLIF(TRIM(JSON_VALUE(v)), ''), NULL)
);
CREATE TEMP FUNCTION missing(v JSON) AS (
  v IS NULL OR JSON_TYPE(v) = 'null'
);
CREATE TEMP FUNCTION optional_number(v JSON) AS (
  missing(v) OR (JSON_TYPE(v) = 'string' AND JSON_VALUE(v) = '')
);
CREATE TEMP FUNCTION source_timestamp(v JSON) AS (
  IF(JSON_TYPE(v) = 'string' AND REGEXP_CONTAINS(JSON_VALUE(v),
    r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+-]\d{2}(:?\d{2})?)\z'),
    SAFE_CAST(JSON_VALUE(v) AS TIMESTAMP), NULL)
);
CREATE TEMP FUNCTION finite_number(v JSON) AS (
  CASE
    WHEN JSON_TYPE(v) NOT IN ('number', 'string') THEN NULL
    WHEN IS_NAN(SAFE_CAST(JSON_VALUE(v) AS FLOAT64))
      OR IS_INF(SAFE_CAST(JSON_VALUE(v) AS FLOAT64)) THEN NULL
    ELSE SAFE_CAST(JSON_VALUE(v) AS FLOAT64)
  END
);
CREATE TEMP FUNCTION nonnegative_integer(v JSON) AS (
  IF(JSON_TYPE(v) IN ('number', 'string')
    AND REGEXP_CONTAINS(JSON_VALUE(v), r'^[0-9]+\z'),
    SAFE_CAST(JSON_VALUE(v) AS INT64), NULL)
);
CREATE TEMP FUNCTION valid_categories(v JSON) AS (
  missing(v) OR JSON_TYPE(v) = 'string' OR (
    JSON_TYPE(v) = 'array' AND NOT EXISTS (
      SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(v)) item
      WHERE JSON_TYPE(item) != 'string'
    )
  )
);
CREATE TEMP FUNCTION category_values(v JSON) AS (
  ARRAY(
    SELECT DISTINCT TRIM(JSON_VALUE(item)) value
    FROM UNNEST(IF(JSON_TYPE(v) = 'string', [v], JSON_QUERY_ARRAY(v))) item
    WHERE JSON_TYPE(item) = 'string' AND TRIM(JSON_VALUE(item)) != ''
    ORDER BY value
  )
);

-- Appends and retries can duplicate a page, but cannot replace its contents.
ASSERT NOT EXISTS (
  SELECT source_uri FROM `${project_name}.${staging_dataset}.raw_pages` AS raw_page
  WHERE run_id = @run_id AND processing_id = @processing_id
  GROUP BY source_uri HAVING COUNT(DISTINCT TO_JSON_STRING(raw_page)) > 1
) AS 'Conflicting raw pages with the same source URI';

CREATE TEMP TABLE pages AS
SELECT *, SAFE.PARSE_JSON(raw_json) AS payload
FROM `${project_name}.${staging_dataset}.raw_pages`
WHERE run_id = @run_id AND processing_id = @processing_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY source_uri ORDER BY source_uri) = 1;

ASSERT (SELECT COUNT(*) FROM pages) = @expected_pages AS 'Raw page count mismatch';
ASSERT NOT EXISTS (
  SELECT 1 FROM pages
  WHERE manifest_sha256 IS DISTINCT FROM @manifest_sha256
    OR raw_json IS NULL OR body_sha256 IS NULL OR source_uri IS NULL
    OR fetched_at IS NULL OR envelope_sha256 IS NULL
    OR LOWER(TO_HEX(SHA256(raw_json))) != body_sha256
    OR payload IS NULL
    OR JSON_TYPE(payload) IS DISTINCT FROM 'object'
    OR JSON_TYPE(JSON_QUERY(payload, '$.status')) IS DISTINCT FROM 'boolean'
    OR JSON_VALUE(payload, '$.status') IS DISTINCT FROM 'true'
    OR JSON_TYPE(JSON_QUERY(payload, '$.results')) IS DISTINCT FROM 'array'
    OR ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.results')) IS DISTINCT FROM record_count
) AS 'Invalid raw page, checksum, manifest identity, or response envelope';
ASSERT (SELECT SUM(record_count) FROM pages) = @expected_count
  AS 'Raw records do not reconcile with manifest';
ASSERT (SELECT MAX(fetched_at) FROM pages) = @observed_at
  AS 'Raw observation time differs from manifest';

CREATE TEMP TABLE normalized AS
WITH records AS (
  SELECT source_uri, body_sha256, fetched_at, record, record_index
  FROM pages, UNNEST(JSON_QUERY_ARRAY(payload, '$.results')) record WITH OFFSET record_index
), typed AS (
  SELECT *,
    LOWER(TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
      @run_id AS run_id, source_uri, body_sha256, record_index
    ))))) AS row_id,
    text_value(record.ticket_id) AS ticket_id,
    source_timestamp(record.timestamp) AS created_at,
    source_timestamp(record.last_activity) AS source_updated_at,
    text_value(record.state) AS state,
    text_value(record.org) AS organization,
    category_values(record.type) AS categories,
    category_values(record.problem_type_abdul) AS problem_categories,
    nonnegative_integer(record.count_reopen) AS reopen_count,
    finite_number(record.star) AS star,
    JSON_QUERY_ARRAY(record.coords) AS coords
  FROM records
), coordinates AS (
  SELECT *,
    finite_number(coords[SAFE_OFFSET(IF(@coordinate_order = 'lon_lat', 0, 1))]) AS lon,
    finite_number(coords[SAFE_OFFSET(IF(@coordinate_order = 'lon_lat', 1, 0))]) AS lat,
    missing(record.coords) OR (JSON_TYPE(record.coords) = 'array' AND ARRAY_LENGTH(coords) = 0)
      AS coords_missing
  FROM typed
), checked AS (
  SELECT *,
    ARRAY_LENGTH(coords) = 2 AND lon BETWEEN -180 AND 180 AND lat BETWEEN -90 AND 90
      AS coords_valid,
    ARRAY(
      SELECT reason FROM UNNEST([
        IF(JSON_TYPE(record) IS DISTINCT FROM 'object', 'record: expected object', NULL),
        IF(ticket_id IS NULL, 'ticket_id: expected nonempty string', NULL),
        IF(created_at IS NULL, 'timestamp: expected timestamp with timezone', NULL),
        IF(source_updated_at IS NULL, 'last_activity: expected timestamp with timezone', NULL),
        IF(source_updated_at < created_at, 'last_activity: precedes timestamp', NULL),
        IF(state IS NULL, 'state: expected nonempty string', NULL),
        IF(NOT missing(record.org) AND JSON_TYPE(record.org) != 'string',
          'org: expected string', NULL),
        IF(NOT valid_categories(record.type), 'type: expected string or array of strings', NULL),
        IF(NOT valid_categories(record.problem_type_abdul),
          'problem_type_abdul: expected string or array of strings', NULL),
        IF(NOT optional_number(record.count_reopen) AND reopen_count IS NULL,
          'count_reopen: expected nonnegative INT64', NULL),
        IF(NOT optional_number(record.star) AND (star IS NULL OR star NOT BETWEEN 0 AND 5),
          'star: expected finite number in 0..5', NULL),
        IF(NOT coords_missing AND NOT COALESCE(
          ARRAY_LENGTH(coords) = 2 AND lon BETWEEN -180 AND 180 AND lat BETWEEN -90 AND 90,
          FALSE), 'coords: expected longitude/latitude pair in range', NULL)
      ]) reason WHERE reason IS NOT NULL
    ) AS rejection_reasons
  FROM coordinates
), curated AS (
  SELECT row_id, @run_id AS run_id, ticket_id, created_at, source_updated_at,
    fetched_at AS observed_at, state,
    CASE WHEN state IN UNNEST(@closed_states) THEN 'closed'
      WHEN state IN UNNEST(@open_states) THEN 'open' ELSE 'unknown' END AS state_group,
    organization, categories, problem_categories, reopen_count, star,
    IF(coords_valid, lon, NULL) AS longitude, IF(coords_valid, lat, NULL) AS latitude,
    source_uri, rejection_reasons
  FROM checked
)
SELECT *,
  LOWER(TO_HEX(SHA256(TO_JSON_STRING(STRUCT(
    ticket_id, created_at, source_updated_at, state, organization, categories,
    problem_categories, reopen_count, star, longitude, latitude
  ))))) AS content_hash,
  @contract_version AS contract_version, @policy_hash AS policy_hash,
  @processing_id AS processing_id, ARRAY_LENGTH(rejection_reasons) = 0 AS is_valid
FROM curated;

ASSERT (SELECT COUNT(*) FROM normalized) = @expected_count AS 'Normalized row count mismatch';

-- Atomically replace this attempt only. A retry cannot append another copy of its outcomes.
-- Serialize runs of this script for the same attempt (Composer does this by dependency).
BEGIN TRANSACTION;
DELETE FROM `${project_name}.${staging_dataset}.complaint_rows`
WHERE run_id = @run_id AND processing_id = @processing_id;
INSERT INTO `${project_name}.${staging_dataset}.complaint_rows` (${staging_names})
SELECT ${staging_names} FROM normalized;
COMMIT TRANSACTION;
