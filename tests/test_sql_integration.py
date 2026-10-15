"""Opt-in GoogleSQL tests using temporary standard tables, no persistent resources.

RUN_BIGQUERY_SQL_TESTS=1 TEST_BIGQUERY_PROJECT=project_name python -m unittest \
    discover -s tests -p test_sql_integration.py -v
This submits billed BigQuery queries; requires ADC + bigquery.jobs.create.
It tests GoogleSQL behavior, not managed Iceberg provisioning/IAM.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

from complaint_pipeline.config import Config
from complaint_pipeline.extract import capture
from complaint_pipeline.ingest import prepare_load
from complaint_pipeline.schema import RAW_FIELDS
from complaint_pipeline.storage import Store, json_bytes
from complaint_pipeline.warehouse import query_parameters, render_sql

ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.getenv("RUN_BIGQUERY_SQL_TESTS") == "1", "Opt-in BigQuery SQL test")
class BigQuerySQLTests(unittest.TestCase):
    def test_normalization_validation_replay_and_publication(self):
        from google.cloud import bigquery

        records = json.loads((ROOT / "examples/complaints.json").read_text())["results"]
        first = records[0]
        extra = [
            ("boolean", {"star": True}),
            ("overflow", {"count_reopen": str(2**63)}),
            ("nan", {"star": "NaN"}),
            ("no-zone", {"timestamp": "2026-10-01 02:00:00"}),
            ("bad-category", {"type": ["A", None]}),
            ("bad-coords", {"coords": [100, None]}),
            ("backwards", {"last_activity": "2026-09-01T00:00:00Z"}),
            ("unknown", {"state": "new-state", "coords": [], "org": None}),
        ]
        records += [first | fields | {"ticket_id": name} for name, fields in extra]
        # Same business fields, different description/category order -> same content hash.
        records += [
            first | {"type": ["A", "B"], "description": "one"},
            first | {"type": ["B", "A", "A"], "description": "two"},
            None,
        ]
        project = os.environ["TEST_BIGQUERY_PROJECT"]
        config = Config(
            project_name=project,
            bronze_root="gs://example-raw/bronze",
            warehouse_root="gs://example-warehouse",
            max_rejected_fraction=0.99,
        )
        with tempfile.TemporaryDirectory() as tmp:
            from dataclasses import replace

            uri = capture(
                replace(config, bronze_root=tmp),
                "sql-test",
                fetcher=lambda *_: json_bytes({"status": True, "results": records}),
                now=lambda: "2026-10-15T00:00:00Z",
            )
            rows = [json.loads(line) for line in prepare_load(Store(), uri, "attempt").splitlines()]
            params = query_parameters(config, uri, "attempt", "normalize")
        # Use temp tables so the validation creates no datasets, buckets, or Iceberg tables.
        bootstrap = render_sql("bootstrap", config)
        import re

        bootstrap = re.sub(r"WITH CONNECTION.*?\);", ";", bootstrap, flags=re.S)
        bootstrap = re.sub(
            r"PARTITION BY[^;]+?OPTIONS \(partition_expiration_days = 30\)",
            "",
            bootstrap,
            flags=re.S,
        )
        bootstrap = re.sub(r"PARTITION BY[^\n]+\n", "", bootstrap)
        bootstrap = re.sub(r"CLUSTER BY[^\n]+\n", "", bootstrap)
        bootstrap = re.sub(r"CREATE OR REPLACE VIEW.*?;", "", bootstrap, flags=re.S)
        bootstrap = bootstrap.replace("CREATE TABLE IF NOT EXISTS", "CREATE TEMP TABLE")
        normalize = render_sql("normalize", config)
        publish = render_sql("publish", config)

        def temp_names(sql):
            # Keep quoted, qualified references so the test doesn't supply an implicit
            # bare-table alias that production table references might not provide.
            return re.sub(
                r"`" + re.escape(project) + r"\.[a-z_]+\.([a-z_]+)`",
                lambda match: f"`_SESSION.{match[1]}`",
                sql,
            )

        params += [bigquery.ScalarQueryParameter("raw_rows", "STRING", json.dumps(rows))]
        load = (
            "INSERT INTO raw_pages SELECT "
            + ", ".join(
                f"SAFE_CAST(JSON_VALUE(row, '$.{name}') AS "
                + ("INT64" if kind == "INTEGER" else kind)
                + ")"
                for name, kind in RAW_FIELDS
            )
            + " FROM UNNEST(JSON_QUERY_ARRAY(PARSE_JSON(@raw_rows))) row;"
        )
        script = (
            temp_names(bootstrap)
            + load
            + temp_names(normalize)
            + """
ASSERT (SELECT COUNT(*) FROM complaint_rows) = 15 AS 'All inputs accounted for';
ASSERT (SELECT COUNTIF(is_valid) FROM complaint_rows) = 6 AS 'Expected valid outcomes';
ASSERT (SELECT state_group FROM complaint_rows WHERE ticket_id = 'unknown') = 'unknown';
ASSERT (SELECT longitude FROM complaint_rows WHERE ticket_id = 'demo-001' LIMIT 1) = 100.5;
ASSERT (SELECT COUNT(DISTINCT content_hash) FROM complaint_rows
  WHERE ticket_id = 'demo-001' AND TO_JSON_STRING(categories) = '["A","B"]') = 1
  AS 'Stable business hash';
CREATE TEMP TABLE first_attempt AS SELECT * FROM complaint_rows;
DROP TABLE pages;
DROP TABLE normalized;
"""
        )
        # Re-run normalization without redefining the helper functions.
        script += (
            temp_names(normalize[normalize.index("-- Appends") :])
            + """
ASSERT (SELECT COUNT(*) FROM complaint_rows) = 15 AS 'Retry did not append';
ASSERT NOT EXISTS (
  SELECT TO_JSON_STRING(t) FROM complaint_rows t
  EXCEPT DISTINCT SELECT TO_JSON_STRING(t) FROM first_attempt t
);
"""
        )
        script += (
            "BEGIN\n"
            + temp_names(publish).replace("@max_rejected_fraction", "0.05")
            + """
ASSERT FALSE AS 'Expected quality gate to reject synthetic invalid records';
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  ASSERT STRPOS(@@error.message, 'Quarantine ratio') > 0 AS 'Expected the quality failure';
END;
ASSERT (SELECT COUNT(*) FROM published_runs) = 0 AS 'Failed publication wrote no receipt';
ASSERT (SELECT COUNT(*) FROM complaints_current) = 0 AS 'Failed publication wrote no Silver';
-- Separate real jobs have separate temp scopes. Our combined test script must clear them.
DROP TABLE IF EXISTS _SESSION.batch;
DROP TABLE IF EXISTS _SESSION.candidates;
"""
        )
        script += (
            temp_names(publish)
            + temp_names(publish)
            + """
ASSERT (SELECT COUNT(*) FROM published_runs) = 1 AS 'Publication receipt is idempotent';
ASSERT (SELECT COUNT(*) FROM complaints_current) = 4 AS 'Ticket deduplication';
ASSERT (SELECT COUNT(*) FROM complaint_observation_history) = 4 AS 'One observation per ticket';
ASSERT (SELECT SUM(tickets) FROM daily_backlog) = 4 AS 'Gold reconciles';
"""
        )
        client = bigquery.Client(project=project, location=config.region)
        job = client.query(
            script,
            job_config=bigquery.QueryJobConfig(
                query_parameters=params, maximum_bytes_billed=1_000_000_000
            ),
        )
        print("SQL integration job:", job.job_id, flush=True)
        job.result()
