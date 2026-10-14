"""Transport and retry tests; SQL semantics are tested against BigQuery separately."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from complaint_pipeline.config import Config
from complaint_pipeline.extract import capture
from complaint_pipeline.ingest import load_raw, prepare_load
from complaint_pipeline.storage import Store, json_bytes
from complaint_pipeline.warehouse import render_sql

ROOT = Path(__file__).resolve().parents[1]


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.body = (ROOT / "examples/complaints.json").read_bytes()
        self.manifest = capture(
            Config(bronze_root=self.temp.name),
            "capture-1",
            fetcher=lambda *_: self.body,
            now=lambda: "2026-10-15T00:00:00Z",
        )

    def test_raw_load_preserves_response_text_including_invalid_records(self):
        payload = prepare_load(Store(), self.manifest, "attempt-1")
        rows = [json.loads(line) for line in payload.splitlines()]
        self.assertEqual(1, len(rows))
        self.assertEqual(self.body.decode(), rows[0]["raw_json"])
        self.assertEqual(4, rows[0]["record_count"])
        self.assertEqual("attempt-1", rows[0]["processing_id"])
        self.assertNotIn("is_valid", rows[0])
        self.assertEqual(payload, prepare_load(Store(), self.manifest, "attempt-1"))

    def test_failed_checksum_prevents_any_load(self):
        page = Store().json(self.manifest)["pages"][0]
        Path(page["uri"]).write_text("{}")
        with self.assertRaisesRegex(ValueError, "checksum"):
            prepare_load(Store(), self.manifest, "attempt-1")

    def test_body_count_and_fetch_metadata_are_verified(self):
        manifest = Store().json(self.manifest)
        page = manifest["pages"][0]
        page["record_count"] += 1
        manifest["record_count"] += 1
        Path(self.manifest).write_bytes(json_bytes(manifest))
        with self.assertRaisesRegex(ValueError, "row count"):
            prepare_load(Store(), self.manifest, "attempt-1")

    def test_legacy_local_config_migrates_without_changing_file(self):
        with self.assertWarnsRegex(UserWarning, "retired Dataflow"):
            config = Config.from_dict({"temp_root": "old", "max_workers": 1})
        self.assertEqual(Config(), config)
        with self.assertRaises(TypeError):
            Config.from_dict({"typo_field": "bad"})

    def test_sql_lesson_copies_match_executable_templates(self):
        for stage, filename in [
            ("bootstrap", "01_tables.sql"),
            ("normalize", "02_normalize.sql"),
            ("publish", "03_publish.sql"),
        ]:
            self.assertEqual(
                render_sql(stage, Config()), (ROOT / "docs/sql" / filename).read_text()
            )

    def test_load_reconnects_after_lost_submission_response(self):
        from google.api_core.exceptions import Conflict

        config = Config(
            project_name="example-project",
            bronze_root="gs://example-raw/bronze",
            warehouse_root="gs://example-warehouse",
        )
        fake_store = MagicMock()
        with (
            patch("complaint_pipeline.ingest.Store", return_value=fake_store),
            patch("complaint_pipeline.ingest.prepare_load", return_value=b"{}\n"),
            patch("google.cloud.bigquery.Client") as factory,
        ):
            client = factory.return_value
            client.load_table_from_uri.side_effect = Conflict("response lost")
            client.get_job.return_value.job_id = "existing"
            self.assertEqual("existing", load_raw(config, "manifest", "attempt-1"))
            job_id = client.load_table_from_uri.call_args.kwargs["job_id"]
            client.get_job.assert_called_once_with(job_id, location=config.region)
            client.get_job.return_value.result.assert_called_once()

    def test_failed_load_is_not_reported_successful(self):
        config = Config(
            project_name="example-project",
            bronze_root="gs://example-raw/bronze",
            warehouse_root="gs://example-warehouse",
        )
        with (
            patch("complaint_pipeline.ingest.Store"),
            patch("complaint_pipeline.ingest.prepare_load", return_value=b"{}\n"),
            patch("google.cloud.bigquery.Client") as factory,
        ):
            factory.return_value.load_table_from_uri.return_value.result.side_effect = RuntimeError(
                "load failed"
            )
            with self.assertRaisesRegex(RuntimeError, "load failed"):
                load_raw(config, "manifest", "attempt-1")
