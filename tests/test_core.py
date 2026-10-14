import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from complaint_pipeline.cli import local_process
from complaint_pipeline.config import Config
from complaint_pipeline.extract import capture, fetch
from complaint_pipeline.normalize import (
    load_manifest,
    normalize,
    page_rows,
    policy_hash,
    timestamp,
)
from complaint_pipeline.storage import Store, json_bytes
from complaint_pipeline.warehouse import render_sql

FIXTURE = Path(__file__).resolve().parents[1] / "examples" / "complaints.json"


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Config(bronze_root=self.temp.name, page_size=2, max_records=4)
        self.records = json.loads(FIXTURE.read_text())["results"]
        self.calls = []

    def fetcher(self, config, params):
        self.calls.append(params)
        rows = self.records[params["offset"] : params["offset"] + params["limit"]]
        return json_bytes({"status": True, "results": rows})

    def capture(self, config=None, run="run-1", fetcher=None):
        return capture(
            config or self.config,
            run,
            fetcher=fetcher or self.fetcher,
            sleep=lambda _: None,
            now=lambda: "2026-10-12T06:00:00+00:00",
        )

    def normalize(self, record=None, **kwargs):
        return normalize(
            self.records[0] if record is None else record,
            run_id="one",
            processing_id="attempt",
            source_uri="page.json",
            page_hash="hash",
            index=0,
            observed_at="2026-10-12T06:00:00Z",
            config=kwargs.get("config", self.config),
        )

    def test_bounded_capture_and_replay_never_refetches(self):
        uri = self.capture()
        before = Store().read(uri)
        self.assertEqual(2, len(self.calls))
        self.assertEqual(4, Store().json(uri)["record_count"])
        self.assertEqual(uri, self.capture())
        self.assertEqual(before, Store().read(uri))
        self.assertEqual(2, len(self.calls))

    def test_partial_failure_resumes_immutable_page(self):
        def failing(config, params):
            if params["offset"] == 2:
                raise TimeoutError("simulated")
            return self.fetcher(config, params)

        with self.assertRaises(TimeoutError):
            self.capture(fetcher=failing)
        self.assertFalse((Path(self.temp.name) / "run_id=run-1/manifest.json").exists())
        self.capture()
        self.assertEqual([0, 2], [p["offset"] for p in self.calls])

    def test_parameters_cannot_change_on_replay(self):
        self.capture()
        with self.assertRaisesRegex(ValueError, "different extraction"):
            self.capture(config=replace(self.config, max_records=3))

    def test_partial_parameters_cannot_change(self):
        def fail_second(config, params):
            if params["offset"]:
                raise TimeoutError()
            return self.fetcher(config, params)

        with self.assertRaises(TimeoutError):
            self.capture(fetcher=fail_second)
        with self.assertRaisesRegex(ValueError, "different extraction"):
            self.capture(config=replace(self.config, max_records=3))

    def test_last_page_limit_and_early_end(self):
        uri = self.capture(config=replace(self.config, max_records=3))
        self.assertEqual(3, Store().json(uri)["record_count"])
        self.assertEqual([2, 1], [p["limit"] for p in self.calls])
        uri = self.capture(config=replace(self.config, max_records=10), run="short")
        self.assertEqual(4, Store().json(uri)["record_count"])

    def test_empty_capture_is_committed_but_not_a_complete_population(self):
        self.records = []
        manifest = Store().json(self.capture())
        self.assertEqual(0, manifest["record_count"])
        self.assertFalse(manifest["source_snapshot_consistent"])
        self.assertEqual("bounded_offset_sample", manifest["coverage"])

    def test_api_envelope_failure_has_no_manifest(self):
        with self.assertRaises(ValueError):
            self.capture(fetcher=lambda *_: b'{"status":false,"results":[]}')
        self.assertFalse((Path(self.temp.name) / "run_id=run-1/manifest.json").exists())

    def test_api_ignoring_limit_fails(self):
        with self.assertRaisesRegex(ValueError, "ignored requested"):
            self.capture(fetcher=lambda *_: json_bytes({"status": True, "results": self.records}))

    def test_corrupted_page_fails_processing(self):
        manifest = Store().json(self.capture())
        page = manifest["pages"][0]
        Path(page["uri"]).write_text("{}")
        with self.assertRaisesRegex(ValueError, "checksum"):
            list(
                page_rows(
                    page, store=Store(), run_id="run-1", processing_id="a", config=self.config
                )
            )

    def test_manifest_count_and_duplicate_pages_checked(self):
        uri = self.capture()
        manifest = Store().json(uri)
        manifest["record_count"] += 1
        Path(uri).write_bytes(json_bytes(manifest))
        with self.assertRaisesRegex(ValueError, "reconcile"):
            load_manifest(Store(), uri)

    def test_local_counts_quarantine_and_deterministic_replay(self):
        uri = self.capture()
        out = Path(self.temp.name) / "normalized"
        counts = local_process(self.config, uri, out, "local")
        self.assertEqual({"input": 4, "accepted": 3, "quarantined": 1}, counts)
        before = (out / "accepted.jsonl").read_bytes()
        local_process(self.config, uri, out, "local")
        self.assertEqual(before, (out / "accepted.jsonl").read_bytes())
        invalid = json.loads((out / "quarantine.jsonl").read_text())
        self.assertEqual(3, len(invalid["rejection_reasons"]))

    def test_valid_thai_fields_and_types(self):
        row = self.normalize()
        self.assertTrue(row["is_valid"])
        self.assertEqual("กำลังดำเนินการ", row["state"])
        self.assertEqual("open", row["state_group"])
        self.assertEqual(0, row["reopen_count"])
        self.assertEqual(100.5, row["longitude"])

    def test_unknown_state_stays_unknown(self):
        self.assertEqual("unknown", self.normalize(self.records[2])["state_group"])

    def test_nullable_fields_remain_nullable(self):
        row = self.normalize(self.records[2] | {"count_reopen": None})
        self.assertTrue(row["is_valid"])
        self.assertIsNone(row["reopen_count"])
        self.assertIsNone(row["latitude"])
        self.assertIsNone(row["organization"])

    def test_coordinate_order_explicit(self):
        row = self.normalize(
            self.records[0] | {"coords": [13.75, 100.5]},
            config=replace(self.config, coordinate_order="lat_lon"),
        )
        self.assertTrue(row["is_valid"])
        self.assertEqual(100.5, row["longitude"])
        wrong = self.normalize(self.records[0] | {"coords": [13.75, 100.5]})
        self.assertFalse(wrong["is_valid"])

    def test_source_timestamps_are_timezone_aware(self):
        self.assertEqual(timestamp("2026-10-12T07:00:00+07:00"), timestamp("2026-10-12T00:00:00Z"))
        for value in (None, "broken", "2026-10-12 00:00:00"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                timestamp(value)

    def test_older_updated_time_is_rejected(self):
        row = self.normalize(self.records[0] | {"last_activity": "2026-09-01T00:00:00Z"})
        self.assertFalse(row["is_valid"])

    def test_invalid_shapes_do_not_crash_processing(self):
        for value in ([], "bad", 1, True):
            with self.subTest(value=value):
                self.assertFalse(self.normalize(value)["is_valid"])

    def test_invalid_numeric_values_are_quarantined(self):
        for field, values in {
            "star": [True, "NaN", "Infinity", 6],
            "count_reopen": [True, "2.5", -1, 2**63],
            "coords": [[100, "NaN"], [None, 1], "bad"],
        }.items():
            for value in values:
                with self.subTest(field=field, value=value):
                    self.assertFalse(self.normalize(self.records[0] | {field: value})["is_valid"])

    def test_content_hash_ignores_description_and_category_order(self):
        a = self.normalize(self.records[0] | {"type": ["A", "B"]})
        b = self.normalize(self.records[0] | {"type": ["B", "A", "A"], "description": "changed"})
        self.assertEqual(a["content_hash"], b["content_hash"])
        c = self.normalize(self.records[0] | {"state": "เสร็จสิ้น"})
        self.assertNotEqual(a["content_hash"], c["content_hash"])

    def test_policy_hash_changes_with_classification(self):
        self.assertNotEqual(
            policy_hash(self.config), policy_hash(replace(self.config, closed_states=()))
        )

    def test_put_once_cannot_overwrite(self):
        path = str(Path(self.temp.name) / "object")
        Store().put_once(path, b"first")
        with self.assertRaises(FileExistsError):
            Store().put_once(path, b"second")
        self.assertEqual(b"first", Store().read(path))

    def test_unsafe_config_and_run_id_rejected(self):
        with self.assertRaises(ValueError):
            Config(silver_dataset="silver`; DROP TABLE x")
        with self.assertRaises(ValueError):
            self.capture(run="../escape")
        with self.assertRaisesRegex(ValueError, "Replace project_name"):
            Config().require_cloud()

    def test_sql_renders_only_identifiers_leaving_parameters_bound(self):
        for name in ("bootstrap", "publish"):
            sql = render_sql(name, self.config)
            self.assertNotIn("${", sql)
            self.assertIn("`project_name.", sql)
        self.assertIn("@manifest_sha256", render_sql("publish", self.config))

    def test_transient_http_errors_retry_with_retry_after(self):
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def read(self, _):
                return b"{}"

        delays = []
        error = HTTPError("https://example.invalid", 429, "retry", {"Retry-After": "5"}, None)
        with patch("complaint_pipeline.extract.urlopen", side_effect=[error, Response()]):
            self.assertEqual(b"{}", fetch(self.config, {"limit": 1}, sleep=delays.append))
        self.assertEqual([5], delays)

    def test_permanent_http_errors_do_not_retry(self):
        error = HTTPError("https://example.invalid", 403, "no", {}, None)
        with patch("complaint_pipeline.extract.urlopen", side_effect=error) as mock:
            with self.assertRaises(HTTPError):
                fetch(self.config, {}, sleep=lambda _: None)
        self.assertEqual(1, mock.call_count)


if __name__ == "__main__":
    unittest.main()
