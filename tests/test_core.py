import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from complaint_pipeline.config import Config
from complaint_pipeline.extract import capture, fetch
from complaint_pipeline.manifest import load_manifest, verified_page
from complaint_pipeline.storage import Store, json_bytes
from complaint_pipeline.warehouse import policy_hash, render_sql

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
            verified_page(Store(), page)

    def test_manifest_count_and_duplicate_pages_checked(self):
        uri = self.capture()
        manifest = Store().json(uri)
        manifest["record_count"] += 1
        Path(uri).write_bytes(json_bytes(manifest))
        with self.assertRaisesRegex(ValueError, "reconcile"):
            load_manifest(Store(), uri)

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
        for name in ("bootstrap", "normalize", "publish"):
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
