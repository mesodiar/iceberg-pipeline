"""The teaching extractor must produce the application's real Bronze contract."""

import json
import runpy
import tempfile
import unittest
from pathlib import Path

from complaint_pipeline.manifest import load_manifest, verified_page
from complaint_pipeline.storage import Store

ROOT = Path(__file__).resolve().parents[1]
LESSON = runpy.run_path(str(ROOT / "tutorials/extract_api.py"))


class TutorialExtractionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.record = json.loads((ROOT / "examples/complaints.json").read_text())["results"][0]

    def capture(self, fetch):
        return LESSON["extract"](
            "project_name-raw",
            "lesson-001",
            output_root=self.temp.name,
            fetch=fetch,
            sleep=lambda _: None,
        )

    def test_pages_can_be_verified_for_raw_load(self):
        records = [self.record | {"ticket_id": f"demo-{i}"} for i in range(205)]
        requests = []

        def fetch(parameters):
            requests.append(parameters)
            start = parameters["offset"]
            return json.dumps({"status": True, "results": records[start : start + 100]}).encode()

        output = self.capture(fetch)
        manifest, _ = load_manifest(Store(), str(output / "manifest.json"))

        class UploadedObjects:
            def read(self, uri):
                return (output / uri.rsplit("/", 1)[1]).read_bytes()

        rows = [
            row
            for page in manifest["pages"]
            for row in json.loads(verified_page(UploadedObjects(), page)["raw_json"])["results"]
        ]
        self.assertEqual([0, 100, 200], [request["offset"] for request in requests])
        self.assertEqual(205, manifest["record_count"])
        self.assertEqual(205, len(rows))
        self.assertTrue(
            all(page["uri"].startswith("gs://project_name-raw/") for page in manifest["pages"])
        )

    def test_capture_does_not_overwrite_existing_directory(self):
        fetch = lambda _: b'{"status":true,"results":[]}'  # noqa: E731
        self.capture(fetch)
        with self.assertRaises(FileExistsError):
            self.capture(fetch)

    def test_failed_response_has_no_manifest(self):
        with self.assertRaises(ValueError):
            self.capture(lambda _: b'{"status":false,"results":[]}')
        self.assertFalse((Path(self.temp.name) / "lesson-001/manifest.json").exists())

    def test_server_ignoring_page_limit_is_rejected(self):
        with self.assertRaises(ValueError):
            self.capture(
                lambda _: json.dumps({"status": True, "results": [self.record] * 101}).encode()
            )
        self.assertFalse((Path(self.temp.name) / "lesson-001/manifest.json").exists())
