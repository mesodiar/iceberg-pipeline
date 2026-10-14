"""Run the actual Beam transform locally, with no GCP credentials or network."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


@unittest.skipUnless(importlib.util.find_spec("apache_beam"), "Install .[beam] for Beam tests")
class BeamTests(unittest.TestCase):
    def test_direct_runner_reconciles_both_outputs(self):
        import apache_beam as beam
        from apache_beam.runners.direct.direct_runner import BundleBasedDirectRunner
        from apache_beam.testing.test_pipeline import TestPipeline
        from apache_beam.testing.util import assert_that, equal_to

        from complaint_pipeline.beam_pipeline import NormalizePage
        from complaint_pipeline.config import Config
        from complaint_pipeline.extract import capture
        from complaint_pipeline.storage import Store, json_bytes

        fixture = Path(__file__).resolve().parents[1] / "examples/complaints.json"
        records = json.loads(fixture.read_text())["results"]
        with tempfile.TemporaryDirectory() as tmp:
            config = Config(bronze_root=tmp, page_size=10, max_records=10)
            uri = capture(
                config,
                "beam",
                fetcher=lambda *_: json_bytes(
                    {
                        "status": True,
                        "results": records,
                    }
                ),
                now=lambda: "2026-10-12T06:00:00Z",
            )
            manifest = Store().json(uri)
            with TestPipeline(runner=BundleBasedDirectRunner()) as pipeline:
                rows = (
                    pipeline
                    | beam.Create(manifest["pages"])
                    | beam.ParDo(NormalizePage(config, "beam", "attempt"))
                    | beam.Map(lambda row: (row["ticket_id"], row["is_valid"]))
                )
                assert_that(
                    rows,
                    equal_to(
                        [
                            ("demo-001", True),
                            ("demo-002", True),
                            ("demo-003", True),
                            ("demo-invalid", False),
                        ]
                    ),
                )
