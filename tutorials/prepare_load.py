"""Lesson: wrap verified API pages as NDJSON for bq load, without cleaning records."""

import os
from pathlib import Path

from complaint_pipeline.ingest import prepare_load
from complaint_pipeline.storage import Store

if __name__ == "__main__":
    # prepare_load checks every page hash against the committed manifest, then writes
    # one object per page with provenance + the exact original raw_json string.
    # Inspect src/complaint_pipeline/ingest.py; no business conversion happens here.
    data = prepare_load(
        Store(os.environ["TUTORIAL_PROJECT"]),
        os.environ["TUTORIAL_MANIFEST"],
        os.environ["TUTORIAL_PROCESSING_ID"],
    )
    output = Path("work/build-along") / (os.environ["TUTORIAL_PROCESSING_ID"] + ".jsonl")
    output.parent.mkdir(parents=True, exist_ok=True)
    # Refuse accidental replacement. Use a new processing ID for a new attempt.
    with output.open("xb") as stream:
        stream.write(data)
    print(output)
