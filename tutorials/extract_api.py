"""Teaching example: capture three API pages locally for upload to GCS.

Read alongside docs/build-along.md. This intentionally has no resume mechanism;
use a new run ID after a failed attempt. The application extractor adds retries
and atomic immutable writes once these basic steps are understood.
"""

import hashlib
import json
import os
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_URL = "https://publicapi.traffy.in.th/share/teamchadchart/search"


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def fetch_page(parameters):
    request = Request(
        API_URL + "?" + urlencode(parameters),
        headers={"User-Agent": "complaint-tutorial/0.1 (bounded sample)"},
    )
    with urlopen(request, timeout=30) as response:
        body = response.read(10_000_001)
    if len(body) > 10_000_000:
        raise ValueError("Response exceeds 10 MB")
    return body


def extract(
    raw_bucket, run_id, *, output_root="work/build-along", fetch=fetch_page, sleep=time.sleep
):
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]+", raw_bucket):
        raise ValueError("raw_bucket must be a bucket name, without gs://")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", run_id):
        raise ValueError("Use a simple run ID containing letters, digits, hyphens or underscores")

    output = Path(output_root) / run_id
    output.mkdir(parents=True, exist_ok=False)  # Never overwrite an earlier lesson capture.
    destination = f"gs://{raw_bucket}/bronze/traffy/run_id={run_id}"
    specification = {
        "api_url": API_URL,
        "page_size": 100,
        "max_records": 300,
        "start_offset": 0,
    }
    specification_hash = sha256(encode(specification))
    pages = []
    for offset in (0, 100, 200):
        if offset:
            sleep(1)
        parameters = {"limit": 100, "offset": offset}
        raw = fetch(parameters)
        fetched_at = datetime.now(UTC).isoformat()
        payload = json.loads(raw)
        if not isinstance(payload, dict) or payload.get("status") is not True:
            raise ValueError("API reported an unsuccessful response")
        records = payload.get("results")
        if not isinstance(records, list) or len(records) > 100:
            raise ValueError("Unexpected results array or ignored page limit")

        # Keep exact original response text. Hash bytes before parsing/transformation.
        envelope = {
            "request_spec_hash": specification_hash,
            "request": parameters,
            "fetched_at": fetched_at,
            "body_sha256": sha256(raw),
            "raw_json": raw.decode("utf-8"),
        }
        filename = f"page-{offset:010d}.json"
        saved_bytes = encode(envelope)
        (output / filename).write_bytes(saved_bytes)
        pages.append(
            {
                "uri": f"{destination}/{filename}",
                "sha256": sha256(saved_bytes),
                "body_sha256": sha256(raw),
                "record_count": len(records),
                "fetched_at": fetched_at,
            }
        )
        if len(records) < 100:
            break

    manifest = {
        "manifest_version": 1,
        "run_id": run_id,
        "request_spec": specification,
        "request_spec_hash": specification_hash,
        "coverage": "bounded_offset_sample",
        "source_snapshot_consistent": False,
        "record_count": sum(page["record_count"] for page in pages),
        "pages": pages,
        "observed_at": max(page["fetched_at"] for page in pages),
        "completed_at": datetime.now(UTC).isoformat(),
    }
    (output / "manifest.json").write_bytes(encode(manifest))
    print(f"Saved {manifest['record_count']} source rows to {output}")
    print(f"Upload destination: {destination}/")
    return output


if __name__ == "__main__":
    extract(os.environ["TUTORIAL_RAW_BUCKET"], os.environ["TUTORIAL_RUN_ID"])
