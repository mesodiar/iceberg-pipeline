"""Validate the capture manifest and exact original API response bytes."""

import json
from datetime import datetime

from .config import safe_run_id
from .extract import digest, response_records


def load_manifest(store, uri):
    raw = store.read(uri)
    manifest = json.loads(raw)
    if manifest.get("manifest_version") != 1 or not isinstance(manifest.get("pages"), list):
        raise ValueError("Unsupported or incomplete manifest")
    safe_run_id(manifest["run_id"])
    if not manifest["pages"] or any(
        type(page.get("record_count")) is not int or page["record_count"] < 0
        for page in manifest["pages"]
    ):
        raise ValueError("Manifest needs pages with nonnegative counts")
    if sum(page["record_count"] for page in manifest["pages"]) != manifest["record_count"]:
        raise ValueError("Manifest record count does not reconcile")
    if len({page["uri"] for page in manifest["pages"]}) != len(manifest["pages"]):
        raise ValueError("Duplicate page URI in manifest")
    return manifest, digest(raw)


def verified_page(store, page):
    raw = store.read(page["uri"])
    if digest(raw) != page["sha256"]:
        raise ValueError("Bronze envelope checksum mismatch")
    envelope = json.loads(raw)
    body = envelope["raw_json"].encode("utf-8")
    if digest(body) != page["body_sha256"] or digest(body) != envelope["body_sha256"]:
        raise ValueError("Bronze response checksum mismatch")
    if envelope["fetched_at"] != page["fetched_at"]:
        raise ValueError("Bronze fetch timestamp mismatch")
    if datetime.fromisoformat(page["fetched_at"].replace("Z", "+00:00")).tzinfo is None:
        raise ValueError("Bronze fetch timestamp needs a timezone")
    if len(response_records(body)) != page["record_count"]:
        raise ValueError("Bronze row count mismatch")
    return envelope
