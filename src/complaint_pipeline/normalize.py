"""Pure transformations shared by the local runner and Dataflow."""

import hashlib
import json
import math
import re
from datetime import UTC, datetime

from . import CONTRACT_VERSION
from .extract import digest, response_records
from .storage import json_bytes

DATA_FIELDS = [
    ("row_id", "STRING"),
    ("run_id", "STRING"),
    ("ticket_id", "STRING"),
    ("created_at", "TIMESTAMP"),
    ("source_updated_at", "TIMESTAMP"),
    ("observed_at", "TIMESTAMP"),
    ("state", "STRING"),
    ("state_group", "STRING"),
    ("organization", "STRING"),
    ("categories", "STRING", "REPEATED"),
    ("problem_categories", "STRING", "REPEATED"),
    ("reopen_count", "INTEGER"),
    ("star", "FLOAT"),
    ("longitude", "FLOAT"),
    ("latitude", "FLOAT"),
    ("content_hash", "STRING"),
    ("source_uri", "STRING"),
    ("contract_version", "STRING"),
    ("policy_hash", "STRING"),
]
STAGING_FIELDS = DATA_FIELDS + [
    ("processing_id", "STRING"),
    ("is_valid", "BOOLEAN"),
    ("rejection_reasons", "STRING", "REPEATED"),
]


def bq_schema():
    return {
        "fields": [
            {
                "name": field[0],
                "type": field[1],
                "mode": field[2] if len(field) == 3 else "NULLABLE",
            }
            for field in STAGING_FIELDS
        ]
    }


def policy_hash(config):
    return digest(
        json_bytes(
            {
                "contract_version": CONTRACT_VERSION,
                "coordinate_order": config.coordinate_order,
                "closed_states": sorted(config.closed_states),
                "open_states": sorted(config.open_states),
            }
        )
    )


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("expected timestamp string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(UTC).isoformat(timespec="microseconds")


def text_value(value, required=False):
    if value is None and not required:
        return None
    if not isinstance(value, str) or (required and not value.strip()):
        raise ValueError("expected nonempty string" if required else "expected string")
    return value.strip() or None


def categories(value):
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
        raise ValueError("expected category string or array of strings")
    return sorted({v.strip() for v in value if v.strip()})


def integer(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not re.fullmatch(r"\d+", str(value)):
        raise ValueError("expected nonnegative integer")
    result = int(value)
    if result > 9_223_372_036_854_775_807:
        raise ValueError("integer exceeds INT64")
    return result


def number(value):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError("expected finite number")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("expected finite number")
    return result


def normalize(record, *, run_id, processing_id, source_uri, page_hash, index, observed_at, config):
    result = {field[0]: [] if len(field) == 3 else None for field in STAGING_FIELDS}
    result.update(
        {
            "row_id": digest(json_bytes([run_id, source_uri, page_hash, index])),
            "run_id": run_id,
            "processing_id": processing_id,
            "source_uri": source_uri,
            "observed_at": timestamp(observed_at),
            "contract_version": CONTRACT_VERSION,
            "policy_hash": policy_hash(config),
        }
    )
    errors = []
    if not isinstance(record, dict):
        errors.append("record: expected object")
        record = {}

    converters = {
        "ticket_id": ("ticket_id", lambda v: text_value(v, required=True)),
        "created_at": ("timestamp", timestamp),
        "source_updated_at": ("last_activity", timestamp),
        "state": ("state", lambda v: text_value(v, required=True)),
        "organization": ("org", text_value),
        "categories": ("type", categories),
        "problem_categories": ("problem_type_abdul", categories),
        "reopen_count": ("count_reopen", integer),
        "star": ("star", number),
    }
    for target, (source, convert) in converters.items():
        try:
            result[target] = convert(record.get(source))
        except (ValueError, TypeError, OverflowError) as exc:
            errors.append(f"{source}: {exc}")
    if result["created_at"] and result["source_updated_at"]:
        if result["source_updated_at"] < result["created_at"]:
            errors.append("last_activity: precedes timestamp")
    if result["star"] is not None and not 0 <= result["star"] <= 5:
        errors.append("star: outside 0..5")
    coords = record.get("coords")
    if coords is not None and coords != []:
        try:
            if not isinstance(coords, list) or len(coords) != 2:
                raise ValueError("expected two coordinates")
            first, second = (number(v) for v in coords)
            lon, lat = (first, second) if config.coordinate_order == "lon_lat" else (second, first)
            if lon is None or lat is None or not (-180 <= lon <= 180 and -90 <= lat <= 90):
                raise ValueError("coordinates outside longitude/latitude ranges")
            result.update(longitude=lon, latitude=lat)
        except (ValueError, TypeError, OverflowError) as exc:
            errors.append(f"coords: {exc}")
    state = result["state"]
    result["state_group"] = (
        "closed"
        if state in config.closed_states
        else "open"
        if state in config.open_states
        else "unknown"
    )
    # Hash curated business fields only; provenance and extraction times are excluded.
    business = {
        key: result[key]
        for key in (
            "ticket_id",
            "created_at",
            "source_updated_at",
            "state",
            "organization",
            "categories",
            "problem_categories",
            "reopen_count",
            "star",
            "longitude",
            "latitude",
        )
    }
    result["content_hash"] = hashlib.sha256(json_bytes(business)).hexdigest()
    result["is_valid"] = not errors
    result["rejection_reasons"] = errors
    return result


def load_manifest(store, uri):
    raw = store.read(uri)
    manifest = json.loads(raw)
    if manifest.get("manifest_version") != 1 or not isinstance(manifest.get("pages"), list):
        raise ValueError("Unsupported or incomplete manifest")
    if sum(page["record_count"] for page in manifest["pages"]) != manifest["record_count"]:
        raise ValueError("Manifest record count does not reconcile")
    if len({page["uri"] for page in manifest["pages"]}) != len(manifest["pages"]):
        raise ValueError("Duplicate page URI in manifest")
    return manifest, digest(raw)


def page_rows(page, *, store, run_id, processing_id, config):
    raw = store.read(page["uri"])
    if digest(raw) != page["sha256"]:
        raise ValueError("Bronze envelope checksum mismatch")
    envelope = json.loads(raw)
    body = envelope["raw_json"].encode("utf-8")
    if digest(body) != page["body_sha256"] or digest(body) != envelope["body_sha256"]:
        raise ValueError("Bronze response checksum mismatch")
    if envelope["fetched_at"] != page["fetched_at"]:
        raise ValueError("Bronze fetch timestamp mismatch")
    records = response_records(body)
    if len(records) != page["record_count"]:
        raise ValueError("Bronze row count mismatch")
    for index, record in enumerate(records):
        yield normalize(
            record,
            run_id=run_id,
            processing_id=processing_id,
            source_uri=page["uri"],
            page_hash=page["body_sha256"],
            index=index,
            observed_at=page["fetched_at"],
            config=config,
        )
