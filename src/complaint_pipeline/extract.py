"""Bounded API sampling. Offset pagination is not a consistent source snapshot."""

import hashlib
import json
import time
from datetime import UTC, datetime
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import safe_run_id
from .storage import Store, join, json_bytes


def digest(data):
    return hashlib.sha256(data).hexdigest()


def utc_now():
    return datetime.now(UTC).isoformat()


def fetch(config, params, sleep=time.sleep):
    url = config.api_url + "?" + urlencode(params)
    for attempt in range(config.max_attempts):
        try:
            request = Request(
                url, headers={"User-Agent": "complaint-pipeline/0.1 (bounded sample)"}
            )
            with urlopen(request, timeout=config.timeout_seconds) as response:
                body = response.read(10_000_001)
                if len(body) > 10_000_000:
                    raise ValueError("API response exceeds 10 MB safety bound")
                return body
        except (HTTPError, URLError, TimeoutError) as exc:
            if isinstance(exc, HTTPError):
                exc.close()
            if isinstance(exc, HTTPError) and exc.code not in {429, 500, 502, 503, 504}:
                raise
            if attempt + 1 == config.max_attempts:
                raise
            delay = min(60, 2**attempt)
            if isinstance(exc, HTTPError):
                retry_after = exc.headers.get("Retry-After", "")
                if retry_after.isdigit():
                    delay = min(120, max(delay, int(retry_after)))
            sleep(delay)
    raise AssertionError("unreachable")


def response_records(body):
    payload = json.loads(body)
    if not isinstance(payload, dict) or payload.get("status") is not True:
        raise ValueError("API returned an unsuccessful or unknown response envelope")
    if not isinstance(payload.get("results"), list):
        raise ValueError("API response has no results array")
    return payload["results"]


def capture(config, run_id, *, store=None, fetcher=fetch, sleep=time.sleep, now=utc_now):
    safe_run_id(run_id)
    store = store or Store(config.project_name if config.project_name != "project_name" else None)
    root = join(config.bronze_root, f"run_id={run_id}")
    manifest_uri = join(root, "manifest.json")
    request_spec = {
        "api_url": config.api_url,
        "page_size": config.page_size,
        "max_records": config.max_records,
        "start_offset": config.start_offset,
    }
    spec_hash = digest(json_bytes(request_spec))
    try:
        manifest = store.json(manifest_uri)
    except FileNotFoundError:
        manifest = None
    if manifest:
        if manifest["request_spec_hash"] != spec_hash:
            raise ValueError("run_id already exists with different extraction parameters")
        return manifest_uri

    pages, total = [], 0
    offset = config.start_offset
    while total < config.max_records:
        limit = min(config.page_size, config.max_records - total)
        params = {"limit": limit, "offset": offset}
        uri = join(root, f"page-{offset:010d}.json")
        try:
            envelope = store.json(uri)
        except FileNotFoundError:
            if pages:
                sleep(config.request_delay_seconds)
            body = fetcher(config, params)
            records = response_records(body)
            if len(records) > limit:
                raise ValueError(
                    "API ignored requested page limit; refusing an unbounded capture"
                ) from None
            envelope = {
                "request_spec_hash": spec_hash,
                "request": params,
                "fetched_at": now(),
                "body_sha256": digest(body),
                "raw_json": body.decode("utf-8"),
            }
            try:
                store.put_once(uri, json_bytes(envelope))
            except FileExistsError:
                envelope = store.json(uri)
        if envelope["request_spec_hash"] != spec_hash or envelope["request"] != params:
            raise ValueError("Existing page belongs to different extraction parameters")
        body = envelope["raw_json"].encode("utf-8")
        if digest(body) != envelope["body_sha256"]:
            raise ValueError("Bronze page checksum mismatch")
        rows = response_records(body)
        if len(rows) > limit:
            raise ValueError("API ignored requested page limit; refusing an unbounded capture")
        pages.append(
            {
                "uri": uri,
                "sha256": digest(store.read(uri)),
                "body_sha256": digest(body),
                "record_count": len(rows),
                "fetched_at": envelope["fetched_at"],
            }
        )
        total += len(rows)
        if len(rows) < limit:
            break
        offset += len(rows)

    manifest = {
        "manifest_version": 1,
        "run_id": run_id,
        "request_spec": request_spec,
        "request_spec_hash": spec_hash,
        "coverage": "bounded_offset_sample",
        "source_snapshot_consistent": False,
        "record_count": total,
        "pages": pages,
        "observed_at": max(page["fetched_at"] for page in pages),
        "completed_at": now(),
    }
    try:
        store.put_once(manifest_uri, json_bytes(manifest))
    except FileExistsError:
        winner = store.json(manifest_uri)
        if winner["request_spec_hash"] != spec_hash or winner["pages"] != pages:
            raise ValueError("Concurrent extraction committed a conflicting manifest") from None
    return manifest_uri
