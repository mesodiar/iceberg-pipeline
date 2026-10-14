"""The default local path never requires GCP credentials."""

import argparse
import json
from dataclasses import replace
from pathlib import Path

from .config import Config, safe_run_id
from .extract import capture
from .normalize import load_manifest, page_rows
from .storage import Store
from .warehouse import publish, render_sql


def local_process(config, manifest_uri, output_dir, processing_id):
    store = Store()
    manifest, _ = load_manifest(store, manifest_uri)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    counts = {"input": 0, "accepted": 0, "quarantined": 0}
    with (
        (output / "accepted.jsonl").open("w", encoding="utf-8") as accepted,
        (output / "quarantine.jsonl").open("w", encoding="utf-8") as rejected,
    ):
        for page in manifest["pages"]:
            for row in page_rows(
                page,
                store=store,
                run_id=manifest["run_id"],
                processing_id=processing_id,
                config=config,
            ):
                counts["input"] += 1
                counts["accepted" if row["is_valid"] else "quarantined"] += 1
                stream = accepted if row["is_valid"] else rejected
                stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
    if counts["input"] != manifest["record_count"]:
        raise ValueError("Manifest count mismatch")
    (output / "summary.json").write_text(json.dumps(counts, indent=2) + "\n")
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    extract = sub.add_parser("extract", help="Capture a bounded sample from the live API")
    extract.add_argument("--config", required=True)
    extract.add_argument("--run-id", required=True, type=safe_run_id)
    extract.add_argument("--bronze-root", help="Override GCS with a local directory")
    fixture = sub.add_parser("capture-fixture", help="Capture a synthetic response, offline")
    fixture.add_argument("--input", required=True)
    fixture.add_argument("--run-id", default="demo", type=safe_run_id)
    fixture.add_argument("--bronze-root", default="work/bronze")
    fixture.add_argument("--observed-at", default="2026-10-12T06:00:00+00:00")
    local = sub.add_parser("normalize", help="Verify and normalize captured Bronze locally")
    local.add_argument("--config", required=True)
    local.add_argument("--manifest", required=True)
    local.add_argument("--output", default="work/normalized")
    local.add_argument("--processing-id", default="local", type=safe_run_id)
    sql = sub.add_parser("render-sql", help="Print SQL for review; does not run it")
    sql.add_argument("--config", required=True)
    sql.add_argument("--name", choices=["bootstrap", "publish"], required=True)
    promotion = sub.add_parser("publish", help="Publish an already completed Dataflow batch")
    promotion.add_argument("--config", required=True)
    promotion.add_argument("--manifest", required=True)
    promotion.add_argument("--processing-id", required=True, type=safe_run_id)
    args = parser.parse_args(argv)
    if args.command == "capture-fixture":
        body = Path(args.input).read_bytes()
        config = Config(bronze_root=args.bronze_root)

        def fetch_fixture(config, params):
            payload = json.loads(body)
            rows = payload["results"][params["offset"] : params["offset"] + params["limit"]]
            return json.dumps(payload | {"results": rows}, ensure_ascii=False).encode("utf-8")

        print(capture(config, args.run_id, fetcher=fetch_fixture, now=lambda: args.observed_at))
        return
    config = Config.load(args.config)
    if args.command == "extract":
        if args.bronze_root:
            config = replace(config, bronze_root=args.bronze_root)
        if config.bronze_root.startswith("gs://"):
            config.require_cloud()
        print(capture(config, args.run_id))
    elif args.command == "normalize":
        print(json.dumps(local_process(config, args.manifest, args.output, args.processing_id)))
    elif args.command == "render-sql":
        print(render_sql(args.name, config))
    elif args.command == "publish":
        print(publish(config, args.manifest, args.processing_id))


if __name__ == "__main__":
    main()
