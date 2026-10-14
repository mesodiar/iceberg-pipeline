"""Capture, load raw pages, and submit visible BigQuery SQL scripts."""

import argparse
import json
from dataclasses import replace
from pathlib import Path

from .config import Config, safe_run_id
from .extract import capture
from .ingest import load_raw, prepare_load
from .storage import Store
from .warehouse import render_sql, run_sql


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    extract = sub.add_parser("extract", help="Capture a bounded sample from the live API")
    extract.add_argument("--config", required=True)
    extract.add_argument("--run-id", required=True, type=safe_run_id)
    extract.add_argument("--bronze-root")
    fixture = sub.add_parser("capture-fixture", help="Capture a synthetic response, offline")
    fixture.add_argument("--input", required=True)
    fixture.add_argument("--run-id", default="demo", type=safe_run_id)
    fixture.add_argument("--bronze-root", default="work/bronze")
    fixture.add_argument("--observed-at", default="2026-10-12T06:00:00+00:00")
    prepare = sub.add_parser("prepare-load", help="Verify pages and write raw NDJSON; no SQL")
    prepare.add_argument("--manifest", required=True)
    prepare.add_argument("--processing-id", required=True, type=safe_run_id)
    prepare.add_argument("--output", required=True)
    sql = sub.add_parser("render-sql", help="Print SQL for review; does not run it")
    sql.add_argument("--config", required=True)
    sql.add_argument("--name", choices=["bootstrap", "normalize", "publish"], required=True)
    for command in ("load-raw", "normalize", "publish"):
        stage = sub.add_parser(command, help=f"Run the cloud {command} stage")
        stage.add_argument("--config", required=True)
        stage.add_argument("--manifest", required=True)
        stage.add_argument("--processing-id", required=True, type=safe_run_id)
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
    if args.command == "prepare-load":
        payload = prepare_load(Store(), args.manifest, args.processing_id)
        Store().put_once(args.output, payload)
        print(args.output)
        return
    config = Config.load(args.config)
    if args.command == "extract":
        if args.bronze_root:
            config = replace(config, bronze_root=args.bronze_root)
        if config.bronze_root.startswith("gs://"):
            config.require_cloud()
        print(capture(config, args.run_id))
    elif args.command == "render-sql":
        print(render_sql(args.name, config))
    elif args.command == "load-raw":
        print(load_raw(config, args.manifest, args.processing_id))
    else:
        print(run_sql(config, args.manifest, args.processing_id, args.command))


if __name__ == "__main__":
    main()
