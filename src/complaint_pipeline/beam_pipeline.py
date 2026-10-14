"""Batch Dataflow entrypoint; FILE_LOADS targets ordinary BigQuery staging."""

import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions

from complaint_pipeline.config import Config, safe_run_id
from complaint_pipeline.normalize import bq_schema, load_manifest, page_rows
from complaint_pipeline.storage import Store, join


class NormalizePage(beam.DoFn):
    def __init__(self, config, run_id, processing_id):
        self.config = config
        self.run_id = run_id
        self.processing_id = processing_id

    def setup(self):
        self.store = Store(self.config.project_name)

    def process(self, page):
        yield from page_rows(
            page,
            store=self.store,
            run_id=self.run_id,
            processing_id=self.processing_id,
            config=self.config,
        )


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_json", required=True)
    parser.add_argument("--manifest_uri", required=True)
    parser.add_argument("--processing_id", required=True)
    args, beam_args = parser.parse_known_args(argv)
    config = Config(**json.loads(args.config_json))
    config.require_cloud()
    safe_run_id(args.processing_id)
    manifest, _ = load_manifest(Store(config.project_name), args.manifest_uri)
    safe_run_id(manifest["run_id"])
    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = False
    pipeline = beam.Pipeline(options=options)
    rows = (
        pipeline
        | "CommittedPages" >> beam.Create(manifest["pages"])
        | "VerifyAndNormalize"
        >> beam.ParDo(NormalizePage(config, manifest["run_id"], args.processing_id))
    )
    _ = rows | "StageAllOutcomes" >> beam.io.WriteToBigQuery(
        table=f"{config.project_name}:{config.staging_dataset}.complaint_rows",
        schema=bq_schema(),
        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
        create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        custom_gcs_temp_location=join(config.temp_root, "bq-loads", args.processing_id),
    )
    _ = (
        rows
        | "InvalidRows" >> beam.Filter(lambda row: not row["is_valid"])
        | "QuarantineJSON" >> beam.Map(lambda row: json.dumps(row, ensure_ascii=False))
        | "WriteQuarantine"
        >> beam.io.WriteToText(
            join(config.quarantine_root, manifest["run_id"], args.processing_id, "rows"),
            file_name_suffix=".jsonl",
            num_shards=1,
        )
    )
    # Flex launchers must exit after submission; Composer waits for completion.
    return pipeline.run()


if __name__ == "__main__":
    main()
