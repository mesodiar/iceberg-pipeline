# Data contract and metric meanings

## Source access

The configured source is the public Bangkok endpoint:
`https://publicapi.traffy.in.th/share/teamchadchart/search`.
The adapter accepts successful JSON envelopes with `status: true` and a `results` array,
including HTTP 201 responses. It sends only `limit` and `offset`.

There is no implemented CDC feed, source snapshot token, update watermark, historical-date
filter, or per-ticket lookup. A repeat sample may not revisit older open tickets. Verify
source-supported update filtering and access limits before adding those features. Never call
this a complete population refresh. Airflow's logical date is not passed as an API filter.

## Bronze

```text
bronze/traffy/run_id=<run_id>/
  page-0000000000.json
  page-0000000100.json
  manifest.json
```

A page is an envelope containing `raw_json` (original response text), `body_sha256`,
`fetched_at`, request parameters, and the extraction-configuration hash. The manifest records
each envelope's checksum and row count. GCS uses generation-match-zero writes. Local writes
use atomic exclusive publication. A failed capture may leave pages but no manifest; rerunning
the same run ID resumes them. A resumed capture can span a long interval, so `observed_at`
is recorded per page. `manifest.observed_at` is the latest page fetch time.

The record identity hashes run ID, page URI, page-body hash, and row position. A replay must use
the original manifest and original object paths. Do not copy a capture to another path and
edit its manifest while keeping the same run ID.

## Normalized fields

| Source | Normalized | Rule |
| --- | --- | --- |
| `ticket_id` | `ticket_id` | Required nonempty string |
| `timestamp` | `created_at` | Required timezone-aware timestamp, converted to UTC |
| `last_activity` | `source_updated_at` | Required timezone-aware timestamp; cannot precede creation |
| `state` | `state`, `state_group` | Preserve Thai text; classify through explicit configuration |
| `org` | `organization` | Nullable string; no assumed district dimension |
| `type` | `categories` | Null → empty array; string → one category; arrays sorted/deduplicated |
| `problem_type_abdul` | `problem_categories` | Same shape handling, kept separate from source categories |
| `count_reopen` | `reopen_count` | Nullable nonnegative INT64; numeric strings supported |
| `star` | `star` | Nullable finite number in 0..5 |
| `coords` | `longitude`, `latitude` | Nullable pair, explicit order, geographic range checks |

The example assumes `lon_lat`; verify it against your source sample. Values are never swapped
heuristically. The initial mapping recognizes `รอรับเรื่อง` and `กำลังดำเนินการ` as open,
and `เสร็จสิ้น` as closed. All other labels are `unknown`, so they cannot silently inflate
the closed count. Review the mapping against actual source semantics before treating it as
an operational KPI. Mapping changes alter `policy_hash` and cannot replace an already
published capture. Increment `CONTRACT_VERSION` when normalization logic changes.

Free text, addresses, photo URLs, and notes remain in restricted Bronze, not in the analytic
projection. Quarantine includes provenance and rejection reasons; the original record is
retrievable from its Bronze page. `content_hash` covers curated business fields, not every
source field. Adding an unmapped API field therefore does not change the analytic hash.

## Tables

`staging.complaint_rows` is a standard BigQuery table with a 30-day partition expiry based
on the original observation date. Historical replay older than that requires you to extend
or disable this expiration first. `processing_id` isolates processing attempts; delivery
retries may append identical staging rows. SQL deduplicates by `row_id`, then checks counts.
Use a new processing ID when rerunning with corrected code after a failed unpublished attempt.

`silver.complaints_current` contains the highest `source_updated_at` version ever accepted
for each ticket, breaking ties by observation time and source row ID. It is a union of sampled
tickets and can contain stale tickets that have not been revisited. It is not current citywide
backlog. Source timestamp corrections that move backwards need an explicit reconciliation
policy; this implementation will not regress current state automatically.

`silver.complaint_observation_history` keeps one observation per ticket per capture. Duplicate
tickets within an extraction are resolved by source update time, fetch time, then row ID.
Unchanged observations across different captures are intentionally retained. The publication
receipt makes replay of a capture a no-op, even after staging expires.

| Gold table | Meaning |
| --- | --- |
| `daily_backlog` | Counts by organization and open/closed/unknown group **within each sample** |
| `complaint_age_distribution` | Age bands of explicitly open tickets **within each sample** |
| `observed_state_changes` | Consecutive observed state or reopen-count differences per ticket |

Dates are Bangkok calendar dates. `daily_backlog` includes all status groups so sample counts
can reconcile; filter `state_group = 'open'` for the sampled open count. Each metric includes
`run_id`, so multiple captures on one day are separate snapshots. Do not sum them into a daily
unique-ticket total. Category arrays are retained in Silver; unnesting them counts category
assignments and can count a ticket more than once.

Transitions are ordered by `observed_at`, not ingestion order. Late-arriving captures trigger
recalculation for affected tickets. A source cache can show an older state on a later fetch;
that still describes what was observed. A negative reopen delta is retained as a source
correction, not silently converted to zero. A null count does not imply zero.

`last_activity` is not a verified completion timestamp. This project does not claim actual
resolution durations, exhaustive transition history, observed deletions, or citywide trends.

## Failure and retry behavior

| Failure | Result / recovery |
| --- | --- |
| HTTP 429 or transient 5xx | Bounded retry/backoff; numeric Retry-After honored up to 120 seconds |
| Permanent HTTP failure | Fail extraction; no manifest commit |
| Process stops during extraction | Resume existing immutable pages with the same run ID/config |
| Checksum mismatch or unknown API envelope | Fail the batch, rather than quarantine every row |
| Invalid record | Stage a rejected outcome and write GCS quarantine |
| BigQuery load failure | Dataflow fails; publication must not run |
| Missing staging records | Manifest reconciliation fails; published tables remain unchanged |
| Rejection ratio above configured threshold | Entire publication is rejected |
| SQL statement fails | Transaction rolls back Silver, Gold, and receipt together |
| Publisher retries after commit | Receipt makes the original run a no-op |
| Different input/policy under a published run ID | Refuse mutation; use a controlled migration |

For this tutorial, serialize **all publishers**, including different DAGs or manual commands.
`max_active_runs=1` protects only this DAG. Staging and GCS quarantine are prepublication
diagnostics and are not part of the Silver/Gold transaction. A receipt is written only after
successful validation; failed jobs remain visible in Airflow, Dataflow, and BigQuery job logs.
