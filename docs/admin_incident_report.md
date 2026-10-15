# `dbtwiz admin incident-report`

Render a page of open and recently cleared incidents.

Reads the run history Elementary uploads (`dbt_invocations`, `dbt_run_results`) and
renders an HTML page of every job's status and incidents:

- **Jobs:** each job's latest run and how often it normally runs. A job counts as not
  running once 1.5 intervals plus 15 minutes have passed since its latest run; the page
  works this out against the viewer's clock, so it stays right on a page left open.
  Clicking a job shows only its incidents.
- **Open incidents** — a model's consecutive failed runs within one job — with their
  blast radius from the manifest: every model downstream, the public/protected ones
  among them, and the exposures downstream.
- **Cleared incidents** within `incidents_window_days`, collapsed.

The page warns when it is older than `incident_report_stale_minutes`, which means the
job refreshing it has stopped. Model names link into dbt docs when
`incident_report_dbt_docs_url` is set, job names to their DAG when
`incident_report_airflow_url` is set, and an error to its BigQuery job when the message
carries the link.

A JSON summary is written beside the page (`.html` replaced by `.json`) for an overview
page to fetch: generation time and stale limit, each job's latest run and open incident
count, and the open incidents.

Both are uploaded when the destination is a `gs://` URI, and written to disk otherwise.
Configured in `[tool.dbtwiz.project]`: `incidents_elementary_dataset` (required),
`incidents_window_days`, `incidents_timezone`, `incident_report_uri` as the default
destination, `incident_report_stale_minutes`, `incident_report_dbt_docs_url` and
`incident_report_airflow_url`.

## Options

### `--destination`

gs://bucket/path or a local file. Defaults to incident_report_uri.

### `--manifest-path`

Manifest the blast radius is read from.

### `--as-of`

Render the history as it stood at this UTC time.

## Examples

Upload the report to the configured destination:
```shell
$ dbtwiz admin incident-report
```

Write it locally as it stood at a past moment:
```shell
$ dbtwiz admin incident-report --destination report.html --as-of 2026-10-14T03:30:00
```
