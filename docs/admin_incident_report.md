# `dbtwiz admin incident-report`

Render a page of open and recently cleared incidents.

Reads the run history Elementary uploads (`dbt_invocations`, `dbt_run_results`) and
renders an HTML page of every job's open incidents — a model's consecutive failed runs
within one orchestrator job — and those cleared within `incidents_window_days`. Each
open incident shows its blast radius from the manifest: every model downstream of it,
the public/protected ones among them, and the exposures downstream.

The page is uploaded when the destination is a `gs://` URI, and written to disk
otherwise. Configured in `[tool.dbtwiz.project]`: `incidents_elementary_dataset`
(required), `incidents_window_days`, `incidents_timezone`, and `incident_report_uri` as
the default destination.

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
