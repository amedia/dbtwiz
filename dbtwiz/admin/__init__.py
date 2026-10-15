import datetime
from pathlib import Path
from typing import Annotated, List

import typer

from ..config.project import project_config
from ..dbt.manifest import Manifest
from ..dbt.target import Target
from ..utils.decorators import description, examples
from ..utils.exceptions import InvalidArgumentsError
from ..utils.logger import error
from .cleanup import empty_development_dataset

app = typer.Typer(help="Administrative commands for dbt project management")

__all__ = ["app", "empty_development_dataset"]


def _validate_backfill_args(
    select: str,
    date_first: str | None,
    date_last: str | None,
    full_refresh: bool,
    retry: bool,
) -> tuple[datetime.date | None, datetime.date | None]:
    """Validate backfill arguments and return (first_date, last_date)."""
    from ..utils.logger import warn

    if retry:
        if full_refresh:
            raise InvalidArgumentsError(
                "--retry cannot be combined with --full-refresh."
            )
        if date_first is not None or date_last is not None:
            warn("Date arguments are ignored when --retry is set.")
        return None, None

    if date_first is None:
        raise InvalidArgumentsError(
            "date_first is required (omit it only when using --retry)."
        )
    try:
        first_date = datetime.date.fromisoformat(date_first)
        last_date = datetime.date.fromisoformat(date_last) if date_last else first_date
    except ValueError:
        raise InvalidArgumentsError("Dates must be on the YYYY-mm-dd format.")
    if last_date < first_date:
        raise InvalidArgumentsError("Last date must be on or after first date.")
    if full_refresh:
        if "+" in select:
            raise InvalidArgumentsError(
                "Full refresh is only supported on single models."
            )
        if last_date != first_date:
            raise InvalidArgumentsError(
                "Full refresh in only supported on single day runs."
            )
    return first_date, last_date


@app.command()
@examples(
    """Example of the basic use-case:
```shell
$ dbtwiz backfill mymodel 2024-01-01 2024-01-31
```

Another example including downstream dependencies and serial execution (needed for models that
depends on previous partitions of their own data, for example):
```shell
$ dbtwiz backfill mymodel+ 2024-01-01 2024-01-15 -p 1
```

After the job has been set up and passed on to Cloud Run, a status page should automatically
be opened in your browser so you can track progress."""
)
def backfill(
    select: Annotated[str, typer.Argument(help="Model selector passed to dbt")],
    date_first: Annotated[
        str,
        typer.Argument(
            help=(
                "Start of backfill period [YYYY-mm-dd]. "
                "Optional (and ignored) when --retry is set."
            ),
            metavar="TEXT",
        ),
    ] = None,
    date_last: Annotated[
        str,
        typer.Argument(
            help="End of backfill period (inclusive) [YYYY-mm-dd]. Defaults to date_first.",
            metavar="TEXT",
        ),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            "-b",
            help=(
                "Number of dates to include in each batch. "
                "When used with --retry, subdivides each failed range to this size; "
                "if omitted on retry, the failed ranges are retried as-is."
            ),
        ),
    ] = None,
    retry: Annotated[
        bool,
        typer.Option(
            "--retry",
            help=(
                "Retry only the failed tasks from the most recent execution of this backfill "
                "job (looked up by selector). When set, date arguments are ignored. "
                "Pass --batch-size to subdivide failed ranges before retrying."
            ),
        ),
    ] = False,
    full_refresh: Annotated[
        bool,
        typer.Option(
            "--full-refresh",
            "-f",
            help=(
                "Build the model with full refresh, which causes existing tables to be deleted "
                "and recreated. Needed when schema has changed between runs. "
                "**This should only be used when backfilling a single date, ie. when "
                "_date_first_ and _date_last_ are the same.**"
            ),
        ),
    ] = False,
    parallelism: Annotated[
        int,
        typer.Option(
            "--parallelism",
            "-p",
            help=(
                "Number of tasks to run in parallel. Set to 1 for serial processing, "
                "useful for models that depend on their own past data where the "
                "processing order is important."
            ),
        ),
    ] = 8,
    status: Annotated[
        bool,
        typer.Option(
            "--status",
            "-s",
            help="Open job status page in browser after starting execution",
        ),
    ] = True,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Output more info about what is going on"),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help=(
                "Print the tasks that would be submitted without actually running the job. "
                "Works with and without --retry."
            ),
        ),
    ] = False,
) -> None:
    """Backfill date-partitioned models in production for a specified date range.

    Spawns Cloud Run jobs to process multiple dates in parallel with configurable batch sizes.
    Use --retry to re-run only the failed tasks from the most recent execution.
    """
    batch_size_overridden = batch_size is not None
    if batch_size is None:
        batch_size = project_config().backfill_default_batch_size or 30

    first_date, last_date = _validate_backfill_args(
        select, date_first, date_last, full_refresh, retry
    )

    from .backfill import backfill as command_backfill

    # Dispatch
    command_backfill(
        selector=select,
        first_date=first_date,
        last_date=last_date,
        batch_size=batch_size,
        full_refresh=full_refresh,
        parallelism=parallelism,
        status=status,
        verbose=verbose,
        retry=retry,
        dry_run=dry_run,
        batch_size_overridden=batch_size_overridden,
    )


@app.command()
@description(
    """Unless overriden, it will default to looking for target called `dev` in profiles.yml.
The user will be prompted before any tables are deleted.

By using defer, it is good practice to routinely clean the dbt dev dataset to ensure up to date production tables are used.
"""
)
def cleandev(
    target: Annotated[
        Target, typer.Option("--target", "-t", help="Target")
    ] = Target.dev,
    force_delete: Annotated[
        bool,
        typer.Option(
            "--force", "-f", help=("Delete without asking for confirmation first")
        ),
    ] = False,
) -> None:
    """Delete all materializations in the dbt development dataset"""
    empty_development_dataset(target_name=target, force_delete=force_delete)


@app.command()
@description(
    """It will identify any tables/views created originally by dbt that are now outdated.
This is identified by comparing to the related manifest (dev or prod).

If using the list option, then it will only list the tables that are no longer present in the manifest.
If not then it will also enable selection of which tables to delete.
"""
)
def orphaned(
    target: Annotated[
        Target, typer.Option("--target", "-t", help="Target")
    ] = Target.dev,
    list_only: Annotated[
        bool,
        typer.Option(
            "--list",
            "-l",
            help=("List orphaned materializations without deleting anything"),
        ),
    ] = False,
    force_delete: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help=("Delete orphaned materializations without asking (dev target only)"),
        ),
    ] = False,
    include_disabled: Annotated[
        bool,
        typer.Option(
            "--include-disabled",
            help=(
                "Include materializations of models disabled in the project "
                "(enabled: false), which are excluded by default"
            ),
        ),
    ] = False,
) -> None:
    """List or delete orphaned materializations in the data warehouse"""
    if list_only and force_delete:
        error("You can't both list and force-delete at the same time.")
    else:
        from .cleanup import handle_orphaned_materializations

        handle_orphaned_materializations(
            target, list_only, force_delete, include_disabled
        )


@app.command()
@description(
    """When run, the current partition expiration definition in BigQuery will be compared with the definition in dbt for the model:
```
partition_expiration_days: <number of days>
```
The tables with differing values will be listed, and it's then possible to select which tables to update partition expiration for.

When comparing, the function uses the production manifest rather then the local version.
"""
)
def partition_expiry(
    model_names: Annotated[
        List[str],
        typer.Option(
            "--model-name",
            "-m",
            help="Name of model to be checked for partition expiry",
        ),
    ] = None,
) -> None:
    """Checks for mismatched partition expiry and allows updating to correct."""
    from .partition import update_partition_expirations

    update_partition_expirations(model_names)


@app.command()
@description(
    """Restores a deleted BigQuery table from a snapshot using BigQuery time travel.

The command will:
1. Verify that the table is actually deleted
2. Parse the provided timestamp into the correct format
3. Use BigQuery's time travel feature to restore from the snapshot
4. Create the recovered table with the specified name (or default to table_name_recovered)

**Note:** BigQuery time travel is limited to 7 days. You can only restore tables that were deleted within the past 7 days.

**Timestamp formats supported:**
- Epoch milliseconds (e.g., 1705315800000)
- ISO 8601 format (e.g., 2024-01-15T10:30:00)
- Date format (e.g., 2024-01-15 10:30:00)
"""
)
@examples(
    """Basic restore example with default recovered table name:
```shell
$ dbtwiz admin restore my-project.my_dataset.my_table 2024-01-15T10:30:00
```

Restore with custom recovered table name:
```shell
$ dbtwiz admin restore my-project.my_dataset.my_table 1705315800000 --recovered-table my-project.my_dataset.my_table_backup
```

Restore with epoch milliseconds:
```shell
$ dbtwiz admin restore my-project.my_dataset.my_table 1705315800000
```"""
)
def restore(
    table_id: Annotated[
        str,
        typer.Argument(
            help="Full table ID (project.dataset.table) of the deleted table to restore"
        ),
    ],
    timestamp: Annotated[
        str,
        typer.Argument(
            help="Snapshot timestamp as epoch milliseconds or date format (YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS)"
        ),
    ],
    recovered_table_id: Annotated[
        str,
        typer.Option(
            "--recovered-table",
            "-r",
            help="Full table ID for the recovered table. Defaults to original table name with '_recovered' suffix",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Output more info about what is going on"),
    ] = False,
) -> None:
    """Restore a deleted BigQuery table from a snapshot using time travel."""
    from .restore import restore as command_restore

    command_restore(
        table_id=table_id,
        timestamp=timestamp,
        recovered_table_id=recovered_table_id,
        verbose=verbose,
    )


@app.command()
@description(
    """Reads descriptions from the production dbt manifest and compares them with the current
BigQuery table and column descriptions. In dry-run mode (default), it lists which tables
have outdated descriptions without making any changes. Use `--apply` to push the updates.

Only columns documented in the manifest are updated; undocumented columns keep their
existing BigQuery descriptions unchanged. Ephemeral models and tables not found in
BigQuery are skipped.
"""
)
def sync_descriptions(
    manifest_path: Annotated[
        Path,
        typer.Option(
            "--manifest-path",
            help="Path to dbt manifest.json. Defaults to the prod manifest download location.",
        ),
    ] = Manifest.PROD_MANIFEST_PATH,
    model_names: Annotated[
        List[str],
        typer.Option(
            "--model-name",
            "-m",
            help="Limit sync to specific model name(s). Can be repeated.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run/--apply",
            help="Show changes without applying them (default: --dry-run)",
        ),
    ] = True,
    impersonate: Annotated[
        bool,
        typer.Option(
            "--impersonate",
            help=(
                "Impersonate the service account configured as "
                "service_account_identifier in pyproject.toml."
            ),
        ),
    ] = False,
) -> None:
    """Sync dbt descriptions from a manifest to BigQuery tables and columns."""
    from .descriptions import sync_descriptions as command_sync_descriptions

    command_sync_descriptions(
        dry_run=dry_run,
        model_names=model_names or None,
        manifest_path=manifest_path,
        impersonate=impersonate,
    )


@app.command()
@description(
    """Updates the clustering configuration for a BigQuery table and re-clusters all existing rows.

The command will:
1. Update the clustering specification on the table metadata
2. Run an UPDATE statement to physically re-cluster all existing rows

If the table has `require_partition_filter` enabled, a partition filter is automatically added
to the UPDATE statement to satisfy BigQuery's requirement.
"""
)
@examples(
    """Update clustering on a table with a single column:
```shell
$ dbtwiz admin update-clustering --table-id my-project.my_dataset.my_table --cluster-column my_column
```

Update clustering with multiple columns (in order):
```shell
$ dbtwiz admin update-clustering --table-id my-project.my_dataset.my_table --cluster-column col_a --cluster-column col_b
```"""
)
def update_clustering(
    table_id: Annotated[
        str,
        typer.Option(
            "--table-id",
            "-t",
            help="Full table ID (project.dataset.table) of the table to update",
        ),
    ],
    cluster_columns: Annotated[
        List[str],
        typer.Option(
            "--cluster-column",
            "-c",
            help="Column to cluster on. Can be repeated to specify multiple columns in order.",
        ),
    ],
) -> None:
    """Update clustering configuration for a BigQuery table and re-cluster existing rows."""
    if not cluster_columns:
        error("At least one --cluster-column is required.")
        return
    from .clustering import update_clustering as command_update_clustering

    command_update_clustering(table_id=table_id, cluster_columns=cluster_columns)


@app.command()
@description(
    """Reads the dbt manifest and project vars (from `vars.yml` and/or `dbt_project.yml`)
to resolve the desired IAM grants for every model, then fetches current table-level IAM
policies in parallel and applies the minimal set of changes needed to reach the desired state.

Grant configuration is resolved from the following sources (in order):
- Explicit `grants` config on the model
- `meta.teams` resolved via the `teams` var (in `vars.yml` or `dbt_project.yml`)
- `meta.access-policy` resolved via the `access-policies` var (in `vars.yml` or `dbt_project.yml`)
- `meta.service-consumers` resolved via the `service-consumers` var (in `vars.yml` or `dbt_project.yml`)
- Auto-grant to `grants_open_access_group` for models with `access: protected` or `access: public`

Models are skipped when they have `meta.skip_grants: true`, use Iceberg (`catalog_name`),
are ephemeral, or belong to a schema listed in `grants_skip_schemas` (pyproject.toml).
"""
)
def update_grants(
    manifest_path: Annotated[
        Path,
        typer.Option(
            "--manifest-path",
            help="Path to dbt manifest.json. Defaults to the prod manifest download location.",
        ),
    ] = Manifest.PROD_MANIFEST_PATH,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run/--apply", help="Show changes without executing them (default)"
        ),
    ] = True,
    resolve_only: Annotated[
        bool,
        typer.Option(
            "--resolve-only",
            help="Only resolve desired grants from the manifest without querying BigQuery",
        ),
    ] = False,
    impersonate: Annotated[
        bool,
        typer.Option(
            "--impersonate",
            help=(
                "Impersonate the service account configured as "
                "service_account_identifier in pyproject.toml."
            ),
        ),
    ] = False,
) -> None:
    """Update BigQuery table IAM grants for all dbt models based on manifest configuration."""
    from .grants import update_grants as command_update_grants

    command_update_grants(
        manifest_path=manifest_path,
        dry_run=dry_run,
        resolve_only=resolve_only,
        impersonate=impersonate,
    )


@app.command()
@description(
    """Reads the run history Elementary uploads (`dbt_invocations`, `dbt_run_results`) and
groups each model's consecutive failed runs within one orchestrator job into an incident.
Only the given job's latest run is evaluated, and one Slack message is posted when that
run pushed an incident past an escalation threshold — 3h, 6h, 12h, … from its first
failed run, doubling from `incident_alert_escalation_hours` — or cleared one that had
been escalated. Thresholds are in time, not runs, so a daily job escalates at its second
failure.

Escalations are grouped by cause. Each one lists its blast radius from the manifest:
every model downstream of the failing ones, the public/protected ones among them, and
the exposures downstream.

The Slack token is read from `DBTWIZ_SLACK_TOKEN`. Configured in `[tool.dbtwiz.project]`:
`incidents_elementary_dataset` (required), `incidents_window_days`,
`incidents_timezone`, `incident_alert_slack_channel`,
`incident_alert_escalation_hours`, `incident_alert_mentions` and `incident_report_url`
for a link to the report.

`incident_alert_mentions` lists who is notified when something escalates; a recovery
mentions nobody. Each entry is one of:

- **An ID (recommended):** `@U111D1DB111` for a person, `@S012ABCDEF` for a user
  group. Used as is, so it can't fail to resolve, and survives renames. A person's ID is
  under their Slack profile → ⋮ → Copy member ID.
- **A handle:** `@firstname.lastname` for a person (username or display name),
  `@data-team` for a user group. Looked up when posting, which needs the `users:read`
  scope (and `usergroups:read` for groups) and pages through every user, so in a large
  workspace it can hit Slack's rate limit. A handle that isn't found is posted as plain
  text with a note rather than holding the alert back.
- **`@channel` or `@here`:** everyone in the channel, or only those active right now.
  `@here` notifies nobody outside working hours.
"""
)
@examples(
    """Show what the hourly job's latest run would post, without posting it:
```shell
$ dbtwiz admin incident-alert --job-id adp__hourly --dry-run
```

Replay a past moment, e.g. to check what an incident looked like while it was ongoing:
```shell
$ dbtwiz admin incident-alert --job-id adp__hourly --as-of 2026-10-14T03:30:00 --dry-run
```"""
)
def incident_alert(
    job_id: Annotated[
        str,
        typer.Option(
            "--job-id",
            help="Orchestrator job (Airflow DAG id) whose latest run is evaluated.",
        ),
    ],
    channel: Annotated[
        str,
        typer.Option(
            "--channel",
            help="Slack channel. Defaults to incident_alert_slack_channel.",
        ),
    ] = None,
    manifest_path: Annotated[
        Path,
        typer.Option(
            "--manifest-path",
            help="Manifest the blast radius is read from.",
        ),
    ] = Path("target", "manifest.json"),
    as_of: Annotated[
        datetime.datetime,
        typer.Option(
            "--as-of",
            formats=["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"],
            help="Evaluate the history as it stood at this UTC time. Only with --dry-run.",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Print the message instead of posting it."),
    ] = False,
) -> None:
    """Post to Slack when a job's latest run escalated or cleared an incident."""
    import os

    from .incident_alert import incident_alert as command_incident_alert

    command_incident_alert(
        job_id=job_id,
        channel=channel,
        slack_token=os.environ.get("DBTWIZ_SLACK_TOKEN"),
        manifest_path=manifest_path,
        as_of=as_of.replace(tzinfo=datetime.timezone.utc) if as_of else None,
        dry_run=dry_run,
    )


@app.command()
@description(
    """Reads the run history Elementary uploads (`dbt_invocations`, `dbt_run_results`) and
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
page to fetch: generation time and stale limit, each job's latest run, when it goes
overdue and its open incident count, and the open incidents. Its field names follow
dlt-saga's report summary (`stale_after_minutes`, `last_attempt_at`, `usual_gap_minutes`,
`overdue_at`, `failing_since`, `report_file`), so one page can read both.

Both are uploaded when the destination is a `gs://` URI, and written to disk otherwise.
Configured in `[tool.dbtwiz.project]`: `incidents_elementary_dataset` (required),
`incidents_window_days`, `incidents_timezone`, `incident_report_uri` as the default
destination, `incident_report_stale_minutes`, `incident_report_dbt_docs_url` and
`incident_report_airflow_url`.
"""
)
@examples(
    """Upload the report to the configured destination:
```shell
$ dbtwiz admin incident-report
```

Write it locally as it stood at a past moment:
```shell
$ dbtwiz admin incident-report --destination report.html --as-of 2026-10-14T03:30:00
```"""
)
def incident_report(
    destination: Annotated[
        str,
        typer.Option(
            "--destination",
            help="gs://bucket/path or a local file. Defaults to incident_report_uri.",
        ),
    ] = None,
    manifest_path: Annotated[
        Path,
        typer.Option(
            "--manifest-path",
            help="Manifest the blast radius is read from.",
        ),
    ] = Path("target", "manifest.json"),
    as_of: Annotated[
        datetime.datetime,
        typer.Option(
            "--as-of",
            formats=["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"],
            help="Render the history as it stood at this UTC time.",
        ),
    ] = None,
) -> None:
    """Render a page of open and recently cleared incidents."""
    from .incident_report import incident_report as command_incident_report

    command_incident_report(
        destination=destination,
        manifest_path=manifest_path,
        as_of=as_of.replace(tzinfo=datetime.timezone.utc) if as_of else None,
    )
