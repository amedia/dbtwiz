import datetime
from typing import Annotated, List

import typer

from ..config.project import project_config
from ..dbt.target import Target
from ..utils.decorators import description, examples
from ..utils.exceptions import InvalidArgumentsError
from ..utils.logger import error
from .cleanup import empty_development_dataset

app = typer.Typer(help="Administrative commands for dbt project management")

__all__ = ["app", "empty_development_dataset"]


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
        str, typer.Argument(help="Start of backfill period [YYYY-mm-dd]")
    ],
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
            help=("Number of dates to include in each batch."),
        ),
    ] = project_config().backfill_default_batch_size or 30,
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
) -> None:
    """Backfill date-partitioned models in production for a specified date range.

    Spawns Cloud Run jobs to process multiple dates in parallel with configurable batch sizes.
    """
    # Validate
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
) -> None:
    """List or delete orphaned materializations in the data warehouse"""
    if list_only and force_delete:
        error("You can't both list and force-delete at the same time.")
    else:
        from .cleanup import handle_orphaned_materializations

        handle_orphaned_materializations(target, list_only, force_delete)


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
