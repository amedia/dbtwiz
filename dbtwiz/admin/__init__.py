from typing import Annotated, List

import typer

from dbtwiz.dbt.target import Target
from dbtwiz.helpers.decorators import description, examples
from dbtwiz.helpers.logger import error


class InvalidArgumentsError(ValueError):
    pass


app = typer.Typer()


@app.command()
@examples(
    """Example of the basic use-case:
```shell
$ dbtwiz backfill mymodel 2024-01-01 2024-01-31
```

Another example including downstream dependencies and serial execution (needed for models that
depends on previous partitions of their own data, for example):
```shell
$ dbtwiz backfill -p1 mymodel+ 2024-01-01 2024-01-15
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
        str, typer.Argument(help="End of backfill period (inclusive) [YYYY-mm-dd]")
    ],
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            "-bs",
            help=("Number of dates to include in each batch."),
        ),
    ] = 1,
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
    """
    The _backfill_ subcommand allows you to (re)run date-partitioned models in production for a
    period spanning one or multiple days. It will spawn a Cloud Run job that will run `dbt` for
    a configurable number of days in parallel.
    """
    # Validate
    try:
        first_date = datetime.date.fromisoformat(date_first)
        last_date = datetime.date.fromisoformat(date_last)
    except ValueError:
        raise InvalidArgumentsError("Dates must be on the YYYY-mm-dd format.")
    if date_last < date_first:
        raise InvalidArgumentsError("Last date must be on or after first date.")
    if full_refresh:
        if "+" in select:
            raise InvalidArgumentsError(
                "Full refresh is only supported on single models."
            )
        if date_last != date_first:
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
    """By using defer, it is good practice to routinely clean the dbt dev dataset to ensure up to date production tables are used.
"""
)
def cleandev(
    force_delete: Annotated[
        bool,
        typer.Option(
            "--force", "-f", help=("Delete without asking for confirmation first")
        ),
    ] = False,
) -> None:
    """Delete all materializations in the dbt development dataset"""
    from .cleanup import empty_development_dataset

    empty_development_dataset(force_delete)


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
