import datetime
from typing import Annotated

import typer

from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.target import Target
from dbtwiz.helpers.decorators import examples

from .backfill import backfill as command_backfill
from .build import build as command_build
from .test import test as command_test

app = typer.Typer()


class InvalidArgumentsError(ValueError):
    pass


@app.command()
def build(
    target: Annotated[
        Target,
        typer.Option(
            "--target",
            "-t",
            help="""Build target. Only dev is supported.\n
As a model developer, you should never have to use this option.
If you need to rebuild models in production, use the [backfill](backfill.md) command.""",
        ),
    ] = Target.dev,
    select: Annotated[
        str,
        typer.Option(
            "--select",
            "-s",
            help="""Model selector. If an existing model matches the given selector string exactly,
it will build with no further interaction. Otherwise an interactive selection
list will show all models which partially matches using fuzzy match to allow
you to refine your search.""",
        ),
    ] = "",
    date: Annotated[
        str,
        typer.Option(
            "--date",
            help="""Date in `YYYY-mm-dd` format.
For partitioned models, this option sets the date to be passed as `data_interval_start`
variable and will be picked up by the `start_date()` macro by the models.""",
        ),
    ] = "",
    use_task_index: Annotated[
        bool,
        typer.Option(
            "--use-task-index",
            help="""This option is only relevant for backfilling, and is set by Cloud Run to
offset the date for partitioned models relative to the start date.""",
        ),
    ] = False,
    full_refresh: Annotated[
        bool,
        typer.Option(
            "--full-refresh",
            "-f",
            help="""Build the model with full refresh, which causes existing tables to be deleted and
recreated. Needed when schema has changed between runs.""",
        ),
    ] = False,
    upstream: Annotated[
        bool,
        typer.Option(
            "--upstream",
            "-u",
            help="""Also build upstream models on which the selected model(s) are dependent.
This will prepend a '+' to your chosen models when passing them on to _dbt_.""",
        ),
    ] = False,
    downstream: Annotated[
        bool,
        typer.Option(
            "--downstream",
            "-d",
            help="""Also build downstream models that are directly or indirectly depdendent on the selected model(s).
This will append a '+' to your chosen models when passing them on to _dbt_.""",
        ),
    ] = False,
    work: Annotated[
        bool,
        typer.Option(
            "--work",
            "-w",
            help="""When used, this option causes interactive selection to include only models that
have *staged* local modifications according to `git status`.""",
        ),
    ] = False,
    repeat_last: Annotated[
        bool,
        typer.Option(
            "--last",
            "-l",
            help="""When you build with _dbtwiz_, it will store a list of selected models in the
file `.dbtwiz/last_select.json` in the current project.

Pass this option to rebuild the same models that you most recently built.""",
        ),
    ] = False,
) -> None:
    """
    Build one or more dbt models, using interactive selection with fuzzy-matching,
    unless an exact model name is passed.
    """
    # Validate
    try:
        if date == "":
            run_date = datetime.date.today()
        else:
            run_date = datetime.date.fromisoformat(date)
    except ValueError:
        raise InvalidArgumentsError("Date must be on the YYYY-mm-dd format.")
    # Dispatch
    command_build(
        target=target.value,
        select=select,
        date=run_date,
        use_task_index=use_task_index,
        full_refresh=full_refresh,
        upstream=upstream,
        downstream=downstream,
        work=work,
        repeat_last=repeat_last,
    )


@app.command()
def test(
    target: Annotated[
        Target, typer.Option("--target", "-t", help="Target. Only dev is supported.")
    ] = Target.dev,
    select: Annotated[
        str, typer.Option("--select", "-s", help="Model selector passed to dbt")
    ] = "",
    date: Annotated[
        str, typer.Option("--date", "-d", help="Date to test model on [YYYY-mm-dd]")
    ] = "",
) -> None:
    """Test dbt models"""
    # Validate
    try:
        if date == "":
            run_date = datetime.date.today()
        else:
            run_date = datetime.date.fromisoformat(date)
    except ValueError:
        raise InvalidArgumentsError("Date must be on the YYYY-mm-dd format.")
    # Dispatch
    command_test(target.value, select, run_date)


@app.command()
def manifest(
    type: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="Which manifest to update. One of ['all', 'dev', 'prod']. Default 'all'.",
        ),
    ] = "all",
):
    """Update dev and production manifests for fast lookup"""
    Manifest.update_manifests(type)


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
    # Dispatch
    command_backfill(
        select, first_date, last_date, full_refresh, parallelism, status, verbose
    )
