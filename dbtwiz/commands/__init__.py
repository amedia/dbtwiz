import datetime
from typing import Annotated

import typer

from ..dbt.manifest import Manifest
from ..dbt.target import Target
from ..utils.exceptions import InvalidArgumentsError
from .build import build as command_build
from .test import test as command_test

app = typer.Typer()


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
    start_date: Annotated[
        str,
        typer.Option(
            "--start-date",
            "-sd",
            help="""Date in `YYYY-mm-dd` format.
For partitioned models, this option sets the date to be passed as `data_interval_start`
variable and will be picked up by the `interval_start()` macro by the models.""",
        ),
    ] = "",
    end_date: Annotated[
        str,
        typer.Option(
            "--end-date",
            "-ed",
            help="""Date in `YYYY-mm-dd` format.
For partitioned models, this option sets the date to be passed as `data_interval_end`
variable and will be picked up by the `interval_end()` macro by the models.""",
        ),
    ] = "",
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            "-bs",
            help=(
                """Number of dates to run for in each batch.
If used outside of backfilling then only the first batch will be run."""
            ),
        ),
    ] = 9999,
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
        start_date = (
            datetime.date.today()
            if start_date == ""
            else datetime.date.fromisoformat(start_date)
        )
        end_date = (
            datetime.date.today()
            if end_date == ""
            else datetime.date.fromisoformat(end_date)
        )
    except ValueError:
        raise InvalidArgumentsError("Date must be on the YYYY-mm-dd format.")
    # Dispatch
    command_build(
        target=target.value,
        select=select,
        start_date=start_date,
        end_date=end_date,
        batch_size=batch_size,
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
