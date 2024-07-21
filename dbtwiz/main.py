import datetime

import typer
from typing_extensions import Annotated

from .build import Build
from .config import UserConfig
from .test import Test
from .backfill import Backfill
from .freshness import Freshness
from .model import Model
from .precommit import SqlFix
from .logging import debug, info, error
from .manifest import Manifest
from .target import Target
from .auth import ensure_auth


class InvalidArgumentsError(ValueError):
    pass


app = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]},
    #add_completion=False,
)


@app.command()
def build(
        target: Annotated[Target, typer.Option(
            "--target", "-t",
            help="Target")] = Target.dev,
        select: Annotated[str, typer.Option(
            "--select", "-s",
            help="Model selector passed to dbt")] = "",
        date: Annotated[str, typer.Option(
            "--date",
            help="Date to run model on [YYYY-mm-dd]")] = "",
        use_task_index: Annotated[bool, typer.Option(
            "--use-task-index",
            help=("Use task index passed by Cloud Run env as an "
                  "offset to the date"))] = False,
        save_state: Annotated[bool, typer.Option(
            "--save-state",
            help=("Save state by uploading the manifest file to the "
                  "state bucket after a successful run"))] = False,
        full_refresh: Annotated[bool, typer.Option(
            "--full-refresh", "-f",
            help="Force full refresh when building the model")] = False,
        upstream: Annotated[bool, typer.Option(
            "--upstream", "-u",
            help="Include building of upstream dependencies of the selected models")] = False,
        downstream: Annotated[bool, typer.Option(
            "--downstream", "-d",
            help="Include building of downstream dependencies of the selected models")] = False,
        work: Annotated[bool, typer.Option(
            "--work", "-w",
            help="Consider only new or changed models for interactive selection")] = False,
) -> None:
    """Build dbt models"""
    # Validate
    try:
        if date == "":
            run_date = datetime.date.today()
        else:
            run_date = datetime.date.fromisoformat(date)
    except ValueError:
        raise InvalidArgumentsError("Date must be on the YYYY-mm-dd format.")
    # Dispatch
    Build.run(
        target.value, select, run_date, use_task_index, save_state,
        full_refresh, upstream, downstream, work
    )


@app.command()
def test(
        target: Annotated[Target, typer.Option(
            "--target", "-t",
            help="Target")] = Target.dev,
        select: Annotated[str, typer.Option(
            "--select", "-s",
            help="Model selector passed to dbt")] = "",
        date: Annotated[str, typer.Option(
            "--date", "-d",
            help="Date to test model on [YYYY-mm-dd]")] = "",
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
    Test.run(target.value, select, run_date)


@app.command()
def freshness(
        target: Annotated[Target, typer.Option(
            "--target", "-t",  # does this have any function when checking sources?
            help="Target")] = Target.dev,
) -> None:
    """Run source freshness tests"""
    Freshness.run(target.value)


@app.command()
def backfill(
        select: Annotated[str, typer.Argument(
            help="Model selector passed to dbt")],
        date_first: Annotated[str, typer.Argument(
            help="Start of backfill period [YYYY-mm-dd]")],
        date_last: Annotated[str, typer.Argument(
            help="End of backfill period (inclusive) [YYYY-mm-dd]")],
        full_refresh: Annotated[bool, typer.Option(
            "--full-refresh", "-f",
            help=("Do a full refresh when building the model. "
                  "not supported when also building dependencies, "
                  "or when running over multiple days"))] = False,
        parallelism: Annotated[int, typer.Option(
            "--parallelism", "-p",
            help=("Number of tasks to run in parallel. Set to 1 for serial processing, "
                  "useful for models that depend on their own past data where the "
                  "processing order is important."))] = 8,
        status: Annotated[bool, typer.Option(
            "--status", "-s",
            help="Open job status page in browser after starting execution")] = True,
        verbose: Annotated[bool, typer.Option(
            "--verbose", "-v",
            help="Output more info about what is going on")] = False,
) -> None:
    """Backfill dbt models by generating job spec and execute through Cloud Run"""
    # Validate
    try:
        first_date = datetime.date.fromisoformat(date_first)
        last_date = datetime.date.fromisoformat(date_last)
    except ValueError:
        raise InvalidArgumentsError("Dates must be on the YYYY-mm-dd format.")
    if date_last < date_first:
        raise InvalidArgumentsError("Last date must be on or after first date.")
    if full_refresh:
        if '+' in select:
            raise InvalidArgumentsError("Full refresh is only supported on single models.")
        if date_last != date_first:
            raise InvalidArgumentsError("Full refresh in only supported on single day runs.")
    # Dispatch
    Backfill.run(
        select, first_date, last_date, full_refresh,
        parallelism, status, verbose
    )


@app.command()
def model(
        name: Annotated[str, typer.Argument(
            help="Model name or path")],
):
    """Output information about a given model"""
    Model.run(name)


@app.command()
def manifest():
    """Update dev and production manifests for fast lookup"""
    Manifest.update_manifests()


@app.command()
def sqlfix():
    """Run sqlfmt-fix and sqlfluff-fix on staged changes"""
    SqlFix.run()


@app.command()
def config(
        key: Annotated[str, typer.Argument(
            help="Configuration setting")],
        value: Annotated[str, typer.Argument(
            help="Configuration value")],
):
    """Update configuration setting"""
    UserConfig.run(key, value)


# if __name__ == "__main__":
def main():
    try:
        app()
    except InvalidArgumentsError as err:
        error(f"ERROR: Invalid arguments - {err}")
        exit(1)
