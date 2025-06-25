from typing import Annotated, List

import typer

from dbtwiz.dbt.target import Target
from dbtwiz.helpers.decorators import description
from dbtwiz.helpers.logger import error


class InvalidArgumentsError(ValueError):
    pass


app = typer.Typer()


@app.command()
@description(
    """The command assumes a profile called `dev` exists in profiles.yml.
The user will be prompted before any tables are deleted..

By using defer, it is good practice to routinely clean the dbt dev dataset to ensure up to date production tables are used.
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
