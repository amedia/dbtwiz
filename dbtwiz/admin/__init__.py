import typer
from typing_extensions import Annotated

from dbtwiz.logging import error
from dbtwiz.target import Target

class InvalidArgumentsError(ValueError):
    pass


app = typer.Typer()


@app.command()
def orphaned(
        target: Annotated[Target, typer.Option(
            "--target", "-t",
            help="Target")] = Target.dev,
        list_only: Annotated[bool, typer.Option(
            "--list", "-l",
            help=("List orphaned materializations without deleting anything"))] = False,
        force_delete: Annotated[bool, typer.Option(
            "--force", "-f",
            help=("Delete orphaned materializations without asking (dev target only)"))] = False,
) -> None:
    """List or delete orphaned materializations in the data warehouse"""
    if list_only and force_delete:
        error("You can't both list and force-delete at the same time.")
    else:
        from .cleanup import handle_orphaned_materializations
        handle_orphaned_materializations(target, list_only, force_delete)


@app.command()
def cleandev(
        force_delete: Annotated[bool, typer.Option(
            "--force", "-f",
            help=("Delete without asking for confirmation first"))] = False,
) -> None:
    """Delete all materializations in the dbt development dataset"""
    from .cleanup import empty_development_dataset
    empty_development_dataset(force_delete)


@app.command()
def partition_expiry() -> None:
    """Checks for mismatched partition expiry and allows updating to correct."""
    from .partition import update_partition_expirations
    update_partition_expirations()
