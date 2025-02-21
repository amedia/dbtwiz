import typer
from typing_extensions import Annotated

from dbtwiz.target import Target

from .cleanup import (
    cleanup_development_dataset,
    cleanup_orphaned_materializations,
)


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
    cleanup_orphaned_materializations(target, list_only, force_delete)


@app.command()
def cleandev(
        force_delete: Annotated[bool, typer.Option(
            "--force", "-f",
            help=("Delete without asking for confirmation first"))] = False,
) -> None:
    """Delete all materializations in the dbt development dataset"""
    cleanup_development_dataset(force_delete)
