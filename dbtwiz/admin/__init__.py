import typer
from typing_extensions import Annotated

from dbtwiz.target import Target

from .cleanup import cleanup_materializations


app = typer.Typer()

@app.command()
def cleanup(
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
    cleanup_materializations(target, list_only, force_delete)
