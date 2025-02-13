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
            help=("List obsolete materializations without deleting anything"))] = False,
        force_delete: Annotated[bool, typer.Option(
            "--force", "-f",
            help=("Delete obsolete materializations without asking (dev target only)"))] = False,
) -> None:
    """List or delete obsolete materializations in the warehouse"""
    cleanup_materializations(target, list_only, force_delete)
