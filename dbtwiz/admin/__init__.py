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
) -> None:
    cleanup_materializations(target)
