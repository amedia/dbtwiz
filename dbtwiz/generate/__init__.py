import typer
from typing_extensions import Annotated

from .model import generate_model


app = typer.Typer()

@app.command()
def model(
        quick: Annotated[bool, typer.Option(
            "--quick", "-q",
            help="Skip non-essential questions to quickly get started")] = False,
):
    """Generate new dbt model"""
    generate_model(quick)
