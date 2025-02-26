import typer
from typing_extensions import Annotated

from dbtwiz.model_step import ModelStep

from .model import (
    generate_model
)


app = typer.Typer()

@app.command()
def model(
        step: Annotated[ModelStep | None, typer.Option(
            "-s", "--step", help="Transformation step")] = None,
        group: Annotated[str | None, typer.Option(
            "-g", "--group", help="Model group")] = None,
):
    """Generate new dbt model"""
    generate_model(step, group)
