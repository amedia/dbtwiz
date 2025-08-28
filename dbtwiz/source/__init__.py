from typing import Annotated, List

import typer

from ..utils.decorators import description
from .create import create_source

app = typer.Typer(help="Commands for dbt source management")


@app.command()
@description(
    """Creates a new dbt source definition interactively.
    
    Guides you through selecting project, dataset, and tables with autocomplete.
    Automatically generates source YAML files with proper descriptions.
    
    The command will:
    1. Help you select the appropriate project and dataset
    2. Provide autocomplete for available tables
    3. Generate proper source YAML structure
    4. Set appropriate descriptions and metadata
    5. Handle both new source definitions and adding to existing ones
    """
)
def create(
    source_name: Annotated[
        str,
        typer.Option(
            "--source_name",
            "-s",
            help="Where the source is located (existing alias used for project+dataset combination)",
        ),
    ] = None,
    source_description: Annotated[
        str,
        typer.Option(
            "--source-description",
            "-sd",
            help="A short description for the project+dataset combination (if this combination is new)",
        ),
    ] = None,
    project_name: Annotated[
        str,
        typer.Option(
            "--project-name", "-p", help="In which project the table is located"
        ),
    ] = None,
    dataset_name: Annotated[
        str,
        typer.Option(
            "--dataset-name", "-d", help="In which dataset the table is located"
        ),
    ] = None,
    table_names: Annotated[
        List[str],
        typer.Option(
            "--table-name", "-t", help="Name(s) of table(s) to be added as source(s)"
        ),
    ] = None,
    table_description: Annotated[
        str,
        typer.Option(
            "--table-description",
            "-td",
            help="A short description for the table, if only one is provided",
        ),
    ] = None,
):
    """Create new dbt source"""
    create_source(
        source_name,
        source_description,
        project_name,
        dataset_name,
        table_names,
        table_description,
    )
