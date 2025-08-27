from typing import Annotated, List

import typer

from dbtwiz.utils.decorators import description

from .create import create_source

app = typer.Typer()


@app.command()
@description(
    """When creating a new source, the function will ask a number of questions about the new source.

1. Project name: You can manually input the project, but it will autocomplete for the existing source projects.
2. Dataset name: Select one of the listed datasets that exist in the given project. If the dataset is new, it will ask for a description.
3. Table name(s): Select one or more of the listed tables that exist in the dataset and aren't defined as source yet. If you only selected one table, it will ask for a description.
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
