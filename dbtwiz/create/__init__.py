from typing import Annotated, List

import typer

from .model import create_model
from .source import create_source

app = typer.Typer()


@app.command()
def model(
    quick: Annotated[
        bool,
        typer.Option(
            "--quick", "-q", help="Skip non-essential questions to quickly get started"
        ),
    ] = False,
    layer: Annotated[
        str, typer.Option("--layer", "-l", help="The layer for the model")
    ] = None,
    source: Annotated[
        str,
        typer.Option(
            "--source",
            "-s",
            help="Which source a staging model should be built on top of",
        ),
    ] = None,
    domain: Annotated[
        str,
        typer.Option("--domain", "-d", help="The domain the model should belong to"),
    ] = None,
    name: Annotated[
        str, typer.Option("--name", "-n", help="The name of the model")
    ] = None,
    description: Annotated[
        str,
        typer.Option("--description", "-ds", help="A short description for the model"),
    ] = None,
    group: Annotated[
        str,
        typer.Option("--group", "-g", help="Which group the model should belong to"),
    ] = None,
    access: Annotated[
        str,
        typer.Option(
            "--access", "-a", help="What the access level should be for the model"
        ),
    ] = None,
    materialization: Annotated[
        str,
        typer.Option(
            "--materialization", "-m", help="How the model should be materialized"
        ),
    ] = None,
    expiration: Annotated[
        str,
        typer.Option(
            "--expiration",
            "-e",
            help="The data expiration policy for an incremental model",
        ),
    ] = None,
    team: Annotated[
        str,
        typer.Option(
            "--team", "-t", help="The team with main responsibility for the model"
        ),
    ] = None,
    frequency: Annotated[
        str,
        typer.Option(
            "--frequency",
            "-f",
            help="How often the model should be updated, as defined by name of frequency tag",
        ),
    ] = None,
    service_consumers: Annotated[
        List[str],
        typer.Option(
            "--service-consumers",
            "-sc",
            help="Which service consumers that need access to the model",
        ),
    ] = None,
    access_policy: Annotated[
        str,
        typer.Option(
            "--access-policy",
            "-ap",
            help="What the access policy should be for the model",
        ),
    ] = None,
):
    """Create new dbt model"""
    create_model(
        quick,
        layer,
        source,
        domain,
        name,
        description,
        group,
        access,
        materialization,
        expiration,
        team,
        frequency,
        service_consumers,
        access_policy,
    )


@app.command()
def source(
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
