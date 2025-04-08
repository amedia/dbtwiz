from enum import Enum
from typing import Annotated, List

import typer

from dbtwiz.helpers.decorators import description, examples

from .create import create_model
from .format import format_sql_files
from .from_sql import convert_sql_to_model
from .inspect import inspect_model
from .move import move_model, update_model_references

app = typer.Typer()


class MoveAction(str, Enum):
    """Enumeration of move actions."""

    move_model = "move-model"
    update_model_references = "update-references"


@app.command()
@description(
    """The dbt model creation functions assume a dbt project folder structure that is models -> layer -> domain:
```
models:
  1_staging (stg as abbreviation)
    <folders for each domain>
      <models prefixed by abbreviated layer and domain, e.g. stg_<domain>__model_identifier>
  2_intermediate (int as abbreviation)
    <as above>
  3_marts (mrt as abbreviation)
    <as above>
  4_bespoke (bsp as abbreviation)
    <as above>
```"""
)
def create(
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
    """Create new dbt model."""
    create_model(
        quick=quick,
        layer=layer,
        source=source,
        domain=domain,
        name=name,
        description=description,
        group=group,
        access=access,
        materialization=materialization,
        expiration=expiration,
        team=team,
        frequency=frequency,
        service_consumers=service_consumers,
        access_policy=access_policy,
    )


@app.command()
@examples(
    """Fix any number of given models, e.g.:
```
dbtwiz model fix mrt_siteconfig__site_groups mrt_siteconfig__sites
```

Fix models you have changed and staged:
```
dbtwiz model fix -s
```

It's also possible to combine the two:
```
dbtwiz model fix mrt_siteconfig__site_groups mrt_siteconfig__sites -s
```"""
)
def fix(
    model_names: Annotated[
        List[str],
        typer.Argument(help="Models to fix."),
    ] = None,
    staged: Annotated[
        bool,
        typer.Option(
            "--staged", "-s", is_flag=True, help="Whether to fix staged sql files."
        ),
    ] = False,
):
    """Run sqlfmt and sqlfix for staged and/or defined sql files."""
    format_sql_files(
        staged=staged, model_names=model_names, sqlfmt_args=[], sqlfluff_command="fix"
    )


@app.command()
def from_sql(
    file_path: Annotated[
        str,
        typer.Option("--file-path", "-f", help="File path for sql file"),
    ],
):
    """Convert a sql file to a dbt model by replacing table references with source and ref."""
    convert_sql_to_model(file_path=file_path)


@app.command()
@examples(
    """Run the following command to inspect a given model:
```
dbtwiz model inspect mrt_siteconfig__site_groups
```

The command will then output the ancestors and descendants for the model, e.g.
```
Ancestors:
- stg_siteconfig__sites
- int_sitegroups_all_local
- int_sitegroups_all_sites
- int_sitegroups_amedia_local
- int_sitegroups_amedia_owned
- int_sitegroups_classes
- int_sitegroups_mainsites
- int_sitegroups_multisites
- int_sitegroups_regions
- int_sitegroups_sections

Descendants:
- mrt_siteconfig__site_groups_map
- mrt_siteconfig__site_groups_map_with_sections
- mrt_subscriptions__purchases_by_sitegroup
- mrt_subscriptions__upgrades_by_sitegroup
- mrt_subscriptions_bt_tables__abo_sites
```"""
)
def inspect(
    name: Annotated[
        str,
        typer.Argument(help="Model name or path"),
    ],
):
    """Output information about a given model."""
    inspect_model(name=name)


@app.command()
@examples(
    """Lint any number of given models, e.g.:
```
dbtwiz model lint mrt_siteconfig__site_groups mrt_siteconfig__sites
```

Lint models you have changed and staged:
```
dbtwiz model lint -s
```

It's also possible to combine the two:
```
dbtwiz model lint mrt_siteconfig__site_groups mrt_siteconfig__sites -s
```"""
)
def lint(
    model_names: Annotated[
        List[str],
        typer.Argument(help="Models to lint."),
    ] = None,
    staged: Annotated[
        bool,
        typer.Option(
            "--staged", "-s", is_flag=True, help="Whether to lint staged sql files."
        ),
    ] = False,
):
    """Run sqlfmt --diff and sqlfluff lint for staged and/or defined sql files."""
    format_sql_files(
        staged=staged,
        model_names=model_names,
        sqlfmt_args=["--diff"],
        sqlfluff_command="lint",
    )


@app.command()
def move(
    old_model_name: Annotated[
        str,
        typer.Option(
            "--old-mode-name",
            "-omn",
            help="Current name for dbt model (excluding file type)",
        ),
    ],
    new_model_name: Annotated[
        str,
        typer.Option(
            "--new-model-name",
            "-nmn",
            help="New name for dbt model (excluding file type)",
        ),
    ],
    old_folder_path: Annotated[
        str,
        typer.Option(
            "--old-folder-path",
            "-ofp",
            help="Current path for dbt model. Required if `move` is True.",
        ),
    ] = None,
    new_folder_path: Annotated[
        str,
        typer.Option(
            "--new-folder-path",
            "-nfp",
            help="New path for dbt model. Required if `move` is True.",
        ),
    ] = None,
    actions: Annotated[
        List[MoveAction],
        typer.Option("--action", "-a", help="Which move actions to execute"),
    ] = [MoveAction.move_model],
    safe: Annotated[
        bool,
        typer.Option(
            "--safe",
            "-s",
            help="if moving model, whether to keep old model as a view to the new or do a hard move.",
        ),
    ] = True,
):
    """
    Moves a model by copying to a new location with a new name,
    and/or by updating the references to the model by other dbt models.
    """
    if MoveAction.move_model in actions:
        move_model(
            old_folder_path=old_folder_path,
            old_model_name=old_model_name,
            new_folder_path=new_folder_path,
            new_model_name=new_model_name,
            safe=safe,
        )
    if MoveAction.update_model_references in actions:
        update_model_references(
            old_model_name=old_model_name,
            new_model_name=new_model_name,
        )
