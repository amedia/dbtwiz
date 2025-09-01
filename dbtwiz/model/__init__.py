from enum import Enum
from typing import Annotated, List

import typer

from ..utils.decorators import description, examples
from ..utils.exceptions import ModelError
from ..utils.logger import warn
from .create import create_model
from .format import format_sql_files
from .inspect import inspect_model
from .move import move_model, update_model_references
from .validate import ModelValidator

app = typer.Typer(help="Commands for dbt model management and validation")

__all__ = ["app", "move_model", "update_model_references"]


class MoveAction(str, Enum):
    """Enumeration of move actions."""

    move_model = "move-model"
    update_model_references = "update-references"


@app.command()
@description(
    """Creates a new dbt model with proper folder structure and YAML configuration.

    Assumes dbt project structure: models/layer/domain/model_name.sql
    - Layers: staging (stg), intermediate (int), marts (mrt), bespoke (bsp)
    - Models are prefixed: stg_domain__name, int_domain__name, etc.

    Requires dbt_project.yml with teams, access-policies, and service-consumers variables.

    The command will guide you through:
    1. Selecting the appropriate layer and domain
    2. Naming the model with proper conventions
    3. Setting access policies and team ownership
    4. Configuring materialization and expiration settings
    5. Defining service consumer access requirements
    """
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
        typer.Option("--staged", "-s", help="Whether to fix staged sql files."),
    ] = False,
):
    """Run sqlfmt and sqlfix for staged and/or defined sql files."""
    format_sql_files(
        staged=staged, model_names=model_names, sqlfmt_args=[], sqlfluff_command="fix"
    )


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
        typer.Option("--staged", "-s", help="Whether to lint staged sql files."),
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
@description(
    """Moves or renames a dbt model with optional reference updates.

    By default, creates a copy at the new location and converts the original to a view
    pointing to the new model. Use --safe=false to delete the original instead.

    Use --action update-references to update all model references automatically.

    This command handles:
    1. Moving the model file to a new location
    2. Updating the model name in the file content
    3. Converting the old model to a view that references the new one
    4. Optionally updating all references in other models
    5. Maintaining data lineage and dependencies
    """
)
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


@app.command()
@examples(
    """Example output when all is ok:
```
Validating yml exists: yml file ok
Validating yml definition: yml file name ok
Validating yml columns: yml ok
Validating sql references: references ok
Validating sql with sqlfmt: validation ok
Validating sql with sqlfluff: validation ok
```
"""
)
def validate(
    model_path: Annotated[
        str,
        typer.Argument(help="Path to model (sql or yml) to be validated."),
    ],
):
    """Validates the yml and sql files for a model."""
    try:
        ModelValidator(model_path=model_path).validate()
    except ModelError:
        warn(
            "The provided path is not a dbt model under 'models/'. "
            "Please select a model file located within the dbt 'models/' directory."
        )
        return
