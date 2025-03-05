import os
import re
from io import StringIO
from pathlib import Path

from dbtwiz.interact import (
    autocomplete_from_list,
    confirm,
    input_text,
    multiselect_from_list,
    select_from_list,
)
from dbtwiz.logging import error, fatal, info, warn
from dbtwiz.model import (
    Group,
    Project,
    access_choices,
    domains_for_layer,
    frequency_choices,
    layer_choices,
    materialization_choices,
    model_base_path,
)
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from ruamel.yaml.scalarstring import LiteralScalarString


def generate_model(quick: bool):
    """Generate new dbt model"""

    project = Project()

    try:
        layer = select_from_list(
            "Select model layer",
            layer_choices())

        domain = autocomplete_from_list(
            "Which domain does your model belong to",
            domains_for_layer(layer),
            must_exist=False,
            allow_blank=False)

        while True:
            name = input_text(
                "What is the name of your model (lowercase, digits and underscores only)",
                pattern=r"^[a-z][a-z0-9_]*[a-z0-9]$")
            base_path = model_base_path(layer, domain, name)
            sql_path = base_path.with_suffix(".sql")
            yml_path = base_path.with_suffix(".yml")
            if sql_path.exists() or yml_path.exists():
                error("A model with that name already exists, please choose another.")
            else:
                break

        description = input_text(
            "Give a short description of your model and its purpose",
            pattern=r"^\S+")

        frequency = service_consumers = access_policy = None

        if not quick:
            group = autocomplete_from_list(
                "Which group does your model belong to",
                Group().choices(),
                must_exist=True,
                allow_blank=True)

            access = select_from_list(
                "What should the access level be for this model",
                access_choices())

            materialization = select_from_list(
                "How should your model be materialized",
                materialization_choices())

            expiration = None
            if materialization == "incremental":
                expiration = select_from_list(
                    "Select data expiration policy for your incremental model",
                    project.data_expirations())

            teams = multiselect_from_list(
                "Select one or more teams to be responsible for this model",
                project.teams())

            if layer != "staging":
                choices = frequency_choices()
                if len(set(teams) & set(["team-ai", "team-ai-analyst", "team-abo"])) > 0:
                    choices.append({"name": "daily_news_cycle", "description": "Model needs to be updated once a day at 03:30"})
                frequency = select_from_list(
                    "How often should your model be updated",
                    choices)

            if layer in ("marts", "bespoke"):
                service_consumers = multiselect_from_list(
                    "Which service consumers need access to your model",
                    project.service_consumers(),
                    allow_none=True)

                access_policy = select_from_list(
                    "What is the access policy for this model",
                    project.access_policies(),
                    allow_none=True)

        create_model_files(
            layer=layer,
            domain=domain,
            name=name,
            description=description,
            materialization=materialization,
            access=access,
            group=group,
            teams=teams,
            service_consumers=service_consumers,
            access_policy=access_policy,
            frequency=frequency,
            expiration=expiration,
        )

    except KeyboardInterrupt:
        warn("Cancelled by user.")


def create_model_files(
        layer: str,
        domain: str,
        name: str,
        description: str,
        materialization: str,
        access=None,
        group=None,
        teams=None,
        service_consumers=None,
        access_policy=None,
        frequency=None,
        expiration=None,
):
    """Create SQL and YAML files for model"""
    base_path = model_base_path(layer, domain, name)
    base_path.parent.mkdir(parents=True, exist_ok=True)

    sql_path = base_path.with_suffix(".sql")
    yml_path = base_path.with_suffix(".yml")
    if sql_path.exists() or yml_path.exists():
        return fatal(f"Model files {sql_path}.(sql,yml) already exist, leaving them be.")


    # Configure yaml format
    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

    # Define the config as a CommentedMap to maintain order
    config = CommentedMap()


    config["materialized"] = materialization

    if materialization == "incremental":
        config["incremental_strategy"] = "insert_overwrite"
        config["partition_by"] = {"field": "partitiondate", "data_type": "date"}
        if expiration:
            config["partition_expiration_days"] = f"{{{{ var('{expiration}') }}}}"
        config["require_partition_filter"] = True
        config["on_schema_change"] = "append_new_columns"

    if frequency:
        config["tags"] = CommentedSeq([frequency])
    if access:
        config["access"] = access
    if group:
        config["group"] = group
    if teams or service_consumers or access_policy:
        config['meta'] = CommentedMap()
        if teams:
            config["meta"]["teams"] = CommentedSeq(teams)
        if access_policy:
            config["meta"]["access-policy"] = access_policy
        if service_consumers:
            config["meta"]["service-consumers"] = CommentedSeq(service_consumers)

    yml_content = CommentedMap()
    yml_content['version'] = 2
    # Add a blank line between 'version' and 'models'
    yml_content.yaml_set_comment_before_after_key('models', before='\n')
    yml_content['models'] = [
        {
            'name': base_path.stem,
            'description': LiteralScalarString(description),
            'config': config
        }
    ]

    info(f"[=== BEGIN {yml_path.relative_to(Path.cwd())} ===]")
    stream = StringIO()
    ruamel_yaml.dump(yml_content, stream)
    info(stream.getvalue().rstrip())
    info(f"[=== END ===]")
    if not confirm("Do you wish to generate the model files"):
        warn("Model generation cancelled.")
        return

    info(f"Generating config file {yml_path}")
    with open(yml_path, "w+") as f:
        ruamel_yaml.dump(yml_content, f)

    info(f"Generating query file {sql_path}")
    with open(sql_path, "w+") as f:
        f.write("{# SQL placeholder #}")
    # Open SQL file in editor
    # FIXME: Make editor user configurable with 'code' as default
    os.system(f"code {sql_path}")
