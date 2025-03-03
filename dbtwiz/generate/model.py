import os
import yaml

from dbtwiz.interact import (
    input_text,
    select_from_list,
    multiselect_from_list,
    autocomplete_from_list,
)

from dbtwiz.logging import info, warn, error, fatal

from dbtwiz.model import (
    Group,
    Project,
    layer_choices,
    materialization_choices,
    access_choices,
    domains_for_layer,
    model_base_path,
)


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
                "What is the name of your model")
            base_path = model_base_path(layer, domain, name)
            sql_path = base_path.with_suffix(".sql")
            yml_path = base_path.with_suffix(".yml")
            if sql_path.exists() or yml_path.exists():
                error("A model with that name already exists, please choose another.")
            else:
                break

        description = input_text(
            "Give a short description of your model and its purpose")

        if not quick:
            group = autocomplete_from_list(
                "Which group does your model belong to",
                Group().choices(),
                must_exist=True,
                allow_blank=True)

            access = select_from_list(
                "What is the access level for this model",
                access_choices())

            materialization = select_from_list(
                "How should your model be materialized",
                materialization_choices())

            teams = multiselect_from_list(
                "Which team(s) should be responsible for this model",
                project.teams())

            service_consumers = access_policy = None

            if layer in ("marts", "bespoke"):
                service_consumers = multiselect_from_list(
                    "Which service consumers need access to your model",
                    project.service_consumers())

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
            access=None,
            group=group,
            teams=teams,
            service_consumers=service_consumers,
            access_policy=access_policy,
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
):
    """Create SQL and YAML files for model"""
    base_path = model_base_path(layer, domain, name)
    base_path.parent.mkdir(parents=True, exist_ok=True)

    sql_path = base_path.with_suffix(".sql")
    yml_path = base_path.with_suffix(".yml")
    if sql_path.exists() or yml_path.exists():
        return fatal(f"Model files {sql_path}.(sql,yml) already exist, leaving them be.")

    config = {}
    config["materialized"] = materialization
    if access:
        config["access"] = access
    if group:
        config["group"] = group
    if teams or service_consumers or access_policy:
        config["meta"] = {}
        if teams:
            config["meta"]["teams"] = teams
        if service_consumers:
            config["meta"]["service-consumers"] = service_consumers
        if access_policy:
            config["meta"]["access-policy"] = access_policy

    yml_content = {
        "version": 2,
        "models": [{
            "name": base_path.stem,
            "description": description,
            "config": config,
        }],
    }

    info(f"Generating config file {yml_path}")
    with open(yml_path, "w+") as f:
        yaml.dump(yml_content, f, sort_keys=False)

    info(f"Generating query file {sql_path}")
    with open(sql_path, "w+") as f:
        f.write("{# SQL placeholder #}")
    # Open SQL file in editor
    # FIXME: Make editor user configurable with 'code' as default
    os.system(f"code {sql_path}")
