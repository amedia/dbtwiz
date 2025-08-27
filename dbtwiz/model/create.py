from io import StringIO
from pathlib import Path

from dbtwiz.core.model import ModelBasePath
from dbtwiz.core.project import (
    Group,
    Project,
    access_choices,
    domains_for_layer,
    frequency_choices,
    get_source_tables,
    layer_choices,
    list_domain_models,
    materialization_choices,
)
from dbtwiz.utils.editor import open_in_editor
from dbtwiz.utils.logger import fatal, info, notice, warn
from dbtwiz.ui.interact import (
    autocomplete_from_list,
    confirm,
    description_validator,
    input_text,
    multiselect_from_list,
    name_validator,
    select_from_list,
)


def select_layer(context):
    """Function for selecting layer."""
    valid_layers = layer_choices()
    has_invalid_selection = context.get("layer") and not any(
        item["name"] == context.get("layer") for item in valid_layers
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('layer')}) for layer is invalid. Please re-select."
        )
    if not context.get("layer") or has_invalid_selection:
        context["layer"] = select_from_list("Select model layer", valid_layers)


def select_source(context):
    """Function for selecting source."""
    if context["layer"] == "staging":
        valid_sources = get_source_tables()[0]
        has_invalid_selection = context.get("source") and not any(
            item == context.get("source") for item in valid_sources
        )
        if has_invalid_selection:
            warn(
                f"The provided value ({context.get('source')}) for source is invalid. Please re-select."
            )
        if not context.get("source") or has_invalid_selection:
            context["source"] = autocomplete_from_list(
                "Which source should the staging model be built on top of",
                valid_sources,
                must_exist=True,
                allow_blank=False,
            )
    elif context["layer"] != "staging" and context.get("source"):
        info("Ignoring source since the model isn't in the staging layer.")
        del context["source"]
        return


def select_domain(context):
    """Function for selecting domain."""
    if domains_for_layer(context["layer"]):
        valid_domains = domains_for_layer(context["layer"])
        has_invalid_selection = context.get("domain") and not any(
            item == context.get("domain") for item in valid_domains
        )
        if has_invalid_selection:
            warn(
                f"The provided value ({context.get('domain')}) for domain is invalid. Please re-select."
            )
        if not context.get("domain") or has_invalid_selection:
            context["domain"] = autocomplete_from_list(
                "Which domain does your model belong to",
                valid_domains,
                must_exist=False,
                allow_blank=False,
            )
    else:
        context["domain"] = input_text(
            "Which domain does your model belong to",
            allow_blank=False,
            validate=name_validator(),
        )


def select_name(context):
    """Function for selecting model name."""
    if context.get("name"):
        name = context.get("name")
    else:
        model_base_path = ModelBasePath(layer=context.get("layer"))
        existing_models = list_domain_models(model_base_path, context.get("domain"))
        name = input_text(
            f"Name your new model (it will be prefixed by {model_base_path.get_prefix(context.get('domain'))})",
            validate=lambda text: (
                all(
                    [
                        name_validator()(text) is True,
                        model_base_path.get_prefix(context.get("domain")) + text
                        not in existing_models,
                        not text.startswith(
                            model_base_path.get_prefix(context["domain"])
                        ),
                    ]
                )
                or "Invalid name format, a model with given name already exists or you've added the model prefix to the name"
            ),
        )

        context["name"] = name
        model_base_path = model_base_path.get_path(name=name, domain=context["domain"])
        context["sql_path"] = model_base_path.with_suffix(".sql")
        context["yml_path"] = model_base_path.with_suffix(".sql")


def select_description(context):
    """Function for selecting model description."""
    if not context.get("description"):
        context["description"] = input_text(
            "Give a short description of your model and its purpose",
            validate=description_validator(),
        )


def select_group(context):
    """Function for selecting group."""
    if context["quick"] and not context.get("group"):
        return
    valid_groups = Group().choices()
    has_invalid_selection = context.get("group") and not any(
        item == context.get("group") for item in valid_groups.keys()
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('group')}) for group is invalid. Please re-select."
        )
    if has_invalid_selection or not context.get("group"):
        context["group"] = autocomplete_from_list(
            "Which group should the model belong to",
            valid_groups,
            must_exist=True,
            allow_blank=True,
            validate=(
                lambda str: (str is not None and str != "") or "You must select a group"
            ),
        )


def select_access(context):
    """Function for selecting access."""
    if context["quick"] and not context.get("access"):
        return
    valid_accesses = access_choices()
    has_invalid_selection = context.get("access") and not any(
        item["name"] == context.get("access") for item in valid_accesses
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('access')}) for access is invalid. Please re-select."
        )
    if has_invalid_selection or not context.get("access"):
        context["access"] = select_from_list(
            "What should the access level be for the model", access_choices()
        )


def select_materialization(context):
    """Function for selecting materialization."""
    if context["quick"] and not context.get("materialization"):
        return
    valid_materializations = materialization_choices()
    has_invalid_selection = context.get("materialization") and not any(
        item["name"] == context.get("materialization")
        for item in valid_materializations
    )
    if (
        context.get("materialization")
        and context["layer"] == "staging"
        and context.get("materialization") != "view"
    ):
        info(
            "Changing materialization to view, which is required for all staging models."
        )
        context["materialization"] = "view"
        return
    elif not context.get("materialization") and context["layer"] == "staging":
        info(
            "Setting materialization to view, which is required for all staging models."
        )
        context["materialization"] = "view"
        return
    elif has_invalid_selection:
        warn(
            f"The provided value ({context.get('materialization')}) for materialization is invalid. Please re-select."
        )
    if has_invalid_selection or (
        not context.get("materialization") and context["layer"] != "staging"
    ):
        context["materialization"] = select_from_list(
            "How should the model be materialized", valid_materializations
        )


def select_expiration(context):
    """Function for selecting expiration."""
    if context["quick"] and not context.get("expiration"):
        return
    if context.get("expiration") and context["materialization"] != "incremental":
        info("Ignoring expiration since the model isn't materialized as incremental.")
        del context["expiration"]
        return
    valid_expirations = context["project"].data_expirations()
    has_invalid_selection = context.get("expiration") and not any(
        item["name"] == context.get("expiration") for item in valid_expirations
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('expiration')}) for expiration is invalid. Please re-select."
        )
    if has_invalid_selection or (
        not context.get("expiration") and context["materialization"] == "incremental"
    ):
        context["expiration"] = select_from_list(
            "Select data expiration policy for the incremental model",
            valid_expirations,
            allow_none=True,
        )


def select_team(context):
    """Function for selecting teams."""
    if context["quick"] and not context.get("team"):
        return
    valid_teams = context["project"].teams()
    has_invalid_selection = context.get("team") and not any(
        item["name"] == context.get("team") for item in valid_teams
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('team')}) for team is invalid. Please re-select."
        )
    if has_invalid_selection or not context.get("team"):
        context["team"] = [
            select_from_list(
                "Select the team with main responsibility for the model",
                context["project"].teams(),
            )
        ]
    else:
        context["team"] = [context["team"]]


def select_frequency(context):
    """Function for selecting frequency."""
    if context["quick"] and not context.get("frequency"):
        return
    elif context.get("frequency") and context["layer"] == "staging":
        info(
            "Ignoring defined frequency since frequency is not allowed for staging models."
        )
        del context["frequency"]
        return
    elif context.get("frequency") and context.get("materialization") == "view":
        info("Ignoring defined frequency since frequency is not applicable for views.")
        del context["frequency"]
        return
    elif context.get("materialization") == "view" or context.get("layer") == "staging":
        return

    valid_frequencies = frequency_choices()
    if len(set(context["team"]) & set(["team-ai", "team-ai-analyst", "team-abo"])) > 0:
        valid_frequencies.append(
            {
                "name": "daily_news_cycle",
                "description": "Model needs to be updated once a day at 03:30",
            }
        )
    has_invalid_selection = context.get("frequency") and not any(
        item["name"] == context.get("frequency") for item in valid_frequencies
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('frequency')}) for frequency is invalid. Please re-select."
        )
    if has_invalid_selection or not context.get("frequency"):
        context["frequency"] = select_from_list(
            "How often should the model be updated",
            valid_frequencies,
            allow_none=True,
        )


def select_service_consumers(context):
    """Function for selecting service consumers."""
    if context["quick"] and not context.get("service_consumers"):
        return
    elif context["layer"] not in ("marts", "bespoke"):
        if context.get("service_consumers"):
            info(
                "Ignoring defined service_consumers since service_consumers are only allowed for marts or bespoke models."
            )
            del context["service_consumers"]
        return
    valid_service_consumers = context["project"].service_consumers()
    has_invalid_selection = False
    if context.get("service_consumers"):
        invalid_service_consumers = [
            item
            for item in context.get("service_consumers")
            if item not in {item["name"] for item in valid_service_consumers}
        ]
        if invalid_service_consumers:
            has_invalid_selection = True
            warn(
                f"The provided value(s) ({','.join(invalid_service_consumers)}) for service_consumers is invalid. Please re-select."
            )
    if has_invalid_selection or not context.get("service_consumers"):
        context["service_consumers"] = multiselect_from_list(
            "Which service consumers need access to the model",
            context["project"].service_consumers(),
            allow_none=True,
        )


def select_access_policy(context):
    """Function for selecting acces policy."""
    if context["quick"] and not context.get("access_policy"):
        return
    elif context["layer"] not in ("marts", "bespoke"):
        if context.get("access_policy"):
            info(
                "Ignoring defined access_policy since access_policy is only allowed for marts or bespoke models."
            )
            del context["access_policy"]
        return
    valid_access_policies = context["project"].access_policies()
    has_invalid_selection = context.get("access_policy") and not any(
        item["name"] == context.get("access_policy") for item in valid_access_policies
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('access_policy')}) for access_policy is invalid. Please re-select."
        )
    if has_invalid_selection or not context.get("access_policy"):
        context["access_policy"] = select_from_list(
            "What is the access policy for the model",
            context["project"].access_policies(),
            allow_none=True,
        )


def get_stg_sql(source):
    """Returns the SQL definition for a staging model"""
    source_name, table_name = source.split(".")
    return f"""with
    source as (select * from {{{{ source("{source_name}", "{table_name}") }}}}),

    renamed as (
        select
            *
        from source
    )

select *
from renamed
"""


def get_config(
    materialization: str,
    expiration=None,
    frequency=None,
    access=None,
    group=None,
    teams=None,
    service_consumers=None,
    access_policy=None,
):
    """Creates the configuration dictionary for a model based on the input values."""
    from ruamel.yaml.comments import CommentedMap, CommentedSeq

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
        config["meta"] = CommentedMap()
        if teams:
            config["meta"]["teams"] = CommentedSeq(teams)
        if access_policy:
            config["meta"]["access-policy"] = access_policy
        if service_consumers:
            config["meta"]["service-consumers"] = CommentedSeq(service_consumers)

    return config


def create_yml_content(model_name, description: str, config: dict):
    """
    Create YAML content as a CommentedMap with a specified version,
    models, and formatting.
    """
    from ruamel.yaml.comments import CommentedMap
    from ruamel.yaml.scalarstring import LiteralScalarString

    yml_content = CommentedMap()
    yml_content["version"] = 2
    # Add a blank line before the 'models' key
    yml_content.yaml_set_comment_before_after_key("models", before="\n")
    yml_content["models"] = [
        {
            "name": model_name,
            "description": LiteralScalarString(description),
            "config": config,
        }
    ]

    return yml_content


def create_model_files(
    layer: str,
    source: str,
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
    """Function that creates SQL and YAML files for model."""
    # Import (for performance) and configure yaml format
    from ruamel.yaml import YAML

    base_path = ModelBasePath(layer=layer).get_path(name=name, domain=domain)

    sql_path = base_path.with_suffix(".sql")
    yml_path = base_path.with_suffix(".yml")

    if yml_path.exists():
        return fatal(
            f"Model yml file {yml_path} already exist, cancelling file creation."
        )

    # Define the config
    config = get_config(
        materialization=materialization,
        expiration=expiration,
        frequency=frequency,
        access=access,
        group=group,
        teams=teams,
        service_consumers=service_consumers,
        access_policy=access_policy,
    )

    # Define the yml content
    yml_content = create_yml_content(base_path.stem, description, config)

    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

    info(f"[=== BEGIN {yml_path.relative_to(Path.cwd())} ===]")
    stream = StringIO()
    ruamel_yaml.dump(yml_content, stream)
    info(stream.getvalue().rstrip())
    info("[=== END ===]")
    if not confirm("Do you wish to create the model files"):
        fatal("Model creation cancelled.")

    # Create folder structure for files
    base_path.parent.mkdir(parents=True, exist_ok=True)

    info("Creating model yml file")
    with open(yml_path, "w+") as f:
        ruamel_yaml.dump(yml_content, f)

    if not sql_path.exists():
        info("Creating model sql file")
        sql = get_stg_sql(source) if layer == "staging" else "{# SQL placeholder #}"
        with open(sql_path, "w+") as f:
            f.write(sql)
        open_in_editor(sql_path)
    open_in_editor(yml_path)

def print_create_model_cli_command(
    context
):
    """Generate the command that would reproduce this run"""
    command_parts = ["dbtwiz model create"]

    for parameter in [
        "quick",
        "layer",
        "source",
        "domain",
        "name",
        "description",
        "group",
        "access",
        "materialization",
        "expiration",
        "team",
        "frequency",
        "service_consumers",
        "access_policy"
    ]:
        if context[parameter]:
            if isinstance(context[parameter], list):
                for sub_parameter in context[parameter]:
                    command_parts.append(f"--{parameter.replace('_', '-')} '{sub_parameter}'")
            else:
                command_parts.append(f"--{parameter.replace('_', '-')} '{context[parameter]}'")

    notice("To repeat/resume the model creation, use this command:\n\n" + " ".join(command_parts))


def create_model(
    quick: bool,
    layer: str,
    source: str,
    domain: str,
    name: str,
    description: str,
    group: str,
    access: str,
    materialization: str,
    expiration: str,
    team: str,
    frequency: str,
    service_consumers,
    access_policy: str,
):
    # TODO: Handle cases when currently open file is in a subfolder. Will probably need path as a parameter.
    """Function that generates a new dbt model"""
    context = {
        "quick": quick,
        "project": Project(),
        "layer": layer,
        "source": source,
        "domain": domain,
        "name": name,
        "description": description,
        "group": group,
        "access": access,
        "materialization": materialization,
        "expiration": expiration,
        "team": team,
        "frequency": frequency,
        "service_consumers": service_consumers,
        "access_policy": access_policy,
    }

    try:
        for func in [
            select_layer,
            select_source,
            select_domain,
            select_name,
            select_description,
            select_materialization,
            select_expiration,
            select_group,
            select_access,
            select_team,
            select_frequency,
            select_service_consumers,
            select_access_policy,
        ]:
            func(context)

        create_model_files(
            layer=context.get("layer"),
            source=context.get("source"),
            domain=context.get("domain"),
            name=context.get("name"),
            description=context.get("description"),
            materialization=context.get("materialization"),
            access=context.get("access"),
            group=context.get("group"),
            teams=context.get("team"),
            service_consumers=context.get("service_consumers"),
            access_policy=context.get("access_policy"),
            frequency=context.get("frequency"),
            expiration=context.get("expiration"),
        )

    finally:
        print_create_model_cli_command(context=context)
