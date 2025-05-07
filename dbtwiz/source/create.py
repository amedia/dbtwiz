import os
from io import StringIO
from pathlib import Path
from typing import List

from dbtwiz.config.user import user_config
from dbtwiz.dbt.project import get_source_tables
from dbtwiz.gcp.bigquery import BigQueryClient
from dbtwiz.helpers.editor import open_in_editor
from dbtwiz.helpers.logger import fatal, info, warn
from dbtwiz.ui.interact import (
    autocomplete_from_list,
    confirm,
    dataset_name_validator,
    description_validator,
    input_text,
    multiselect_from_list,
    select_from_list,
    table_name_validator,
)


def get_existing_source(
    context, source_name=None, project_name=None, dataset_name=None
):
    """Function for getting an existing source, should it exist."""
    return next(
        (
            source
            for source in context.get("sources")
            if (
                (
                    source.get("project") == project_name
                    and source.get("dataset") == dataset_name
                )
                or source.get("name") == source_name
            )
        ),
        None,
    )


def select_project(context):
    """Function for selecting project."""
    if context.get("source"):
        return
    while True:
        project = context.get("project_name")
        if not project:
            project = autocomplete_from_list(
                "What is the project for the source",
                items=context["projects"],
                must_exist=False,
            )
        exists_check = context["client"].check_project_exists(project)
        if exists_check == "Exists":
            context["project_name"] = project
            break
        else:
            warn(exists_check)
            if context.get("project_name"):
                del context["project_name"]
            action = select_from_list(
                "What would you like to do",
                items=[
                    {"name": "Try a different project", "value": "retry"},
                    {"name": "Add values manually", "value": "manual"},
                    {"name": "Cancel", "value": "cancel"},
                ],
            )
            if action == "manual":
                context["manual_mode"] = True
                context["project_name"] = project
                break
            elif action == "retry":
                continue
            else:
                fatal("Cancelling")


def select_dataset(context):
    """Function for selecting dataset."""
    # If an existing source is used, dataset selection is skipped
    if context.get("source"):
        return
    if context["manual_mode"]:
        if not context.get("dataset_name"):
            context["dataset_name"] = input_text(
                "What is the dataset for the source",
                allow_blank=False,
                validate=dataset_name_validator(),
            )
    else:
        project_name = context["project_name"]
        valid_datasets, error_message = context["client"].list_datasets_in_project(
            project_name
        )
        if error_message:
            fatal(error_message)
        elif not valid_datasets:
            fatal(f"No datasets found in the project '{project_name}'.")

        has_invalid_selection = context.get("dataset_name") and not any(
            item == context.get("dataset_name") for item in valid_datasets
        )

        if has_invalid_selection:
            warn(
                f"The provided value ({context.get('dataset_name')}) for dataset name is invalid. Please re-select."
            )
        if not context.get("dataset_name") or has_invalid_selection:
            context["dataset_name"] = autocomplete_from_list(
                "What is the dataset for the source", items=valid_datasets
            )

        # Check if the project+dataset combination already exists as a source
        existing_source = get_existing_source(
            context, project_name=project_name, dataset_name=context["dataset_name"]
        )
        if existing_source:
            info(
                f"Using existing source '{existing_source['name']}' for project '{project_name}' and dataset '{context['dataset_name']}'."
            )
            context["source"] = existing_source


def set_source_name(context):
    """Function for setting source name."""
    if not context.get("source"):
        context["source_name"] = (
            (f"{context['project_name']}__{context['dataset_name']}")
            .replace("-", "_")
            .replace("`", "")
            .lower()
        )
        info(
            f"Adding alias {context['source_name']} for project '{context['project_name']}' and dataset '{context['dataset_name']}'"
        )


def select_source_description(context):
    """Function for selecting source description."""
    if not context.get("source"):
        context["source_description"] = input_text(
            f"Describe the type of data that reside in '{context['source_name']}'",
            allow_blank=False,
            validate=description_validator(),
        )


def configure_missing_source(context):
    """Function for defining a source dict, should it not exist."""
    if not context.get("source"):
        context["source"] = {
            "name": context["source_name"],
            "description": context["source_description"],
            "project": context["project_name"],
            "dataset": context["dataset_name"],
            "tables": [],
            "file": Path("sources")
            / context["project_name"].replace("-", "_")
            / f"{context['source_name']}.yml",
        }


def select_tables(context):
    """Function for selecting table(s)."""
    if not context["manual_mode"]:
        # Fetch all tables in the given project and dataset
        tables_in_dataset, error_message = context["client"].fetch_tables_in_dataset(
            context["source"]["project"], context["source"]["dataset"]
        )
        if tables_in_dataset:
            # Filter out tables that already exist as sources
            available_tables = [
                table
                for table in tables_in_dataset
                if table not in context["source"]["tables"]
            ]
            if not available_tables:
                fatal(
                    "All tables in the specified project and dataset already have sources."
                )
            else:
                invalid_table_names = [
                    item
                    for item in context.get("table_names") or []
                    if item not in available_tables
                ]
                has_invalid_selection = (
                    context.get("table_names") and len(invalid_table_names) > 0
                )

                if has_invalid_selection:
                    warn(
                        f"The provided table name(s) ({','.join(invalid_table_names)}) either doesn't exist or a source is already created. Please re-select."
                    )
                if not context.get("table_names") or has_invalid_selection:
                    context["tables"] = multiselect_from_list(
                        "Select table(s)",
                        items=available_tables,
                        allow_none=False,
                    )
                else:
                    context["tables"] = context.get("table_names")
        elif error_message:
            warn(error_message)
            action = select_from_list(
                "What would you like to do",
                items=[
                    {"name": "Add values manually", "value": "manual"},
                    {"name": "Cancel", "value": "cancel"},
                ],
            )
            if action == "manual":
                context["manual_mode"] = True
            else:
                fatal("Cancelling")

    if context["manual_mode"]:
        context["tables"] = [
            input_text(
                "What is the name of the table",
                allow_blank=False,
                validate=lambda text: (
                    all(
                        [
                            table_name_validator(context.get("dataset_name"))(text)
                            is True,
                            text not in context["source"]["tables"],
                        ]
                    )
                    or "Invalid name format or source already exists for given table name"
                ),
            )
        ]
        context["columns"] = []


def select_table_description(context):
    """Function for selecting table description. Skipped if multiple tables selected."""
    if context.get("tables") and len(context.get("tables")) == 1:
        context["table_description"] = input_text(
            "Give a short description for the source table",
            validate=description_validator(),
        )


def write_source_file(
    client: BigQueryClient,
    source_file: str,
    source_name: str,
    source_description: str,
    project_name: str,
    dataset_name: str,
    tables: List,
    table_description: str,
) -> None:
    """Create or update the source YAML file with the new table."""
    # Import (for performance) and configure yaml format
    from ruamel.yaml import YAML
    from ruamel.yaml.comments import CommentedMap
    from ruamel.yaml.scalarstring import PreservedScalarString

    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

    if source_file.exists():
        with open(source_file, "r", encoding="utf-8") as file:
            data = ruamel_yaml.load(file) or {"version": 2, "sources": []}
    else:
        data = CommentedMap()
        data["version"] = 2
        data["sources"] = []
        # Add a blank line between 'version' and 'sources'
        data.yaml_set_comment_before_after_key("sources", before="\n")

    # Find or create the source entry
    source_entry = next((s for s in data["sources"] if s["name"] == source_name), None)
    is_new_source = not source_entry
    if is_new_source:
        source_entry = {
            "name": source_name,
            "database": project_name,
            "schema": dataset_name,
            "description": source_description,
            "tables": [],
        }
        data["sources"].append(source_entry)

    new_table_entries = []
    for table_name in tables:
        columns, _ = client.fetch_table_columns(project_name, dataset_name, table_name)

        # Add the new table
        table_entry = {
            "name": table_name,
            "description": PreservedScalarString(table_description)
            if table_description
            else "",  # Use `|` for multiline descriptions
        }
        if columns:  # Only add columns if they exist (not in manual mode)
            table_entry["columns"] = columns
        new_table_entries.append(table_entry)
        if "tables" in source_entry:
            source_entry["tables"].append(table_entry)
        else:
            source_entry["tables"] = [table_entry]

    info(f"[=== BEGIN {source_file} ===]")
    stream = StringIO()
    if is_new_source:
        ruamel_yaml.dump(source_entry, stream)
    else:
        ruamel_yaml.dump(new_table_entries, stream)
    info(stream.getvalue().rstrip())
    info("[=== END ===]")
    if not confirm("Do you wish to add the source table"):
        fatal("Source table addition cancelled.")

    source_file.parent.mkdir(parents=True, exist_ok=True)
    # Write the updated YAML file
    with open(source_file, "w", encoding="utf-8") as file:
        ruamel_yaml.dump(data, file)

    open_in_editor(source_file)


def create_source(
    source_name: str,
    source_description: str,
    project_name: str,
    dataset_name: str,
    table_names: List[str],
    table_description: str,
) -> None:
    """Function for creating a new source."""
    _, existing_sources = get_source_tables()
    client = BigQueryClient()
    context = {
        "client": client,
        "manual_mode": False,
        "source_name": source_name,
        "source_description": source_description,
        "project_name": project_name,
        "dataset_name": dataset_name,
        "table_names": table_names,
        "table_description": table_description,
        "sources": existing_sources,
        "projects": sorted(list(set(source["project"] for source in existing_sources))),
    }

    for func in [
        select_project,
        select_dataset,
        set_source_name,
        select_source_description,
        configure_missing_source,
        select_tables,
        select_table_description,
    ]:
        func(context)

    write_source_file(
        client=client,
        source_file=context["source"]["file"],
        source_name=context["source"]["name"],
        source_description=context["source"]["description"],
        project_name=context["source"]["project"],
        dataset_name=context["source"]["dataset"],
        tables=context.get("tables"),
        table_description=context.get("table_description"),
    )
