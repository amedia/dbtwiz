import copy
import os
from io import StringIO
from pathlib import Path
# from typing import Dict, Tuple

from dbtwiz.bigquery import (
    check_project_exists,
    fetch_table_columns,
    fetch_tables_in_dataset,
    list_datasets_in_project,
)
from dbtwiz.interact import (
    autocomplete_from_list,
    confirm,
    description_validator,
    input_text,
    name_validator,
    select_from_list,
)
from dbtwiz.logging import error, fatal, info, warn
from dbtwiz.model import get_source_tables
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import PreservedScalarString


def select_source(context):
    """Function for selecting source."""
    valid_sources = context["sources"]
    has_invalid_selection = context.get("source_name") and not any(
        item["name"] == context.get("source_name") for item in valid_sources
    )
    if has_invalid_selection:
        warn(
            f"The provided value ({context.get('source_name')}) for source_name is invalid. Please re-select."
        )
    source_name = autocomplete_from_list(
        "Where is the source table located", items={
            **{"Create a new source": ""},
            **{
                source['name']: f"{source['project']}.{source['dataset']}: {' '.join(source.get('description', '').split())[:80]}"
                for source in valid_sources
            }
        }
    )
    context["source"] = next((s for s in valid_sources if s["name"] == source_name), None)


def select_project(context):
    """Function for selecting project."""
    if context["source"]:
        return
    while True:
        project = autocomplete_from_list(
            "What is the project for the source",
            items=context["projects"],
            must_exist=False,
        )
        exists_check = check_project_exists(project)
        if exists_check == "Exists":
            context["project"] = project
            break
        else:
            error(exists_check)
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
                context["project"] = project
                break
            elif action == "retry":
                continue
            else:
                fatal("Cancelling")


def get_existing_source(context, source_name, project_name, dataset_name):
    """Function for getting the existing source, should it exist."""
    return next(
        (
            source
            for source in context["sources"]
            if (
                (
                    source["project"] == project_name
                    and source["dataset"] == dataset_name
                )
                or source["name"] == source_name
            )
        ),
        None,
    )


def select_dataset(context):
    """Function for selecting dataset."""
    if context["source"]:
        return
    if context["manual_mode"]:
        context["dataset"] = input_text(
            "What is the dataset for the source",
            allow_blank=False,
            validate=name_validator(),
        )
    else:
        project = context["project"]
        datasets, error_message = list_datasets_in_project(project)
        if error_message:
            fatal(error_message)
        elif not datasets:
            fatal(f"No datasets found in the project '{project}'.")

        dataset = autocomplete_from_list(
            "What is the dataset for the source", items=datasets
        )
        context["dataset"] = dataset

        # Check if the project+dataset combination already exists as a source
        existing_source = get_existing_source(context, None, project, dataset)
        if existing_source:
            info(
                f"Using existing source '{existing_source['name']}' for project '{project}' and dataset '{dataset}'."
            )
            context["source"] = existing_source


def select_table(context):
    """Function for selecting table."""
    while True:
        table_name = input_text(
            "What is the name of the table",
            allow_blank=False,
            validate=name_validator(),
        )

        # Check if the table already exists in the source
        if table_name in context["source"]["tables"]:
            warn(f"Error: The table '{table_name}' already exists in this source.")
            action = select_from_list(
                "What would you like to do",
                items=[
                    {
                        "name": "Provide a different table name",
                        "value": "retry",
                    },
                    {"name": "Cancel", "value": "cancel"},
                ],
            )

            if action == "retry":
                continue  # Retry table name input
        else:
            context["table_name"] = table_name
            break  # Valid table name, proceed


def select_source_name(context):
    """Function for selecting source name."""
    if not context["source"]:
        while True:
            source_name = input_text(
                "Give a short name for the new source (project+dataset combination)",
                allow_blank=False,
                validate=name_validator(),
            )
            existing_source = get_existing_source(
                context, source_name, context["project"], context["dataset"]
            )
            if not existing_source:
                context["source_name"] = source_name
                break  # Valid table name, proceed
            if existing_source:
                warn(f"Error: The source name '{source_name}' already exists.")
                action = select_from_list(
                    "What would you like to do",
                    items=[
                        {
                            "name": "Provide a different source name",
                            "value": "retry",
                        },
                        {"name": "Cancel", "value": "cancel"},
                    ],
                )
                if action == "retry":
                    continue  # Retry table name input'
                else:
                    fatal("Cancelling")


def select_source_description(context):
    """Function for selecting source description."""
    if not context["source"]:
        context["source_description"] = input_text(
            "Give a short description for the new source",
            allow_blank=False,
            validate=description_validator(),
        )


def configure_missing_source(context):
    """Function for defining a source dict, should it not exist."""
    if not context["source"]:
        context["source"] = {
            "name": context["source_name"],
            "description": context["source_description"],
            "project": context["project"],
            "dataset": context["dataset"],
            "tables": [],
            "file": Path("sources") / f"{context['source_name']}.yml",
        }


def select_table(context):
    """Function for selecting table."""
    if not context["manual_mode"]:
        # Fetch all tables in the given project and dataset
        tables_in_dataset, error_message = fetch_tables_in_dataset(
            context["source"]["project"], context["source"]["dataset"]
        )
        if not tables_in_dataset:
            warn(error_message)
            action = select_from_list(
                "What would you like to do",
                items=[
                    {"name": "Try a different source", "value": "retry"},
                    {"name": "Add values manually", "value": "manual"},
                    {"name": "Cancel", "value": "cancel"},
                ],
            )

            if action == "retry":
                return True
            elif action == "manual":
                context["manual_mode"] = True  # Enter manual mode

    if not context["manual_mode"]:
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

        # Ask for the table name
        context["table_name"] = autocomplete_from_list(
            "Select table",
            items=available_tables,
            # Ensure the table exists in the dataset and is not already a source
            validate=lambda x: x in available_tables,
        )

        # Fetch column details for the selected table
        context["columns"], error_message = fetch_table_columns(
            context["source"]["project"],
            context["source"]["dataset"],
            context["table_name"],
        )
        if not context["columns"]:
            fatal(error_message)

    if context["manual_mode"]:
        while True:
            table_name = input_text(
                "What is the name of the table",
                allow_blank=False,
                validate=name_validator(),
            )

            # Check if the table already exists in the source
            if table_name in context["source"]["tables"]:
                warn(f"Error: The table '{table_name}' already exists in this source.")
                action = select_from_list(
                    "What would you like to do",
                    items=[
                        {
                            "name": "Provide a different table name",
                            "value": "retry",
                        },
                        {"name": "Cancel", "value": "cancel"},
                    ],
                )

                if action == "retry":
                    continue  # Retry table name input
            else:
                context["table_name"] = table_name
                break  # Valid table name, proceed

        context["columns"] = []
    return False


def select_table_description(context):
    """Function for selecting table description."""
    context["table_description"] = input_text(
        "Give a short description for the source table",
        validate=description_validator(),
    )


def create_source_file(
    source_file,
    source_name,
    source_description,
    project_name,
    dataset_name,
    table_name,
    table_description,
    columns=None,
) -> None:
    """Create or update the source YAML file with the new table."""
    # Configure yaml format
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
            "description": source_description,
            "database": project_name,
            "schema": dataset_name,
            "tables": [],
        }
        data["sources"].append(source_entry)

    # Add the new table
    table_entry = {
        "name": table_name,
        "description": PreservedScalarString(
            table_description
        ),  # Use `|` for multiline descriptions
    }
    if columns:  # Only add columns if they exist (not in manual mode)
        table_entry["columns"] = columns
    source_entry["tables"].append(table_entry)

    info(f"[=== BEGIN {source_file} ===]")
    stream = StringIO()
    if is_new_source:
        ruamel_yaml.dump(source_entry, stream)
    else:
        ruamel_yaml.dump(table_entry, stream)
    info(stream.getvalue().rstrip())
    info(f"[=== END ===]")
    if not confirm("Do you wish to add the source table"):
        fatal("Source table addition cancelled.")

    source_file.parent.mkdir(parents=True, exist_ok=True)
    # Write the updated YAML file
    with open(source_file, "w", encoding="utf-8") as file:
        ruamel_yaml.dump(data, file)

    # FIXME: Make editor user configurable with 'code' as default
    os.system(f"code {source_file}")


def generate_source(
        source_name: str,
        source_description: str,
        project_name: str,
        dataset_name: str,
        table_name: str,
        table_description: str,
) -> None:
    """Function for generating a new source."""
    _, existing_sources = get_source_tables()
    initial_context = {
        "manual_mode": False,
        "source_name": source_name,
        "source_description": source_description,
        "project_name": project_name,
        "dataset_name": dataset_name,
        "table_name": table_name,
        "table_description": table_description,
        "sources": existing_sources,
        "projects": sorted(
            list(set(source["project"] for source in existing_sources))
        )
    }
    while True:
        context = copy.deepcopy(initial_context)
        select_source(context)
        select_project(context)
        select_dataset(context)
        select_source_name(context)
        select_source_description(context)
        configure_missing_source(context)
        if not select_table(context):
            break
        else:
            # If no valid table - retry source selection
            continue
    select_table_description(context)

    create_source_file(
        source_file=context["source"]["file"],
        source_name=context["source"]["name"],
        source_description=context["source"]["description"],
        project_name=context["source"]["project"],
        dataset_name=context["source"]["dataset"],
        table_name=context["table_name"],
        table_description=context["table_description"],
        columns=context["columns"],
    )
