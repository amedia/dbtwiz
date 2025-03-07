import os
import re
from io import StringIO
from pathlib import Path
from typing import Dict, Tuple

from dbtwiz.bigquery import (check_project_exists, fetch_table_columns,
                             fetch_tables_in_dataset, list_datasets_in_project)
from dbtwiz.interact import (autocomplete_from_list, confirm, input_text,
                             select_from_list)
from dbtwiz.logging import error, fatal, info, warn
from dbtwiz.model import get_source_tables
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.scalarstring import PreservedScalarString


def ask_for_source() -> Tuple[Dict, str]:
    """Ask the user to select an existing source or create a new one."""
    manual_mode = False
    _, sources = get_source_tables()
    source_choices = [{"name": "Create a new source", "value": {}}] + [
        {
            "name": f"{source['name']} (Project: {source['project']}, dataset: {source['dataset']})",
            "value": source,
        }
        for source in sources
    ]
    selected_choice = select_from_list(
        "Where is the source table located", items=source_choices, use_shortcuts=False
    )

    # Check if the user chose "Create a new source"
    if selected_choice == {}:
        projects = sorted(list(set(source["project"] for source in sources)))
        project = autocomplete_from_list(
            "What is the project for the source", items=projects, must_exist=False
        )

        if project:
            exists_check = check_project_exists(project)
            if exists_check != "Exists":
                error(exists_check)
                action = select_from_list(
                    "What would you like to do",
                    items=[
                        {"name": "Try a different project", "value": "retry"},
                        {"name": "Add values manually", "value": "manual"},
                        {"name": "Cancel", "value": "cancel"},
                    ],
                )

                if action == "retry":
                    return ask_for_source()
                elif action == "manual":
                    manual_mode = True

        if project and not manual_mode:
            # Fetch datasets in the selected project
            datasets, error_message = list_datasets_in_project(project)
            if error_message:
                fatal(error_message)
            elif not datasets:
                fatal(f"No datasets found in the project '{project}'.")

            dataset = autocomplete_from_list(
                "What is the dataset for the source", items=datasets
            )

            # Check if the project+dataset combination already exists as a source
            existing_source = next(
                (
                    source
                    for source in sources
                    if source["project"] == project and source["dataset"] == dataset
                ),
                None,
            )
            if existing_source:
                info(
                    f"Using existing source '{existing_source['name']}' for project '{project}' and dataset '{dataset}'."
                )
                return existing_source
        else:
            # Manual mode: Skip BigQuery validation
            dataset = input_text(
                "What is the dataset for the source",
                allow_blank=False,
                validate=lambda string: (
                    re.match(r"^[a-z][a-z0-9_]*[a-z0-9]$", string) is not None
                )
                or "The dataset can only contain lowercase, digits and underscores, must start with a character and not end with underscore",
            )

        source_name = input_text(
            "Give a short name for the new source (project+dataset combination)",
            allow_blank=False,
            validate=lambda string: (
                re.match(r"^[a-z][a-z0-9_]*[a-z0-9]$", string) is not None
            )
            or "The source name can only contain lowercase, digits and underscores, must start with a character and not end with underscore",
        )

        source_description = input_text(
            "Give a short description for the new source",
            allow_blank=False,
            validate=lambda string: (re.match(r"^\S+", string) is not None)
            or "The description must not start with a space",
        )

        # Create a new source entry
        selected_source = {
            "name": source_name,
            "description": source_description,
            "project": project,
            "dataset": dataset,
            "tables": [],
            "file": Path("sources") / f"{source_name}.yml",
        }
    else:
        selected_source = selected_choice

    return selected_source, manual_mode


def create_source_file(source, table_name, description, columns=None) -> None:
    """Create or update the source YAML file with the new table."""
    # Configure yaml format
    ruamel_yaml = YAML()
    ruamel_yaml.preserve_quotes = True
    ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

    source_file = source["file"]

    if source_file.exists():
        with open(source_file, "r", encoding="utf-8") as file:
            data = ruamel_yaml.load(file) or {"version": 2, "sources": []}
    else:
        data = CommentedMap()
        data['version'] = 2
        data['sources'] = []
        # Add a blank line between 'version' and 'sources'
        data.yaml_set_comment_before_after_key('sources', before='\n')

    # Find or create the source entry
    source_entry = next(
        (s for s in data["sources"] if s["name"] == source["name"]), None
    )
    is_new_source = not source_entry
    if is_new_source:
        source_entry = {
            "name": source["name"],
            "description": source["description"],
            "database": source["project"],
            "schema": source["dataset"],
            "tables": [],
        }
        data["sources"].append(source_entry)

    # Add the new table
    table_entry = {
        "name": table_name,
        "description": PreservedScalarString(
            description
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


def generate_source():
    """Generate new dbt source"""
    source, manual_mode = ask_for_source()

    if not manual_mode:
        # Fetch all tables in the given project and dataset
        tables_in_dataset, error_message = fetch_tables_in_dataset(
            source["project"], source["dataset"]
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
                return generate_source()  # Restart the process
            elif action == "manual":
                manual_mode = True  # Enter manual mode

        if not manual_mode:
            # Filter out tables that already exist as sources
            existing_tables = source["tables"]
            available_tables = [
                table for table in tables_in_dataset if table not in existing_tables
            ]

            if not available_tables:
                fatal(
                    "All tables in the specified project and dataset already have sources."
                )

            # Ask for the table name
            table_name = autocomplete_from_list(
                "What is the name of the table",
                items=available_tables,
                # Ensure the table exists in the dataset and is not already a source
                validate=lambda x: x in available_tables,
            )

            # Fetch column details for the selected table
            columns, error_message = fetch_table_columns(
                source["project"], source["dataset"], table_name
            )
            if not columns:
                fatal(error_message)

    if manual_mode:
        # Manual mode: Skip BigQuery validation
        while True:
            table_name = input_text(
                "What is the name of the table",
                allow_blank=False,
                validate=lambda string: (
                    re.match(r"^[a-z][a-z0-9_]*[a-z0-9]$", string) is not None
                )
                or "The table name can only contain lowercase, digits and underscores, must start with a character and not end with underscore",
            )

            # Check if the table already exists in the source
            if table_name in source["tables"]:
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
                break  # Valid table name, proceed

        columns = None  # No columns in manual mode

    # Ask for the table description
    description = input_text(
        "Give a short description for the source table",
        validate=lambda string: (re.match(r"^\S+", string) is not None)
        or "The description must not start with a space",
    )

    # Create or update the source YAML file
    create_source_file(source, table_name, description, columns)
    info(f"Source table '{table_name}' added to '{source['file']}' successfully!")
