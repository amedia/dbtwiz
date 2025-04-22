import os
import re
from copy import deepcopy
from io import StringIO
from pathlib import Path

from ruamel.yaml import YAML

from dbtwiz.helpers.logger import error, info, status


def _write_file(file_path, file_content):
    """Generic function for writing file."""
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(file_content)


def _read_file(file_path, yml_loader=None):
    """Generic function for reading file."""
    with open(file_path, "r", encoding="utf-8") as file:
        if yml_loader:
            return yml_loader.load(file)
        else:
            return file.read()
    return None


def _safe_delete_file(file_path):
    """Safely deletes a file should it exist."""
    if os.path.exists(file_path):
        os.remove(file_path)


def move_model(
    old_folder_path: str,
    old_model_name: str,
    new_folder_path: str,
    new_model_name: str,
    safe: bool = True,
) -> None:
    """
    Moves a model by copying with a new name to a new location.

    If parameter safe is True then retains the original model,
    but changes it to be a view pointing to the new model.
    Otherwise, the old model is deleted.
    """
    try:
        status(
            message=r"\[dbt] " + f"Migrating model [italic]{old_model_name}[/italic] to [bold]{new_model_name}[/bold]"
        )

        old_folder_path = Path(old_folder_path)
        new_folder_path = Path(new_folder_path)
        # Define old and new file paths
        old_sql_file = next(old_folder_path.rglob(f"{old_model_name}.sql"), None)
        if not old_sql_file:
            error(f"Couldn't find file {old_model_name}.sql in path {old_folder_path}")
            return
        old_yml_file = old_sql_file.with_suffix(".yml")
        new_sql_file = new_folder_path / f"{new_model_name}.sql"
        new_yml_file = new_folder_path / f"{new_model_name}.yml"

        # Ensure the new directory exists
        os.makedirs(os.path.dirname(new_sql_file), exist_ok=True)

        ruamel_yaml = YAML()
        ruamel_yaml.preserve_quotes = True
        ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

        # Step 1: Create new SQL and YML content in memory
        # Read the old files
        old_sql_content = _read_file(old_sql_file)
        old_yml_content = _read_file(old_yml_file, yml_loader=ruamel_yaml)

        # Create new file contents
        new_sql_content = old_sql_content
        new_yml_content = deepcopy(old_yml_content)
        new_yml_content["models"][0]["name"] = new_model_name

        # Step 2: Modify the old SQL content
        if safe:
            old_sql_content_updated = f"select * from {{{{ ref('{new_model_name}') }}}}"

            # Step 3: Update the old YML file to materialize as a view and add meta element
            # Clean up old file config since we're changing to view
            old_yml_content_updated = deepcopy(old_yml_content)
            for config_key in [
                "full_refresh",
                "incremental_strategy",
                "on_schema_change",
                "partition_by",
                "partition_expiration_days",
                "require_partition_filter",
                "tags",
            ]:
                if config_key in old_yml_content_updated["models"][0]["config"]:
                    del old_yml_content_updated["models"][0]["config"][config_key]

            old_yml_content_updated["models"][0]["name"] = old_model_name
            if old_yml_content_updated["models"][0]["config"]["materialized"] in (
                "table",
                "incremental",
            ):
                old_yml_content_updated["models"][0]["config"]["materialized"] = "view"
            if "meta" in old_yml_content_updated["models"][0].get("config", {}):
                old_yml_content_updated["models"][0]["config"]["meta"][
                    "is_tmp_old_copy"
                ] = True
            else:
                old_yml_content_updated["models"][0]["config"]["meta"] = {
                    "is_tmp_old_copy": True
                }

            old_yml_content_updated_str = StringIO()
            ruamel_yaml.dump(old_yml_content_updated, old_yml_content_updated_str)
            old_yml_content_updated_str = old_yml_content_updated_str.getvalue()

        new_yml_content_str = StringIO()
        ruamel_yaml.dump(new_yml_content, new_yml_content_str)
        new_yml_content_str = new_yml_content_str.getvalue()

        # Step 5: Write all changes to disk (only if all in-memory operations succeeded)
        _write_file(new_sql_file, new_sql_content)
        _write_file(new_yml_file, new_yml_content_str)

        if safe:
            _write_file(old_sql_file, old_sql_content_updated)
            _write_file(old_yml_file, old_yml_content_updated_str)
        else:
            # Delete the old files
            _safe_delete_file(old_sql_file)
            _safe_delete_file(old_yml_file)
            info(f"Deleted old dbt files for {old_model_name}", style="yellow")

        status(
            message=r"\[dbt] " + f"Migrating model [italic]{old_model_name}[/italic] to [bold]{new_model_name}[/bold]",
            status_text="done",
            style="green",
        )

    except Exception as e:
        error(f"Error updating dbt files for model {old_model_name}: {e}")
        _safe_delete_file(new_sql_file)
        _safe_delete_file(new_yml_file)
        info(f"Rolled back changes for {old_model_name}.")


def update_model_references(old_model_name: str, new_model_name: str) -> None:
    """
    Updates all references to the old model name in dbt SQL files to the new model name.

    Args:
        old_model_name: The old model name (e.g., 'stg_old_domain__old_model_name').
        new_model_name: The new model name (e.g., 'stg_new_domain__new_model_name').
    """
    try:
        # Define the regex pattern to match `{{ ref('model_name') }}` with flexible spacing
        ref_pattern = re.compile(
            r"\{\{\s*ref\s*\(\s*['\"]"
            + re.escape(old_model_name)
            + r"['\"]\s*\)\s*\}\}",
            re.IGNORECASE,
        )

        status(
            message=r"\[dbt] " + f"Updating references from [italic]{old_model_name}[/italic] to [bold]{new_model_name}[/bold]"
        )
        reference_changes = 0
        # Walk through the dbt project directory to find all SQL files
        for root, _, files in os.walk(Path("models")):
            for file in files:
                if file.endswith(".sql"):
                    file_path = Path(root) / file

                    # Replace all occurrences of the old model name with the new model name
                    content = _read_file(file_path)
                    updated_content, replacements = ref_pattern.subn(
                        f'{{{{ ref("{new_model_name}") }}}}', content
                    )
                    reference_changes += replacements

                    # If replacements were made, write the updated content back to the file
                    if replacements > 0:
                        _write_file(file_path, updated_content)

        change_suffix = "" if reference_changes == 1 else "s"
        status(
            message=r"\[dbt] " + f"Updating references from [italic]{old_model_name}[/italic] to [bold]{new_model_name}[/bold]",
            status_text=f"done ({reference_changes} change{change_suffix})",
            style="green",
        )

    except Exception as e:
        error(f"Error updating references in dbt files: {e}")
