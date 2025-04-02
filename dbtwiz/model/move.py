import os
from copy import deepcopy
from io import StringIO
from pathlib import Path

from ruamel.yaml import YAML

from dbtwiz.logging import error, info


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
        # Read the old SQL file
        with open(old_sql_file, "r", encoding="utf-8") as file:
            old_sql_content = file.read()

        # Create new SQL content (same as old SQL content)
        new_sql_content = old_sql_content

        # Read the old YML file
        with open(old_yml_file, "r", encoding="utf-8") as file:
            old_yml_content = ruamel_yaml.load(file)

        # Create new YML content with the new model name
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
        # Write new SQL file
        with open(new_sql_file, "w", encoding="utf-8") as file:
            file.write(new_sql_content)

        # Write new YML file
        with open(new_yml_file, "w", encoding="utf-8") as file:
            file.write(new_yml_content_str)

        if safe:
            # Update old SQL file
            with open(old_sql_file, "w", encoding="utf-8") as file:
                file.write(old_sql_content_updated)

            # Update old YML file
            with open(old_yml_file, "w", encoding="utf-8") as file:
                file.write(old_yml_content_updated_str)
        else:
            # Delete the old files
            os.remove(old_sql_file)
            os.remove(old_yml_file)
            info(f"Deleted old dbt files for {old_model_name}", style="yellow")

        info(
            f"Successfully updated dbt files for model: {old_model_name} -> {new_model_name}"
        )

    except Exception as e:
        error(f"Error updating dbt files for model {old_model_name}: {e}")
        # Clean up partially created files if any
        if os.path.exists(new_sql_file):
            os.remove(new_sql_file)
        if os.path.exists(new_yml_file):
            os.remove(new_yml_file)
        info(f"Rolled back changes for {old_model_name}.")
