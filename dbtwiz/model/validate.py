from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from ruamel.yaml import YAML

from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.project import ModelBasePath
from dbtwiz.gcp.bigquery import BigQueryClient


class YmlValidator:
    def __init__(self, model_path: Union[str, Path]):
        """
        Initialize with:
        - model_path: Path to model file (either .sql or .yml)
        """
        self.bq_client = BigQueryClient()
        self.model_base = ModelBasePath(path=model_path)
        self.ruamel_yaml = YAML()
        self.ruamel_yaml.preserve_quotes = True
        self.ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

    def validate_and_update_yml_columns(self) -> Tuple[bool, str]:
        """
        Validate and update YML columns using the initialized path.

        Returns:
            Tuple of (success: bool, message: str)
        """
        yml_path = self.model_base.path.with_suffix(".yml")
        if not yml_path.exists():
            return False, f"YML file not found at {yml_path}"

        table_columns, error = self._get_table_columns(self.model_base.model_name)
        if error:
            return False, error

        return self._update_yml_columns(yml_path, table_columns)

    def _get_table_columns(
        self, model_name: str
    ) -> Tuple[Optional[List[str]], Optional[str]]:
        """Get columns from either dev or prod table in BigQuery using relation_name."""
        # Try dev manifest first
        manifest = Manifest()
        dev_model_details = manifest.model_by_name(model_name)

        if dev_model_details and (
            relation_name := dev_model_details.get("relation_name")
        ):
            try:
                # Remove backticks and split
                project, dataset, table_name = relation_name.replace("`", "").split(".")
                columns, error = self.bq_client.fetch_table_columns(
                    project, dataset, table_name
                )

                # Only return error if it's a permission issue (not NotFound)
                if error and "does not exist" not in error:
                    return None, f"Dev: {error}"
                if columns:
                    return columns, None
            except ValueError as e:
                # Continue to prod if relation_name format is invalid
                pass

        # Fall back to prod manifest
        manifest = Manifest(Manifest.PROD_MANIFEST_PATH)
        prod_model_details = manifest.model_by_name(model_name)

        if prod_model_details and (
            relation_name := prod_model_details.get("relation_name")
        ):
            try:
                # Remove backticks and split
                project, dataset, table_name = relation_name.replace("`", "").split(".")
                columns, error = self.bq_client.fetch_table_columns(
                    project, dataset, table_name
                )
                if error:
                    return None, f"Prod: {error}"
                if columns:
                    return columns, None
            except ValueError as e:
                return None, f"Prod: Invalid relation_name format: {relation_name}"

        return (
            None,
            f"Could not find table columns for model {model_name} in either dev or prod",
        )

    def _update_yml_columns(
        self, yml_path: Path, table_columns: List[Dict[str, Any]]
    ) -> Tuple[bool, str]:
        """Update YML file with current table columns (including types/descriptions)."""
        try:
            with open(yml_path, "r", encoding="utf-8") as f:
                yml_content = self.ruamel_yaml.load(f)

            if not yml_content:
                return False, "YML file is empty or invalid"

            updated = False
            messages = []

            for model_def in yml_content.get("models", []):
                if not isinstance(model_def, dict):
                    continue

                if "columns" not in model_def:
                    model_def["columns"] = []
                    updated = True

                # Create lookups
                yml_cols = {col["name"]: col for col in model_def.get("columns", [])}
                table_cols = {col["name"]: col for col in table_columns}

                # Build new columns list
                new_columns = []
                for col_name, table_col in table_cols.items():
                    new_col = yml_cols.get(col_name, {"name": col_name})

                    # Update data_type if different
                    if (
                        "data_type" in table_col
                        and new_col.get("data_type") != table_col["data_type"]
                    ):
                        new_col["data_type"] = table_col["data_type"]
                        messages.append(f"Updated data_type for {col_name}")
                        updated = True

                    # Add description if missing in yml
                    if "description" in table_col and (
                        not new_col.get("description")
                        or new_col.get("description") == ""
                    ):
                        new_col["description"] = table_col["description"]
                        messages.append(f"Updated description for {col_name}")
                        updated = True

                    new_columns.append(new_col)

                # Check for removed columns
                removed_cols = set(yml_cols.keys()) - set(table_cols.keys())
                if removed_cols:
                    messages.extend(f"Removed column: {col}" for col in removed_cols)
                    updated = True

                if updated:
                    model_def["columns"] = new_columns

            if updated:
                with open(yml_path, "w", encoding="utf-8") as f:
                    self.ruamel_yaml.dump(yml_content, f)
                return True, "YML updated successfully. " + ", ".join(messages)
            return True, "YML columns are already up to date"

        except Exception as e:
            return False, f"Error updating YML file: {str(e)}"
