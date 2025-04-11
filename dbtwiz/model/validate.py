import os
import re
from pathlib import Path
from re import Match
from typing import Any, Dict, List, Optional, Tuple, Union

from ruamel.yaml import YAML

from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.project import ModelBasePath
from dbtwiz.gcp.bigquery import BigQueryClient
from dbtwiz.helpers.logger import info, warn


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
            info(f"Creating missing YML file for model {self.model_base.model_name}:")
            os.system(
                f"dbtwiz model create -l {self.model_base.layer} -d {self.model_base.domain} -n {self.model_base.identifier}"
            )
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
            except ValueError:
                # Continue to prod if relation_name format is invalid
                pass

        # Fall back to prod manifest
        manifest = Manifest(Manifest.PROD_MANIFEST_PATH)
        prod_model_details = manifest.model_by_name(model_name)

        if prod_model_details and (
            relation_name := prod_model_details.get("relation_name")
        ):
            try:
                project, dataset, table_name = relation_name.replace("`", "").split(".")
                columns, error = self.bq_client.fetch_table_columns(
                    project, dataset, table_name
                )
                if error:
                    return None, f"Prod: {error}"
                if columns:
                    return columns, None
            except ValueError:
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
                        updated = True

                    # Add description if missing in yml
                    if "description" not in new_col:
                        new_col["description"] = table_col.get("description")
                        updated = True

                    new_columns.append(new_col)

                # Check for removed columns
                removed_cols = set(yml_cols.keys()) - set(table_cols.keys())
                if removed_cols:
                    updated = True

                if updated:
                    model_def["columns"] = new_columns

            if updated:
                with open(yml_path, "w", encoding="utf-8") as f:
                    self.ruamel_yaml.dump(yml_content, f)
                return True, f"{yml_path}: updated successfully."
            return True, f"{yml_path}: already up to date"

        except Exception as e:
            return False, f"Error updating YML file: {str(e)}"


class SqlValidator:
    def __init__(self, model_path: Union[str, Path]):
        self.model_base = ModelBasePath(path=model_path)

    def _get_table_replacement(
        self, match: Match[str], lookup_dict: Dict, unresolved_tables: List[str]
    ):
        """Replaces a table match with a corresponding ref or source."""
        project = match.group(1).strip("`")
        dataset = match.group(2).strip("`")
        table = match.group(3).strip("`")

        lookup_key = f"{project.lower()}.{dataset.lower()}.{table.lower()}"
        reference = lookup_dict.get(lookup_key)

        if reference:
            ref_type, ref_value = reference
            if ref_type == "ref":
                return f'{{{{ ref("{ref_value}") }}}}'
            else:
                source_name, table_name = ref_value
                return f'{{{{ source("{source_name}", "{table_name}") }}}}'
        else:
            unresolved_tables.append(f"{project}.{dataset}.{table}")
            return match.group(0)

    def _replace_table_references(
        self, sql_content: str, lookup_dict: dict
    ) -> Tuple[str, List[str]]:
        """Replace table references while handling all backtick cases."""
        pattern = r"(`?[^`\s]+`?)\.(`?[^`\s]+`?)\.(`?[^`\s]+`?)"
        unresolved_tables = []

        new_sql = []
        last_pos = 0

        # Process content sequentially
        for match in re.finditer(pattern, sql_content):
            # Add text before match
            new_sql.append(sql_content[last_pos : match.start()])
            # Add replacement
            new_sql.append(
                self._get_table_replacement(
                    match=match,
                    lookup_dict=lookup_dict,
                    unresolved_tables=unresolved_tables,
                )
            )
            last_pos = match.end()

        # Add remaining content
        new_sql.append(sql_content[last_pos:])
        return "".join(new_sql), list(set(unresolved_tables))

    def convert_sql_to_model(self):
        """Replace fully-qualified names with dbt ref() / source()."""
        file_path = self.model_base.path.with_suffix(".sql")

        Manifest.download_prod_manifest()
        manifest_data = Manifest(Manifest.PROD_MANIFEST_PATH)

        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        new_sql, unresolved = self._replace_table_references(
            sql_content=sql_content, lookup_dict=manifest_data.table_reference_lookup()
        )

        if new_sql != sql_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(new_sql)
            info(f"Updated references in {file_path}")

        if unresolved:
            warn("Unresolved tables:\n  - " + "\n  - ".join(unresolved))

    def validate_formatting(self) -> Tuple[bool, str]:
        """Validate SQL formatting using sqlfluff"""
        try:
            from sqlfluff import lint

            self.sql_content = self.model_base.path.with_suffix(".sql").read_text(
                encoding="utf-8"
            )
            result = lint(self.sql_content, dialect="bigquery")
            print(result)
            if result.violations:
                return False, f"SQL formatting issues: {result.violations}"
            return True, "SQL formatting is valid"
        except Exception as e:
            return False, f"SQL format validation failed: {str(e)}"

    def format_file(self) -> Tuple[bool, str]:
        """Format SQL using sqlfmt"""
        from sqlfmt.api import Mode, run

        report = run([self.model_base.path.with_suffix(".sql")], Mode())
        if report.number_changed > 0:
            report.display_report()

    def full_validation(self) -> Tuple[bool, str]:
        """Run all SQL validations"""
        self.convert_sql_to_model()
        # self.validate_formatting()
        self.format_file()


class ModelValidator:
    def __init__(self, model_path: Union[str, Path]):
        self.model_base = ModelBasePath(path=model_path)

    def validate(self) -> bool:
        """Run complete model validation"""

        # YML Validation
        YmlValidator(model_path=self.model_base.path).validate_and_update_yml_columns()

        # SQL Validation
        SqlValidator(
            model_path=self.model_base.path.with_suffix(".sql")
        ).full_validation()
