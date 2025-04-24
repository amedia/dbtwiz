import os
import re
from pathlib import Path
from re import Match
from typing import Any, Dict, List, Optional, Tuple, Union

from ruamel.yaml import YAML

from dbtwiz.config.project import ProjectConfig
from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.model import ModelBasePath
from dbtwiz.gcp.auth import ensure_app_default_auth
from dbtwiz.gcp.bigquery import BigQueryClient
from dbtwiz.helpers.logger import status, warn


class YmlValidator:
    def __init__(self, model_path: Union[str, Path]):
        """Init function for yml validator."""
        ensure_app_default_auth()
        self.bq_client = BigQueryClient()
        self.model_base = ModelBasePath(path=model_path)
        self.ruamel_yaml = YAML()
        self.ruamel_yaml.preserve_quotes = True
        self.ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

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
            updated = False

            with open(yml_path, "r", encoding="utf-8") as f:
                yml_content = self.ruamel_yaml.load(f)

            if not yml_content:
                return False, "YML file is empty or invalid"

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
                updated = True if removed_cols else updated
                model_def["columns"] = new_columns if updated else model_def["columns"]

            if updated:
                with open(yml_path, "w", encoding="utf-8") as f:
                    self.ruamel_yaml.dump(yml_content, f)
                return True, "updated columns successfully"

            return True, "yml ok"

        except Exception as e:
            return False, f"yml update failed: {str(e)}"

    def validate_yml_exists(self) -> Tuple[bool, str]:
        """Validates that the yml exists, or triggers yml creation if not."""
        yml_path = self.model_base.path.with_suffix(".yml")
        if not yml_path.exists():
            warn(
                f"{self.model_base.model_name}: yml file missing - [italic]creating[/italic]"
            )
            os.system(
                f"dbtwiz model create -l {self.model_base.layer} -d {self.model_base.domain} -n {self.model_base.identifier}"
            )
            if yml_path.exists():
                return True, f"yml file created successfully: {yml_path.name}"
            if not yml_path.exists():
                return False, f"yml file not found: {yml_path.name}"
        return True, "yml file ok"

    def _validate_model_name(self, model_def: dict) -> List[str]:
        """Validates that the model has a correct name, and returns identified errors."""
        validation_errors = []

        current_name = model_def.get("name", "")

        # Check 1: YML filename matches model name
        if current_name != self.model_base.path.stem:
            validation_errors.append(
                f"model name [italic]{current_name}[/italic] doesn't match yml filename [italic]{self.model_base.path.stem}[/italic]"
            )

        # Check 2: Model name matches folder structure
        name_parts = current_name.split("__")
        if len(name_parts) != 2:
            validation_errors.append(
                f"model name [italic]{current_name}[/italic] doesn't follow <layer>_<domain>__<identifier> convention"
            )
            return validation_errors

        current_prefix, _ = name_parts

        # Check layer (first part of prefix)
        current_layer_abbr = current_prefix.split("_")[0]
        expected_layer_abbr = self.model_base.layer_abbreviation

        # Check domain (second part of prefix)
        current_domain = "_".join(current_prefix.split("_")[1:])
        expected_domain = self.model_base.domain

        # Build detailed mismatch messages
        if current_layer_abbr != expected_layer_abbr:
            validation_errors.append(
                f"prefix [italic]{current_prefix}[/italic] suggests layer [italic]{current_layer_abbr}[/italic] "
                f"but model is in [italic]{expected_layer_abbr}[/italic] layer folder"
            )

        if current_domain != expected_domain:
            validation_errors.append(
                f"prefix [italic]{current_prefix}[/italic] suggests domain [italic]{current_domain}[/italic] "
                f"but model is in [italic]{expected_domain}[/italic] domain folder"
            )

        return validation_errors

    def validate_yml_definition(self) -> Tuple[bool, str]:
        """Validates YML definition (currently only the model name)."""
        yml_path = self.model_base.path.with_suffix(".yml")

        # Load YML content
        with open(yml_path, "r", encoding="utf-8") as f:
            yml_content = self.ruamel_yaml.load(f)

        if (
            not yml_content
            or "models" not in yml_content
            or len(yml_content.get("models", [])) == 0
        ):
            return False, "yml file is empty or missing 'models' key"
        elif len(yml_content.get("models", [])) > 1:
            return False, "yml file contains more than one model definition"

        validation_errors = self._validate_model_name(
            model_def=yml_content.get("models")[0]
        )

        if validation_errors:
            error_msg = "failed\n" + "\n".join(f"• {e}" for e in validation_errors)
            return False, error_msg
        return True, "yml file name ok"

    def validate_yml_columns(self) -> Tuple[bool, str]:
        """Validate and update YML columns using the initialized path."""
        yml_path = self.model_base.path.with_suffix(".yml")
        table_columns, error = self._get_table_columns(self.model_base.model_name)
        if error:
            return False, error

        return self._update_yml_columns(yml_path, table_columns)


class SqlValidator:
    def __init__(self, model_path: Union[str, Path]):
        """Init function for sql validator."""
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

        results = []
        status = True
        if new_sql != sql_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(new_sql)
            results.append("updated all references")

        if unresolved:
            results.append("unresolved tables:\n  - " + "\n  - ".join(unresolved))
            status = False

        if new_sql == sql_content and not unresolved:
            results.append("references ok")

        return status, "\n".join(results)

    def sqlfluff_format_violations(self, violations: List, file_path: Path) -> str:
        """Formats SQLFluff violations from SQLBaseError objects."""
        messages = []
        for violation in violations:
            # Get basic error info
            error_info = (
                f"{file_path.name}:{violation.line_no}:{violation.line_pos} "
                f"[{violation.rule_code()}] {violation.description}"
            )

            # For syntax errors, add the unexpected token if available
            if hasattr(violation, "unexpected_token"):
                error_info += f"\nUnexpected token: {violation.unexpected_token}"

            messages.append(error_info)

        return "\n\n".join(messages)

    def sqlfluff_validate_and_fix_file(self) -> Tuple[bool, str]:
        """Validate and fix SQL formatting using sqlfluff"""
        from sqlfluff.core import FluffConfig, Linter
        from sqlfluff.core.config.loader import load_config_at_path

        self.sql_content = self.model_base.path.with_suffix(".sql").read_text(
            encoding="utf-8"
        )

        config = FluffConfig(
            load_config_at_path(ProjectConfig().root_path().absolute())
        )
        linter = Linter(config=config)
        # initial results:
        try:
            lint_results = linter.lint_string(
                self.sql_content,
                fname=self.model_base.path.with_suffix(".sql"),
                fix=True,
            )
        except Exception as e:
            if "DuplicateResourceNameError" in str(e):
                # Parse the dbt error message for duplicate paths
                error_msg = str(e)
                duplicates = []
                current_line = ""

                # Process the error message line by line
                for line in error_msg.split("\n"):
                    if line.strip().startswith("- "):
                        duplicates.append(line.strip()[2:])
                    elif "dbt found" in line and "models with the name" in line:
                        current_line = line.strip()

                if duplicates:
                    formatted_duplicates = "\n".join(f"• {dup}" for dup in duplicates)
                    return False, (f"failed\n{current_line}\n{formatted_duplicates}")
                return False, f"duplicate models detected: {error_msg}"
            return False, f"validation failed with error {e}"

        if lint_results.get_violations():
            fixed_sql = lint_results.fix_string()[0]
            self.model_base.path.with_suffix(".sql").write_text(
                fixed_sql, encoding="utf-8"
            )

            # Verify fixes were applied
            new_lint_result = linter.lint_string(
                fixed_sql, fname=self.model_base.path.with_suffix(".sql")
            )

            if new_lint_result.violations:
                formatted_output = self.sqlfluff_format_violations(
                    new_lint_result.get_violations(),
                    self.model_base.path.with_suffix(".sql"),
                )

                return (
                    False,
                    f"attempted fixes, but issues remain: \n{formatted_output}",
                )
            else:
                return True, "applied fixes"

        return True, "validation ok"

    def sqlfmt_format_file(self) -> Tuple[bool, str]:
        """Format SQL using sqlfmt"""
        from sqlfmt.api import Mode, run
        from sqlfmt.config import load_config_file

        pyproject_path = ProjectConfig().root_path() / "pyproject.toml"
        config = load_config_file([pyproject_path])
        mode = Mode(**config)

        report = run([self.model_base.path.with_suffix(".sql")], mode)
        if report.number_changed > 0:
            return True, "applied fixes"
        elif report.number_errored > 0:
            return False, "sqlfmt failed"
        else:
            return True, "validation ok"


class ModelValidator:
    def __init__(self, model_path: Union[str, Path]):
        """Init function for model validator."""
        self.model_base = ModelBasePath(path=model_path)

    def validate(self) -> bool:
        """Run complete model validation"""
        # Initialize validators
        yml_validator = YmlValidator(model_path=self.model_base.path)
        sql_validator = SqlValidator(model_path=self.model_base.path)

        # Run validation
        for func, desc in [
            (yml_validator.validate_yml_exists, "Validating yml exists"),
            (yml_validator.validate_yml_definition, "Validating yml definition"),
            (yml_validator.validate_yml_columns, "Validating yml columns"),
            (sql_validator.convert_sql_to_model, "Validating sql references"),
            (sql_validator.sqlfmt_format_file, "Validating sql with sqlfmt"),
            (
                sql_validator.sqlfluff_validate_and_fix_file,
                "Validating sql with sqlfluff",
            ),
        ]:
            status(message=desc)
            success, result = func()
            status(
                message=desc, status_text=result, style="green" if success else "red"
            )
