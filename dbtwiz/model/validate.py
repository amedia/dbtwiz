import json
import re
import subprocess
import tempfile
from pathlib import Path
from re import Match
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ruamel.yaml import YAML

from ..config.project import project_config
from ..core.model import ModelBasePath
from ..dbt.manifest import Manifest
from ..dbt.run import (
    dbt_check_command,
    dbt_core_installed,
    dbt_lint_command,
    disable_sqlfluff_dbt_version_check,
)
from ..integrations.bigquery import BigQueryClient
from ..integrations.gcp_auth import ensure_app_default_auth
from ..utils.logger import status, warn
from .config_rules import evaluate_rules


def check_violations(run_results: dict, model_name: str) -> List[str]:
    """The violations of one model in the run_results.json of `dbt check`, one per row.

    A check's message holds each violating row as `key=value, ...` on its own line.
    Only rows about `model_name` are kept: rows without a `unique_id` are project-wide
    aggregates (e.g. test coverage), which no single model can fix, and a row about
    another node is not this model's violation.
    """
    violations = []
    for result in run_results.get("results", []):
        check = result["unique_id"].split(".")[-1]
        if result["status"] == "error":
            violations.append(f"{check}: {result.get('message') or 'failed to run'}")
            continue
        if result["status"] not in ("warn", "fail"):
            continue
        for line in (result.get("message") or "").splitlines():
            fields = dict(
                part.split("=", 1) for part in line.strip().split(", ") if "=" in part
            )
            # model.<project>.<name>, plus .v<n> for a versioned model
            unique_id = fields.get("unique_id", "").split(".")
            if (
                len(unique_id) < 3
                or unique_id[0] != "model"
                or unique_id[2] != model_name
            ):
                continue
            details = [
                f"{key}: {value}"
                for key, value in fields.items()
                if key not in ("unique_id", "name", "original_file_path") and value
            ]
            violations.append(check + (f" ({', '.join(details)})" if details else ""))
    return violations


class YmlValidator:
    """Validator for YML configuration files in dbt models."""

    def __init__(self, model_path: Union[str, Path]) -> None:
        """Initialize the YML validator with the given model path.

        Args:
            model_path: Path to the model file to validate
        """
        ensure_app_default_auth()
        self.bq_client = BigQueryClient()
        self.model_base = ModelBasePath(path=model_path)
        self.ruamel_yaml = YAML()
        self.ruamel_yaml.preserve_quotes = True
        self.ruamel_yaml.indent(mapping=2, sequence=4, offset=2)

    # ============================================================================
    # PUBLIC METHODS - Validation
    # ============================================================================

    def validate_yml_exists(self) -> Tuple[bool, str]:
        """Validates that the yml exists, or triggers yml creation if not."""
        import subprocess

        yml_path = self.model_base.path.with_suffix(".yml")
        if not yml_path.exists():
            warn(
                f"{self.model_base.model_name}: yml file missing - [italic]creating[/italic]"
            )
            subprocess.run(
                [
                    "dbtwiz",
                    "model",
                    "create",
                    "-l",
                    self.model_base.layer,
                    "-d",
                    self.model_base.domain,
                    "-n",
                    self.model_base.identifier,
                ],
            )
            if yml_path.exists():
                return True, f"yml file created successfully: {yml_path.name}"
            if not yml_path.exists():
                return False, f"yml file not found: {yml_path.name}"
        return True, "yml file ok"

    def validate_yml_definition(self, checked_by_dbt: bool = False) -> Tuple[bool, str]:
        """Validates the YML definition: the model name and its primary key constraint.

        With `checked_by_dbt`, the rules `dbt check` covers on dbt v2 are left to it (the
        name's layer and domain against its folders, whether a primary key exists), and
        only the yml itself is checked here: one model, named after its file, with a
        primary key declared in one place.
        """
        yml_content, load_error = self._load_model_yml()
        if load_error:
            return False, load_error

        if len(yml_content.get("models", [])) > 1:
            return False, "yml file contains more than one model definition"

        model_def = yml_content.get("models")[0]
        validation_errors = self._validate_model_name(
            model_def=model_def, check_folders=not checked_by_dbt
        )

        if (
            pk_constraint_error := self._validate_yml_primary_key_constraint(
                model_def=model_def, require=not checked_by_dbt
            )
        ) is not None:
            validation_errors.append(pk_constraint_error)

        if validation_errors:
            error_msg = "failed\n" + "\n".join(f"• {e}" for e in validation_errors)
            return False, error_msg
        return True, "yml definition ok" if checked_by_dbt else "yml file name ok"

    def validate_model_config_rules(self) -> Tuple[bool, str]:
        """Validate the model's config block against the project's configured rules.

        Rules come from `[[tool.dbtwiz.project.model_config_rules]]` in pyproject.toml, and
        dbtwiz has no built-in notion of what any of them mean: a project declares
        conditions and assertions over dotted config paths, so domain naming - a GDPR flag,
        a retention category - stays in the consuming project. A project that declares no
        rules gets a no-op check. See `dbtwiz.model.config_rules` for the rule vocabulary.

        Only the model's own yml is inspected; config inherited from dbt_project.yml, the
        manifest and BigQuery are all left out of it.
        """
        rules = project_config().model_config_rule_entries()
        if not rules:
            return True, "no model config rules configured"

        yml_content, load_error = self._load_model_yml()
        if load_error:
            return False, load_error

        # Lazy import: reading dbt_project.yml is only worthwhile once rules exist.
        from ..core.project import Project

        # `or {}` rather than a get default: a `config:` or `meta:` key present but empty
        # parses to None, which would otherwise blow up the lookups below.
        config = yml_content["models"][0].get("config") or {}
        violations = evaluate_rules(config, rules, Project().variables())

        if violations:
            return False, "failed\n" + "\n".join(f"• {v}" for v in violations)
        return True, "model config rules ok"

    def validate_yml_columns(self) -> Tuple[bool, str]:
        """Validate and update YML columns using the initialized path.

        Returns:
            Tuple of (success, message) indicating validation result
        """
        yml_path = self.model_base.path.with_suffix(".yml")
        table_columns, error = self._get_table_columns(self.model_base.model_name)
        if error:
            return False, error

        return self._update_yml_columns(yml_path, table_columns)

    def validate_dbt_checks(self) -> Tuple[bool, str]:
        """Run the project's `dbt check` checks on this model (dbt v2 only).

        Checks are where a dbt v2 project declares its structural rules (e.g.
        dbt_project_evaluator or a project's own checks package), so they replace the
        name, primary key and config rules dbtwiz checks itself.

        The results are written to a temporary target folder, so they cannot be
        mistaken for an earlier run's and the project's own target folder is left alone.
        """
        with tempfile.TemporaryDirectory() as target_path:
            run_results = Path(target_path) / "run_results.json"
            result = subprocess.run(
                dbt_check_command(self.model_base.model_name, Path(target_path)),
                capture_output=True,
                # dbt prints its violation tables with box-drawing characters
                encoding="utf-8",
                errors="replace",
            )
            if not run_results.exists():
                return False, f"dbt check failed:\n{result.stderr or result.stdout}"

            violations = check_violations(
                json.loads(run_results.read_text(encoding="utf-8")),
                self.model_base.model_name,
            )
        if violations:
            return False, "failed\n" + "\n".join(f"• {v}" for v in violations)
        return True, "checks ok"

    # ============================================================================
    # PRIVATE METHODS - Internal Helper Functions
    # ============================================================================

    def _load_model_yml(self) -> Tuple[Optional[dict], Optional[str]]:
        """Load the model's yml file, checking that it defines at least one model.

        Returns:
            Tuple of (yml content, error message), one of which is always None
        """
        yml_path = self.model_base.path.with_suffix(".yml")
        with open(yml_path, "r", encoding="utf-8") as f:
            yml_content = self.ruamel_yaml.load(f)

        if not yml_content or not yml_content.get("models"):
            return None, "yml file is empty or missing 'models' key"
        return yml_content, None

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
        Manifest.download_prod_manifest()
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

    def _validate_model_name(
        self, model_def: dict, check_folders: bool = True
    ) -> List[str]:
        """Validates that the model has a correct name, and returns identified errors.

        The yml's name must match its file name, since dbt applies the yml to the model
        it names. With `check_folders`, the name's layer and domain must also match the
        folders the model is in.
        """
        validation_errors = []

        current_name = model_def.get("name", "")

        # Check 1: YML filename matches model name
        if current_name != self.model_base.path.stem:
            validation_errors.append(
                f"model name [italic]{current_name}[/italic] doesn't match yml filename [italic]{self.model_base.path.stem}[/italic]"
            )

        if not check_folders:
            return validation_errors

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

    def _validate_yml_primary_key_constraint(
        self, model_def: dict, require: bool = True
    ) -> Optional[str]:
        """Validates the model's primary key constraint, and with `require` that it has one."""
        column_level_pk = False

        model_level_pk = any(
            c.get("type") == "primary_key" for c in model_def.get("constraints", [])
        )

        column_level_pk_columns = [
            col["name"]
            for col in model_def.get("columns", [])
            if any(c.get("type") == "primary_key" for c in col.get("constraints", []))
        ]
        column_level_pk = len(column_level_pk_columns) > 0

        if not model_level_pk and not column_level_pk:
            return "primary key constraint is missing" if require else None
        elif model_level_pk and column_level_pk:
            return "primary key constraint is defined at both model and column levels"
        elif column_level_pk and len(column_level_pk_columns) > 1:
            return "primary key constraint is defined for multiple columns - it must be changed to a model level constraint"
        return None


class SqlValidator:
    """Validator for SQL files in dbt models."""

    def __init__(self, model_path: Union[str, Path]) -> None:
        """Initialize the SQL validator with the given model path.

        Args:
            model_path: Path to the model file to validate
        """
        self.model_base = ModelBasePath(path=model_path)

    # ============================================================================
    # PUBLIC METHODS - SQL Validation and Formatting
    # ============================================================================

    def convert_sql_to_model(self) -> Tuple[bool, str]:
        """Replace fully-qualified names with dbt ref() / source().

        Returns:
            Tuple of (success, message) indicating conversion result
        """
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
            results.append(
                "found potential table references that don't match known models/sources:\n  - "
                + "\n  - ".join(unresolved)
                + "\n-> this may be expected (e.g., struct fields)"
            )
            status = False

        if new_sql == sql_content and not unresolved:
            results.append("references ok")

        return status, "\n".join(results)

    def sqlfluff_validate_and_fix_file(self) -> Tuple[bool, str]:
        """Validate and fix SQL formatting using sqlfluff"""
        from sqlfluff.core import FluffConfig, Linter
        from sqlfluff.core.config.loader import load_config_at_path

        self.sql_content = self.model_base.path.with_suffix(".sql").read_text(
            encoding="utf-8"
        )

        config = FluffConfig(
            load_config_at_path(project_config().root_path().absolute())
        )
        disable_sqlfluff_dbt_version_check()
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

    def dbt_lint_validate_and_fix_file(self) -> Tuple[bool, str]:
        """Validate and fix SQL using dbt lint, for projects on dbt v2 only"""
        sql_path = self.model_base.path.with_suffix(".sql")

        violations, error = self._dbt_lint_violations(sql_path)
        if error:
            return False, error
        if not violations:
            return True, "validation ok"

        subprocess.run(dbt_lint_command([sql_path], fix=True), capture_output=True)

        violations, error = self._dbt_lint_violations(sql_path)
        if error:
            return False, error
        if violations:
            formatted_output = self.dbt_lint_format_violations(violations, sql_path)
            return False, f"attempted fixes, but issues remain: \n{formatted_output}"
        return True, "applied fixes"

    def sqlfmt_format_file(self) -> Tuple[bool, str]:
        """Format SQL using sqlfmt"""
        from sqlfmt.api import Mode, run
        from sqlfmt.config import load_config_file

        pyproject_path = project_config().root_path() / "pyproject.toml"
        config = load_config_file(config_path=pyproject_path, files=[pyproject_path])
        mode = Mode(**config)

        report = run([self.model_base.path.with_suffix(".sql")], mode)
        if report.number_changed > 0:
            return True, "applied fixes"
        elif report.number_errored > 0:
            return False, "sqlfmt failed"
        else:
            return True, "validation ok"

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

    def dbt_lint_format_violations(
        self, violations: List[dict], file_path: Path
    ) -> str:
        """Formats violations from dbt lint's json output."""
        return "\n\n".join(
            f"{file_path.name}:{v['start_line_no']}:{v['start_line_pos']} "
            f"[{v['code']}] {v['description']}"
            for v in violations
        )

    # ============================================================================
    # PRIVATE METHODS - Internal Helper Functions
    # ============================================================================

    def _dbt_lint_violations(
        self, sql_path: Path
    ) -> Tuple[Optional[List[dict]], Optional[str]]:
        """Run dbt lint on one file, returning its violations or an error message."""
        result = subprocess.run(
            dbt_lint_command([sql_path], json_output=True),
            capture_output=True,
            text=True,
        )
        # Warnings go to stderr, so stdout is only the json array.
        output = next(
            (line for line in result.stdout.splitlines() if line.startswith("[")),
            None,
        )
        if result.returncode != 0 or output is None:
            return None, f"dbt lint failed:\n{result.stderr or result.stdout}"
        return [v for entry in json.loads(output) for v in entry["violations"]], None

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
        """Replace table references while handling backticks."""
        pattern = r"(`?[a-zA-Z0-9_-]+`?)\.(`?[a-zA-Z0-9_-]+`?)\.(`?[a-zA-Z0-9_-]+`?)"
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


def validation_steps(
    yml_validator: YmlValidator,
    sql_validator: SqlValidator,
    dbt_core: bool,
    dbt_check: bool,
) -> List[Tuple[Callable[[], Tuple[bool, str]], str]]:
    """The validation steps, in order.

    With `dbt_check`, the project's `dbt check` checks replace dbtwiz's own name,
    primary key and config rules, and the model is checked with `dbt check`. The
    linter follows the engine, and the steps that need BigQuery or edit files always
    run.
    """
    steps = [
        (yml_validator.validate_yml_exists, "Validating yml exists"),
        (
            lambda: yml_validator.validate_yml_definition(checked_by_dbt=dbt_check),
            "Validating yml definition",
        ),
    ]
    if not dbt_check:
        steps.append(
            (yml_validator.validate_model_config_rules, "Validating model config rules")
        )
    steps += [
        (yml_validator.validate_yml_columns, "Validating yml columns"),
        (sql_validator.convert_sql_to_model, "Validating sql references"),
        (sql_validator.sqlfmt_format_file, "Validating sql with sqlfmt"),
        (
            (
                sql_validator.sqlfluff_validate_and_fix_file,
                "Validating sql with sqlfluff",
            )
            if dbt_core
            else (
                sql_validator.dbt_lint_validate_and_fix_file,
                "Validating sql with dbt lint",
            )
        ),
    ]
    if dbt_check:
        steps.append((yml_validator.validate_dbt_checks, "Validating with dbt check"))
    return steps


class ModelValidator:
    """Main validator class that orchestrates validation of dbt models."""

    def __init__(self, model_path: Union[str, Path]) -> None:
        """Initialize the model validator with the given model path.

        Args:
            model_path: Path to the model file to validate
        """
        self.model_base = ModelBasePath(path=model_path)

    # ============================================================================
    # PUBLIC METHODS
    # ============================================================================

    def validate(self) -> bool:
        """Run complete model validation"""
        # Initialize validators
        yml_validator = YmlValidator(model_path=self.model_base.path)
        sql_validator = SqlValidator(model_path=self.model_base.path)

        # `dbt check` exists only on dbt v2, so dbt-core always uses dbtwiz's own rules
        dbt_core = dbt_core_installed()
        # `is True`: pyproject values are set after pydantic validation, so a quoted
        # "false" would otherwise count as on
        dbt_check = not dbt_core and project_config().validate_with_dbt_check is True

        # Run validation
        for func, desc in validation_steps(
            yml_validator, sql_validator, dbt_core=dbt_core, dbt_check=dbt_check
        ):
            status(message=desc)
            success, result = func()
            status(
                message=desc, status_text=result, style="green" if success else "red"
            )
