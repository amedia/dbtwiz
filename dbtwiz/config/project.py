import functools
import tomllib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator
from rich.markup import escape

from ..utils.logger import fatal, warn

# Shown alongside any complaint about a malformed model config rule, so the error itself
# documents the shape a rule is supposed to have.
MODEL_CONFIG_RULE_EXAMPLE = [
    "Each rule declares optional conditions and at least one assertion, e.g.:",
    "  [[tool.dbtwiz.project.model_config_rules]]",
    '  when    = { "<config path>" = <value the path must equal> }',
    '  require = ["<config path that must be set>"]',
    '  max     = { "<config path>" = "{{ var(\'<a project variable>\') }}" }',
    '  message = "<why this rule exists>"',
]


def _is_path_table(value: Any) -> bool:
    """A `when` table maps config paths to the values they must equal."""
    return isinstance(value, dict) and all(isinstance(path, str) for path in value)


def _is_path_list(value: Any) -> bool:
    """A `require` list holds config paths."""
    return isinstance(value, list) and all(isinstance(path, str) for path in value)


def _is_value_table(value: Any) -> bool:
    """A `forbid` table maps config paths to lists of disallowed values."""
    return _is_path_table(value) and all(
        isinstance(values, list) for values in value.values()
    )


def _is_bound_table(value: Any) -> bool:
    """A `max`/`min` table maps config paths to scalar bounds."""
    return _is_path_table(value) and not any(
        isinstance(bound, (list, dict)) for bound in value.values()
    )


@functools.cache
def project_config():
    """Read and cache settings from project configuration"""
    return load_project_config()


def project_path(target: str = "") -> Path:
    """Get Path to the given target relative to the project root directory"""
    return project_config().root_path() / target


def project_dbtwiz_path(target: str = "") -> Path:
    """Get Path to the given target relative to the project .dbtwiz directory"""
    dot_path = project_config().root_path() / ".dbtwiz"
    Path.mkdir(dot_path, exist_ok=True)
    return project_config().root_path() / ".dbtwiz" / target


def load_project_config() -> "ProjectConfig":
    """Load project configuration from pyproject.toml"""
    # Create config - ProjectConfig will automatically determine root path and parse config
    config_obj = ProjectConfig()
    return config_obj


class ProjectConfig(BaseModel):
    """Project-specific settings from pyproject.toml"""

    # Backfill settings
    backfill_default_batch_size: Optional[int] = Field(
        30, ge=1, le=365, description="Default batch size for backfills"
    )

    # Docker settings
    docker_image_url_dbt: Optional[str] = Field(
        None, description="Docker image URL for dbt operations"
    )
    docker_image_manifest_path: Optional[str] = Field(
        None, description="Path to manifest in Docker image"
    )
    docker_image_profiles_path: Optional[str] = Field(
        None, description="Path to profiles in Docker image"
    )

    # Service account settings
    service_account_identifier: Optional[str] = Field(
        None, description="Service account identifier for GCP operations"
    )
    service_account_project: Optional[str] = Field(
        None, description="Service account project for GCP operations"
    )
    service_account_region: Optional[str] = Field(
        None, description="Service account region for GCP operations"
    )

    # Project settings
    user_project: Optional[str] = Field(
        None, description="User project for GCP operations"
    )

    # Storage settings
    bucket_state_project: Optional[str] = Field(
        None, description="Project containing the state bucket"
    )
    bucket_state_identifier: Optional[str] = Field(
        None, description="Bucket identifier for state storage"
    )

    # Model settings
    default_materialization: Optional[str] = Field(
        "table", description="Default model materialization"
    )
    default_partition_expiration_days: Optional[int] = Field(
        365, ge=1, description="Default partition expiration in days"
    )

    # Team settings
    teams: List[str] = Field(
        default_factory=list, description="Available teams for model ownership"
    )
    access_policies: List[str] = Field(
        default_factory=list, description="Available access policies"
    )
    service_consumers: List[str] = Field(
        default_factory=list, description="Available service consumers"
    )

    # Grants settings
    grants_skip_schemas: List[str] = Field(
        default_factory=list,
        description="Schemas to skip when validating and applying grants",
    )
    grants_open_access_group: Optional[str] = Field(
        None,
        description="Group principal auto-granted viewer access on models with access: protected or public",
    )
    grants_role: Optional[str] = Field(
        None,
        description="IAM role to manage (defaults to roles/bigquery.dataViewer)",
    )

    # Source settings
    source_reader_service_accounts: Dict[str, str] = Field(
        default_factory=dict,
        description="Service account emails mapped to a description of their purpose, all of which must have read access to source tables",
    )
    source_reader_unchecked_projects: List[str] = Field(
        default_factory=list,
        description="GCP project IDs where SA read access checks are skipped (e.g. because access is already granted at project level)",
    )

    # Layer layout — each repo must declare its own. Map each logical layer
    # name to its folder under models/, the abbreviation used in model name
    # prefixes (<abbr>_<domain>__<name>), and an optional description shown
    # in the interactive `dbtwiz model create` prompt.
    layers: Optional[Dict[str, Dict[str, str]]] = Field(
        None,
        description="Mapping of layer name to {folder, abbreviation, description?}",
    )

    # Model config rules — generic assertions on a model's own yml config block, checked by
    # `dbtwiz model validate`. dbtwiz has no built-in notion of what any rule means: a
    # project declares conditions (`when`) and assertions (`require`/`forbid`/`max`/`min`)
    # over dotted config paths, so domain naming stays in the consuming project. Defaults to
    # empty, making the check a no-op for a project that declares no rules.
    model_config_rules: List[Dict[str, Any]] = Field(
        default_factory=list,
        description=(
            "Assertions on a model's config block. Each entry: when (optional dict of "
            "dotted config path to expected value; the rule applies when all match), "
            "require (optional list of paths that must be set), forbid (optional dict of "
            "path to a list of disallowed values), max/min (optional dict of path to a "
            "numeric bound, resolving {{ var('...') }} references), message (optional "
            "explanation shown when the rule fails)"
        ),
    )

    # Expiration variables offered when creating an incremental model. Each project names
    # its own retention variables, so the list is declared rather than guessed from
    # variable names; the number of days behind each comes from the project's variables.
    expiration_vars: List[Dict[str, str]] = Field(
        default_factory=list,
        description=(
            "Project variables offered as data expiration policies, in the order they "
            "should be listed. Each entry: var (required - the variable name) and "
            "description (optional - shown in the interactive prompt)"
        ),
    )

    # Internal fields (not from config file)
    root: Optional[Path] = Field(
        None, description="Project root path (set internally)", exclude=True
    )
    config: Optional[dict] = Field(
        None, description="Parsed configuration data", exclude=True
    )

    def __init__(self, **data):
        """Initialize the ProjectConfig with proper setup."""
        super().__init__(**data)
        # If root is not provided, determine it automatically
        if self.root is None:
            self._determine_root_path()
        # Parse the configuration file
        self._parse_config()

    @field_validator("default_materialization")
    @classmethod
    def validate_materialization(cls, v):
        """Validate materialization value"""
        if v is not None:
            valid_materializations = ["table", "view", "incremental", "ephemeral"]
            if v not in valid_materializations:
                raise ValueError(
                    f"materialization must be one of {valid_materializations}"
                )
        return v

    @field_validator("backfill_default_batch_size")
    @classmethod
    def validate_batch_size(cls, v):
        """Validate batch size value"""
        if v is not None and (v < 1 or v > 365):
            raise ValueError("batch_size must be between 1 and 365")
        return v

    # ============================================================================
    # PUBLIC METHODS
    # ============================================================================

    def root_path(self) -> Path:
        """Return the root path of the project.

        Returns:
            Path object pointing to the project root directory
        """
        return self.root

    def layer_entries(self) -> Dict[str, Dict[str, str]]:
        """Return the resolved per-layer config, in declared order.

        Raises a fatal error if `[tool.dbtwiz.project.layers]` is missing or
        any entry lacks `folder` / `abbreviation`.
        """
        if not self.layers:
            # Escaped: log messages are rendered as rich markup, which would otherwise
            # swallow the bracketed config section names this message is all about.
            fatal(
                escape(
                    "Missing [tool.dbtwiz.project.layers] in pyproject.toml. Declare the "
                    "layers this project uses, mapping each to its folder under models/ "
                    "and the abbreviation used as the model name prefix "
                    "(<abbreviation>_<domain>__<name>), plus an optional description "
                    "shown when creating a model:\n"
                    "  [tool.dbtwiz.project.layers]\n"
                    '  <layer> = { folder = "<folder under models/>", '
                    'abbreviation = "<prefix>", description = "<shown in prompts>" }'
                )
            )
        for name, entry in self.layers.items():
            missing = [k for k in ("folder", "abbreviation") if k not in entry]
            if missing:
                fatal(
                    escape(
                        f"Layer '{name}' in [tool.dbtwiz.project.layers] is missing "
                        f"required field(s): {', '.join(missing)}"
                    )
                )
        return self.layers

    def layer_details(self) -> Dict[str, Tuple[str, str]]:
        """Resolve layer name → (folder, abbreviation) mapping."""
        return {
            name: (entry["folder"], entry["abbreviation"])
            for name, entry in self.layer_entries().items()
        }

    def model_config_rule_entries(self) -> List[Dict[str, Any]]:
        """Return the configured model config rules, validating their shape.

        An empty list is valid (no rules configured). Values read from pyproject.toml are
        assigned after pydantic validation, so the field's type annotation is not enforced
        for them - each rule is checked here instead, to fail with an actionable message
        rather than misbehave later on a malformed rule.
        """
        if not isinstance(self.model_config_rules, list):
            fatal(self._model_config_rule_error("must be a list of rule tables"))

        for index, rule in enumerate(self.model_config_rules):
            self._validate_model_config_rule(f"Rule #{index + 1}", rule)

        return self.model_config_rules

    def expiration_var_entries(self) -> List[Dict[str, str]]:
        """Return the configured expiration variables, validating their shape.

        An empty list is valid: a project that offers no expiration policies simply gets
        none. Values read from pyproject.toml are assigned after pydantic validation, so
        the field's type annotation is not enforced for them and each entry is checked
        here instead.
        """
        if not isinstance(self.expiration_vars, list):
            fatal(self._expiration_var_error("must be a list of tables"))

        for index, entry in enumerate(self.expiration_vars):
            label = f"Entry #{index + 1}"
            if not isinstance(entry, dict):
                fatal(self._expiration_var_error(f"{label} must be a table"))
            if unknown := [key for key in entry if key not in ("var", "description")]:
                fatal(
                    self._expiration_var_error(
                        f"{label} has unknown key(s) {', '.join(sorted(unknown))}. "
                        "Valid keys are var, description"
                    )
                )
            if not isinstance(entry.get("var"), str) or not entry["var"]:
                fatal(
                    self._expiration_var_error(
                        f"{label} needs 'var' set to a project variable name"
                    )
                )
            if not isinstance(entry.get("description", ""), str):
                fatal(
                    self._expiration_var_error(
                        f"{label} has a 'description' that is not text"
                    )
                )

        return self.expiration_vars

    # ============================================================================
    # PRIVATE METHODS - Internal Helper Functions
    # ============================================================================

    def _validate_model_config_rule(self, label: str, rule: Any) -> None:
        """Check that a single model config rule has a usable shape."""
        # Lazy import: the rule engine owns the rule vocabulary, and importing it at module
        # level would make dbtwiz.model and dbtwiz.config import each other.
        from ..model.config_rules import ASSERTION_KEYS, RULE_KEYS

        if not isinstance(rule, dict):
            fatal(self._model_config_rule_error(f"{label} must be a table"))

        if unknown := [key for key in rule if key not in RULE_KEYS]:
            fatal(
                self._model_config_rule_error(
                    f"{label} has unknown key(s) {', '.join(sorted(unknown))}. "
                    f"Valid keys are {', '.join(RULE_KEYS)}"
                )
            )
        if not any(key in rule for key in ASSERTION_KEYS):
            fatal(
                self._model_config_rule_error(
                    f"{label} asserts nothing - it needs at least one of "
                    f"{', '.join(ASSERTION_KEYS)}"
                )
            )

        for key, is_valid, expected in (
            ("when", _is_path_table, "a table of config path to expected value"),
            ("require", _is_path_list, "a list of config paths"),
            ("forbid", _is_value_table, "a table of config path to a list of values"),
            ("max", _is_bound_table, "a table of config path to a number or variable"),
            ("min", _is_bound_table, "a table of config path to a number or variable"),
            ("message", lambda value: isinstance(value, str), "text"),
        ):
            if key in rule and not is_valid(rule[key]):
                fatal(
                    self._model_config_rule_error(
                        f"{label} has a '{key}' that is not {expected}"
                    )
                )

    def _model_config_rule_error(self, problem: str) -> str:
        """Build a config error message for model_config_rules, with a worked example."""
        # Escaped: log messages are rendered as rich markup, which would otherwise
        # swallow the bracketed config section names this message is all about.
        return escape(
            "\n".join(
                [f"Invalid [[tool.dbtwiz.project.model_config_rules]]: {problem}."]
                + MODEL_CONFIG_RULE_EXAMPLE
            )
        )

    def _expiration_var_error(self, problem: str) -> str:
        """Build a config error message for expiration_vars, with a worked example."""
        # Escaped: log messages are rendered as rich markup, which would otherwise
        # swallow the bracketed config section name this message is all about.
        return escape(
            f"Invalid 'expiration_vars' in [tool.dbtwiz.project]: {problem}.\n"
            "Declare the project variables to offer as expiration policies, e.g.:\n"
            "  expiration_vars = [\n"
            '    { var = "<a project variable>", description = "<shown in prompts>" },\n'
            "  ]"
        )

    def _determine_root_path(self) -> None:
        """Search upward from current path to find project root.

        Raises:
            SystemExit: If no pyproject.toml file is found (via fatal function)
        """
        path_list = [Path.cwd()] + list(Path.cwd().parents)
        for path in path_list:
            if (path / "pyproject.toml").exists():
                self.root = path
                return
        fatal("No pyproject.toml file found in current or upstream directories.")

    def _parse_config(self) -> None:
        """Parse the 'pyproject.toml' file and store the configuration.

        Raises:
            SystemExit: If the file cannot be parsed (via fatal function)
        """
        project_file = self.root_path() / "pyproject.toml"
        try:
            with open(project_file, "rb") as f:
                config = tomllib.load(f)
                self.config = (
                    config.get("tool", {}).get("dbtwiz", {}).get("project", {})
                )

                # Update Pydantic fields with parsed values for backward compatibility
                # Only set fields that are actually defined in the Pydantic model
                model_fields = self.model_fields.keys()
                for key, value in self.config.items():
                    if key in model_fields:
                        setattr(self, key, value)

        except Exception as ex:
            fatal(f"Failed to parse file {project_file}: {ex}")

    # ============================================================================
    # SPECIAL METHODS
    # ============================================================================

    def __getattr__(self, name: str) -> Any:
        """Dynamically handle attribute access and warn if the setting is missing.

        Args:
            name: Name of the configuration attribute to access

        Returns:
            Configuration value or None if not found
        """
        # Skip Pydantic internal attributes and random strings
        if name.startswith("_") or not name.isidentifier():
            raise AttributeError(f"'{name}' object has no attribute '{name}'")

        # Only handle actual configuration keys
        if self.config and name in self.config:
            value = self.config[name]
            if value is not False and (not value or value == ""):
                warn(
                    f"'{name}' config is undefined in tool.dbtwiz.project config in pyproject.toml"
                )
            return value
        else:
            warn(
                f"'{name}' is missing from tool.dbtwiz.project config in pyproject.toml"
            )
            return None  # or raise AttributeError if you prefer

    def __dir__(self) -> List[str]:
        """Include dynamic attributes for autocompletion.

        Returns:
            List of available attribute names for autocompletion
        """
        return list(self.config.keys()) + list(super().__dir__())
