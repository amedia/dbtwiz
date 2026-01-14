import os
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

from ..config.project import project_path
from .model import ModelBasePath


class Group:
    """Project's model groups configuration."""

    # ============================================================================
    # CLASS CONSTANTS
    # ============================================================================
    YAML_PATH = project_path() / "models" / "model_groups.yml"

    def __init__(self) -> None:
        """Initialize the class and read the model groups."""
        from yaml import safe_load  # Lazy import for improved performance

        with open(self.YAML_PATH, "r", encoding="utf-8") as f:
            data = safe_load(f)
        self.groups: List[Dict[str, Any]] = data["groups"]

    # ============================================================================
    # PUBLIC METHODS
    # ============================================================================

    def choices(self) -> Dict[str, str]:
        """Return a dict with group names and descriptions.

        Returns:
            Dictionary mapping group names to their descriptions
        """
        return dict(
            [
                (g["name"], g.get("config", {}).get("meta", {}).get("description", ""))
                for g in self.groups
            ]
        )

    def yaml_relative_path(self) -> Path:
        """Relative path to YAML file for model groups.

        Returns:
            Path object relative to the project root
        """
        return self.YAML_PATH.relative_to(project_path())


class Profile:
    """Project's profiles configuration."""

    def __init__(self, project_root: Path = None):
        """Initialize the class and read the profiles.

        Args:
            project_root: Optional project root path. If not provided, will be determined automatically.
        """
        from yaml import safe_load  # Lazy import for improved performance

        # Determine project root if not provided
        if project_root is None:
            project_root = project_path()

        # Try multiple possible profiles locations
        profiles_dirs = [
            Path(os.environ.get("DBT_PROFILES_DIR"))
            if os.environ.get("DBT_PROFILES_DIR")
            else None,
            project_root / ".profiles",
            Path.home() / ".dbt",
        ]

        self.YAML_PATH = None
        for profiles_dir in profiles_dirs:
            if profiles_dir and (profiles_dir / "profiles.yml").exists():
                self.YAML_PATH = profiles_dir / "profiles.yml"
                break

        if not self.YAML_PATH:
            raise FileNotFoundError(
                "Couldn't find profiles.yml. Checked: "
                + ", ".join(str(d) for d in profiles_dirs if d)
                + ". Is the DBT_PROFILES_DIR env var set?"
            )

        with open(self.YAML_PATH, "r", encoding="utf-8") as f:
            # Get the profile name from the project config
            project_instance = Project(project_root)
            profile_name = project_instance.profile()
            self.profiles: Dict[str, Any] = safe_load(f)[profile_name]["outputs"]

    # ============================================================================
    # PUBLIC METHODS
    # ============================================================================

    def profile_config(self, target_name: str) -> Dict[str, Any]:
        """Return a dict with the profile configuration.

        Args:
            target_name: Name of the target profile to retrieve

        Returns:
            Dictionary containing the resolved profile configuration
        """
        return self._resolve_profile(raw_config=self.profiles[target_name])

    # ============================================================================
    # PRIVATE METHODS - Internal Helper Functions
    # ============================================================================

    def _resolve_profile(self, raw_config: Dict[str, Any]) -> Dict[str, Any]:
        """Get fully rendered profile values including env_var resolution.

        Args:
            raw_config: Raw profile configuration dictionary

        Returns:
            Dictionary with resolved profile values
        """
        from jinja2 import (  # Lazy import for improved performance
            BaseLoader,
            Environment,
        )

        # Create custom renderer
        def render_value(value: Any) -> Any:
            if not isinstance(value, str):
                return value

            template = Environment(loader=BaseLoader(), autoescape=True).from_string(
                value
            )
            return template.render(env_var=os.getenv)

        # Return rendered values
        return {k: render_value(v) for k, v in raw_config.items()}


class Project:
    """Project's variable settings and configuration management."""

    def __init__(self, project_root: Path = None):
        """Initialize the project configuration.

        Args:
            project_root: Optional project root path. If not provided, will be determined automatically.
        """
        from yaml import safe_load  # Lazy import for improved performance

        # Determine project root if not provided
        if project_root is None:
            project_root = project_path()

        self.YAML_PATH = project_root / "dbt_project.yml"

        with open(self.YAML_PATH, "r", encoding="utf-8") as f:
            self.data: Dict[str, Any] = safe_load(f)

    # ============================================================================
    # PUBLIC METHODS - Core Configuration
    # ============================================================================

    def name(self) -> str:
        """Get the project name."""
        return self.data.get("name")

    def profile(self) -> str:
        """Get the project profile."""
        return self.data.get("profile")

    # ============================================================================
    # PUBLIC METHODS - Team and Access Management
    # ============================================================================

    def teams(self) -> List[Dict[str, str]]:
        """List of teams defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("vars", {}).get("teams", {}).items()
        ]

    def service_consumers(self) -> List[Dict[str, str]]:
        """List of service consumers defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("vars", {})
            .get("service-consumers", {})
            .items()
        ]

    def access_policies(self) -> List[Dict[str, str]]:
        """List of access policies defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("vars", {})
            .get("access-policies", {})
            .items()
        ]

    def data_expirations(self) -> List[Dict[str, str]]:
        """List of data expiration policies"""
        return [
            {
                "name": key,
                "description": f"Used for {key.replace('-', ' ').replace(' expiration', '')} ({value} days)",
            }
            for key, value in self.data.get("vars", {}).items()
            if key.endswith("-data-expiration")
        ]


def layer_choices() -> List[Dict[str, str]]:
    """Get available dbt layers with descriptions.

    Returns:
        List of dictionaries containing layer names and descriptions
    """
    return [
        {
            "name": "staging",
            "description": "Initial building blocks mapping the source data",
        },
        {
            "name": "intermediate",
            "description": "Logic to prepare data to be joined into products at later stages",
        },
        {
            "name": "marts",
            "description": "Data products made available to several consumers",
        },
        {
            "name": "bespoke",
            "description": "Data products tailored to one specific consumer",
        },
    ]


def materialization_choices() -> List[Dict[str, str]]:
    """Get available dbt materializations with descriptions.

    Returns:
        List of dictionaries containing materialization names and descriptions
    """
    return [
        {"name": "view", "description": "Default"},
        {"name": "table", "description": "Typically used for smaller mart models"},
        {"name": "incremental", "description": "Used for large models"},
        {
            "name": "scd2",
            "description": "Used for slowly changing dimensions built from a partitioned table",
        },
        {
            "name": "ephemeral",
            "description": "Should very rarely be used, only for logically splitting up the code",
        },
    ]


def access_choices() -> List[Dict[str, str]]:
    """Get available access levels with descriptions.

    Returns:
        List of dictionaries containing access level names and descriptions
    """
    return [
        {
            "name": "private",
            "description": "Usable only by other models in the same group",
        },
        {"name": "protected", "description": "Usable by models outside the group"},
        {"name": "public", "description": "For marts models"},
    ]


def frequency_choices() -> List[Dict[str, str]]:
    """Get available update frequencies with descriptions.

    Returns:
        List of dictionaries containing frequency names and descriptions
    """
    return [
        {"name": "hourly", "description": "Model needs to be updated every hour"},
        {"name": "daily", "description": "Model needs to be updated once a day"},
    ]


def get_source_tables() -> Tuple[
    Dict[str, str], List[Dict[str, Union[str, List[str]]]]
]:
    """Read all existing sources from YAML files in the sources directory.

    Returns: A tuple containing:
            - dict_part (Dict[str, str]): A dictionary of source tables (source_name.table_name: description).
            - list_part (List[Dict[str, Union[str, List[str]]]]): A list of sources (source_name, database, schema, table_names, file).
    """
    from yaml import safe_load  # Lazy import for improved performance

    dbt_source_tables = {}
    dbt_sources = []
    for yml_file in Path("sources").rglob("*"):
        if yml_file.suffix in {".yml", ".yaml"}:
            with open(yml_file, "r", encoding="utf-8") as f:
                content = safe_load(f)
                sources = content.get("sources", [])
                for source in sources:
                    # Add all tables to dict
                    for table in source.get("tables", []):
                        dbt_source_tables[f"{source['name']}.{table['name']}"] = (
                            table.get("description")
                        )
                    # All all sources to list
                    dbt_sources.append(
                        {
                            "name": source["name"],
                            "description": source.get("description"),
                            "project": source["database"],
                            "dataset": source["schema"],
                            "tables": [
                                table["name"] for table in source.get("tables", [])
                            ],
                            "file": yml_file,
                        }
                    )

    dbt_source_tables = dict(sorted(dbt_source_tables.items()))
    dbt_sources = sorted(dbt_sources, key=lambda x: x["name"])

    return dbt_source_tables, dbt_sources


def domains_for_layer(layer: str) -> List[str]:
    """Get list of domains in the given layer for this project.

    Args:
        layer: Layer name to get domains for

    Returns:
        Sorted list of domain names
    """
    domains = [f.name for f in (project_path() / "models").glob(f"*_{layer}/*")]
    return sorted(domains)


def list_domain_models(base_path: ModelBasePath, domain: str) -> List[str]:
    """List existing models in the given layer/domain for this project.

    Args:
        base_path: ModelBasePath instance for the layer
        domain: Domain name to list models for

    Returns:
        List of model names (without .yml extension)
    """
    yml_files = []
    for path in base_path.get_domain_path(domain).rglob("*.yml"):
        yml_files.append(str(path.stem))

    return yml_files
