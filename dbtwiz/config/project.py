import functools
import tomllib
from pathlib import Path
from typing import Any, List, Optional

from pydantic import BaseModel, Field, field_validator

from ..utils.logger import fatal, warn


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
    # Search upward for pyproject.toml
    path_list = [Path.cwd()] + list(Path.cwd().parents)
    project_root = None

    for path in path_list:
        if (path / "pyproject.toml").exists():
            project_root = path
            break

    if not project_root:
        fatal("No pyproject.toml file found in current or upstream directories.")

    pyproject_path = project_root / "pyproject.toml"
    try:
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)
            project_config_data = (
                config.get("tool", {}).get("dbtwiz", {}).get("project", {})
            )

        # Create config with project root path
        config_obj = ProjectConfig(**project_config_data)
        config_obj.root = project_root
        return config_obj

    except Exception as ex:
        fatal(f"Failed to parse file {pyproject_path}: {ex}")


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

    # Internal fields (not from config file)
    root: Optional[Path] = Field(
        None, description="Project root path (set internally)", exclude=True
    )

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

    # ============================================================================
    # PRIVATE METHODS - Internal Helper Functions
    # ============================================================================

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
                self._config = (
                    config.get("tool", {}).get("dbtwiz", {}).get("project", {})
                )
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
        if name in self._config:
            value = self._config[name]
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
        return list(self._config.keys()) + list(super().__dir__())
