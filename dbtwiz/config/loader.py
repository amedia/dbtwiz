"""Configuration loader with Pydantic validation and error handling."""

import tomllib
from pathlib import Path
from typing import Optional

from pydantic import ValidationError

from ..utils.logger import fatal, warn
from .project import ProjectConfig
from .user import UserConfig


def load_user_config(config_path: Optional[Path] = None) -> UserConfig:
    """Load and validate user configuration.

    Args:
        config_path: Optional path to config file. If None, uses default location.

    Returns:
        Validated UserConfig instance

    Raises:
        SystemExit: If configuration is invalid or cannot be loaded
    """
    if config_path is None:
        config_path = Path.home() / ".config" / "dbtwiz" / "config.toml"

    if not config_path.exists():
        # Create default config
        config_path.parent.mkdir(parents=True, exist_ok=True)
        return UserConfig()

    try:
        with open(config_path, "rb") as f:
            config_data = tomllib.load(f)

        return UserConfig(**config_data)
    except tomllib.TOMLDecodeError as e:
        fatal(f"Invalid TOML in user config {config_path}: {e}")
    except ValidationError as e:
        fatal(f"Invalid user configuration in {config_path}:\n{e}")
    except Exception as e:
        fatal(f"Failed to load user config from {config_path}: {e}")


def load_project_config(project_path: Optional[Path] = None) -> ProjectConfig:
    """Load and validate project configuration.

    Args:
        project_path: Optional path to project root. If None, searches upward from cwd.

    Returns:
        Validated ProjectConfig instance

    Raises:
        SystemExit: If configuration is invalid or cannot be loaded
    """
    if project_path is None:
        # Search upward for pyproject.toml
        project_path = _find_project_root()

    pyproject_path = project_path / "pyproject.toml"
    if not pyproject_path.exists():
        fatal(f"No pyproject.toml found in {project_path}")

    try:
        with open(pyproject_path, "rb") as f:
            pyproject_data = tomllib.load(f)

        # Extract dbtwiz section
        dbtwiz_config = pyproject_data.get("tool", {}).get("dbtwiz", {})
        project_config_data = dbtwiz_config.get("project", {})

        # Create config with project root path
        config = ProjectConfig(**project_config_data)
        config.root = project_path

        return config
    except tomllib.TOMLDecodeError as e:
        fatal(f"Invalid TOML in pyproject.toml {pyproject_path}: {e}")
    except ValidationError as e:
        fatal(f"Invalid project configuration in {pyproject_path}:\n{e}")
    except Exception as e:
        fatal(f"Failed to load project config from {pyproject_path}: {e}")


def _find_project_root() -> Path:
    """Search upward from current directory to find project root with pyproject.toml.

    Returns:
        Path to project root directory

    Raises:
        SystemExit: If no pyproject.toml is found
    """
    path_list = [Path.cwd()] + list(Path.cwd().parents)
    for path in path_list:
        if (path / "pyproject.toml").exists():
            return path

    fatal("No pyproject.toml file found in current or upstream directories.")


def validate_configs() -> None:
    """Validate all configuration files and report any issues.

    This function can be called during startup to catch configuration
    errors early and provide helpful error messages.
    """
    try:
        # Try to load user config
        user_config = load_user_config()
        print("✓ User configuration loaded successfully")

        # Try to load project config
        project_config = load_project_config()
        print("✓ Project configuration loaded successfully")

        # Validate specific required fields
        _validate_required_fields(user_config, project_config)

    except Exception as e:
        fatal(f"Configuration validation failed: {e}")


def _validate_required_fields(
    user_config: UserConfig, project_config: ProjectConfig
) -> None:
    """Validate that required configuration fields are present and valid.

    Args:
        user_config: Loaded user configuration
        project_config: Loaded project configuration
    """
    # Check for critical missing project config
    critical_fields = [
        "user_project",
        "service_account_identifier",
        "service_account_project",
    ]

    missing_fields = []
    for field in critical_fields:
        if not getattr(project_config, field):
            missing_fields.append(field)

    if missing_fields:
        warn(
            f"Missing critical project configuration fields: {', '.join(missing_fields)}"
        )
        warn("Some dbtwiz features may not work correctly without these fields")
