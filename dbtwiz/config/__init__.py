"""Configuration management for dbtwiz."""

from .loader import validate_configs
from .project import (
    ProjectConfig,
    load_project_config,
    project_config,
    project_dbtwiz_path,
    project_path,
)
from .user import UserConfig, load_user_config, user_config, user_config_path

__all__ = [
    # User configuration
    "UserConfig",
    "user_config",
    "user_config_path",
    "load_user_config",
    # Project configuration
    "ProjectConfig",
    "project_config",
    "project_path",
    "project_dbtwiz_path",
    "load_project_config",
    # Configuration loader
    "validate_configs",
]
