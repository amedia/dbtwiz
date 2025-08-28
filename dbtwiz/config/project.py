import functools
import tomllib
from pathlib import Path
from typing import Any, List

from ..utils.logger import fatal, warn


@functools.cache
def project_config():
    """Read and cache settings from project configuration"""
    return ProjectConfig()


def project_path(target: str = "") -> Path:
    """Get Path to the given target relative to the project root directory"""
    return project_config().root_path() / target


def project_dbtwiz_path(target: str = "") -> Path:
    """Get Path to the given target relative to the project .dbtwiz directory"""
    dot_path = project_config().root_path() / ".dbtwiz"
    Path.mkdir(dot_path, exist_ok=True)
    return project_config().root_path() / ".dbtwiz" / target


class ProjectConfig:
    """Project-specific settings from pyproject.toml"""

    def __init__(self) -> None:
        """Initialize the class by determining the root path and parsing the config."""
        self._config = {}
        self._determine_root_path()
        self._parse_config()

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
