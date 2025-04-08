import functools
import tomllib
from pathlib import Path

from dbtwiz.helpers.log_types import fatal


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

    def root_path(self):
        """Return the root path of the project."""
        return self.root

    def _determine_root_path(self):
        """Search upward from current path to find project root"""
        path_list = [Path.cwd()] + list(Path.cwd().parents)
        for path in path_list:
            if (path / "pyproject.toml").exists():
                self.root = path
                return
        fatal("No pyproject.toml file found in current or upstream directories.")

    def _parse_config(self):
        """Parse the 'pyproject.toml' file and store the configuration."""
        project_file = self.root_path() / "pyproject.toml"
        try:
            with open(project_file, "rb") as f:
                config = tomllib.load(f)
                self._config = (
                    config.get("tool", {}).get("dbtwiz", {}).get("project", {})
                )
        except Exception as ex:
            fatal(f"Failed to parse file {project_file}: {ex}")

    def __getattr__(self, name):
        """Dynamically handle attribute access and warn if the setting is missing."""
        if name in self._config:
            value = self._config[name]
            if not value or value == "":
                fatal(
                    f"'{name}' config is undefined in tool.dbtwiz.project config in pyproject.toml"
                )
            return value
        else:
            fatal(
                f"'{name}' is missing from tool.dbtwiz.project config in pyproject.toml"
            )
            return None  # or raise AttributeError if you prefer

    def __dir__(self):
        """Include dynamic attributes for autocompletion."""
        return list(self._config.keys()) + list(super().__dir__())
