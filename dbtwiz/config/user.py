import functools
from pathlib import Path
import tomllib
from typing import Any, Dict
import typer

from dbtwiz.helpers.logger import error, fatal, info


@functools.cache
def user_config():
    """Read and cache settings from user configuration"""
    return UserConfig()


def user_config_path(target: str = "") -> Path:
    """Get Path to the given target within the user configuration directory"""
    return user_config().config_path() / target


class UserConfig:
    """User-specific settings from config.toml"""

    DEFAULTS = {
        "auth_check": True,
        "editor": "code",
        "model_formatter": "fmt -s",
        "theme": "light",
    }


    def __init__(self) -> None:
        """Initialize the class by determining the root path and parsing the config."""
        self._parse_config()


    def config_path(self) -> Path:
        """Path to user configuration directory"""
        return Path(typer.get_app_dir("dbtwiz"))


    def _config_file(self) -> Path:
        """Path to user configuration directory"""
        return self.config_path() / "config.toml"


    def _parse_config(self):
        """Parse the config file"""
        config_file = self._config_file()
        if not config_file.exists():
            self._make_default_config()
        try:
            with open(self._config_file(), "rb") as f:
                self._config = tomllib.load(f)
        except Exception as ex:
            fatal(f"Failed to parse file {self._config_file()}: {ex}")


    def _make_default_config(self):
        """Generate default configuration"""
        defaults = ""
        for key, value in UserConfig.DEFAULTS.items():
            defaults += f"{key} = "
            if isinstance(value, str):
                defaults += '"' + value + '"\n'
            elif isinstance(value, bool):
                defaults += str(value).lower() + "\n"
            else:
                defaults += str(value) + "\n"
        with open(self._config_file(), "w") as f:
            f.write(defaults)


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
