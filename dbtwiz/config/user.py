import functools
import inspect
from pathlib import Path
import platform
import tomllib
from typing import Any, Dict
import typer


@functools.cache
def user_config():
    """Read and cache settings from user configuration"""
    return UserConfig()


def user_config_path(target: str = "") -> Path:
    """Get Path to the given target within the user configuration directory"""
    return user_config().config_path() / target


class UserConfig:
    """User-specific settings from config.toml"""

    SETTINGS = [
        {
            "key": "auth_check",
            "default": True,
            "help": """
            When true, check for existing GCP auth token, and ask for
            automatic reauthentication if needed.
            """,
        },
        {
            "key": "editor",
            "default": "code",
            "help": """
            Command for opening model source files in editor.
            """,
        },
        {
            "key": "log_debug",
            "default": False,
            "help": """
            Enable debug logging of some internal dbtwiz operations. You won't
            need this unless you're working on or helping troubleshoot dbtwiz.
            """,
        },
        {
            "key": "model_formatter",
            "default": "fmt -s",
            "default_mac": "cat -s",
            "default_win": "powershell cat",
            "help": """
            Command for showing prerendered model info files in the interactive
            fzf-based selector. The default should work in GitHub Codespaces,
            on Linux or on MacOS with GNU coreutils installed.
            Here are some alternatives:
             - Linux/Codespace or MacOS with GNU coreutils: "fmt -s"
             - MacOS without GNU coreutils: "cat -s"
             - Windows: "powershell cat"
            """,
        },
        {
            "key": "theme",
            "default": "light",
            "help": """
            Set to "light" to use a color scheme suitable for a light background,
            or to "dark" for better contrasts against a dark background.
            """,
        },
    ]


    def __init__(self) -> None:
        """Initialize the class by determining the root path and parsing the config."""
        self._parse_config()
        self._append_missing_defaults()


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
            self._config = {}
            return
        try:
            with open(self._config_file(), "rb") as f:
                self._config = tomllib.load(f)
        except Exception as ex:
            from dbtwiz.helpers.logger import fatal
            fatal(f"Failed to parse file {self._config_file()}: {ex}")


    def _toml_item(self, setting) -> str:
        """Format setting for inclusion in Toml"""
        key = setting["key"]
        if "default_win" in setting and platform.system() == "Windows":
            value = setting["default_win"]
        elif "default_mac" in setting and platform.system() == "Darwin":
            value = setting["default_mac"]
        else:
            value = setting["default"]
        lines = [f"# {row}" for row in inspect.cleandoc(setting["help"]).splitlines()]
        if isinstance(value, str):
            lines.append(f"{key} = \"{value}\"")
        elif isinstance(value, bool):
            lines.append(f"{key} = {str(value).lower()}")
        else:
            lines.append(f"{key} = {value}")
        return "\n".join(lines)


    def _append_missing_defaults(self):
        """Add missing defaults to config file and parse again"""
        self.config_path().mkdir(parents=True, exist_ok=True)
        with open(self._config_file(), "a") as f:
            for setting in UserConfig.SETTINGS:
                key = setting["key"]
                if key not in self._config:
                    f.write(self._toml_item(setting) + "\n\n")
        self._parse_config()


    def __getattr__(self, name):
        """Dynamically handle attribute access and warn if the setting is missing."""
        from dbtwiz.helpers.logger import fatal
        if name in self._config:
            value = self._config[name]
            if value is not False and (not value or value == ""):
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
