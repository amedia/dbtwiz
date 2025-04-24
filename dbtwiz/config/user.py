import configparser
import functools
from pathlib import Path
from typing import Any, Dict

import typer

from dbtwiz.helpers.logger import error, info


@functools.cache
def user_config():
    """Read and cache settings from user configuration"""
    return UserConfig()


def user_config_path(target: str = "") -> Path:
    """Get Path to the given target within the user configuration directory"""
    return user_config().CONFIG_PATH / target


def dark_mode() -> bool:
    """Are we using a dark theme?"""
    return user_config().get("general", "theme") == "dark"


class InvalidConfig(ValueError):
    """Class for invalid config."""

    pass


class UserConfig:
    """User-specific configuration"""

    CONFIG_PATH = Path(typer.get_app_dir("dbtwiz"))
    CONFIG_FILE = CONFIG_PATH / "config.ini"

    SECTIONS = ["general", "model_info", "theme"]

    THEMES: Dict[str, Any] = dict(
        names=["light", "dark"],
        colors=dict(
            name=[30, 115],
            path=[27, 147],
            tags=[28, 106],
            group=[94, 178],
            materialized=[54, 212],
            owner=[136, 208],
            policy=[136, 208],
            dep_stg=[34, 118],
            dep_int=[24, 123],
            dep_mart=[20, 75],
            description=[102, 144],
            deprecated=[124, 196],
        ),
    )

    SECTIONS = [
        {
            "name": "general",
            "description": "General settings",
            "settings": [
                ("auth_check", bool, True, ""),
                ("editor", str, "code", "Command used to open a file for editing"),
                ("theme", str, "light", "Colour theme for model information"),
            ],
        },
        {
            "name": "model_info",
            "description": "Settings relating to model information output",
            "settings": [
                ("formatter", str, "fmt -s", "Command used to format model information text"),
            ],
        },
        {
            "name": "theme",
            "description": "ANSI colour codes for highlighting",
            "settings": [
                ("name", int, 30, "Model name"),
                ("path", int, 27, "Model file path"),
                ("tags", int, 28, "Model tag list"),
                ("group", int, 94, "Model group list"),
                ("materialized", int, 54, "Model materialization type"),
                ("owner", int, 136, "Model owner"),
                ("policy", int, 136, "Model policies"),
                ("dep_stg", int, 34, "Model dependencies (staging)"),
                ("dep_int", int, 24, "Model dependencies (intermediate)"),
                ("dep_mart", int, 20, "Model dependencies (marts)"),
                ("description", int, 102, "Model description"),
                ("deprecated", int, 124, "Model description for deprecated models"),
            ]
        },
    ]


    def __init__(self):
        """Get the configuration, from file if it exists or create a new one with defaults"""
        self.parser = configparser.ConfigParser()
        if Path.exists(self.CONFIG_FILE):
            self.parser.read(self.CONFIG_FILE)
            return
        self._set_defaults()

    def get(self, section, key) -> str:
        """Retrieve a string value from the specified section and key."""
        return self.parser.get(section, key)

    def getint(self, section, key) -> int:
        """Retrieve an integer value from the specified section and key."""
        return self.parser.getint(section, key)

    def getboolean(self, section, key) -> int:
        """Retrieve a boolean value from the specified section and key."""
        return self.parser.getboolean(section, key)

    def color(self, key) -> int:
        """Get the color value of a named key in the current theme"""
        return self.parser.getint("theme", key)

    def update(self, section, key, value) -> None:
        """Update a setting and write changes to configuration file"""
        try:
            if not self.parser.has_section(section):
                raise InvalidConfig(f"Unknown configuration section: {section}")
            if not self.parser.has_option(section, key):
                raise InvalidConfig(f"Unknown configuration setting: {section}:{key}")

            if section == "general" and key == "theme":
                if value not in self.THEMES["names"]:
                    raise InvalidConfig(
                        f"Invalid theme: {value} - must be one of {self.THEMES['names']}"
                    )
                self._set_theme(theme=value)
            else:
                self.parser.set(section, key, value)
            self._write_to_file()
            info(f"Configuration setting {section}:{key} updated.")
        except InvalidConfig as ex:
            error(str(ex))

    def _write_to_file(self) -> None:
        """Write the current parser configuration to a file."""
        Path.mkdir(self.CONFIG_PATH, exist_ok=True)
        with open(self.CONFIG_FILE, "w+") as f:
            self.parser.write(f)

    def _set_defaults(self) -> None:
        """Initialize the parser with default settings and sections."""
        for section in self.SECTIONS:
            self.parser.add_section(section)
        self.parser.set("general", "auth_check", "yes")
        self.parser.set("model_info", "formatter", "fmt -s")
        self._set_theme(self.THEMES["names"][0])

    def _set_theme(self, theme) -> None:
        """Apply the specified theme to the parser configuration."""
        self.parser.set("general", "theme", theme)
        theme_index = self.THEMES["names"].index(theme)
        for key, value in self.THEMES["colors"].items():
            self.parser.set("theme", key, str(value[theme_index]))
