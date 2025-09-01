import functools
import inspect
import platform
import tomllib
from pathlib import Path
from typing import Any, ClassVar, Dict, List

import typer
from pydantic import BaseModel, Field, field_validator


@functools.cache
def user_config():
    """Read and cache settings from user configuration"""
    return load_user_config()


def user_config_path(target: str = "") -> Path:
    """Get Path to the given target within the user configuration directory"""
    return user_config().config_path() / target


def load_user_config() -> "UserConfig":
    """Load user configuration from file or create with defaults"""
    config_path = Path(typer.get_app_dir("dbtwiz")) / "config.toml"

    if config_path.exists():
        try:
            with open(config_path, "rb") as f:
                config_data = tomllib.load(f)
            return UserConfig(**config_data)
        except Exception:
            pass

    # Return default config
    return UserConfig()


class UserConfig(BaseModel):
    """User-specific settings from config.toml"""

    # Authentication settings
    auth_check: bool = Field(
        True,
        description="When true, check for existing GCP auth token, and ask for automatic reauthentication if needed.",
    )

    # Editor settings
    editor_command: str = Field(
        "code {}",
        description="Command for opening model source files in editor, with empty curly braces where the file path should be inserted. If curly braces are left out, the file name will be appended at the end. Some examples: - Visual Studio Code: 'code {}' - Emacs (with running server): 'emacsclient -n {}'",
    )

    # Logging settings
    log_debug: bool = Field(
        False,
        description="Enable debug logging of some internal dbtwiz operations. You won't need this unless you're working on or helping troubleshoot dbtwiz.",
    )

    # SQL formatter settings
    sql_formatter: str = Field(
        "fmt -s",
        description="Command for showing prerendered model info files in the interactive fzf-based selector. A sensible default is chosen based on the current platform.",
    )

    # UI settings
    theme: str = Field(
        "light",
        description="Set to 'light' to use a color scheme suitable for a light background, or to 'dark' for better contrasts against a dark background.",
    )

    # ============================================================================
    # CLASS CONSTANTS (kept for backward compatibility)
    # ============================================================================
    SETTINGS: ClassVar[List[Dict[str, Any]]] = [
        {
            "key": "auth_check",
            "default": True,
            "help": """
            When true, check for existing GCP auth token, and ask for
            automatic reauthentication if needed.
            """,
        },
        {
            "key": "editor_command",
            "default": "code {}",
            "help": """
            Command for opening model source files in editor, with empty
            curly braces where the file path should be inserted. If curly
            braces are left out, the file name will be appended at the end.
            Some examples:
            - Visual Studio Code: "code {}"
            - Emacs (with running server): "emacsclient -n {}"
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
            "key": "sql_formatter",
            "default": "fmt -s",
            "default_mac": "cat -s",
            "default_win": "powershell cat",
            "help": """
            Command for showing prerendered model info files in the interactive
            fzf-based selector. A sensible default is chosen based on the
            current platform.
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

    @field_validator("theme")
    @classmethod
    def validate_theme(cls, v):
        """Validate theme value"""
        valid_themes = ["light", "dark"]
        if v not in valid_themes:
            raise ValueError(f"theme must be one of {valid_themes}")
        return v

    @field_validator("sql_formatter", mode="before")
    @classmethod
    def set_platform_specific_formatter(cls, v):
        """Set platform-specific default formatter if not specified"""
        if v == "fmt -s":  # Only override the default
            if platform.system() == "Windows":
                return "powershell cat"
            elif platform.system() == "Darwin":
                return "cat -s"
        return v

    # ============================================================================
    # PUBLIC METHODS
    # ============================================================================

    def config_path(self) -> Path:
        """Path to user configuration directory."""
        return Path(typer.get_app_dir("dbtwiz"))

    # ============================================================================
    # PRIVATE METHODS - Internal Helper Functions
    # ============================================================================

    def _config_file(self) -> Path:
        """Path to user configuration file."""
        return self.config_path() / "config.toml"

    def _parse_config(self) -> None:
        """Parse the config file.

        Raises:
            SystemExit: If the file cannot be parsed (via fatal function)
        """
        config_file = self._config_file()
        if not config_file.exists():
            self._config = {}
            return
        try:
            with open(self._config_file(), "rb") as f:
                self._config = tomllib.load(f)
        except Exception as ex:
            from ..utils.logger import fatal

            fatal(f"Failed to parse file {self._config_file()}: {ex}")

    def _toml_item(self, setting: Dict[str, Any]) -> str:
        """Format setting for inclusion in Toml.

        Args:
            setting: Dictionary containing setting configuration

        Returns:
            Formatted TOML string for the setting
        """
        key = setting["key"]
        if "default_win" in setting and platform.system() == "Windows":
            value = setting["default_win"]
        elif "default_mac" in setting and platform.system() == "Darwin":
            value = setting["default_mac"]
        else:
            value = setting["default"]
        lines = [f"# {row}" for row in inspect.cleandoc(setting["help"]).splitlines()]
        if isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        elif isinstance(value, bool):
            lines.append(f"{key} = {str(value).lower()}")
        else:
            lines.append(f"{key} = {value}")
        return "\n".join(lines)

    def _append_missing_defaults(self) -> None:
        """Add missing defaults to config file and parse again.

        This method ensures that all required configuration settings have
        default values, creating them if they don't exist.
        """
        self.config_path().mkdir(parents=True, exist_ok=True)
        with open(self._config_file(), "a") as f:
            for setting in UserConfig.SETTINGS:
                key = setting["key"]
                if key not in self._config:
                    f.write(self._toml_item(setting) + "\n\n")
        self._parse_config()

    # ============================================================================
    # SPECIAL METHODS
    # ============================================================================

    def __getattr__(self, name: str) -> Any:
        """Dynamically handle attribute access and warn if the setting is missing.

        Args:
            name: Name of the configuration attribute to access

        Returns:
            Configuration value or None if not found

        Raises:
            SystemExit: If the configuration is missing (via fatal function)
        """
        from ..utils.logger import fatal

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

    def __dir__(self) -> List[str]:
        """Include dynamic attributes for autocompletion.

        Returns:
            List of available attribute names for autocompletion
        """
        return list(self._config.keys()) + list(super().__dir__())
