import os
from pathlib import Path

from ..config.user import user_config
from ..utils.logger import warn


def open_in_editor(path: Path) -> int:
    """Open the file at the given path in the user's configured editor.

    Args:
        path: Path to the file to open

    Returns:
        Exit code from the editor command (0 for success, non-zero for failure)

    Note:
        The editor command is configured in the user's config.toml file.
        If the command contains '{}', it will be replaced with the file path.
        Otherwise, the path will be appended to the command.
    """
    editor = str(user_config().editor_command)
    if "{}" in editor:
        command = editor.replace("{}", str(path))
    else:
        command = f"{editor} {path}"
    value = os.system(command)
    if value != 0:
        warn(f"Failed to open file in editor. '{command}' returned status {value}.")
    return value
