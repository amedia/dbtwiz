import os
import subprocess
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
        # Split the command into parts for subprocess
        cmd_parts = command.split()
    else:
        # Split the editor command and append the path
        cmd_parts = editor.split() + [str(path)]

    try:
        result = subprocess.run(cmd_parts, check=False)
        return result.returncode
    except (FileNotFoundError, OSError) as e:
        warn(f"Failed to open file in editor. '{' '.join(cmd_parts)}' failed: {e}")
        return 1
