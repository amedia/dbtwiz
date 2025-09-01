import os
import shlex
import shutil
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
    # Use absolute path to avoid cwd issues
    file_path = str(Path(path).resolve())

    # Robustly parse command into args across platforms
    try:
        if "{}" in editor:
            # Keep placeholder as a separate token if possible
            parts = shlex.split(editor, posix=(os.name != "nt"))
            replaced = False
            for i, token in enumerate(parts):
                if token == "{}":
                    parts[i] = file_path
                    replaced = True
            if not replaced:
                # Fallback: replace then split
                parts = shlex.split(
                    editor.replace("{}", file_path), posix=(os.name != "nt")
                )
            cmd_parts = parts
        else:
            cmd_parts = shlex.split(editor, posix=(os.name != "nt")) + [file_path]
    except ValueError:
        # Fallback to simple split if shlex fails
        cmd_parts = editor.split() + [file_path]

    try:
        # On Windows, resolve the executable explicitly
        if os.name == "nt" and cmd_parts:
            exe = cmd_parts[0]
            resolved = (
                shutil.which(exe)
                or shutil.which(f"{exe}.cmd")
                or shutil.which(f"{exe}.exe")
            )
            if resolved:
                cmd_parts[0] = resolved
        result = subprocess.run(cmd_parts, check=False, shell=False)
        return result.returncode
    except (FileNotFoundError, OSError) as e:
        warn(f"Failed to open file in editor. '{' '.join(cmd_parts)}' failed: {e}")
        return 1
