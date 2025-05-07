from pathlib import Path
import os

from dbtwiz.config.user import user_config
from dbtwiz.helpers.logger import debug, warn


def open_in_editor(path: Path) -> int:
    """Open the file at the given path in the user's configured editor"""
    editor = str(user_config().editor_command)
    if "{}" in editor:
        command = editor.replace("{}", str(path))
    else:
        command = f"{editor} {path}"
    value = os.system(command)
    if value != 0:
        warn(f"Failed to open file in editor. Command '{command}' returned status {value}.")
    return value
