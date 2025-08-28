import subprocess
from pathlib import Path
from typing import List

from ..utils.logger import fatal


def get_staged_files(folders: List[str], file_types: List[str]) -> List[Path]:
    """Identify and retrieve staged files in the Git repository.

    This function runs a `git status` command to fetch the staged files in the repository.
    It filters files by the given folders and file types and returns their paths.

    Args:
        folders: List of folder names to filter by (e.g., ['models', 'macros'])
        file_types: List of file extensions to filter by (e.g., ['.sql', '.yml'])

    Returns:
        List of Path objects for staged files matching the criteria

    Raises:
        SystemExit: If git command fails (via fatal function)

    Note:
        Only returns files that are staged (added or modified) and match both
        the folder and file type criteria.
    """
    git_status = subprocess.run(
        [
            "git",
            "status",
            "--short",
            "--untracked-files=no",
            "--no-ahead-behind",
            "--no-renames",
        ],
        capture_output=True,
    )
    if git_status.returncode > 0:
        fatal(git_status.stderr.decode("utf-8"))

    files: List[Path] = []
    for line in git_status.stdout.decode("utf-8").splitlines():
        staged, *_, filename = line.split(" ")
        if staged in ["A", "M"]:
            path = Path(filename)
            if path.parts[0] in folders and path.suffix in file_types:
                files.append(path)

    return files
