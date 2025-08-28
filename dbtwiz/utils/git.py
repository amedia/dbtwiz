import subprocess
from pathlib import Path
from typing import List

from ..utils.logger import fatal


def get_staged_files(folders: List[str], file_types: List[str]) -> List[str]:
    """
    Identify and retrieve staged files in the Git repository.

    This function runs a `git status` command to fetch the staged files in the repository.
    It filters out files with the given file types and folders and returns their paths.
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

    files = list()
    for line in git_status.stdout.decode("utf-8").splitlines():
        staged, *_, filename = line.split(" ")
        if staged in ["A", "M"]:
            path = Path(filename)
            if path.parts[0] in folders and path.suffix in file_types:
                files.append(path)

    return files
