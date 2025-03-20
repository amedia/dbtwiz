import subprocess
from pathlib import Path

from dbtwiz.logging import fatal, info

QUERY_FOLDERS = ["models", "macros", "tests", "seeds", "analyses"]


def sqlfix() -> None:
    """Runs sqlfmt and sqlfix."""
    query_files = staged_queries()
    if len(query_files) == 0:
        info("No staged SQL changes detected.")
        return

    info("Running sqlfmt on changed SQL files.")
    subprocess.run(["sqlfmt", "--line-length=100"] + query_files)

    info("Running sqlfluff fix on changed SQL files.")
    subprocess.run(["sqlfluff", "fix"] + query_files)


def staged_queries():
    """
    Identify and retrieve SQL query files that are staged for commit in the Git repository.

    This function runs a `git status` command to fetch the staged files in the repository.
    It filters out SQL files within specific query folders (as defined by `QUERY_FOLDERS`)
    and returns their paths.
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

    query_files = list()
    for line in git_status.stdout.decode("utf-8").splitlines():
        staged, *_, filename = line.split(" ")
        if staged in ["A", "M"]:
            path = Path(filename)
            if path.parts[0] in QUERY_FOLDERS and path.suffix == ".sql":
                query_files.append(path)

    return query_files
