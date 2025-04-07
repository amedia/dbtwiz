import subprocess
from pathlib import Path
from typing import List

from dbtwiz.logging import info
from dbtwiz.utils.git import get_staged_files


def _find_files_by_file_name(folder: str, file_name: str):
    matching_files = list(Path(folder).rglob(f"{file_name}.sql"))
    return matching_files


def fix_sql_files(staged: bool, model_names: List[str]) -> None:
    """Runs sqlfmt and sqlfix for staged and/or defined sql files."""
    files = []
    if staged:
        files = get_staged_files(
            folders=["models", "macros", "tests", "seeds", "analyses"],
            file_types=[".sql"],
        )
    if model_names:
        for name in model_names:
            files = list(
                set(files + _find_files_by_file_name(folder="models", file_name=name))
            )

    if len(files) == 0:
        info("No files identified for fixing.")
        return

    info("Running sqlfmt")
    subprocess.run(["sqlfmt"] + files)

    info("Running sqlfluff fix")
    subprocess.run(["sqlfluff", "fix"] + files)


def lint_sql_files(staged: bool, model_names: List[str]) -> None:
    """Runs sqlfmt --diff and sqlfluff lint for staged and/or defined sql files."""
    files = []
    if staged:
        files = get_staged_files(
            folders=["models", "macros", "tests", "seeds", "analyses"],
            file_types=[".sql"],
        )
    if model_names:
        for name in model_names:
            files = list(
                set(files + _find_files_by_file_name(folder="models", file_name=name))
            )

    if len(files) == 0:
        info("No files identified for fixing.")
        return

    info("Running sqlfmt")
    subprocess.run(["sqlfmt", "--diff"] + files)

    info("Running sqlfluff lint")
    subprocess.run(["sqlfluff", "lint"] + files)
