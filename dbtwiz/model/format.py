import subprocess
from pathlib import Path
from typing import List, Optional

from dbtwiz.helpers.git import get_staged_files
from dbtwiz.helpers.logger import info


def format_sql_files(
    staged: bool,
    model_names: Optional[List[str]],
    sqlfmt_args: List[str],
    sqlfluff_command: str,
) -> None:
    """Generic function to process SQL files with configurable commands."""
    files = []
    if staged:
        files = get_staged_files(
            folders=["models", "macros", "tests", "seeds", "analyses"],
            file_types=[".sql"],
        )
    if model_names:
        for name in model_names:
            files = list(set(files + list(Path("models").rglob(f"{name}.sql"))))

    if len(files) == 0:
        info("No files identified for processing.")
        return

    if sqlfmt_args:
        info(f"Running sqlfmt with arguments: {' '.join(sqlfmt_args)}")
    else:
        info("Running sqlfmt")
    subprocess.run(["sqlfmt"] + sqlfmt_args + files)

    info(f"Running sqlfluff {sqlfluff_command}")
    subprocess.run(["sqlfluff", sqlfluff_command] + files)
