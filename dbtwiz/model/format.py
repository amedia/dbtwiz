import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from ..dbt.run import dbt_core_installed, dbt_lint_command
from ..utils.git import get_staged_files
from ..utils.logger import info


def format_sql_files(
    staged: bool,
    model_names: Optional[List[str]],
    sqlfmt_args: List[str],
    fix: bool,
) -> None:
    """Run sqlfmt, then sqlfluff (with dbt-core) or `dbt lint` (dbt v2 only)."""
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

    if dbt_core_installed():
        sqlfluff_command = "fix" if fix else "lint"
        info(f"Running sqlfluff {sqlfluff_command}")
        subprocess.run(
            [sys.executable, "-m", "dbtwiz.dbt.sqlfluff_cli", sqlfluff_command] + files
        )
    else:
        info("Running dbt lint --fix" if fix else "Running dbt lint")
        subprocess.run(dbt_lint_command(files, fix=fix))
