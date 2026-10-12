"""Tests for choosing between sqlfluff (dbt-core) and dbt lint (dbt v2 only)."""

from importlib.metadata import PackageNotFoundError
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dbtwiz.dbt.run import dbt_core_installed, dbt_lint_command
from dbtwiz.model.format import format_sql_files

_PROJECT = SimpleNamespace(root_path=lambda: Path("/project"))


@pytest.mark.parametrize(
    "installed, expected",
    [("1.11.3", True), ("2.0.8", False), (PackageNotFoundError("dbt-core"), False)],
)
def test_dbt_core_installed(installed, expected):
    """dbt-core 2.x is the v2 engine's wheel, so only 1.x counts as dbt-core."""
    with patch("dbtwiz.dbt.run.version", side_effect=[installed]):
        assert dbt_core_installed() is expected


def test_dbt_lint_command_refuses_fix_with_json():
    """dbt lint silently skips --fix under --format json."""
    with pytest.raises(ValueError):
        dbt_lint_command(["a.sql"], fix=True, json_output=True)


@pytest.mark.parametrize(
    "core, fix, expected",
    [
        (True, False, ["sqlfluff", "lint", "models/a.sql"]),
        (True, True, ["sqlfluff", "fix", "models/a.sql"]),
        (
            False,
            False,
            ["dbt", "lint", "--quiet", "--project-dir", "/project", "models/a.sql"],
        ),
        (
            False,
            True,
            [
                "dbt",
                "lint",
                "--quiet",
                "--fix",
                "--project-dir",
                "/project",
                "models/a.sql",
            ],
        ),
    ],
)
def test_format_sql_files_picks_linter(core, fix, expected):
    """sqlfmt always runs; the linter depends on whether dbt-core is installed."""
    with (
        patch("dbtwiz.model.format.subprocess.run") as run,
        patch("dbtwiz.model.format.dbt_core_installed", return_value=core),
        patch("dbtwiz.dbt.run.dbt_executable", return_value="dbt"),
        patch("dbtwiz.dbt.run.project_config", return_value=_PROJECT),
        patch("dbtwiz.model.format.Path.rglob", return_value=[Path("models/a.sql")]),
    ):
        format_sql_files(staged=False, model_names=["a"], sqlfmt_args=[], fix=fix)

    assert run.call_args_list[0].args[0][0] == "sqlfmt"
    command = [str(part).replace("\\", "/") for part in run.call_args_list[1].args[0]]
    assert command == expected
