"""Tests for dbtwiz.core.project.Project variable loading."""

from pathlib import Path

from yaml import safe_dump

from dbtwiz.core.project import Project

_DBT_PROJECT_BASE = {
    "name": "sample",
    "version": "1.0.0",
    "profile": "default",
}


def _write(path: Path, data: dict) -> None:
    path.write_text(safe_dump(data), encoding="utf-8")


def test_reads_vars_from_dbt_project_yml(tmp_path: Path) -> None:
    """Vars defined only in dbt_project.yml still work (backward compatible)."""
    _write(
        tmp_path / "dbt_project.yml",
        {**_DBT_PROJECT_BASE, "vars": {"teams": {"team-a": {"description": "A"}}}},
    )

    project = Project(project_root=tmp_path)

    assert [t["name"] for t in project.teams()] == ["team-a"]


def test_reads_vars_from_vars_yml(tmp_path: Path) -> None:
    """Vars extracted into a dedicated vars.yml are picked up."""
    _write(tmp_path / "dbt_project.yml", dict(_DBT_PROJECT_BASE))
    _write(
        tmp_path / "vars.yml",
        {
            "vars": {
                "teams": {"team-a": {"description": "A"}},
                "access-policies": {"person-data": {"description": "PII"}},
                "service-consumers": {"looker": {"description": "Looker"}},
                "behavioural-data-expiration": 550,
            }
        },
    )

    project = Project(project_root=tmp_path)

    assert [t["name"] for t in project.teams()] == ["team-a"]
    assert [a["name"] for a in project.access_policies()] == ["person-data"]
    assert [c["name"] for c in project.service_consumers()] == ["looker"]
    assert [e["name"] for e in project.data_expirations()] == [
        "behavioural-data-expiration"
    ]


def test_merges_both_files_with_vars_yml_winning(tmp_path: Path) -> None:
    """Vars from both files merge; vars.yml wins on key collisions."""
    _write(
        tmp_path / "dbt_project.yml",
        {
            **_DBT_PROJECT_BASE,
            "vars": {
                "teams": {"team-a": {"description": "from dbt_project"}},
                "customer-data-expiration": 1096,
            },
        },
    )
    _write(
        tmp_path / "vars.yml",
        {"vars": {"teams": {"team-b": {"description": "from vars.yml"}}}},
    )

    project = Project(project_root=tmp_path)

    # vars.yml's `teams` key replaces dbt_project.yml's `teams` key.
    assert [t["name"] for t in project.teams()] == ["team-b"]
    # Non-colliding keys from dbt_project.yml are preserved.
    assert [e["name"] for e in project.data_expirations()] == [
        "customer-data-expiration"
    ]


def test_no_vars_anywhere(tmp_path: Path) -> None:
    """A project without any vars does not raise."""
    _write(tmp_path / "dbt_project.yml", dict(_DBT_PROJECT_BASE))

    project = Project(project_root=tmp_path)

    assert project.teams() == []
    assert project.access_policies() == []
    assert project.service_consumers() == []
    assert project.variables() == {}


def test_variables_exposes_the_merged_set(tmp_path: Path) -> None:
    """Consumers resolving variable references see vars from both files."""
    _write(
        tmp_path / "dbt_project.yml",
        {**_DBT_PROJECT_BASE, "vars": {"short-retention": 30, "capped-retention": 90}},
    )
    _write(tmp_path / "vars.yml", {"vars": {"capped-retention": 1096}})

    project = Project(project_root=tmp_path)

    assert project.variables() == {"short-retention": 30, "capped-retention": 1096}
