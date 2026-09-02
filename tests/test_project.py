"""Tests for dbtwiz.core.project.Project variable loading."""

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from yaml import safe_dump

from dbtwiz.core.project import Project

_DBT_PROJECT_BASE = {
    "name": "sample",
    "version": "1.0.0",
    "profile": "default",
}


def _write(path: Path, data: dict) -> None:
    path.write_text(safe_dump(data), encoding="utf-8")


@contextmanager
def _declared_expiration_vars(entries: list):
    """Stub the expiration variables declared in the project's pyproject.toml."""
    stub = SimpleNamespace(expiration_var_entries=lambda: entries)
    with patch("dbtwiz.core.project.project_config", return_value=stub):
        yield


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
    assert project.variables()["behavioural-data-expiration"] == 550


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
    assert project.variables()["customer-data-expiration"] == 1096


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


class TestDataExpirations:
    """Test the expiration policies offered when creating a model."""

    @staticmethod
    def _project(tmp_path: Path, variables: dict) -> Project:
        """Build a Project whose variables are the given mapping."""
        _write(tmp_path / "dbt_project.yml", {**_DBT_PROJECT_BASE, "vars": variables})
        return Project(project_root=tmp_path)

    def test_offers_declared_vars_in_declared_order(self, tmp_path: Path) -> None:
        """The config decides which variables are offered, and in which order."""
        project = self._project(
            tmp_path, {"long-retention": 1096, "short-retention": 30}
        )

        with _declared_expiration_vars(
            [
                {"var": "short-retention", "description": "Working data"},
                {"var": "long-retention", "description": "Customer data"},
            ]
        ):
            expirations = project.data_expirations()

        assert [e["name"] for e in expirations] == ["short-retention", "long-retention"]
        assert expirations[0]["description"] == "Working data (30 days)"
        assert expirations[1]["description"] == "Customer data (1096 days)"

    def test_offers_vars_that_no_naming_convention_would_match(
        self, tmp_path: Path
    ) -> None:
        """Any variable name works, not just ones ending in -data-expiration."""
        project = self._project(tmp_path, {"keep_for_a_while": 90})

        with _declared_expiration_vars([{"var": "keep_for_a_while"}]):
            expirations = project.data_expirations()

        assert expirations == [{"name": "keep_for_a_while", "description": "(90 days)"}]

    def test_offers_a_declared_var_missing_from_project_vars(
        self, tmp_path: Path
    ) -> None:
        """A declared variable with no value is still offered, without a day count."""
        project = self._project(tmp_path, {})

        with _declared_expiration_vars(
            [{"var": "absent-retention", "description": "Not declared anywhere"}]
        ):
            expirations = project.data_expirations()

        assert expirations == [
            {"name": "absent-retention", "description": "Not declared anywhere"}
        ]

    def test_no_declaration_offers_nothing(self, tmp_path: Path) -> None:
        """Without the config there is nothing to offer, and nothing is guessed."""
        project = self._project(tmp_path, {"behavioural-data-expiration": 550})

        with _declared_expiration_vars([]):
            assert project.data_expirations() == []
