"""Tests for model validation on dbt v2, where `dbt check` replaces dbtwiz's own rules."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dbtwiz.dbt.run import dbt_check_command
from dbtwiz.model.validate import YmlValidator, check_violations, validation_steps

_PROJECT = SimpleNamespace(root_path=lambda: Path("/project"))


def test_dbt_check_command():
    """One model, results written to the given target folder."""
    with (
        patch("dbtwiz.dbt.run.dbt_executable", return_value="dbt"),
        patch("dbtwiz.dbt.run.project_config", return_value=_PROJECT),
    ):
        command = [
            part.replace("\\", "/")
            for part in dbt_check_command("int_a__b", Path("/tmp/check"))
        ]
    assert command == [
        "dbt",
        "check",
        "--quiet",
        "--select",
        "int_a__b",
        "--project-dir",
        "/project",
        "--target-path",
        "/tmp/check",
    ]


def test_check_violations():
    """One line per violating row of the model; other results are left out."""
    run_results = {
        "results": [
            {
                "unique_id": "check.my_checks.intermediate_dependent_on_marts",
                "status": "warn",
                "message": "unique_id=model.p.int_a__b, parent=mrt_a__c\n"
                "  unique_id=model.p.int_a__b, parent=mrt_a__d\n"
                "  unique_id=model.p.int_a__bc, parent=mrt_a__c",
            },
            {
                "unique_id": "check.my_checks.models_without_primary_key",
                "status": "warn",
                "message": "unique_id=model.p.int_a__b.v2, "
                "original_file_path=models/int_a__b.sql",
            },
            {
                "unique_id": "check.my_checks.other_model_only",
                "status": "warn",
                "message": "unique_id=model.p.int_x__y\n  unique_id=seed.p.int_a__b",
            },
            {
                "unique_id": "check.dbt_project_evaluator.fct_test_coverage",
                "status": "warn",
                "message": "total_models=990, tested_models=70",
            },
            {"unique_id": "check.p.passing", "status": "pass", "message": None},
            {
                "unique_id": "check.p.sources",
                "status": "skipped",
                "message": "selection matched no nodes",
            },
            {"unique_id": "check.p.broken", "status": "error", "message": "bad sql"},
            {"unique_id": "check.p.silent", "status": "error", "message": None},
        ]
    }
    assert check_violations(run_results, "int_a__b") == [
        "intermediate_dependent_on_marts (parent: mrt_a__c)",
        "intermediate_dependent_on_marts (parent: mrt_a__d)",
        "models_without_primary_key",
        "broken: bad sql",
        "silent: failed to run",
    ]


@pytest.mark.parametrize(
    "dbt_core, dbt_check, present, absent",
    [
        (
            True,
            False,
            ["Validating model config rules", "Validating sql with sqlfluff"],
            ["Validating with dbt check", "Validating sql with dbt lint"],
        ),
        (
            False,
            False,
            ["Validating model config rules", "Validating sql with dbt lint"],
            ["Validating with dbt check", "Validating sql with sqlfluff"],
        ),
        (
            False,
            True,
            ["Validating with dbt check", "Validating sql with dbt lint"],
            ["Validating model config rules", "Validating sql with sqlfluff"],
        ),
    ],
)
def test_validation_steps(dbt_core, dbt_check, present, absent):
    """dbt check replaces the config rules when enabled; the linter follows the engine."""
    descriptions = [
        desc
        for _, desc in validation_steps(MagicMock(), MagicMock(), dbt_core, dbt_check)
    ]
    shared = ["Validating yml exists", "Validating yml definition"]
    assert all(d in descriptions for d in shared + ["Validating yml columns"])
    assert all(d in descriptions for d in present)
    assert not any(d in descriptions for d in absent)


def _yml_validator(file_name: str, yml_name: str) -> SimpleNamespace:
    """A YmlValidator stand-in for a model in models/3_intermediate/dom/, without a key."""
    yml = SimpleNamespace(
        model_base=SimpleNamespace(
            path=Path(f"models/3_intermediate/dom/{file_name}.yml"),
            layer_abbreviation="int",
            domain="dom",
        ),
        _load_model_yml=lambda: ({"models": [{"name": yml_name}]}, None),
    )
    for method in ("_validate_model_name", "_validate_yml_primary_key_constraint"):
        setattr(
            yml,
            method,
            lambda method=method, **kwargs: getattr(YmlValidator, method)(
                yml, **kwargs
            ),
        )
    return yml


def test_yml_definition_left_to_dbt_check():
    """With dbt check, the folders and a missing primary key are left to the checks."""
    yml = _yml_validator("int_other__a", "int_other__a")

    assert YmlValidator.validate_yml_definition(yml, checked_by_dbt=True) == (
        True,
        "yml definition ok",
    )
    success, message = YmlValidator.validate_yml_definition(yml)
    assert success is False
    assert "suggests domain" in message
    assert "primary key constraint is missing" in message


def test_yml_name_still_checked_with_dbt_check():
    """dbt applies a yml to the model it names, so its name must match its file."""
    yml = _yml_validator("int_dom__a", "int_dom__b")

    success, message = YmlValidator.validate_yml_definition(yml, checked_by_dbt=True)
    assert success is False
    assert "doesn't match yml filename" in message


def test_primary_key_shape_still_checked_on_dbt_v2():
    """dbt check sees whether a primary key exists, not how the yml declares it."""
    model_def = {
        "constraints": [{"type": "primary_key", "columns": ["id"]}],
        "columns": [{"name": "id", "constraints": [{"type": "primary_key"}]}],
    }
    assert (
        YmlValidator._validate_yml_primary_key_constraint(
            None, model_def=model_def, require=False
        )
        == "primary key constraint is defined at both model and column levels"
    )
