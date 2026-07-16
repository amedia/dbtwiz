"""Tests for orphaned-materialization cleanup handling of disabled models."""

import json
from unittest.mock import MagicMock, patch

import pytest

from dbtwiz.admin.cleanup import (
    add_git_deletion_info,
    find_orphaned_tables,
    handle_orphaned_materializations,
)
from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.target import Target

DISABLED_REASON = "disabled in dbt project (enabled: false)"


def _write_manifest(path, disabled):
    """Write a minimal manifest.json with a disabled section to the given path."""
    manifest = {
        "nodes": {},
        "sources": {},
        "disabled": disabled,
        "parent_map": {},
        "child_map": {},
    }
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return path


class TestDisabledRelations:
    """Tests for Manifest.disabled_relations."""

    def test_uses_relation_name(self, tmp_path):
        path = _write_manifest(
            tmp_path / "manifest.json",
            {
                "model.test.disabled_model": [
                    {
                        "resource_type": "model",
                        "relation_name": "`proj`.`ds`.`disabled_model`",
                    }
                ]
            },
        )
        relations = Manifest(path).disabled_relations()
        assert relations == {"proj.ds.disabled_model": DISABLED_REASON}

    def test_falls_back_to_database_schema_alias(self, tmp_path):
        path = _write_manifest(
            tmp_path / "manifest.json",
            {
                "model.test.disabled_model": [
                    {
                        "resource_type": "model",
                        "relation_name": None,
                        "database": "proj",
                        "schema": "ds",
                        "alias": "aliased_name",
                        "name": "disabled_model",
                    }
                ]
            },
        )
        relations = Manifest(path).disabled_relations()
        assert relations == {"proj.ds.aliased_name": DISABLED_REASON}

    def test_ignores_non_materialized_resources(self, tmp_path):
        path = _write_manifest(
            tmp_path / "manifest.json",
            {
                "test.test.some_test": [
                    {"resource_type": "test", "relation_name": "`proj`.`ds`.`x`"}
                ]
            },
        )
        assert Manifest(path).disabled_relations() == {}

    def test_empty_when_no_disabled_section(self, tmp_path):
        path = tmp_path / "manifest.json"
        path.write_text(
            json.dumps(
                {
                    "nodes": {},
                    "sources": {},
                    "parent_map": {},
                    "child_map": {},
                }
            ),
            encoding="utf-8",
        )
        assert Manifest(path).disabled_relations() == {}


class TestAddGitDeletionInfo:
    """Tests for add_git_deletion_info handling of disabled tables."""

    def test_annotates_disabled_without_git_lookup(self):
        disabled = {"proj.ds.disabled_model": DISABLED_REASON}
        with patch(
            "dbtwiz.admin.cleanup.parse_git_log_output", return_value=[]
        ) as mock_git:
            choices = add_git_deletion_info(
                ["proj.ds.disabled_model"], disabled=disabled
            )
        assert choices == [
            {
                "name": "proj.ds.disabled_model",
                "value": "proj.ds.disabled_model",
                "description": DISABLED_REASON,
            }
        ]
        # git log is parsed once up front, but no match lookup is needed for
        # disabled tables (they still exist in the repo).
        mock_git.assert_called_once()


class TestHandleOrphanedMaterializations:
    """Tests for excluding/including disabled models in the orphaned list."""

    @pytest.fixture
    def patched_env(self):
        data = {
            "proj": {
                "ds": {
                    "manifest": ["active_model"],
                    "bigquery": ["active_model", "orphan_normal", "disabled_model"],
                }
            }
        }
        disabled = {"proj.ds.disabled_model": DISABLED_REASON}

        manifest_instance = MagicMock()
        manifest_instance.models.return_value = {}
        manifest_instance.nodes = {}
        manifest_instance.disabled_relations.return_value = disabled

        project_conf = MagicMock()
        project_conf.orphan_cleanup_bq_region = "region-eu"
        project_conf.orphan_cleanup_projects = ["proj"]
        project_conf.user_project = "user-proj"

        with (
            patch("dbtwiz.admin.cleanup.ensure_auth"),
            patch("dbtwiz.admin.cleanup.BigQueryClient"),
            patch("dbtwiz.admin.cleanup.build_data_structure", return_value=data),
            patch("dbtwiz.admin.cleanup.project_config", return_value=project_conf),
            patch("dbtwiz.admin.cleanup.Manifest") as mock_manifest_cls,
            patch("dbtwiz.admin.cleanup.info") as mock_info,
        ):
            mock_manifest_cls.return_value = manifest_instance
            yield mock_info

    def test_excludes_disabled_by_default(self, patched_env):
        mock_info = patched_env
        handle_orphaned_materializations(Target.dev, list_only=True, force_delete=False)
        messages = " ".join(str(call.args[0]) for call in mock_info.call_args_list)
        assert "orphan_normal" in messages
        assert "disabled_model" not in messages
        assert "Ignoring 1" in messages

    def test_includes_disabled_when_opted_in(self, patched_env):
        mock_info = patched_env
        handle_orphaned_materializations(
            Target.dev, list_only=True, force_delete=False, include_disabled=True
        )
        messages = " ".join(str(call.args[0]) for call in mock_info.call_args_list)
        assert "orphan_normal" in messages
        assert "disabled_model" in messages
        assert DISABLED_REASON in messages


def test_find_orphaned_tables_basic():
    data = {
        "proj": {
            "ds": {
                "manifest": ["a"],
                "bigquery": ["a", "b"],
            }
        }
    }
    assert find_orphaned_tables(data) == ["proj.ds.b"]
