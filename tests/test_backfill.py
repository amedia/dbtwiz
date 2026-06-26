"""Tests for backfill batch size estimation."""

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def make_model(name="my_model", alias=None):
    return {"name": name, "alias": alias or name, "config": {"materialized": "incremental"}}


def write_compiled_sql(tmp_path: Path, model_name: str) -> Path:
    path = tmp_path / "target" / "compiled" / "project" / "models" / f"{model_name}.sql"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("SELECT 1", encoding="utf-8")
    return path


class TestEstimateBatchSize:
    DEFAULT = 30
    TARGET_10GB = 10 * 10**9
    SAMPLE = date(2024, 1, 31)

    @pytest.fixture(autouse=True)
    def mock_dbt(self):
        mock_runner_cls = MagicMock()
        mock_runner_cls.return_value.invoke.return_value.success = True
        self.mock_runner = mock_runner_cls
        mock_module = MagicMock()
        mock_module.dbtRunner = mock_runner_cls
        with patch.dict(sys.modules, {"dbt.cli.main": mock_module}):
            yield

    @pytest.fixture(autouse=True)
    def mock_suppress(self):
        with patch("dbtwiz.utils.contextmanagers.suppress_output") as m:
            m.return_value.__enter__ = MagicMock(return_value=None)
            m.return_value.__exit__ = MagicMock(return_value=False)
            yield

    @pytest.fixture(autouse=True)
    def mock_profile(self):
        with patch("dbtwiz.admin.backfill.Profile") as m:
            m.return_value.profile_config.return_value = {"execution_project": "test-project"}
            yield

    def _run(self, models, tmp_path, bytes_per_day_list):
        from dbtwiz.admin.backfill import estimate_batch_size

        mock_bq = MagicMock()
        mock_bq.get_client.return_value.query.side_effect = [
            MagicMock(total_bytes_processed=b) for b in bytes_per_day_list
        ]

        # BigQueryClient is imported lazily inside estimate_batch_size, so patching
        # at the source module works. If the import ever moves to module top-level
        # in backfill.py, this must change to "dbtwiz.admin.backfill.BigQueryClient".
        with patch("dbtwiz.integrations.bigquery.BigQueryClient", return_value=mock_bq), \
             patch("dbtwiz.admin.backfill.project_config") as mock_cfg:
            mock_cfg.return_value.root_path.return_value = tmp_path
            return estimate_batch_size(
                models=models,
                sample_date=self.SAMPLE,
                default_batch_size=self.DEFAULT,
                target_bytes=self.TARGET_10GB,
            )

    def test_calculates_batch_size_from_dry_run(self, tmp_path):
        write_compiled_sql(tmp_path, "my_model")
        assert self._run([make_model()], tmp_path, [2 * 10**9]) == 5  # 10 GB / 2 GB = 5

    def test_floor_division(self, tmp_path):
        write_compiled_sql(tmp_path, "my_model")
        assert self._run([make_model()], tmp_path, [3 * 10**9]) == 3  # floor(10 / 3)

    def test_minimum_is_one_when_scan_exceeds_target(self, tmp_path):
        write_compiled_sql(tmp_path, "my_model")
        assert self._run([make_model()], tmp_path, [50 * 10**9]) == 1

    def test_uses_minimum_across_models(self, tmp_path):
        write_compiled_sql(tmp_path, "model_a")
        write_compiled_sql(tmp_path, "model_b")
        result = self._run(
            [make_model("model_a"), make_model("model_b")],
            tmp_path,
            [1 * 10**9, 5 * 10**9],  # 10 days and 2 days → min is 2
        )
        assert result == 2

    def test_falls_back_to_default_when_compile_fails(self, tmp_path):
        self.mock_runner.return_value.invoke.return_value.success = False
        # Compile failure returns immediately with default, no per-model fallback
        assert self._run([make_model(), make_model("other")], tmp_path, []) == self.DEFAULT

    def test_falls_back_to_default_when_no_compiled_file(self, tmp_path):
        # Do not create the compiled file
        assert self._run([make_model()], tmp_path, [2 * 10**9]) == self.DEFAULT

    def test_falls_back_to_default_when_dry_run_returns_zero(self, tmp_path):
        write_compiled_sql(tmp_path, "my_model")
        assert self._run([make_model()], tmp_path, [0]) == self.DEFAULT

    def test_alias_used_for_display_not_glob(self, tmp_path):
        # Compiled file is found by model name, not alias
        write_compiled_sql(tmp_path, "my_model")
        result = self._run([make_model("my_model", alias="aliased_table")], tmp_path, [2 * 10**9])
        assert result == 5
