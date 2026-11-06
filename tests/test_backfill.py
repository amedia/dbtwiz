"""Tests for backfill batch size estimation."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


def make_model(name="my_model", alias=None):
    return {
        "name": name,
        "alias": alias or name,
        "config": {"materialized": "incremental"},
    }


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
        """dbt is driven as a subprocess, so the compile call is the thing to stub."""
        with patch("dbtwiz.admin.backfill.subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
            self.mock_run = mock_run
            yield

    @pytest.fixture(autouse=True)
    def mock_profile(self):
        with patch("dbtwiz.admin.backfill.Profile") as m:
            m.return_value.profile_config.return_value = {
                "execution_project": "test-project"
            }
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
        with (
            patch("dbtwiz.integrations.bigquery.BigQueryClient", return_value=mock_bq),
            patch("dbtwiz.admin.backfill.project_config") as mock_cfg,
        ):
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
        self.mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="compile failed"
        )
        # Compile failure returns immediately with default, no per-model fallback
        assert (
            self._run([make_model(), make_model("other")], tmp_path, []) == self.DEFAULT
        )

    def test_falls_back_to_default_when_no_compiled_file(self, tmp_path):
        # Do not create the compiled file
        assert self._run([make_model()], tmp_path, [2 * 10**9]) == self.DEFAULT

    def test_falls_back_to_default_when_dry_run_returns_zero(self, tmp_path):
        write_compiled_sql(tmp_path, "my_model")
        assert self._run([make_model()], tmp_path, [0]) == self.DEFAULT

    def test_alias_used_for_display_not_glob(self, tmp_path):
        # Compiled file is found by model name, not alias
        write_compiled_sql(tmp_path, "my_model")
        result = self._run(
            [make_model("my_model", alias="aliased_table")], tmp_path, [2 * 10**9]
        )
        assert result == 5


class TestEstimateBatchSizeFromRuntime:
    DEFAULT = 30
    TARGET_SECONDS = 480
    TARGET_BYTES = 10 * 10**9
    SAMPLE = date(2024, 1, 31)

    def _run(self, models, runtimes, dry_run_batch_size=None):
        from dbtwiz.admin.backfill import estimate_batch_size_from_runtime

        with (
            patch(
                "dbtwiz.admin.backfill.fetch_single_day_runtimes", return_value=runtimes
            ),
            patch(
                "dbtwiz.admin.backfill.estimate_batch_size",
                return_value=dry_run_batch_size,
            ) as mock_dry_run,
        ):
            result = estimate_batch_size_from_runtime(
                models=models,
                sample_date=self.SAMPLE,
                default_batch_size=self.DEFAULT,
                target_seconds=self.TARGET_SECONDS,
                target_bytes=self.TARGET_BYTES,
            )
        return result, mock_dry_run

    def test_calculates_batch_size_from_runtime(self):
        result, mock_dry_run = self._run([make_model()], {"my_model": 40.0})
        assert result == 12  # 480 s / 40 s
        mock_dry_run.assert_not_called()

    def test_capped_at_default(self):
        result, _ = self._run([make_model()], {"my_model": 2.0})
        assert result == self.DEFAULT

    def test_minimum_is_one_when_runtime_exceeds_target(self):
        result, _ = self._run([make_model()], {"my_model": 900.0})
        assert result == 1

    def test_falls_back_to_dry_run_without_history(self):
        result, mock_dry_run = self._run([make_model()], {}, dry_run_batch_size=5)
        assert result == 5
        assert mock_dry_run.call_args.kwargs["models"] == [make_model()]

    def test_dry_runs_only_unmeasured_models(self):
        measured, unmeasured = make_model("model_a"), make_model("model_b")
        result, mock_dry_run = self._run(
            [measured, unmeasured], {"model_a": 40.0}, dry_run_batch_size=20
        )
        assert result == 12  # min of 12 from run time and 20 from the dry run
        assert mock_dry_run.call_args.kwargs["models"] == [unmeasured]


class TestFetchSingleDayRuntimes:
    def test_skipped_without_elementary_dataset(self):
        from dbtwiz.admin.backfill import fetch_single_day_runtimes

        with (
            patch("dbtwiz.admin.backfill.project_config") as mock_cfg,
            patch("dbtwiz.integrations.bigquery.BigQueryClient") as mock_bq,
        ):
            mock_cfg.return_value.incidents_elementary_dataset = None
            assert fetch_single_day_runtimes(["my_model"]) == {}
            mock_bq.assert_not_called()

    def test_empty_when_query_fails(self):
        from dbtwiz.admin.backfill import fetch_single_day_runtimes

        mock_bq = MagicMock()
        mock_bq.get_client.return_value.query.side_effect = RuntimeError("denied")
        with (
            patch("dbtwiz.admin.backfill.project_config") as mock_cfg,
            patch("dbtwiz.integrations.bigquery.BigQueryClient", return_value=mock_bq),
        ):
            mock_cfg.return_value.incidents_elementary_dataset = "project.elementary"
            assert fetch_single_day_runtimes(["my_model"]) == {}

    def test_maps_model_name_to_runtime(self):
        from dbtwiz.admin.backfill import fetch_single_day_runtimes

        mock_bq = MagicMock()
        mock_bq.get_client.return_value.query.return_value.result.return_value = [
            {"name": "my_model", "p90_execution_seconds": 40.0}
        ]
        with (
            patch("dbtwiz.admin.backfill.project_config") as mock_cfg,
            patch("dbtwiz.integrations.bigquery.BigQueryClient", return_value=mock_bq),
        ):
            mock_cfg.return_value.incidents_elementary_dataset = "project.elementary"
            assert fetch_single_day_runtimes(["my_model"]) == {"my_model": 40.0}


class TestBackfillTaskArgs:
    def test_skips_checks_on_dbt_v2(self):
        from dbtwiz.commands.build import backfill_task_args

        with patch("dbtwiz.commands.build.dbt_core_installed", return_value=False):
            assert backfill_task_args() == {"write-json": False, "skip-checks": True}

    def test_no_skip_checks_on_dbt_core(self):
        from dbtwiz.commands.build import backfill_task_args

        with patch("dbtwiz.commands.build.dbt_core_installed", return_value=True):
            assert backfill_task_args() == {"write-json": False}
