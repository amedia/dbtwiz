"""Tests for dbtwiz CLI functionality."""

from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from dbtwiz.cli.main import app


class TestCLI:
    """Test CLI functionality."""

    @pytest.fixture
    def cli_runner(self):
        """Create a CLI runner for testing."""
        return CliRunner()

    @patch("dbtwiz.model.inspect_model")
    def test_cli_model_inspect(self, mock_inspect, cli_runner):
        """Test model inspect command."""
        result = cli_runner.invoke(app, ["model", "inspect", "test_model"])

        assert result.exit_code == 0
        mock_inspect.assert_called_once_with(name="test_model")

    @patch("dbtwiz.model.format_sql_files")
    def test_cli_model_lint(self, mock_lint, cli_runner):
        """Test model lint command."""
        result = cli_runner.invoke(app, ["model", "lint", "test_model"])

        assert result.exit_code == 0
        mock_lint.assert_called_once()

    @patch("dbtwiz.model.format_sql_files")
    def test_cli_model_fix(self, mock_fix, cli_runner):
        """Test model fix command."""
        result = cli_runner.invoke(app, ["model", "fix", "test_model"])

        assert result.exit_code == 0
        mock_fix.assert_called_once()

    @patch("dbtwiz.commands.command_build")
    def test_cli_build(self, mock_build, cli_runner):
        """Test build command."""
        result = cli_runner.invoke(
            app, ["build", "--select", "test_model", "--target", "dev"]
        )

        assert result.exit_code == 0
        mock_build.assert_called_once()

    @patch("dbtwiz.commands.command_test")
    def test_cli_test(self, mock_test, cli_runner):
        """Test test command."""
        result = cli_runner.invoke(
            app, ["test", "--select", "test_model", "--target", "dev"]
        )

        assert result.exit_code == 0
        mock_test.assert_called_once()

    @patch("dbtwiz.commands.Manifest.update_manifests")
    def test_cli_manifest(self, mock_update, cli_runner):
        """Test manifest command."""
        result = cli_runner.invoke(app, ["manifest", "--type", "all"])

        assert result.exit_code == 0
        mock_update.assert_called_once_with("all")

    @patch("dbtwiz.admin.backfill.backfill")
    def test_cli_admin_backfill(self, mock_backfill, cli_runner):
        """Test admin backfill command."""
        result = cli_runner.invoke(
            app, ["admin", "backfill", "test_model", "2024-01-01", "2024-01-31"]
        )

        assert result.exit_code == 0
        mock_backfill.assert_called_once()

    @patch("dbtwiz.admin.cleanup.empty_development_dataset")
    def test_cli_admin_cleandev(self, mock_cleanup, cli_runner):
        """Test admin cleandev command."""
        result = cli_runner.invoke(app, ["admin", "cleandev"])

        assert result.exit_code == 0
        mock_cleanup.assert_called_once()

    @patch("dbtwiz.admin.cleanup.handle_orphaned_materializations")
    def test_cli_admin_orphaned(self, mock_orphaned, cli_runner):
        """Test admin orphaned command."""
        result = cli_runner.invoke(app, ["admin", "orphaned"])

        assert result.exit_code == 0
        mock_orphaned.assert_called_once()

    @patch("dbtwiz.admin.partition.update_partition_expirations")
    def test_cli_admin_partition_expiry(self, mock_update, cli_runner):
        """Test admin partition_expiry command."""
        result = cli_runner.invoke(app, ["admin", "partition-expiry"])

        assert result.exit_code == 0
        mock_update.assert_called_once()

    def test_cli_invalid_command(self, cli_runner):
        """Test CLI with invalid command."""
        result = cli_runner.invoke(app, ["invalid_command"])
        assert result.exit_code != 0

    def test_cli_invalid_option(self, cli_runner):
        """Test CLI with invalid option."""
        result = cli_runner.invoke(app, ["--invalid-option"])
        assert result.exit_code != 0


class TestCLIErrorHandling:
    """Test CLI error handling."""

    @pytest.fixture
    def cli_runner(self):
        """Create a CLI runner for testing."""
        return CliRunner()

    @patch("dbtwiz.model.create.create_model")
    def test_cli_model_create_error(self, mock_create, cli_runner):
        """Test CLI handles model creation errors."""
        mock_create.side_effect = Exception("Model creation failed")

        result = cli_runner.invoke(
            app,
            [
                "model",
                "create",
                "--quick",
                "--layer",
                "staging",
                "--domain",
                "test",
                "--name",
                "test_model",
            ],
        )

        assert result.exit_code != 0


class TestCLIValidation:
    """Test CLI input validation."""

    @pytest.fixture
    def cli_runner(self):
        """Create a CLI runner for testing."""
        return CliRunner()

    def test_cli_admin_backfill_required_arguments(self, cli_runner):
        """Test CLI requires necessary arguments for admin backfill."""
        result = cli_runner.invoke(app, ["admin", "backfill"])
        assert result.exit_code != 0

    def test_cli_admin_backfill_invalid_date_format(self, cli_runner):
        """Test CLI validates date format for admin backfill."""
        result = cli_runner.invoke(
            app, ["admin", "backfill", "test_model", "invalid-date", "2024-01-31"]
        )
        assert result.exit_code != 0
