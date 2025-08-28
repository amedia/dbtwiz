"""Tests for dbtwiz configuration system."""

import tomllib
from pathlib import Path
from unittest.mock import mock_open, patch

import pytest

from dbtwiz.config import (
    ProjectConfig,
    UserConfig,
    load_project_config,
    load_user_config,
)


class TestUserConfig:
    """Test UserConfig class."""

    def test_user_config_defaults(self):
        """Test UserConfig creates with correct defaults."""
        config = UserConfig()

        assert config.auth_check is True
        assert config.editor_command == "code {}"
        assert config.log_debug is False
        assert config.theme == "light"

    def test_user_config_custom_values(self):
        """Test UserConfig accepts custom values."""
        config = UserConfig(
            auth_check=False, editor_command="vim {}", log_debug=True, theme="dark"
        )

        assert config.auth_check is False
        assert config.editor_command == "vim {}"
        assert config.log_debug is True
        assert config.theme == "dark"

    def test_user_config_theme_validation(self):
        """Test UserConfig theme validation."""
        # Valid themes should work
        config = UserConfig(theme="light")
        assert config.theme == "light"

        config = UserConfig(theme="dark")
        assert config.theme == "dark"

        # Invalid theme should raise error
        with pytest.raises(ValueError, match="theme must be one of"):
            UserConfig(theme="invalid")

    def test_user_config_platform_specific_formatter(self):
        """Test platform-specific formatter defaults."""
        # Test that the validator correctly sets platform-specific defaults
        with patch("platform.system", return_value="Windows"):
            # Create config with the default value that triggers the validator
            config = UserConfig(model_formatter="fmt -s")
            assert config.model_formatter == "powershell cat"

        with patch("platform.system", return_value="Darwin"):
            config = UserConfig(model_formatter="fmt -s")
            assert config.model_formatter == "cat -s"

        with patch("platform.system", return_value="Linux"):
            config = UserConfig(model_formatter="fmt -s")
            assert config.model_formatter == "fmt -s"

    def test_user_config_config_path(self):
        """Test config_path method returns correct path."""
        with patch("typer.get_app_dir", return_value="/tmp/test"):
            config = UserConfig()
            assert config.config_path() == Path("/tmp/test")

    def test_user_config_settings_constant(self):
        """Test SETTINGS constant is properly defined."""
        assert hasattr(UserConfig, "SETTINGS")
        assert isinstance(UserConfig.SETTINGS, list)
        assert len(UserConfig.SETTINGS) > 0

        # Check that all settings have required keys
        for setting in UserConfig.SETTINGS:
            assert "key" in setting
            assert "default" in setting
            assert "help" in setting


class TestProjectConfig:
    """Test ProjectConfig class."""

    def test_project_config_defaults(self):
        """Test ProjectConfig creates with correct defaults."""
        config = ProjectConfig()

        assert config.backfill_default_batch_size == 30
        assert config.default_materialization == "table"
        assert config.default_partition_expiration_days == 365
        assert config.teams == []
        assert config.access_policies == []
        assert config.service_consumers == []

    def test_project_config_custom_values(self):
        """Test ProjectConfig accepts custom values."""
        config = ProjectConfig(
            backfill_default_batch_size=15,
            docker_image_url_dbt="gcr.io/test/dbt:latest",
            service_account_identifier="test@test.iam.gserviceaccount.com",
            teams=["team1", "team2"],
        )

        assert config.backfill_default_batch_size == 15
        assert config.docker_image_url_dbt == "gcr.io/test/dbt:latest"
        assert config.service_account_identifier == "test@test.iam.gserviceaccount.com"
        assert config.teams == ["team1", "team2"]

    def test_project_config_validation(self):
        """Test ProjectConfig field validation."""
        # Valid materialization
        config = ProjectConfig(default_materialization="incremental")
        assert config.default_materialization == "incremental"

        # Invalid materialization
        with pytest.raises(ValueError, match="materialization must be one of"):
            ProjectConfig(default_materialization="invalid")

        # Valid batch size
        config = ProjectConfig(backfill_default_batch_size=100)
        assert config.backfill_default_batch_size == 100

        # Invalid batch size (too high)
        with pytest.raises(
            ValueError, match="Input should be less than or equal to 365"
        ):
            ProjectConfig(backfill_default_batch_size=400)

        # Invalid batch size (too low)
        with pytest.raises(
            ValueError, match="Input should be greater than or equal to 1"
        ):
            ProjectConfig(backfill_default_batch_size=0)

    def test_project_config_root_path(self):
        """Test root_path method returns correct path."""
        config = ProjectConfig()
        config.root = Path("/tmp/test")
        assert config.root_path() == Path("/tmp/test")


class TestConfigLoading:
    """Test configuration loading functions."""

    @patch("builtins.open", new_callable=mock_open)
    @patch("pathlib.Path.exists", return_value=True)
    def test_load_user_config_from_file(self, mock_exists, mock_file):
        """Test loading user config from existing file."""
        config_data = {"auth_check": False, "editor_command": "vim {}", "theme": "dark"}

        mock_file.return_value.__enter__.return_value.read.return_value = str(
            config_data
        ).encode()

        with patch("tomllib.load", return_value=config_data):
            config = load_user_config()

        assert config.auth_check is False
        assert config.editor_command == "vim {}"
        assert config.theme == "dark"

    def test_load_user_config_defaults(self):
        """Test loading user config with defaults when file doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            config = load_user_config()

        assert config.auth_check is True
        assert config.theme == "light"

    @patch("pathlib.Path.exists", return_value=True)
    def test_load_project_config_success(self, mock_exists):
        """Test loading project config successfully."""
        config_data = {
            "tool": {
                "dbtwiz": {
                    "project": {
                        "backfill_default_batch_size": 15,
                        "teams": ["team1", "team2"],
                    }
                }
            }
        }

        # Mock the path finding logic
        with patch("pathlib.Path.cwd", return_value=Path("/tmp/test")):
            with patch("builtins.open", mock_open(read_data=str(config_data).encode())):
                with patch("tomllib.load", return_value=config_data):
                    config = load_project_config()

        assert config.backfill_default_batch_size == 15
        assert config.teams == ["team1", "team2"]
        assert config.root == Path("/tmp/test")

    def test_load_project_config_no_pyproject(self):
        """Test loading project config when pyproject.toml doesn't exist."""
        # Mock Path.exists to return False for all paths
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(Exception):  # fatal() raises click.exceptions.Exit
                load_project_config()

    def test_load_project_config_invalid_toml(self):
        """Test loading project config with invalid TOML."""
        with patch("pathlib.Path.cwd", return_value=Path("/tmp/test")):
            with patch("pathlib.Path.exists", return_value=True):
                with patch("builtins.open", mock_open(read_data=b"invalid toml")):
                    with patch(
                        "tomllib.load", side_effect=tomllib.TOMLDecodeError("", "", 0)
                    ):
                        with pytest.raises(
                            Exception
                        ):  # fatal() raises click.exceptions.Exit
                            load_project_config()


@pytest.mark.integration
class TestConfigIntegration:
    """Test configuration system integration."""

    def test_user_config_serialization(self):
        """Test UserConfig can be serialized and deserialized."""
        original = UserConfig(auth_check=False, editor_command="vim {}", theme="dark")

        # Convert to dict and back
        config_dict = original.model_dump()
        restored = UserConfig(**config_dict)

        assert restored.auth_check == original.auth_check
        assert restored.editor_command == original.editor_command
        assert restored.theme == original.theme

    def test_project_config_serialization(self):
        """Test ProjectConfig can be serialized and deserialized."""
        original = ProjectConfig(
            backfill_default_batch_size=15,
            teams=["team1", "team2"],
            access_policies=["policy1"],
        )

        # Convert to dict and back
        config_dict = original.model_dump()
        restored = ProjectConfig(**config_dict)

        assert (
            restored.backfill_default_batch_size == original.backfill_default_batch_size
        )
        assert restored.teams == original.teams
        assert restored.access_policies == original.access_policies
