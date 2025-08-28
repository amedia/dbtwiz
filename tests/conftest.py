"""Pytest configuration and common fixtures for dbtwiz tests."""

import os
import tempfile
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock

import pytest

from dbtwiz.config import ProjectConfig, UserConfig


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Return the test data directory path."""
    return Path(__file__).parent / "data"


@pytest.fixture(scope="session")
def sample_dbt_project() -> Path:
    """Return a sample dbt project directory for testing."""
    return test_data_dir() / "sample_dbt_project"


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def mock_user_config() -> UserConfig:
    """Create a mock user configuration for testing."""
    return UserConfig(
        auth_check=False,
        editor_command="echo {}",
        log_debug=True,
        sql_formatter="cat",
        theme="light",
    )


@pytest.fixture
def mock_project_config() -> ProjectConfig:
    """Create a mock project configuration for testing."""
    return ProjectConfig(
        backfill_default_batch_size=15,
        docker_image_url_dbt="gcr.io/test/dbt:latest",
        docker_image_manifest_path="/app/manifest.json",
        docker_image_profiles_path="/app/profiles",
        service_account_identifier="test-service@test.iam.gserviceaccount.com",
        service_account_project="test-project",
        service_account_region="europe-west1",
        user_project="test-user-project",
        bucket_state_project="test-bucket-project",
        bucket_state_identifier="test-bucket",
        default_materialization="table",
        default_partition_expiration_days=365,
        teams=["team1", "team2"],
        access_policies=["policy1", "policy2"],
        service_consumers=["consumer1", "consumer2"],
    )


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client for testing."""
    mock_client = MagicMock()
    mock_client.list_datasets.return_value = [
        MagicMock(dataset_id="dataset1"),
        MagicMock(dataset_id="dataset2"),
    ]
    mock_client.list_tables.return_value = [
        MagicMock(table_id="table1"),
        MagicMock(table_id="table2"),
    ]
    return mock_client


@pytest.fixture
def mock_storage_client():
    """Create a mock Google Cloud Storage client for testing."""
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob
    return mock_client


@pytest.fixture
def mock_manifest():
    """Create a mock dbt manifest for testing."""
    return {
        "nodes": {
            "model.test.model1": {
                "name": "model1",
                "resource_type": "model",
                "path": "models/staging/test/model1.sql",
                "depends_on": {"nodes": [], "macros": []},
                "config": {"materialized": "table"},
            },
            "model.test.model2": {
                "name": "model2",
                "resource_type": "model",
                "path": "models/marts/test/model2.sql",
                "depends_on": {"nodes": ["model.test.model1"], "macros": []},
                "config": {"materialized": "table"},
            },
        },
        "sources": {
            "source.test.source1": {
                "name": "source1",
                "resource_type": "source",
                "path": "models/staging/test/source1.yml",
            }
        },
    }


@pytest.fixture(autouse=True)
def setup_test_env():
    """Set up test environment variables."""
    # Set test environment
    os.environ["DBTWIZ_TESTING"] = "true"
    os.environ["DBTWIZ_LOG_LEVEL"] = "DEBUG"

    # Clear any existing config
    if "DBTWIZ_CONFIG_DIR" in os.environ:
        del os.environ["DBTWIZ_CONFIG_DIR"]

    yield

    # Cleanup
    if "DBTWIZ_TESTING" in os.environ:
        del os.environ["DBTWIZ_TESTING"]
    if "DBTWIZ_LOG_LEVEL" in os.environ:
        del os.environ["DBTWIZ_LOG_LEVEL"]


@pytest.fixture
def cli_runner():
    """Create a CLI runner for testing Typer commands."""
    from typer.testing import CliRunner

    return CliRunner()
