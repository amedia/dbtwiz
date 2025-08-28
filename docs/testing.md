# Testing Guide for dbtwiz

This document provides comprehensive information about testing dbtwiz, including setup, running tests, and understanding test results.

## Table of Contents

- [Overview](#overview)
- [Test Setup](#test-setup)
- [Running Tests](#running-tests)
- [Test Structure](#test-structure)
- [Test Categories](#test-categories)
- [Coverage Reports](#coverage-reports)
- [Continuous Integration](#continuous-integration)
- [Writing Tests](#writing-tests)
- [Troubleshooting](#troubleshooting)

## Overview

dbtwiz uses a comprehensive testing infrastructure built on:

- **pytest**: Primary test framework
- **pytest-cov**: Coverage reporting
- **pytest-mock**: Mocking utilities
- **pytest-xdist**: Parallel test execution
- **GitHub Actions**: Continuous Integration

## Test Setup

### Prerequisites

1. **Python 3.11+**: Required for running tests
2. **Poetry**: Package management and dependency resolution
3. **Git**: Version control

### Installation

```bash
# Install test dependencies
poetry install --with test

# Verify installation
poetry run pytest --version
```

### Test Dependencies

The following testing packages are automatically installed:

- `pytest`: Core testing framework
- `pytest-cov`: Coverage measurement
- `pytest-mock`: Mocking utilities
- `pytest-xdist`: Parallel execution
- `pytest-asyncio`: Async test support
- `pytest-freezer`: Time freezing for tests
- `pytest-randomly`: Random test ordering
- `pytest-timeout`: Test timeout management
- `coverage`: Coverage analysis

## Running Tests

### Basic Test Execution

```bash
# Run all tests
poetry run pytest

# Run with verbose output
poetry run pytest -v

# Run with coverage
poetry run pytest --cov=dbtwiz

# Run specific test file
poetry run pytest tests/test_config.py

# Run specific test class
poetry run pytest tests/test_config.py::TestUserConfig

# Run specific test method
poetry run pytest tests/test_config.py::TestUserConfig::test_user_config_defaults
```

### Test Runner Script

Use the provided test runner for comprehensive testing:

```bash
python run_tests.py
```

This script will:
1. Install test dependencies
2. Run tests with different configurations
3. Generate coverage reports
4. Provide a summary of results

### Advanced Test Options

```bash
# Run only fast tests (exclude slow markers)
poetry run pytest -m "not slow"

# Run only unit tests
poetry run pytest -m unit

# Run only integration tests
poetry run pytest -m integration

# Run tests in parallel (4 workers)
poetry run pytest -n 4

# Run tests with timeout (30 seconds)
poetry run pytest --timeout=30

# Generate HTML coverage report
poetry run pytest --cov=dbtwiz --cov-report=html

# Generate XML coverage report (for CI)
poetry run pytest --cov=dbtwiz --cov-report=xml
```

## Test Structure

```
tests/
├── __init__.py              # Test package initialization
├── conftest.py              # Pytest configuration and fixtures
├── data/                    # Test data files
│   └── sample_dbt_project/  # Sample dbt project for testing
├── test_config.py           # Configuration system tests
├── test_cli.py              # CLI functionality tests
└── test_integration.py      # Integration tests
```

### Key Test Files

- **`conftest.py`**: Contains shared fixtures and test configuration
- **`test_config.py`**: Tests for the Pydantic-based configuration system
- **`test_cli.py`**: Tests for CLI commands and functionality
- **`test_integration.py`**: End-to-end integration tests

## Test Categories

### Unit Tests

Unit tests focus on testing individual functions and classes in isolation:

```python
def test_user_config_defaults():
    """Test UserConfig creates with correct defaults."""
    config = UserConfig()
    assert config.auth_check is True
    assert config.theme == "light"
```

### Integration Tests

Integration tests verify that multiple components work together:

```python
@pytest.mark.integration
def test_config_loading_integration():
    """Test complete configuration loading workflow."""
    # Test the entire config loading process
    pass
```

### CLI Tests

CLI tests verify command-line interface functionality:

```python
def test_cli_help(cli_runner):
    """Test CLI help command."""
    result = cli_runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "dbtwiz" in result.stdout
```

### Mock Tests

Tests that use mocking to isolate components:

```python
@patch('dbtwiz.config.load_user_config')
def test_config_with_mock(mock_load):
    """Test configuration with mocked dependencies."""
    mock_load.return_value = UserConfig(theme="dark")
    # Test implementation
```

## Test Markers

dbtwiz uses pytest markers to categorize tests:

- **`@pytest.mark.unit`**: Unit tests (fast, isolated)
- **`@pytest.mark.integration`**: Integration tests (slower, end-to-end)
- **`@pytest.mark.slow`**: Slow tests (can be excluded)
- **`@pytest.mark.cli`**: CLI-specific tests
- **`@pytest.mark.gcp`**: Tests requiring GCP access
- **`@pytest.mark.dbt`**: Tests requiring dbt
- **`@pytest.mark.skip_ci`**: Tests to skip in CI

### Using Markers

```bash
# Run only unit tests
poetry run pytest -m unit

# Run all tests except slow ones
poetry run pytest -m "not slow"

# Run integration tests only
poetry run pytest -m integration
```

## Coverage Reports

### Coverage Configuration

Coverage is configured in `pyproject.toml`:

```toml
[tool.coverage.run]
source = ["dbtwiz"]
omit = [
    "*/tests/*",
    "*/__pycache__/*",
    "*/venv/*"
]
branch = true

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "if self.debug:"
]
show_missing = true
precision = 2
```

### Coverage Reports

```bash
# Terminal coverage report
poetry run pytest --cov=dbtwiz --cov-report=term-missing

# HTML coverage report
poetry run pytest --cov=dbtwiz --cov-report=html

# XML coverage report (for CI)
poetry run pytest --cov=dbtwiz --cov-report=xml
```

### Coverage Targets

- **Minimum coverage**: 80% (configured in pytest.ini)
- **Branch coverage**: Enabled for comprehensive testing
- **Missing lines**: Shown in reports for easy identification

## Continuous Integration

### GitHub Actions

Tests run automatically on:

- **Push to main/develop**: Full test suite
- **Pull requests**: All tests except integration
- **Python versions**: 3.11 and 3.12

### CI Pipeline

1. **Install dependencies**: Poetry-based installation
2. **Linting**: Ruff checks
3. **Type checking**: MyPy validation
4. **Unit tests**: Fast test execution
5. **Coverage**: XML report generation
6. **Integration tests**: End-to-end validation (main branch only)
7. **Security checks**: Safety and Bandit analysis

### CI Commands

```yaml
# Linting
poetry run ruff check dbtwiz

# Type checking
poetry run mypy dbtwiz

# Tests with coverage
poetry run pytest --cov=dbtwiz --cov-report=xml

# Integration tests
poetry run pytest -m integration
```

## Writing Tests

### Test Naming Convention

- **Test files**: `test_*.py`
- **Test classes**: `Test*`
- **Test methods**: `test_*`

### Test Structure

```python
class TestUserConfig:
    """Test UserConfig class."""
    
    def test_user_config_defaults(self):
        """Test UserConfig creates with correct defaults."""
        # Arrange
        expected_auth_check = True
        
        # Act
        config = UserConfig()
        
        # Assert
        assert config.auth_check == expected_auth_check
```

### Using Fixtures

```python
def test_with_fixture(mock_user_config):
    """Test using a fixture."""
    assert mock_user_config.auth_check is False
    assert mock_user_config.theme == "light"
```

### Mocking External Dependencies

```python
@patch('dbtwiz.integrations.bigquery.BigQueryClient')
def test_bigquery_integration(mock_client):
    """Test BigQuery integration with mocked client."""
    mock_instance = mock_client.return_value
    mock_instance.list_datasets.return_value = ["dataset1", "dataset2"]
    
    # Test implementation
```

### Testing CLI Commands

```python
def test_cli_command(cli_runner):
    """Test CLI command execution."""
    result = cli_runner.invoke(app, ["model", "create", "--help"])
    assert result.exit_code == 0
    assert "Creates a new dbt model" in result.stdout
```

## Troubleshooting

### Common Issues

#### Import Errors

```bash
# Ensure you're in the project root
cd /path/to/dbtwiz

# Install dependencies
poetry install --with test

# Check Python path
poetry run python -c "import dbtwiz; print('Import successful')"
```

#### Test Discovery Issues

```bash
# Check test discovery
poetry run pytest --collect-only

# Verify test file naming
ls tests/test_*.py
```

#### Coverage Issues

```bash
# Check coverage configuration
poetry run pytest --cov=dbtwiz --cov-report=term-missing

# Verify source paths
poetry run coverage run -m pytest tests/
poetry run coverage report
```

#### Performance Issues

```bash
# Run tests in parallel
poetry run pytest -n auto

# Profile slow tests
poetry run pytest --durations=10

# Exclude slow tests
poetry run pytest -m "not slow"
```

### Debug Mode

Enable debug output for troubleshooting:

```bash
# Set debug environment
export DBTWIZ_LOG_LEVEL=DEBUG
export DBTWIZ_TESTING=true

# Run tests with debug
poetry run pytest -v --tb=long
```

### Getting Help

1. **Check test output**: Look for detailed error messages
2. **Review configuration**: Verify `pyproject.toml` settings
3. **Check dependencies**: Ensure all test packages are installed
4. **Review test structure**: Verify test file organization

## Best Practices

### Test Design

1. **Single responsibility**: Each test should test one thing
2. **Descriptive names**: Test names should clearly describe what they test
3. **Arrange-Act-Assert**: Structure tests with clear sections
4. **Minimal dependencies**: Use mocks to isolate units under test

### Test Maintenance

1. **Keep tests fast**: Avoid slow operations in unit tests
2. **Use fixtures**: Share common test setup
3. **Mock external services**: Don't depend on external systems
4. **Update with code**: Keep tests in sync with implementation

### Coverage Goals

1. **Aim for 80%+**: Set realistic coverage targets
2. **Focus on critical paths**: Ensure important code is tested
3. **Exclude boilerplate**: Don't test generated or trivial code
4. **Monitor trends**: Track coverage over time

## Conclusion

This testing infrastructure provides:

- **Comprehensive coverage**: Unit, integration, and CLI tests
- **Automated execution**: CI/CD pipeline integration
- **Quality metrics**: Coverage reporting and analysis
- **Developer productivity**: Fast feedback and easy debugging

For questions or issues, refer to the test output, check the configuration files, or review the test structure documentation.
