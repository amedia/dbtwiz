"""dbtwiz - Python package with CLI helper tool for dbt in GCP using BigQuery.

This package provides various functions that can be useful for dbt development
and administration, particularly in GCP/BigQuery environments.
"""

from importlib.metadata import metadata

from .cli import main
from .core import Group, ModelBasePath, Project
from .utils import (
    BigQueryError,
    DbtwizError,
    InvalidArgumentsError,
    ManifestError,
    ModelError,
    ValidationError,
    debug,
    error,
    fatal,
    info,
    log_function_call,
    log_function_result,
    notice,
    status,
    warn,
)

# Get version and author from package metadata
try:
    package_metadata = metadata("dbtwiz")
    __version__ = package_metadata["Version"]
    __author__ = package_metadata["Author"]
except Exception:
    # Fallback values if metadata cannot be read
    __version__ = "unknown"
    __author__ = "unknown"

__all__ = [
    # CLI
    "main",
    # Core business logic
    "Project",
    "Group",
    "ModelBasePath",
    # Utility functions
    "debug",
    "info",
    "warn",
    "error",
    "fatal",
    "notice",
    "status",
    "log_function_call",
    "log_function_result",
    # Exceptions
    "DbtwizError",
    "ValidationError",
    "BigQueryError",
    "ManifestError",
    "ModelError",
    "InvalidArgumentsError",
]
