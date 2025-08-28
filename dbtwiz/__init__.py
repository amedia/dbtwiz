"""dbtwiz - Python package with CLI helper tool for dbt in GCP using BigQuery.

This package provides various functions that can be useful for dbt development
and administration, particularly in GCP/BigQuery environments.
"""

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

__version__ = "0.2.16"
__author__ = "Amedia Produkt og Teknologi"

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
