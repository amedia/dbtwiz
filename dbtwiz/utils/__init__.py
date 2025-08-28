"""Utility functions for dbtwiz."""

from .editor import open_in_editor
from .exceptions import (
    BigQueryError,
    DbtwizError,
    InvalidArgumentsError,
    ManifestError,
    ModelError,
    ValidationError,
)

from .logger import (
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

__all__ = [
    # Logging functions
    "debug",
    "info",
    "warn",
    "error",
    "fatal",
    "notice",
    "status",
    "log_function_call",
    "log_function_result",
    # Utility functions
    "open_in_editor",
    # Exceptions
    "DbtwizError",
    "ValidationError",
    "BigQueryError",
    "ManifestError",
    "ModelError",
    "InvalidArgumentsError",
]
