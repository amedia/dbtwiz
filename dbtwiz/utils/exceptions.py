"""Custom exceptions for dbtwiz package."""

from typing import Any, Optional


class DbtwizError(Exception):
    """Base exception for all dbtwiz errors."""

    def __init__(self, message: str, details: Optional[Any] = None):
        self.message = message
        self.details = details
        super().__init__(self.message)


class ValidationError(DbtwizError):
    """Raised when input validation fails."""

    pass


class BigQueryError(DbtwizError):
    """Raised when BigQuery operations fail."""

    pass


class ManifestError(DbtwizError):
    """Raised when manifest operations fail."""

    pass


class ModelError(DbtwizError):
    """Raised when model operations fail."""

    pass


class InvalidArgumentsError(ValidationError):
    """Raised when command line arguments are invalid."""

    pass
