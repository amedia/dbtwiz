"""dbtwiz - Python package with CLI helper tool for dbt in GCP using BigQuery.

This package provides various functions that can be useful for dbt development
and administration, particularly in GCP/BigQuery environments.
"""

from .cli import main
from .core import Project, Group, ModelBasePath
from .utils import debug, info, warn, error, fatal, notice, status

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
]
