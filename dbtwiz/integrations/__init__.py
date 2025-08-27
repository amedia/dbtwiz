"""External integrations for dbtwiz."""

from dbtwiz.integrations.gcp_auth import ensure_app_default_auth
from dbtwiz.integrations.bigquery import BigQueryClient

__all__ = [
    "ensure_app_default_auth",
    "BigQueryClient"
]
