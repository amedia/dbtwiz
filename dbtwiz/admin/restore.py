import time
from datetime import datetime
from typing import Optional

from ..config.project import project_config
from ..integrations.bigquery import BigQueryClient
from ..integrations.gcp_auth import ensure_auth
from ..utils.exceptions import InvalidArgumentsError
from ..utils.logger import error, fatal, info


def parse_timestamp(timestamp_str: str) -> int:
    """Parse a timestamp string to epoch milliseconds.

    Supports multiple formats:
    - Epoch milliseconds (e.g., "1234567890123")
    - ISO 8601 format (e.g., "2024-01-15T10:30:00")
    - Date format (e.g., "2024-01-15 10:30:00")

    Args:
        timestamp_str: Timestamp string in various formats

    Returns:
        Timestamp in epoch milliseconds

    Raises:
        InvalidArgumentsError: If timestamp format is invalid
    """
    timestamp_str = timestamp_str.strip()

    # Try parsing as epoch milliseconds first
    if timestamp_str.isdigit():
        return int(timestamp_str)

    # Try parsing as ISO 8601 or common date formats
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(timestamp_str, fmt)
            # Convert to epoch milliseconds
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue

    raise InvalidArgumentsError(
        f"Invalid timestamp format: {timestamp_str}. "
        "Supported formats: epoch milliseconds, ISO 8601 (YYYY-MM-DDTHH:MM:SS), "
        "or date format (YYYY-MM-DD HH:MM:SS)"
    )


def _validate_timestamp_age(snapshot_timestamp_ms: int) -> None:
    """Validate that timestamp is within BigQuery's time travel window."""
    current_time_ms = int(time.time() * 1000)
    time_diff_days = (current_time_ms - snapshot_timestamp_ms) / (1000 * 60 * 60 * 24)

    if time_diff_days > 7:
        error(
            f"Snapshot timestamp is {time_diff_days:.1f} days old. "
            "BigQuery time travel is limited to 7 days."
        )
        fatal("The requested snapshot may not be available.")


def _perform_restore(
    client: BigQueryClient,
    table_id: str,
    snapshot_timestamp_ms: int,
    recovered_table_id: Optional[str],
) -> None:
    """Perform the restore operation with error handling."""
    try:
        recovered_id = client.restore_table(
            table_id=table_id,
            snapshot_timestamp_ms=snapshot_timestamp_ms,
            recovered_table_id=recovered_table_id,
        )
        info(f"\nTable successfully restored to: {recovered_id}", style="green")
    except client.NotFound:
        fatal(
            "Table snapshot not found. The table may not have existed at the specified time, "
            "or the snapshot may be outside BigQuery's time travel window (7 days)."
        )
    except client.Forbidden as e:
        fatal(f"Access denied: {e}")
    except Exception as e:
        fatal(f"Failed to restore table: {e}")


def restore(
    table_id: str,
    timestamp: str,
    recovered_table_id: Optional[str] = None,
    verbose: bool = False,
) -> None:
    """Restore a deleted BigQuery table from a snapshot.

    Args:
        table_id: Full table ID (project.dataset.table) of the deleted table
        timestamp: Snapshot timestamp (epoch ms or date format)
        recovered_table_id: Optional full table ID for the recovered table
        verbose: Enable verbose output
    """
    # Ensure authentication
    ensure_auth()

    # Parse the timestamp
    try:
        snapshot_timestamp_ms = parse_timestamp(timestamp)
    except InvalidArgumentsError as e:
        fatal(str(e))

    if verbose:
        dt = datetime.fromtimestamp(snapshot_timestamp_ms / 1000)
        info(f"Using snapshot timestamp: {dt.isoformat()} ({snapshot_timestamp_ms} ms)")

    # Initialize BigQuery client with service account impersonation
    client = BigQueryClient(
        impersonation_service_account=project_config().service_account_identifier,
        default_project=project_config().service_account_project,
    )

    # Check if the table is actually deleted
    try:
        if client.check_table_exists(table_id):
            fatal(
                f"Table {table_id} still exists in BigQuery. "
                "This command is for restoring deleted tables."
            )
    except client.Forbidden as e:
        # If we don't have permission to check, proceed anyway and let restore fail with proper message
        if verbose:
            info(f"Unable to verify table status: {e}", style="yellow")

    # Check if recovered table already exists
    if recovered_table_id:
        try:
            if client.check_table_exists(recovered_table_id):
                fatal(
                    f"Table {recovered_table_id} already exists in BigQuery. "
                    "You must specify a different location for the recovered table."
                )
        except client.Forbidden as e:
            if verbose:
                info(f"Unable to verify recovered table status: {e}", style="yellow")

    # Verify the timestamp is not too old
    _validate_timestamp_age(snapshot_timestamp_ms)

    # Perform the restore operation
    _perform_restore(client, table_id, snapshot_timestamp_ms, recovered_table_id)
