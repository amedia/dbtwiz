from typing import Any, Dict, List, Optional, Tuple

from ruamel.yaml.scalarstring import PreservedScalarString

from ..utils.exceptions import BigQueryError
from ..utils.logger import error, fatal, info, status

MILLISECONDS_PER_DAY = 1000 * 60 * 60 * 24
DEPRECATION_MESSAGE = "THIS OBJECT IS DEPRECATED"
BACKUP_MESSAGE = "THIS OBJECT IS FOR BACKUP PURPOSES ONLY"
GCP_LOCATION = "EU"


class BigQueryClient:
    """Class for BigQuery client operations."""

    def __init__(
        self,
        impersonation_service_account: Optional[str] = None,
        default_project: Optional[str] = None,
    ) -> None:
        """Initialize the BigQuery client.

        Args:
            impersonation_service_account: Optional service account to impersonate
            default_project: Optional default project ID for BigQuery operations
        """
        from google.api_core.exceptions import Forbidden, NotFound

        self.Forbidden = Forbidden
        self.NotFound = NotFound
        self.impersonation_service_account: Optional[str] = (
            impersonation_service_account
        )
        self.default_project: Optional[str] = default_project
        self._bigquery: Optional[Any] = None
        self._client: Optional[Any] = None
        self._credentials: Optional[Any] = None
        self._authorized_session: Optional[Any] = None

    # ============================================================================
    # PUBLIC METHODS - Client Management
    # ============================================================================

    def get_bigquery(self) -> Any:
        """Get or import the BigQuery package.

        Returns:
            The BigQuery module for client operations
        """
        if self._bigquery is None:
            from google.cloud import bigquery

            self._bigquery = bigquery
        return self._bigquery

    def get_client(self) -> Any:
        """Get or set the BigQuery client.

        Returns:
            Configured BigQuery client instance
        """
        if self._client is None:
            bigquery = self.get_bigquery()
            credentials = self.get_credentials()
            self._client = bigquery.Client(
                credentials=credentials, project=self.default_project
            )
        return self._client

    def get_credentials(self) -> Any:
        """Retrieve or create credentials, optionally with impersonation.

        Returns:
            Google Cloud credentials for BigQuery operations
        """
        if self._credentials is None:
            from google.auth import default
            from google.auth.impersonated_credentials import Credentials

            credentials, project = default()

            # Use the project from default credentials if no default_project is provided
            if not self.default_project:
                self.default_project = project

            # Impersonate the target service account if specified
            if self.impersonation_service_account:
                self._credentials = Credentials(
                    source_credentials=credentials,
                    target_principal=self.impersonation_service_account,
                    target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
            else:
                self._credentials = credentials

        return self._credentials

    def get_authorized_session(self) -> Any:
        """Retrieve or create authorized session.

        Returns:
            Authorized session for HTTP requests
        """
        if self._authorized_session is None:
            from google.auth.transport.requests import AuthorizedSession

            self._authorized_session = AuthorizedSession(self.get_credentials())

        return self._authorized_session

    # ============================================================================
    # PUBLIC METHODS - Core BigQuery Operations
    # ============================================================================

    def list_datasets_in_project(self, project: str) -> Tuple[List[str], str]:
        """Fetch all datasets in the given project from BigQuery.

        Args:
            project: Project ID to list datasets from

        Returns:
            Tuple of (dataset_list, error_message)
        """
        try:
            datasets = list(self.get_client().list_datasets(project=project))
            return sorted([dataset.dataset_id for dataset in datasets]), ""
        except self.Forbidden:
            return [], "Error: You do not have access to the project."
        except self.NotFound:
            return [], "Error: The project does not exist."
        except Exception as e:
            error(
                "Failed to fetch datasets from BigQuery",
                context={"operation": "list datasets", "project": project},
                exception=e,
            )
            return [], f"Error: Failed to fetch datasets from BigQuery: {e}"

    def fetch_tables_in_dataset(
        self, project: str, dataset: str
    ) -> Tuple[List[str], str]:
        """Fetch all tables in the given project and dataset from BigQuery.

        Args:
            project: Project ID containing the dataset
            dataset: Dataset ID to list tables from

        Returns:
            Tuple of (table_list, error_message)
        """
        dataset_ref = f"{project}.{dataset}"
        try:
            tables = list(self.get_client().list_tables(dataset_ref))
            return [table.table_id for table in tables], ""
        except self.NotFound:
            return [], f"Error: The dataset '{dataset_ref}' does not exist in BigQuery."
        except self.Forbidden:
            return [], f"Error: You do not have access to the dataset '{dataset_ref}'."
        except Exception as e:
            return [], f"Error: Failed to fetch tables from BigQuery: {e}"

    def fetch_table_columns(
        self, project: str, dataset: str, table_name: str
    ) -> Tuple[Optional[List[str]], str]:
        """Fetch column names and descriptions from BigQuery.

        Args:
            project: Project ID containing the table
            dataset: Dataset ID containing the table
            table_name: Name of the table to fetch columns from

        Returns:
            Tuple of (column_list, error_message)
        """
        table_ref = f"{project}.{dataset}.{table_name}"
        try:
            table = self.get_client().get_table(table_ref)
            columns = self.parse_schema(table.schema)
            return columns, ""
        except self.NotFound:
            return None, f"Error: The table '{table_name}' does not exist in BigQuery."
        except self.Forbidden:
            return None, f"Error: You do not have access to the table '{table_name}'."
        except Exception as e:
            return None, f"Error: Failed to fetch table details from BigQuery: {e}"

    def check_project_exists(self, project: str) -> str:
        """Check whether the given project exists in BigQuery.

        Args:
            project: Project ID to check

        Returns:
            Status message indicating project existence and accessibility
        """
        try:
            # Check if the project exists and is accessible
            datasets = list(self.get_client().list_datasets(project=project))
            if not datasets:
                return (
                    f"Warning: The project '{project}' exists but contains no datasets."
                )
            else:
                return "Exists"
        except self.NotFound:
            return f"Error: The project '{project}' does not exist."
        except self.Forbidden:
            return f"Error: You do not have access to the project '{project}'."
        except Exception as e:
            return f"Error: Failed to verify project '{project}': {e}"

    def ensure_dataset_exists(self, table_id: str) -> Any:
        """Ensure a BigQuery dataset exists, and create it if not.

        Args:
            table_id: Full table ID (project.dataset.table)

        Returns:
            BigQuery dataset object

        Raises:
            SystemExit: If dataset creation fails (via fatal function)
        """
        try:
            # Split into project and dataset components
            project_id, dataset_name, _ = table_id.split(".")
            dataset_id = f"{project_id}.{dataset_name}"

            # Check if dataset exists
            try:
                dataset = self.get_client().get_dataset(dataset_id)
                return dataset
            except self.NotFound:
                # Dataset doesn't exist - create it
                dataset = self.get_bigquery().Dataset(dataset_id)
                dataset.location = GCP_LOCATION
                dataset = self.get_client().create_dataset(dataset)
                info(f"Created dataset {dataset_id}")
                return dataset

        except Exception as e:
            fatal(f"Error ensuring dataset {dataset_id} exists: {e}")

    def run_query(self, query: str) -> Any:
        """Run a query in BigQuery.

        Args:
            query: SQL query string to execute

        Returns:
            BigQuery query job object
        """
        return self.get_client().query(query, location=GCP_LOCATION)

    def delete_table(self, table_id: str) -> None:
        """Delete a table from BigQuery.

        Args:
            table_id: Full table ID (project.dataset.table)
        """
        self.get_client().delete_table(table_id)

    def check_table_exists(self, table_id: str) -> bool:
        """Check if a table exist in BigQuery.

        Args:
            table_id: Full table ID (project.dataset.table)

        Returns:
            True if table exist, False if it doesn't
        """
        try:
            self.get_client().get_table(table_id)
            return True  # Table exists
        except self.NotFound:
            return False  # Table is missing/deleted

    def restore_table(
        self, table_id: str, snapshot_timestamp_ms: int, recovered_table_id: str = None
    ) -> str:
        """Restore a deleted table from a snapshot using BigQuery time travel.

        Args:
            table_id: Full table ID of the deleted table (project.dataset.table)
            snapshot_timestamp_ms: Snapshot timestamp in epoch milliseconds
            recovered_table_id: Optional full table ID for the recovered table.
                              If not provided, defaults to table_id

        Returns:
            The full table ID of the recovered table

        Raises:
            NotFound: If the snapshot doesn't exist
            Forbidden: If access is denied
            Exception: For other BigQuery errors
        """
        client = self.get_client()

        # Default recovered table ID if not provided
        if not recovered_table_id:
            project, dataset, table = table_id.split(".")
            recovered_table_id = f"{project}.{dataset}.{table}"

        # Construct the snapshot table ID using the decorator
        snapshot_table_id = f"{table_id}@{snapshot_timestamp_ms}"

        # Get the location of the table from the source dataset
        project_id, dataset_name, _ = table_id.split(".")
        dataset_id = f"{project_id}.{dataset_name}"
        dataset = client.get_dataset(dataset_id)
        location = dataset.location

        # Run the copy job to restore the table
        info(
            f"Restoring table from snapshot {snapshot_table_id} to {recovered_table_id}"
        )
        job = client.copy_table(
            snapshot_table_id,
            recovered_table_id,
            location=location,
        )
        job.result()  # Wait for the job to complete

        info(
            f"Successfully restored data from {table_id} to {recovered_table_id}",
            style="green",
        )
        return recovered_table_id

    def get_bigquery_partition_expiration(self, table_id: str) -> int:
        """Get the current partition expiration for a table in BigQuery."""
        table = self.get_client().get_table(table_id)
        if (
            table.time_partitioning
            and table.time_partitioning.expiration_ms is not None
        ):
            return (
                table.time_partitioning.expiration_ms // MILLISECONDS_PER_DAY
            )  # Convert ms to days
        return -1  # Return -1 if no expiration is set

    def update_bigquery_partition_expiration(
        self, table_id: str, expiration_days: int
    ) -> None:
        """Update the partition expiration for a table in BigQuery.

        Args:
            table_id: Full table ID (project.dataset.table)
            expiration_days: Number of days for partition expiration
        """
        table = self.get_client().get_table(table_id)
        if table.time_partitioning:
            # Create a new TimePartitioning object with the updated expiration
            updated_partitioning = self.get_bigquery().TimePartitioning(
                type_=table.time_partitioning.type_,
                field=table.time_partitioning.field,
                expiration_ms=expiration_days
                * MILLISECONDS_PER_DAY,  # Convert days to ms
            )
            # Update the table with the new TimePartitioning configuration
            table.time_partitioning = updated_partitioning
            info(
                f"Updating partition expiration for {table_id} to {expiration_days} days"
            )
            self.get_client().update_table(table, ["time_partitioning"])
        else:
            info(f"Table {table_id} is not partitioned. Skipping update.")

    def update_table_constraints(
        self, table_id: str, table_constraints: Dict, should_update: bool = True
    ) -> None:
        """
        Updates the table constraints (primary/foreign keys) using the BigQuery REST API.

        Args:
            table_id: The full table ID (e.g., 'project.dataset.table').
            table_constraints: The table constraints to apply.
        """
        if not should_update:
            return

        try:
            # Construct the REST API URL
            project_id, dataset_id, table_id_only = table_id.split(".")
            table_path = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id_only}"

            # Prepare the request body
            body = {
                "tableReference": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id_only,
                },
            }

            # If table_constraints is None, remove constraints
            if table_constraints is None:
                body["tableConstraints"] = table_constraints
            else:
                # Convert table_constraints to a dictionary
                new_table_constraints = {}
                if table_constraints.primary_key:
                    new_table_constraints["primaryKey"] = {
                        "columns": table_constraints.primary_key.columns,
                    }
                if table_constraints.foreign_keys:
                    new_table_constraints["foreignKeys"] = [
                        {
                            "name": fk.name,
                            "referencedTable": {
                                "projectId": fk.referenced_table.project_id,
                                "datasetId": fk.referenced_table.dataset_id,
                                "tableId": fk.referenced_table.table_id,
                            },
                            "columnReferences": [
                                {
                                    "referencingColumn": ref.referencing_column,
                                    "referencedColumn": ref.referenced_column,
                                }
                                for ref in fk.column_references
                            ],
                        }
                        for fk in table_constraints.foreign_keys
                    ]
                body["tableConstraints"] = new_table_constraints

            # Create an authorized session
            authed_session = self.get_authorized_session()
            # Send the PATCH request to update the table constraints
            response = authed_session.patch(table_path, json=body)
            response.raise_for_status()

        except Exception as e:
            error(f"Error updating table constraints for {table_id}: {e}")

    def get_sql_type_name(self, field_type: str) -> str:
        """Maps a field type to the correct SQL type name."""
        type_map = {
            "INTEGER": "INT64",
            "INT": "INT64",
            "SMALLINT": "INT64",
            "BIGINT": "INT64",
            "TINYINT": "INT64",
            "BYTEINT": "INT64",
            "FLOAT": "FLOAT64",
            "FLOAT64": "FLOAT64",
            "NUMERIC": "NUMERIC",
            "DECIMAL": "NUMERIC",
            "BIGNUMERIC": "BIGNUMERIC",
            "BIGDECIMAL": "BIGNUMERIC",
            "STRING": "STRING",
            "BYTES": "BYTES",
            "BOOLEAN": "BOOL",
            "BOOL": "BOOL",
            "DATE": "DATE",
            "DATETIME": "DATETIME",
            "TIME": "TIME",
            "TIMESTAMP": "TIMESTAMP",
            "INTERVAL": "INTERVAL",
            "RANGE": "RANGE",
            "GEOGRAPHY": "GEOGRAPHY",
            "JSON": "JSON",
            "RECORD": "STRUCT",
            "STRUCT": "STRUCT",
            "ARRAY": "ARRAY",
        }

        return type_map.get(field_type.upper(), field_type.upper())

    def parse_schema(self, fields: List[Any], prefix: str = "") -> List[Dict[str, Any]]:
        """Parse the schema for a table and return all columns with their details.

        For RECORD types, recursively adds all nested columns.

        Args:
            fields: List of BigQuery schema fields
            prefix: Optional prefix for nested field names

        Returns:
            List of dicts with keys: name, data_type, description (if available)
        """
        schema_details = []

        for field in fields:
            if field.field_type == "RECORD":
                # Recursively unnest fields within the struct
                nested_fields = self.parse_schema(
                    field.fields, prefix=f"{prefix}{field.name}."
                )
                schema_details.extend(nested_fields)
            else:
                field_type = self.get_sql_type_name(field.field_type).lower()
                if field.mode == "REPEATED":
                    data_type = f"array<{field_type}>"
                else:
                    data_type = field_type
                column = {
                    "name": f"{prefix}{field.name}",
                    "data_type": data_type,
                }
                column["description"] = (
                    PreservedScalarString(field.description)
                    if field.description
                    else ""
                )
                schema_details.append(column)

        return schema_details

    def create_table_copy(self, old_table_id: str, new_table_id: str) -> None:
        """
        Creates a copy of a BigQuery table or view with the new table id.

        Args:
            old_table_id: The full table ID of the source table/view (e.g., 'project_a.dataset_old.table_old').
            new_table_id: The full table ID of the destination table/view (e.g., 'project_b.dataset_new.table_new').
        """
        try:
            client = self.get_client()
            bigquery = self.get_bigquery()

            # Verify table states - skip if not as expected
            table_state_check = self._check_expected_table_states(
                tables=[
                    (old_table_id, "exists"),
                    (new_table_id, "missing"),
                ],
                action_description="create table copy",
            )
            if not table_state_check:
                return

            # If copying to other dataset, ensure it exists
            if old_table_id.split(".")[:2] != new_table_id.split(".")[:2]:
                self.ensure_dataset_exists(new_table_id)

            # Get table metadata and iam policy
            old_table = client.get_table(old_table_id)

            # Check if the source object is a table or a view
            status(
                message=r"\[bigquery] " + f"Creating copy [bold]{new_table_id}[/bold]"
            )
            if old_table.table_type == "TABLE":
                # Create a new table with the same definition as the source table
                new_table = bigquery.Table(new_table_id)
                # Copy all table properties
                self._copy_properties(
                    source_table=old_table,
                    destination_table=new_table,
                    property_type="TABLE",
                )

                # Create the new table in BigQuery
                new_table = client.create_table(new_table)
                self.update_table_constraints(
                    table_id=new_table_id,
                    table_constraints=old_table.table_constraints,
                    should_update=old_table.table_constraints is not None,
                )
                # Copy data from the old table to the new table
                job_config = bigquery.CopyJobConfig()
                job = client.copy_table(old_table, new_table, job_config=job_config)
                job.result()  # Wait for the job to complete

            elif old_table.table_type == "VIEW":
                # Create a new view with the same definition as the source view
                new_table = bigquery.Table(new_table_id)
                new_table.view_query = old_table.view_query
                self._copy_properties(
                    source_table=old_table,
                    destination_table=new_table,
                    property_type="VIEW",
                )
                new_table = client.create_table(new_table)
                new_table.schema = old_table.schema
                new_table = client.update_table(new_table, ["schema"])

            else:
                raise ValueError(f"Unsupported table type: {old_table.table_type}")

            status(
                message=r"\[bigquery] " + f"Creating copy [bold]{new_table_id}[/bold]",
                status_text="done",
                style="green",
            )

            # Replicate grants from the old table/view to the new table/view
            self._copy_iam_policy(
                source_table_id=old_table_id, target_table_id=new_table_id
            )

        except Exception as e:
            error(
                f"Error copying table/view {old_table_id} to {new_table_id}",
                context={"old_table": old_table_id, "new_table": new_table_id},
                exception=e,
            )
            raise BigQueryError(
                f"Failed to copy table {old_table_id} to {new_table_id}"
            ) from e

    def migrate_table(
        self,
        old_table_id: str,
        new_table_id: str,
        backup_table_id: str,
    ) -> None:
        """
        Replaces an exisiting table/view with a view to the given new table/view, which must already exist.
        Before doing so, it creates a backup table in the original dataset.

        Args:
            old_table_id: The full table ID of the original table/view (e.g., 'project_a.dataset_old.table_old').
            new_table_id: The full table ID of the new table/view (e.g., 'project_b.dataset_new.table_new').
            backup_table_id: The full table ID of the backup table/view (e.g., 'project_a.dataset_old.table_old__bck').
        """
        # Initialize view_id for potential rollback
        view_id = old_table_id

        try:
            client = self.get_client()

            # Verify table states - skip if not as expected
            if not self._verify_migration_prerequisites(
                old_table_id, new_table_id, backup_table_id
            ):
                return

            # Get table metadata
            old_table = client.get_table(old_table_id)
            backup_table_name = backup_table_id.split(".")[-1]

            # Create backup
            status(
                message=r"\[bigquery] "
                + f"Renaming [bold]{old_table_id}[/bold] to [bold]{backup_table_name}[/bold]"
            )

            self._create_backup(old_table, backup_table_id, new_table_id)

            status(
                message=r"\[bigquery] "
                + f"Renaming [bold]{old_table_id}[/bold] to [bold]{backup_table_name}[/bold]",
                status_text="done",
                style="green",
            )

            # Create replacement view
            status(message=r"\[bigquery] " + f"Creating view [bold]{view_id}[/bold]")

            self._create_replacement_view(old_table, view_id, new_table_id)

            status(
                message=r"\[bigquery] " + f"Creating view [bold]{view_id}[/bold]",
                status_text="done",
                style="green",
            )

            # Replicate grants from the old table/view to the view
            self._copy_iam_policy(source_table_id=old_table_id, target_table_id=view_id)

        except Exception as e:
            error(f"Error renaming table/view or creating view: {e}")
            self._rollback_migration(
                client, old_table, backup_table_id, old_table_id, view_id
            )

    # ============================================================================
    # PRIVATE METHODS - Internal Helper Functions
    # ============================================================================

    def _copy_properties(self, source_table, destination_table, property_type) -> None:
        """
        Copies all relevant properties from a source table to a destination table.
        """
        # Copy universal properties
        destination_table.description = source_table.description
        destination_table.labels = source_table.labels
        if hasattr(source_table, "default_collation"):
            destination_table.default_collation = source_table.default_collation

        # Copy table specific options
        if property_type == "TABLE":
            # Copy schema (includes column descriptions)
            destination_table.schema = source_table.schema

            # Copy general properties
            destination_table.time_partitioning = source_table.time_partitioning
            destination_table.range_partitioning = source_table.range_partitioning
            destination_table.clustering_fields = source_table.clustering_fields
            destination_table.expires = source_table.expires
            destination_table.encryption_configuration = (
                source_table.encryption_configuration
            )
            destination_table.friendly_name = source_table.friendly_name

            # Copy table options
            destination_table.require_partition_filter = (
                source_table.require_partition_filter
            )
            destination_table.max_staleness = source_table.max_staleness

            if hasattr(source_table, "external_data_configuration"):
                destination_table.external_data_configuration = (
                    source_table.external_data_configuration
                )
            if hasattr(source_table, "materialized_view"):
                destination_table.materialized_view = source_table.materialized_view

        # Copy view specific options
        elif property_type == "VIEW":
            if hasattr(source_table, "view_options"):
                destination_table.view_options = source_table.view_options

    def _check_expected_table_states(
        self, tables: List[Tuple[str, str]], action_description: str
    ) -> bool:
        """Checks if a table/view exists in BigQuery, and whether it's defined as backup or deprecated."""
        state_results = []
        for table_id, expected_state in tables:
            try:
                table = self.get_client().get_table(table_id)
                if table.description and BACKUP_MESSAGE in table.description:
                    table_state = "backup"
                elif (
                    table.table_type == "VIEW"
                    and table.description
                    and DEPRECATION_MESSAGE in table.description
                ):
                    table_state = "deprecated"
                else:
                    table_state = "exists"
            except self.NotFound:
                table_state = "missing"

            if table_state and table_state != expected_state:
                state_results.append(
                    f"- {table_id}: expected state was `{expected_state}` but had `{table_state}`"
                )

        if len(state_results) > 0:
            error(
                f"Skipping {action_description} since BigQuery state wasn't as expected:\n"
                + "\n".join(state_results)
            )
            return False
        return True

    def _copy_iam_policy(self, source_table_id, target_table_id):
        """Safely copies IAM policies"""
        client = self.get_client()

        source_policy = client.get_iam_policy(source_table_id)
        target_policy = client.get_iam_policy(target_table_id)

        # Merge policies (preserve existing target permissions)
        target_policy.bindings.extend(
            b for b in source_policy.bindings if b not in target_policy.bindings
        )

        client.set_iam_policy(target_table_id, target_policy)

    def _verify_migration_prerequisites(
        self, old_table_id: str, new_table_id: str, backup_table_id: str
    ) -> bool:
        """Verify that all prerequisites for migration are met."""
        # Verify table states - skip if not as expected
        table_state_check = self._check_expected_table_states(
            tables=[
                (old_table_id, "exists"),
                (new_table_id, "exists"),
                (backup_table_id, "missing"),
            ],
            action_description="migrate table",
        )
        if not table_state_check:
            return False

        # If backup table dataset is different from old or new, verify dataset exists
        if backup_table_id.split(".")[:2] not in (
            new_table_id.split(".")[:2],
            old_table_id.split(".")[:2],
        ):
            self.ensure_dataset_exists(new_table_id)

        return True

    def _create_backup(
        self, old_table, backup_table_id: str, new_table_id: str
    ) -> None:
        """Create a backup of the old table/view."""
        if old_table.table_type == "TABLE":
            self._backup_table(old_table, backup_table_id, new_table_id)
        elif old_table.table_type == "VIEW":
            self._backup_view(old_table, backup_table_id, new_table_id)
        else:
            raise ValueError(f"Unsupported table type: {old_table.table_type}")

    def _backup_table(self, old_table, backup_table_id: str, new_table_id: str) -> None:
        """Backup a table by renaming it."""
        client = self.get_client()

        # Remove constraints before renaming
        self.update_table_constraints(
            table_id=old_table.reference,
            table_constraints=None,
            should_update=old_table.table_constraints is not None,
        )

        # Rename table
        backup_table_name = backup_table_id.split(".")[-1]
        query = f"""
        alter table `{old_table.reference}`
        rename to `{backup_table_name}`;
        """

        query_job = client.query(query)
        query_job.result()  # Wait for the job to complete

        # Update description and reapply constraints
        backup_table = client.get_table(backup_table_id)
        backup_table.description = f"{BACKUP_MESSAGE}. USE {new_table_id}."
        client.update_table(backup_table, ["description"])

        self.update_table_constraints(
            table_id=backup_table_id,
            table_constraints=old_table.table_constraints,
            should_update=old_table.table_constraints is not None,
        )

    def _backup_view(self, old_table, backup_table_id: str, new_table_id: str) -> None:
        """Backup a view by creating a copy and deleting the original."""
        client = self.get_client()
        bigquery = self.get_bigquery()

        # Create backup view
        bck_view = bigquery.Table(backup_table_id)
        bck_view.view_query = old_table.view_query
        self._copy_properties(
            source_table=old_table,
            destination_table=bck_view,
            property_type="VIEW",
        )
        bck_view.description = f"{BACKUP_MESSAGE}. USE {new_table_id}."
        bck_view = client.create_table(bck_view)
        bck_view.schema = old_table.schema
        client.update_table(bck_view, ["schema"])

        # Delete the original view
        client.delete_table(old_table.reference)

    def _create_replacement_view(
        self, old_table, view_id: str, new_table_id: str
    ) -> None:
        """Create a replacement view at the original table location."""
        client = self.get_client()
        bigquery = self.get_bigquery()

        view = bigquery.Table(view_id)
        view.view_query = f"select * from `{new_table_id}`"
        self._copy_properties(
            source_table=old_table, destination_table=view, property_type="VIEW"
        )
        view.description = f"{DEPRECATION_MESSAGE}. USE {new_table_id}."
        view = client.create_table(view)
        view.schema = old_table.schema
        client.update_table(view, ["schema"])

        # Remove constraints
        self.update_table_constraints(
            table_id=view_id,
            table_constraints=old_table.table_constraints,
            should_update=old_table.table_constraints is not None,
        )

    def _rollback_migration(
        self, client, old_table, backup_table_id: str, old_table_id: str, view_id: str
    ) -> None:
        """Rollback migration changes if any step fails."""
        try:
            if old_table.table_type == "TABLE":
                self._rollback_table_migration(client, backup_table_id, old_table_id)
            elif old_table.table_type == "VIEW":
                self._rollback_view_migration(client, backup_table_id)

            # Clean up replacement view if it was created
            if view_id and client.get_table(view_id, retry=None):
                client.delete_table(view_id)
                info(f"Rollback: Deleted {view_id}")
        except Exception as rollback_error:
            error(f"Failed to rollback migration: {rollback_error}")

    def _rollback_table_migration(
        self, client, backup_table_id: str, old_table_id: str
    ) -> None:
        """Rollback table migration by renaming backup back to original."""
        old_table_name = old_table_id.split(".")[-1]
        revert_query = f"""
        alter table `{backup_table_id}`
        rename to `{old_table_name}`;
        """
        try:
            revert_job = client.query(revert_query)
            revert_job.result()
            info(f"Rollback: Renamed {backup_table_id} back to {old_table_id}")
        except Exception as revert_error:
            error(f"Failed to roll back rename operation: {revert_error}")

    def _rollback_view_migration(self, client, backup_table_id: str) -> None:
        """Rollback view migration by deleting backup view."""
        if client.get_table(backup_table_id, retry=None):
            client.delete_table(backup_table_id)
            info(f"Rollback: Deleted {backup_table_id}")
